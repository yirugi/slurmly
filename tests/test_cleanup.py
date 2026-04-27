"""Phase 3 — cleanup helpers (path validation, plan, execute, dry-run)."""

from __future__ import annotations

import pytest

from slurmly import CleanupCandidate, CleanupPlan, InvalidConfig, SlurmSSHClient, SubmittedJob
from slurmly.cleanup import (
    assert_safe_to_remove,
    build_find_stale,
    build_rm_rf,
    jobs_root_prefix,
)
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport

BASE = "/scratch/u/slurmly"


def _client(transport: FakeTransport) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir=BASE,
        account="acct1",
    )


def _job(internal_id: str = "slurmly-deadbeef") -> SubmittedJob:
    base = f"{BASE}/jobs/{internal_id}"
    return SubmittedJob(
        internal_job_id=internal_id,
        slurm_job_id="1",
        remote_job_dir=base,
        remote_script_path=f"{base}/run.sh",
        stdout_path=f"{base}/stdout.log",
        stderr_path=f"{base}/stderr.log",
        submitted_at="2026-04-26T17:00:00Z",
    )


# --- pure path validation ---------------------------------------------------


def test_jobs_root_prefix_includes_trailing_slash():
    assert jobs_root_prefix(BASE) == "/scratch/u/slurmly/jobs/"


def test_assert_safe_accepts_valid_job_dir():
    assert_safe_to_remove(f"{BASE}/jobs/slurmly-abc12345", remote_base_dir=BASE)


def test_assert_safe_rejects_outside_base():
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove("/etc/passwd", remote_base_dir=BASE)


def test_assert_safe_rejects_dotdot_traversal():
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove(
            f"{BASE}/jobs/../../../etc", remote_base_dir=BASE
        )


def test_assert_safe_rejects_prefix_collision():
    """A path like /scratch/u/slurmly-evil/jobs/x must not match /scratch/u/slurmly."""
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove(
            "/scratch/u/slurmly-evil/jobs/x", remote_base_dir=BASE
        )


def test_assert_safe_rejects_jobs_root_itself():
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove(f"{BASE}/jobs/", remote_base_dir=BASE)


def test_assert_safe_rejects_empty_and_root():
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove("", remote_base_dir=BASE)
    with pytest.raises(InvalidConfig):
        assert_safe_to_remove("/", remote_base_dir=BASE)


def test_build_find_stale_shape():
    cmd = build_find_stale(BASE, older_than_days=14)
    assert "find " in cmd
    assert "/scratch/u/slurmly/jobs" in cmd
    # 14 days = 14 * 1440 minutes = 20160
    assert "-mmin +20160" in cmd
    assert "-mindepth 1 -maxdepth 1" in cmd


def test_build_find_stale_supports_fractional_days():
    # 0.5 days = 720 minutes
    cmd = build_find_stale(BASE, older_than_days=0.5)
    assert "-mmin +720" in cmd


def test_build_find_stale_zero_days_means_more_than_zero_minutes():
    cmd = build_find_stale(BASE, older_than_days=0)
    assert "-mmin +0" in cmd


def test_build_rm_rf_quotes_path():
    assert build_rm_rf("/a b/c") == "rm -rf -- '/a b/c'"


# --- client integration -----------------------------------------------------


async def test_cleanup_remote_job_dir_dry_run_does_not_run_rm():
    transport = FakeTransport()
    client = _client(transport)
    result = await client.cleanup_remote_job_dir(_job(), dry_run=True)
    assert result.dry_run is True
    assert result.removed == [f"{BASE}/jobs/slurmly-deadbeef"]
    assert transport.runs == []


async def test_cleanup_remote_job_dir_executes_rm_rf():
    transport = FakeTransport(canned_responses=[(0, "", "")])
    client = _client(transport)
    result = await client.cleanup_remote_job_dir(_job())
    assert result.errors == []
    assert result.removed == [f"{BASE}/jobs/slurmly-deadbeef"]
    assert "rm -rf" in transport.runs[0].command


async def test_cleanup_refuses_path_outside_base():
    transport = FakeTransport()
    client = _client(transport)
    bad_job = SubmittedJob(
        internal_job_id="slurmly-cafef00d",
        slurm_job_id="1",
        remote_job_dir="/etc/passwd",  # outside base
        remote_script_path="/etc/passwd/run.sh",
        stdout_path="/etc/passwd/o",
        stderr_path="/etc/passwd/e",
    )
    result = await client.cleanup_remote_job_dir(bad_job)
    assert result.removed == []
    assert any("not under remote_base_dir" in e for e in result.errors)
    assert transport.runs == []


async def test_plan_cleanup_lists_stale_dirs():
    found = (
        f"{BASE}/jobs/slurmly-aaaaaaaa\n"
        f"{BASE}/jobs/slurmly-bbbbbbbb\n"
    )
    transport = FakeTransport(canned_responses=[(0, found, "")])
    client = _client(transport)
    plan = await client.plan_cleanup(older_than_days=30)
    assert plan.older_than_days == 30
    assert [c.path for c in plan.candidates] == [
        f"{BASE}/jobs/slurmly-aaaaaaaa",
        f"{BASE}/jobs/slurmly-bbbbbbbb",
    ]


async def test_plan_cleanup_filters_unsafe_lines():
    """A find that somehow returns a path outside the base is silently dropped."""
    found = (
        f"{BASE}/jobs/slurmly-aaaaaaaa\n"
        "/etc/passwd\n"
    )
    transport = FakeTransport(canned_responses=[(0, found, "")])
    client = _client(transport)
    plan = await client.plan_cleanup(older_than_days=7)
    paths = [c.path for c in plan.candidates]
    assert paths == [f"{BASE}/jobs/slurmly-aaaaaaaa"]


async def test_cleanup_plan_dry_run_skips_rm():
    plan = CleanupPlan(
        remote_base_dir=BASE,
        older_than_days=7,
        candidates=[CleanupCandidate(path=f"{BASE}/jobs/slurmly-aaaaaaaa")],
    )
    transport = FakeTransport()
    client = _client(transport)
    result = await client.cleanup(plan, dry_run=True)
    assert result.dry_run is True
    assert result.removed == [f"{BASE}/jobs/slurmly-aaaaaaaa"]
    assert transport.runs == []


async def test_cleanup_plan_executes_rm_for_each_candidate():
    plan = CleanupPlan(
        remote_base_dir=BASE,
        older_than_days=7,
        candidates=[
            CleanupCandidate(path=f"{BASE}/jobs/slurmly-aaaaaaaa"),
            CleanupCandidate(path=f"{BASE}/jobs/slurmly-bbbbbbbb"),
        ],
    )
    transport = FakeTransport(canned_responses=[(0, "", ""), (0, "", "")])
    client = _client(transport)
    result = await client.cleanup(plan)
    assert len(result.removed) == 2
    assert all("rm -rf" in r.command for r in transport.runs)


async def test_cleanup_revalidates_at_execution_time():
    """A plan from a different base is re-checked and rejected."""
    plan = CleanupPlan(
        remote_base_dir="/elsewhere",
        older_than_days=7,
        candidates=[CleanupCandidate(path="/elsewhere/jobs/slurmly-x")],
    )
    transport = FakeTransport()
    client = _client(transport)
    result = await client.cleanup(plan)
    assert result.skipped and "not under remote_base_dir" in result.skipped[0]
    assert transport.runs == []
