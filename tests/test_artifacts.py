"""Phase 4 — artifact helpers (path validation, list, download)."""

from __future__ import annotations

import pytest

from slurmly import Artifact, ArtifactDownload, SlurmSSHClient, SubmittedJob
from slurmly.artifacts import (
    assert_artifact_path_safe,
    resolve_artifact_path,
)
from slurmly.exceptions import InvalidConfig
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport


BASE = "/scratch/u/slurmly"


def _client(transport: FakeTransport) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="t",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir=BASE,
        account="a1",
    )


def _job() -> SubmittedJob:
    return SubmittedJob(
        internal_job_id="slurmly-12345678",
        slurm_job_id="42",
        remote_job_dir=f"{BASE}/jobs/slurmly-12345678",
        remote_script_path=f"{BASE}/jobs/slurmly-12345678/run.sh",
        stdout_path=f"{BASE}/jobs/slurmly-12345678/stdout.log",
        stderr_path=f"{BASE}/jobs/slurmly-12345678/stderr.log",
    )


# --- path validation ------------------------------------------------------


def test_safe_path_under_jobs_root_passes():
    assert_artifact_path_safe(
        f"{BASE}/jobs/slurmly-12345678/metrics.json", remote_base_dir=BASE
    )


def test_safe_path_rejects_sibling_prefix_collision():
    """`/scratch/u/slurmly-evil/...` must not match base `/scratch/u/slurmly`."""
    with pytest.raises(InvalidConfig):
        assert_artifact_path_safe(
            "/scratch/u/slurmly-evil/jobs/x/y", remote_base_dir=BASE
        )


def test_safe_path_rejects_jobs_root_itself():
    with pytest.raises(InvalidConfig):
        assert_artifact_path_safe(f"{BASE}/jobs", remote_base_dir=BASE)


def test_safe_path_rejects_traversal_segment():
    with pytest.raises(InvalidConfig):
        assert_artifact_path_safe(
            f"{BASE}/jobs/slurmly-12345678/../etc/passwd", remote_base_dir=BASE
        )


def test_safe_path_rejects_outside_base():
    with pytest.raises(InvalidConfig):
        assert_artifact_path_safe("/etc/passwd", remote_base_dir=BASE)


def test_resolve_artifact_path_joins_relative_name():
    p = resolve_artifact_path(
        remote_job_dir=f"{BASE}/jobs/slurmly-12345678",
        name="results/out.json",
        remote_base_dir=BASE,
    )
    assert p == f"{BASE}/jobs/slurmly-12345678/results/out.json"


def test_resolve_artifact_path_rejects_absolute():
    with pytest.raises(InvalidConfig):
        resolve_artifact_path(
            remote_job_dir=f"{BASE}/jobs/slurmly-12345678",
            name="/etc/passwd",
            remote_base_dir=BASE,
        )


def test_resolve_artifact_path_rejects_traversal():
    with pytest.raises(InvalidConfig):
        resolve_artifact_path(
            remote_job_dir=f"{BASE}/jobs/slurmly-12345678",
            name="../../etc/passwd",
            remote_base_dir=BASE,
        )


# --- list_artifacts -------------------------------------------------------


async def test_list_artifacts_returns_typed_results():
    job_dir = f"{BASE}/jobs/slurmly-12345678"
    stdout = "\n".join(
        [
            f"{job_dir}/run.sh",
            f"{job_dir}/stdout.log",
            f"{job_dir}/results/metrics.json",
        ]
    )
    transport = FakeTransport(canned_responses=[(0, stdout, "")])
    client = _client(transport)
    artifacts = await client.list_artifacts(_job())
    assert len(artifacts) == 3
    assert all(isinstance(a, Artifact) for a in artifacts)
    names = {a.name for a in artifacts}
    assert names == {"run.sh", "stdout.log", "metrics.json"}
    # The find command was issued under the job dir.
    cmd = transport.runs[0].command
    assert "find " in cmd
    assert job_dir in cmd
    assert "-type f" in cmd


async def test_list_artifacts_filters_unsafe_paths_from_output():
    """A maliciously-symlinked find result outside the base must not surface."""
    job_dir = f"{BASE}/jobs/slurmly-12345678"
    stdout = "\n".join(
        [
            f"{job_dir}/run.sh",
            "/etc/passwd",  # forged or symlink-traversed
        ]
    )
    transport = FakeTransport(canned_responses=[(0, stdout, "")])
    client = _client(transport)
    artifacts = await client.list_artifacts(_job())
    assert [a.path for a in artifacts] == [f"{job_dir}/run.sh"]


async def test_list_artifacts_refuses_unsafe_job_dir_before_remote_call():
    transport = FakeTransport()
    client = _client(transport)
    bad = SubmittedJob(
        internal_job_id="slurmly-12345678",
        slurm_job_id="42",
        remote_job_dir="/etc",
        remote_script_path="/etc/run.sh",
        stdout_path="/etc/o",
        stderr_path="/etc/e",
    )
    with pytest.raises(InvalidConfig):
        await client.list_artifacts(bad)
    assert transport.runs == []


# --- download_artifact ----------------------------------------------------


async def test_download_artifact_returns_content():
    transport = FakeTransport()
    transport.files[f"{BASE}/jobs/slurmly-12345678/metrics.json"] = '{"loss": 0.1}'
    client = _client(transport)
    res = await client.download_artifact(_job(), "metrics.json")
    assert isinstance(res, ArtifactDownload)
    assert res.content == '{"loss": 0.1}'
    assert res.path.endswith("/metrics.json")


async def test_download_artifact_rejects_traversal_name():
    transport = FakeTransport()
    client = _client(transport)
    with pytest.raises(InvalidConfig):
        await client.download_artifact(_job(), "../../etc/passwd")


async def test_download_artifact_rejects_absolute_name():
    transport = FakeTransport()
    client = _client(transport)
    with pytest.raises(InvalidConfig):
        await client.download_artifact(_job(), "/etc/passwd")


async def test_download_artifact_truncated_flag_when_capped():
    transport = FakeTransport()
    transport.files[f"{BASE}/jobs/slurmly-12345678/big.txt"] = "x" * 100
    client = _client(transport)
    res = await client.download_artifact(_job(), "big.txt", max_bytes=20)
    assert res.truncated is True
    assert len(res.content) == 20
