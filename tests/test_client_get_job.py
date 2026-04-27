"""Phase 2 — get_job / get_jobs end-to-end against FakeTransport."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from slurmly import JobInfo, SlurmSSHClient
from slurmly.exceptions import InvalidJobSpec, SlurmJobNotFound
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


def _client(transport: FakeTransport, **profile_kwargs) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
        submit_grace_window_seconds=120,
        **profile_kwargs,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )


def _f(name: str) -> str:
    return (FIXTURES / name).read_text()


# --- happy paths ----------------------------------------------------------


async def test_get_job_returns_squeue_when_active():
    transport = FakeTransport(
        canned_responses=[(0, _f("squeue_running.json"), "")]
    )
    client = _client(transport)
    info = await client.get_job("16680653")
    assert isinstance(info, JobInfo)
    assert info.lifecycle == "running"
    assert info.source == "squeue"
    # only squeue was called — no sacct fallback
    assert len(transport.runs) == 1
    assert "squeue --json" in transport.runs[0].command


async def test_get_job_falls_through_to_sacct_when_squeue_empty():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, _f("sacct_completed.json"), ""),
        ]
    )
    client = _client(transport)
    info = await client.get_job("16680647")
    assert info.source == "sacct"
    assert info.lifecycle == "succeeded"
    assert len(transport.runs) == 2
    assert "sacct --json" in transport.runs[1].command
    assert "--starttime=now-14days" in transport.runs[1].command


async def test_get_job_passes_cluster_suffix_to_both_commands():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, _f("sacct_completed.json"), ""),
        ]
    )
    client = _client(transport)
    info = await client.get_job("16680647", cluster="anvil")
    assert "--clusters=anvil" in transport.runs[0].command
    assert "--clusters=anvil" in transport.runs[1].command
    assert info.cluster == "anvil"


# --- transient gap / not found -------------------------------------------


async def test_get_job_transient_gap_within_grace_window():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = datetime.now(timezone.utc) - timedelta(seconds=30)
    info = await client.get_job("9999", submitted_at=submitted_at)
    assert info.visibility == "transient_gap"
    assert info.lifecycle == "unknown"
    assert info.source == "none"


async def test_get_job_transient_gap_accepts_iso_z_string():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    ts = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    info = await client.get_job("9999", submitted_at=ts)
    assert info.visibility == "transient_gap"


async def test_get_job_raises_not_found_outside_grace_window():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = datetime.now(timezone.utc) - timedelta(seconds=600)
    with pytest.raises(SlurmJobNotFound):
        await client.get_job("9999", submitted_at=submitted_at)


async def test_get_job_raises_not_found_when_submitted_at_missing():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    with pytest.raises(SlurmJobNotFound):
        await client.get_job("9999")


# --- array reject --------------------------------------------------------


async def test_get_job_rejects_array_job_id_before_remote_call():
    transport = FakeTransport()
    client = _client(transport)
    with pytest.raises(InvalidJobSpec):
        await client.get_job("12345_0")
    assert transport.runs == []


async def test_get_job_rejects_non_numeric_job_id():
    transport = FakeTransport()
    client = _client(transport)
    with pytest.raises(InvalidJobSpec):
        await client.get_job("not-a-number")
    assert transport.runs == []


# --- supports_squeue_json=False ------------------------------------------


async def test_get_job_skips_squeue_when_json_unsupported():
    transport = FakeTransport(
        canned_responses=[(0, _f("sacct_completed.json"), "")]
    )
    client = _client(transport, supports_squeue_json=False)
    info = await client.get_job("16680647")
    assert info.source == "sacct"
    # only one call, and it's sacct
    assert len(transport.runs) == 1
    assert "sacct --json" in transport.runs[0].command


async def test_get_job_uses_parsable2_when_sacct_json_unsupported():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_empty.json"), ""),
            (0, _f("sacct_parsable2_completed.txt"), ""),
        ]
    )
    client = _client(transport, supports_sacct_json=False)
    info = await client.get_job("16680647")
    assert info.source == "sacct"
    assert info.lifecycle == "succeeded"
    assert "sacct --parsable2" in transport.runs[1].command


# --- get_jobs ------------------------------------------------------------


async def test_get_jobs_collects_visible_only():
    transport = FakeTransport(
        canned_responses=[
            (0, _f("squeue_running.json"), ""),  # 16680653 -> running
            (0, _f("squeue_empty.json"), ""),
            (0, '{"jobs": []}', ""),  # missing for 9999
        ]
    )
    client = _client(transport)
    out = await client.get_jobs(["16680653", "9999"])
    assert "16680653" in out
    assert "9999" not in out
    assert out["16680653"].lifecycle == "running"


# --- non-zero squeue exit treated as miss --------------------------------


async def test_get_job_accepts_array_task_id_when_array_support_enabled():
    """`12345_3` is a valid query when ClusterProfile.supports_array_jobs=True."""
    payload = {
        "jobs": [
            {
                "job_id": 12345,
                "name": "x",
                "user_name": "u",
                "partition": "shared",
                "account": "a",
                "qos": "cpu",
                "job_state": ["RUNNING"],
                "array_job_id": {"set": True, "infinite": False, "number": 12345},
                "array_task_id": {"set": True, "infinite": False, "number": 3},
            }
        ]
    }
    transport = FakeTransport(canned_responses=[(0, json.dumps(payload), "")])
    client = _client(transport, supports_array_jobs=True)
    info = await client.get_job("12345_3")
    assert info.lifecycle == "running"
    assert "--jobs=12345_3" in transport.runs[0].command


async def test_get_job_squeue_nonzero_exit_falls_through_to_sacct():
    transport = FakeTransport(
        canned_responses=[
            (1, "", "slurm_load_jobs error: Invalid job id specified\n"),
            (0, _f("sacct_completed.json"), ""),
        ]
    )
    client = _client(transport)
    info = await client.get_job("16680647")
    assert info.source == "sacct"
    assert info.lifecycle == "succeeded"
