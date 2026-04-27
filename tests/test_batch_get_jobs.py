"""Phase 4 — batched get_jobs: single-shot squeue + sacct, chunking, fallbacks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slurmly import SlurmSSHClient
from slurmly.exceptions import InvalidJobSpec
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


def _client(
    transport: FakeTransport,
    *,
    supports_squeue_json: bool = True,
    supports_sacct_json: bool = True,
    supports_array_jobs: bool = False,
) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
        supports_squeue_json=supports_squeue_json,
        supports_sacct_json=supports_sacct_json,
        supports_array_jobs=supports_array_jobs,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )


def _squeue_payload(rows: list[dict]) -> str:
    return json.dumps({"jobs": rows})


def _row(job_id: int, state: str, name: str = "x") -> dict:
    return {
        "job_id": job_id,
        "name": name,
        "user_name": "u",
        "partition": "shared",
        "account": "a1",
        "qos": "cpu",
        "job_state": [state],
        "array_job_id": {"set": True, "infinite": False, "number": 0},
        "array_task_id": {"set": False, "infinite": False, "number": 0},
    }


# --- happy paths ----------------------------------------------------------


async def test_get_jobs_uses_single_squeue_call_when_all_active():
    transport = FakeTransport(
        canned_responses=[
            (0, _squeue_payload([_row(101, "RUNNING"), _row(102, "PENDING")]), "")
        ]
    )
    client = _client(transport)
    out = await client.get_jobs(["101", "102"])
    assert set(out.keys()) == {"101", "102"}
    assert out["101"].lifecycle == "running"
    assert out["102"].lifecycle == "queued"
    # one squeue, no sacct fallback because nothing was missing.
    assert len(transport.runs) == 1
    assert "squeue --json" in transport.runs[0].command
    assert "--jobs=101,102" in transport.runs[0].command


async def test_get_jobs_falls_back_to_sacct_for_missing_ids():
    transport = FakeTransport(
        canned_responses=[
            # squeue returns only 101
            (0, _squeue_payload([_row(101, "RUNNING")]), ""),
            # sacct then sees 102
            (0, (FIXTURES / "sacct_completed.json").read_text(), ""),
        ]
    )
    client = _client(transport)
    out = await client.get_jobs(["101", "16680647"])
    assert "101" in out
    assert "16680647" in out
    assert out["16680647"].source == "sacct"
    # squeue once, sacct once.
    assert len(transport.runs) == 2
    assert "sacct --json" in transport.runs[1].command


async def test_get_jobs_chunks_when_input_exceeds_limit(monkeypatch):
    import slurmly.client as client_mod

    monkeypatch.setattr(client_mod, "_BATCH_GET_JOBS_CHUNK", 2)
    transport = FakeTransport(
        canned_responses=[
            (0, _squeue_payload([_row(1, "RUNNING"), _row(2, "RUNNING")]), ""),
            (0, _squeue_payload([_row(3, "RUNNING")]), ""),
        ]
    )
    client = _client(transport)
    out = await client.get_jobs(["1", "2", "3"])
    assert set(out.keys()) == {"1", "2", "3"}
    assert len(transport.runs) == 2  # two squeue chunks; both fully resolved


async def test_get_jobs_dedups_input():
    transport = FakeTransport(
        canned_responses=[(0, _squeue_payload([_row(101, "RUNNING")]), "")]
    )
    client = _client(transport)
    out = await client.get_jobs(["101", "101", "101"])
    assert list(out.keys()) == ["101"]
    assert len(transport.runs) == 1


async def test_get_jobs_skips_squeue_when_unsupported():
    transport = FakeTransport(
        canned_responses=[
            (0, (FIXTURES / "sacct_completed.json").read_text(), ""),
        ]
    )
    client = _client(transport, supports_squeue_json=False)
    out = await client.get_jobs(["16680647"])
    assert "16680647" in out
    # Only sacct was called.
    assert len(transport.runs) == 1
    assert "sacct --json" in transport.runs[0].command


async def test_get_jobs_validates_each_id():
    transport = FakeTransport()
    client = _client(transport, supports_array_jobs=False)
    with pytest.raises(InvalidJobSpec):
        await client.get_jobs(["123", "abc"])
    # No remote calls when validation fails.
    assert transport.runs == []


async def test_get_jobs_empty_input_returns_empty_dict():
    transport = FakeTransport()
    client = _client(transport)
    out = await client.get_jobs([])
    assert out == {}
    assert transport.runs == []


async def test_get_jobs_chunk_boundary_at_exact_multiple(monkeypatch):
    """N == chunk_size: exactly one squeue call, no leftover chunk."""
    import slurmly.client as client_mod

    monkeypatch.setattr(client_mod, "_BATCH_GET_JOBS_CHUNK", 5)
    rows = [_row(i, "RUNNING") for i in range(1, 6)]
    transport = FakeTransport(canned_responses=[(0, _squeue_payload(rows), "")])
    client = _client(transport)
    out = await client.get_jobs([str(i) for i in range(1, 6)])
    assert set(out.keys()) == {"1", "2", "3", "4", "5"}
    assert len(transport.runs) == 1


async def test_get_jobs_chunk_boundary_off_by_one(monkeypatch):
    """N == chunk_size + 1: two squeue calls, [5] then [1]."""
    import slurmly.client as client_mod

    monkeypatch.setattr(client_mod, "_BATCH_GET_JOBS_CHUNK", 5)
    transport = FakeTransport(
        canned_responses=[
            (0, _squeue_payload([_row(i, "RUNNING") for i in range(1, 6)]), ""),
            (0, _squeue_payload([_row(6, "RUNNING")]), ""),
        ]
    )
    client = _client(transport)
    out = await client.get_jobs([str(i) for i in range(1, 7)])
    assert len(out) == 6
    assert len(transport.runs) == 2
    # First chunk has 5 ids, second chunk has 1.
    assert "--jobs=1,2,3,4,5" in transport.runs[0].command
    assert "--jobs=6" in transport.runs[1].command


async def test_get_jobs_returns_subset_when_some_missing():
    transport = FakeTransport(
        canned_responses=[
            (0, _squeue_payload([_row(101, "RUNNING")]), ""),
            # sacct returns nothing useful for 999
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    out = await client.get_jobs(["101", "999"])
    assert set(out.keys()) == {"101"}
