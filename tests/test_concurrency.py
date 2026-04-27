"""Phase 3 — concurrency semaphore + retry_safe flagging."""

from __future__ import annotations

import asyncio

import pytest

from slurmly import InvalidConfig, JobSpec, SlurmSSHClient
from slurmly.profiles import ClusterProfile
from slurmly.ssh import CommandResult
from slurmly.testing import FakeTransport


def _client(transport: FakeTransport, **kwargs) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
        **kwargs,
    )


def test_max_concurrent_commands_must_be_positive():
    with pytest.raises(InvalidConfig):
        _client(FakeTransport(), max_concurrent_commands=0)


async def test_semaphore_caps_in_flight_calls():
    """Two concurrent runs against a semaphore=1 client must serialize."""
    in_flight = 0
    peak = 0
    gate = asyncio.Event()

    class GatingTransport(FakeTransport):
        async def run(self, command, *, timeout=None, check=True, retry_safe=False):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                # Hold the first arrival until the second one is queued.
                await asyncio.sleep(0.01)
                return CommandResult(
                    command=command, exit_status=0, stdout="hello\n", stderr=""
                )
            finally:
                in_flight -= 1

    transport = GatingTransport()
    client = _client(transport, max_concurrent_commands=1)

    async def one():
        # Use a no-arg op that goes through `_run` — the tail probe is convenient.
        return await client._run(
            "echo hi", operation="probe.test", check=False, retry_safe=True
        )

    await asyncio.gather(one(), one(), one())
    gate.set()
    assert peak == 1
    assert client.max_concurrent_commands == 1


async def test_sbatch_is_not_retry_safe():
    """sbatch must be issued with retry_safe=False to avoid duplicate submits."""
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (0, "1\n", "")]
    )
    client = _client(transport)
    await client.submit(JobSpec(name="t", command=["x"], partition="shared"))

    sbatch_calls = [r for r in transport.runs if "sbatch" in r.command]
    assert len(sbatch_calls) == 1
    assert sbatch_calls[0].retry_safe is False


async def test_max_concurrent_one_serializes_strictly():
    """With max=1, three concurrent ops must execute one at a time (peak=1)."""
    in_flight = 0
    peak = 0
    seen = 0

    class CountingTransport(FakeTransport):
        async def run(self, command, *, timeout=None, check=True, retry_safe=False):
            nonlocal in_flight, peak, seen
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                await asyncio.sleep(0)  # yield so other tasks can race in
                return CommandResult(command=command, exit_status=0, stdout="", stderr="")
            finally:
                in_flight -= 1
                seen += 1

    client = _client(CountingTransport(), max_concurrent_commands=1)

    async def one():
        return await client._run("x", operation="probe", check=False, retry_safe=True)

    await asyncio.gather(one(), one(), one())
    assert seen == 3
    assert peak == 1


async def test_status_lookups_are_retry_safe():
    """Read-only commands like squeue are flagged retry_safe=True."""
    from pathlib import Path

    transport = FakeTransport(
        canned_responses=[
            (
                0,
                (
                    Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"
                    / "squeue_running.json"
                ).read_text(),
                "",
            )
        ]
    )
    client = _client(transport)
    await client.get_job("16680653")
    assert transport.runs[0].retry_safe is True
