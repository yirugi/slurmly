"""Phase 3 — hooks fire in the expected order around client operations."""

from __future__ import annotations

from pathlib import Path

import pytest

from slurmly import (
    CommandFinishedEvent,
    CommandStartedEvent,
    JobSpec,
    SlurmlyHooks,
    SlurmSSHClient,
    StatusCheckedEvent,
    SubmitFailedEvent,
    SubmitSucceededEvent,
    CancelRequestedEvent,
)
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


class RecordingHooks(SlurmlyHooks):
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    async def on_command_started(self, event: CommandStartedEvent) -> None:
        self.events.append(("command_started", event))

    async def on_command_finished(self, event: CommandFinishedEvent) -> None:
        self.events.append(("command_finished", event))

    async def on_submit_succeeded(self, event: SubmitSucceededEvent) -> None:
        self.events.append(("submit_succeeded", event))

    async def on_submit_failed(self, event: SubmitFailedEvent) -> None:
        self.events.append(("submit_failed", event))

    async def on_status_checked(self, event: StatusCheckedEvent) -> None:
        self.events.append(("status_checked", event))

    async def on_cancel_requested(self, event: CancelRequestedEvent) -> None:
        self.events.append(("cancel_requested", event))


def _client(transport: FakeTransport, hooks: SlurmlyHooks | None = None) -> SlurmSSHClient:
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
        hooks=hooks,
    )


async def test_submit_emits_command_and_submit_events_in_order():
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (0, "16680647\n", "")]
    )
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    await client.submit(JobSpec(name="t", command=["x"], partition="shared"))

    kinds = [k for k, _ in hooks.events]
    # mkdir start/finish, sbatch start/finish, then submit_succeeded
    assert kinds == [
        "command_started",
        "command_finished",
        "command_started",
        "command_finished",
        "submit_succeeded",
    ]
    # Pair operation names to commands
    started = [e for k, e in hooks.events if k == "command_started"]
    assert started[0].operation == "mkdir"
    assert started[1].operation == "sbatch"
    # sbatch must NOT be retry_safe
    assert started[1].retry_safe is False


async def test_submit_failure_emits_submit_failed():
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (1, "", "boom\n")]
    )
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    with pytest.raises(Exception):
        await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert any(k == "submit_failed" for k, _ in hooks.events)
    failed = next(e for k, e in hooks.events if k == "submit_failed")
    assert failed.error_type == "SbatchError"


async def test_cancel_emits_cancel_requested():
    transport = FakeTransport()
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    await client.cancel("12345")
    kinds = [k for k, _ in hooks.events]
    # cancel_requested fires BEFORE the scancel command starts
    assert kinds[0] == "cancel_requested"
    assert "command_started" in kinds


async def test_get_job_emits_status_checked():
    transport = FakeTransport(
        canned_responses=[(0, (FIXTURES / "squeue_running.json").read_text(), "")]
    )
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    await client.get_job("16680653")
    statuses = [e for k, e in hooks.events if k == "status_checked"]
    assert len(statuses) == 1
    assert statuses[0].lifecycle == "running"
    assert statuses[0].source == "squeue"


async def test_default_hooks_are_no_op():
    # No hooks supplied — client must still operate.
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (0, "1\n", "")]
    )
    client = _client(transport)
    job = await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert job.slurm_job_id == "1"


# --- exception propagation -------------------------------------------------


class _BoomOnStarted(SlurmlyHooks):
    async def on_command_started(self, event: CommandStartedEvent) -> None:
        raise RuntimeError("hook boom")


class _BoomOnFinished(SlurmlyHooks):
    async def on_command_finished(self, event: CommandFinishedEvent) -> None:
        raise RuntimeError("finish boom")


async def test_hook_exception_in_on_command_started_propagates():
    """Hook exceptions are not swallowed — they raise to the caller."""
    transport = FakeTransport(canned_responses=[(0, "", "")])
    client = _client(transport, hooks=_BoomOnStarted())
    with pytest.raises(RuntimeError, match="hook boom"):
        await client.cancel("12345")


async def test_hook_exception_in_on_command_finished_propagates():
    transport = FakeTransport(canned_responses=[(0, "", "")])
    client = _client(transport, hooks=_BoomOnFinished())
    with pytest.raises(RuntimeError, match="finish boom"):
        await client.cancel("12345")


# --- coverage of phase-3/4 ops --------------------------------------------


async def test_hooks_fire_on_tail_stdout():
    transport = FakeTransport(canned_responses=[(0, "log line\n", "")])
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    from slurmly import SubmittedJob

    job = SubmittedJob(
        internal_job_id="slurmly-deadbeef",
        slurm_job_id="1",
        remote_job_dir="/scratch/u/slurmly/jobs/slurmly-deadbeef",
        remote_script_path="/scratch/u/slurmly/jobs/slurmly-deadbeef/run.sh",
        stdout_path="/scratch/u/slurmly/jobs/slurmly-deadbeef/stdout.log",
        stderr_path="/scratch/u/slurmly/jobs/slurmly-deadbeef/stderr.log",
    )
    await client.tail_stdout(job, lines=20)
    started = [e for k, e in hooks.events if k == "command_started"]
    assert any(e.operation == "tail" for e in started)
    finished = [e for k, e in hooks.events if k == "command_finished"]
    assert finished and finished[-1].exit_status == 0


async def test_hooks_fire_on_cleanup():
    transport = FakeTransport(canned_responses=[(0, "", "")])
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    from slurmly import SubmittedJob

    job = SubmittedJob(
        internal_job_id="slurmly-cafef00d",
        slurm_job_id="1",
        remote_job_dir="/scratch/u/slurmly/jobs/slurmly-cafef00d",
        remote_script_path="/scratch/u/slurmly/jobs/slurmly-cafef00d/run.sh",
        stdout_path="/scratch/u/slurmly/jobs/slurmly-cafef00d/stdout.log",
        stderr_path="/scratch/u/slurmly/jobs/slurmly-cafef00d/stderr.log",
    )
    await client.cleanup_remote_job_dir(job)
    started = [e for k, e in hooks.events if k == "command_started"]
    assert any(e.operation == "cleanup" for e in started)


async def test_status_checked_source_sacct_when_squeue_misses():
    transport = FakeTransport(
        canned_responses=[
            (0, '{"jobs": []}', ""),
            (0, (FIXTURES / "sacct_completed.json").read_text(), ""),
        ]
    )
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    await client.get_job("16680647")
    statuses = [e for k, e in hooks.events if k == "status_checked"]
    assert len(statuses) == 1
    assert statuses[0].source == "sacct"
    assert statuses[0].lifecycle == "succeeded"


async def test_submit_succeeded_payload_carries_internal_and_slurm_ids():
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (0, "55555\n", "")]
    )
    hooks = RecordingHooks()
    client = _client(transport, hooks)
    job = await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    succeeded = next(e for k, e in hooks.events if k == "submit_succeeded")
    assert succeeded.slurm_job_id == "55555"
    assert succeeded.internal_job_id == job.internal_job_id
    assert succeeded.internal_job_id.startswith("slurmly-")
