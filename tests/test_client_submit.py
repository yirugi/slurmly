"""End-to-end submit/cancel/tail flow against FakeTransport."""

from __future__ import annotations

import json

import pytest

from slurmly import JobSpec, SlurmSSHClient
from slurmly.exceptions import SbatchError, SbatchParseError
from slurmly.profiles import ClusterProfile, ExecutionProfile
from slurmly.ssh import CommandResult
from slurmly.testing import FakeTransport


def _make_client(transport: FakeTransport, **kwargs) -> SlurmSSHClient:
    profile = kwargs.pop(
        "profile",
        ClusterProfile(
            name="test",
            default_partition="shared",
            allowed_partitions=["shared", "debug"],
            gpu_directive="gpus_per_node",
        ),
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        execution_profiles=kwargs.pop("execution_profiles", {}),
        account=kwargs.pop("account", "acct1"),
    )


# --- submit happy path ----------------------------------------------------


async def test_submit_happy_path_records_expected_remote_calls():
    transport = FakeTransport(
        canned_responses=[
            (0, "", ""),  # mkdir -p
            (0, "16680647\n", ""),  # sbatch --parsable
        ]
    )
    client = _make_client(transport)
    spec = JobSpec(name="hello", command=["echo", "hi"], partition="shared")

    job = await client.submit(spec)

    # Sequence: mkdir, then sbatch (uploads happen in between, recorded separately).
    assert len(transport.runs) == 2
    assert transport.runs[0].command.startswith("mkdir -p ")
    assert "sbatch --parsable run.sh" in transport.runs[1].command
    assert "cd " in transport.runs[1].command  # uses cd <job_dir> && sbatch

    # Two uploads: metadata.json then run.sh
    assert len(transport.uploads) == 2
    paths = [u.path for u in transport.uploads]
    assert paths[0].endswith("metadata.json")
    assert paths[1].endswith("run.sh")
    assert transport.uploads[1].mode == 0o700

    # Result shape
    assert job.slurm_job_id == "16680647"
    assert job.cluster is None
    assert job.internal_job_id.startswith("slurmly-")
    assert job.remote_script_path.endswith("run.sh")


async def test_cancel_already_terminated_job_does_not_raise():
    """scancel against a finished job exits 0 with empty output — no exception."""
    transport = FakeTransport(canned_responses=[(0, "", "")])
    client = _make_client(transport)
    result = await client.cancel("99999")
    assert result.slurm_job_id == "99999"
    assert result.raw_stderr == ""
    # Default signal is None (sends TERM by default in scancel).
    assert "scancel" in transport.runs[0].command


async def test_cancel_with_custom_signal_passes_it_through():
    transport = FakeTransport(canned_responses=[(0, "", "")])
    client = _make_client(transport)
    await client.cancel("12345", signal="USR1")
    assert "--signal=USR1" in transport.runs[0].command


async def test_submit_preserves_raw_stdout_and_stderr():
    transport = FakeTransport(
        canned_responses=[
            (0, "", ""),
            (0, "16680647\n", "Note: small warning\n"),
        ]
    )
    client = _make_client(transport)
    job = await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert job.submit_stdout == "16680647\n"
    assert job.submit_stderr == "Note: small warning\n"


async def test_submit_with_cluster_suffix():
    transport = FakeTransport(
        canned_responses=[
            (0, "", ""),
            (0, "16680647;anvil\n", ""),
        ]
    )
    client = _make_client(transport)
    job = await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert job.slurm_job_id == "16680647"
    assert job.cluster == "anvil"


async def test_submit_metadata_payload_shape():
    transport = FakeTransport(
        canned_responses=[(0, "", ""), (0, "1\n", "")]
    )
    client = _make_client(transport)
    spec = JobSpec(name="t", command=["echo", "hi"], partition="shared")
    job = await client.submit(spec)

    metadata_upload = next(u for u in transport.uploads if u.path.endswith("metadata.json"))
    doc = json.loads(metadata_upload.content)
    assert doc["internal_job_id"] == job.internal_job_id
    assert doc["paths"]["script"].endswith("run.sh")
    assert doc["spec"]["name"] == "t"
    assert doc["spec"]["account"] == "acct1"  # client account injected


# --- submit failure modes -------------------------------------------------


async def test_sbatch_nonzero_raises_sbatch_error():
    transport = FakeTransport(
        canned_responses=[
            (0, "", ""),
            (1, "", "sbatch: error: invalid partition\n"),
        ]
    )
    client = _make_client(transport)
    with pytest.raises(SbatchError) as ei:
        await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert "invalid partition" in ei.value.stderr


async def test_sbatch_unparsable_output_raises_parse_error():
    transport = FakeTransport(
        canned_responses=[
            (0, "", ""),
            (0, "this is not a job id\n", ""),
        ]
    )
    client = _make_client(transport)
    with pytest.raises(SbatchParseError):
        await client.submit(JobSpec(name="t", command=["x"], partition="shared"))


async def test_profile_validation_runs_before_remote_calls():
    """Submitting with a disallowed partition must not touch SSH at all."""
    transport = FakeTransport()
    client = _make_client(transport)
    with pytest.raises(Exception):
        await client.submit(
            JobSpec(name="t", command=["x"], partition="forbidden_partition")
        )
    assert transport.runs == []
    assert transport.uploads == []


async def test_submit_requires_account_when_no_default():
    transport = FakeTransport()
    profile = ClusterProfile(
        default_partition="shared",
        allowed_partitions=["shared"],
    )
    client = SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
    )
    with pytest.raises(Exception):
        await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    assert transport.runs == []


# --- cancel ---------------------------------------------------------------


async def test_cancel_builds_scancel_command():
    transport = FakeTransport()
    client = _make_client(transport)
    result = await client.cancel("12345", signal="TERM")
    assert "scancel" in transport.runs[0].command
    assert "--signal=TERM" in transport.runs[0].command
    assert "12345" in transport.runs[0].command
    assert result.slurm_job_id == "12345"
    assert result.signal == "TERM"


# --- tail -----------------------------------------------------------------


async def test_tail_stdout_existing_file():
    transport = FakeTransport(
        handler=lambda cmd: CommandResult(
            command=cmd, exit_status=0, stdout="line1\nline2\n", stderr=""
        )
    )
    client = _make_client(transport)
    spec = JobSpec(name="t", command=["x"], partition="shared")
    transport.canned_responses = [(0, "", ""), (0, "16680647\n", "")]
    job = await client.submit(spec)

    chunk = await client.tail_stdout(job, lines=50)
    assert chunk.exists is True
    assert chunk.content == "line1\nline2\n"
    assert chunk.lines_requested == 50


async def test_tail_stderr_missing_file_returns_exists_false():
    transport = FakeTransport(
        handler=lambda cmd: CommandResult(
            command=cmd, exit_status=1, stdout="", stderr=""
        )
    )
    transport.canned_responses = [(0, "", ""), (0, "1\n", "")]
    client = _make_client(transport)
    spec = JobSpec(name="t", command=["x"], partition="shared")
    job = await client.submit(spec)

    chunk = await client.tail_stderr(job, lines=200)
    assert chunk.exists is False
    assert chunk.content == ""


# --- render_only ---------------------------------------------------------


def test_render_only_does_not_touch_transport():
    transport = FakeTransport()
    client = _make_client(transport)
    rendered = client.render_only(
        JobSpec(name="dryrun", command=["echo", "hi"], partition="shared")
    )
    assert transport.runs == []
    assert transport.uploads == []
    assert rendered.script.startswith("#!/usr/bin/env bash")
    assert rendered.internal_job_id.startswith("slurmly-")


def test_render_only_validates_partition():
    transport = FakeTransport()
    client = _make_client(transport)
    with pytest.raises(Exception):
        client.render_only(
            JobSpec(name="t", command=["x"], partition="not-allowed")
        )


# --- execution profile dispatch -------------------------------------------


def test_unknown_execution_profile_rejected():
    transport = FakeTransport()
    client = _make_client(transport, execution_profiles={"a": ExecutionProfile(name="a")})
    with pytest.raises(Exception):
        client.render_only(
            JobSpec(
                name="t",
                command=["x"],
                partition="shared",
                execution_profile="missing",
            )
        )
