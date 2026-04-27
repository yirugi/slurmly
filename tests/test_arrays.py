"""Phase 4 — job arrays: spec validation, renderer, parser, get_job."""

from __future__ import annotations

from pathlib import Path

import pytest

from slurmly import JobInfo, JobSpec, SlurmSSHClient
from slurmly.exceptions import InvalidJobSpec
from slurmly.paths import job_paths
from slurmly.profiles import ClusterProfile
from slurmly.scripts.renderer import render_script
from slurmly.slurm.parser import parse_squeue_json_str
from slurmly.testing import FakeTransport
from slurmly.validators import is_valid_array_spec

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


def _client(transport: FakeTransport, *, supports_array_jobs: bool = True) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
        supports_array_jobs=supports_array_jobs,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )


# --- spec validation ------------------------------------------------------


def test_array_spec_grammar_simple_range():
    assert is_valid_array_spec("0-9")


def test_array_spec_grammar_with_step():
    assert is_valid_array_spec("0-99:2")


def test_array_spec_grammar_with_concurrency_cap():
    assert is_valid_array_spec("0-99%4")


def test_array_spec_grammar_list():
    assert is_valid_array_spec("1,3,5,7")


def test_array_spec_grammar_rejects_letters():
    assert not is_valid_array_spec("0-a")


def test_array_spec_grammar_rejects_double_percent():
    assert not is_valid_array_spec("0-9%2%4")


# --- renderer -------------------------------------------------------------


def _profile() -> ClusterProfile:
    return ClusterProfile(
        name="t",
        default_partition="shared",
        allowed_partitions=["shared"],
        supports_array_jobs=True,
    )


def test_renderer_emits_array_directive():
    spec = JobSpec(
        name="batch",
        command_template="echo task=$SLURM_ARRAY_TASK_ID",
        array="0-9%2",
        partition="shared",
        account="a1",
    )
    paths = job_paths(
        remote_base_dir="/scratch/u/slurmly",
        internal_job_id="slurmly-aaaaaaaa",
        is_array=True,
    )
    script = render_script(
        spec=spec,
        profile=_profile(),
        execution_profile=None,
        stdout_path=paths["stdout"],
        stderr_path=paths["stderr"],
        remote_job_dir=paths["job_dir"],
    )
    assert "#SBATCH --array=0-9%2" in script
    # Per-task log filenames must use %A/%a so tasks don't clobber each other.
    assert "stdout-%A_%a.log" in script
    assert "stderr-%A_%a.log" in script
    # And the body must be the verbatim template — not shlex-quoted.
    assert "echo task=$SLURM_ARRAY_TASK_ID" in script


def test_paths_array_uses_per_task_pattern():
    p = job_paths(
        remote_base_dir="/base",
        internal_job_id="slurmly-12345678",
        is_array=True,
    )
    assert p["stdout"].endswith("/stdout-%A_%a.log")
    assert p["stderr"].endswith("/stderr-%A_%a.log")


def test_paths_default_uses_single_log():
    p = job_paths(
        remote_base_dir="/base",
        internal_job_id="slurmly-12345678",
    )
    assert p["stdout"].endswith("/stdout.log")
    assert p["stderr"].endswith("/stderr.log")


# --- parser: array_job_id / array_task_id population ----------------------


def _squeue_with_array(parent: int, task: int, name: str = "arr") -> str:
    """Synthesize a minimal squeue --json payload with an array task row."""
    import json as _json

    payload = {
        "jobs": [
            {
                "job_id": parent * 1000 + task,
                "name": name,
                "user_name": "u",
                "partition": "shared",
                "account": "a1",
                "qos": "cpu",
                "job_state": ["RUNNING"],
                "array_job_id": {"set": True, "infinite": False, "number": parent},
                "array_task_id": {"set": True, "infinite": False, "number": task},
            }
        ]
    }
    return _json.dumps(payload)


def test_parser_populates_array_fields_for_task_row():
    info = parse_squeue_json_str(_squeue_with_array(123, 7), "123_7")
    assert info is not None
    assert info.array_job_id == "123"
    assert info.array_task_id == "7"
    assert info.lifecycle == "running"


def test_parser_no_array_fields_for_plain_job():
    fixture = (FIXTURES / "squeue_running.json").read_text()
    info = parse_squeue_json_str(fixture, "16680653")
    assert info is not None
    # The Anvil fixture has array_job_id={set:true, number:0} which is the
    # "this isn't an array" sentinel — we should not surface it.
    assert info.array_job_id is None
    assert info.array_task_id is None


# --- client: get_job accepts array suffix only when profile allows --------


async def test_get_job_accepts_array_suffix_when_profile_supports():
    transport = FakeTransport(
        canned_responses=[(0, _squeue_with_array(123, 7), "")]
    )
    client = _client(transport, supports_array_jobs=True)
    info = await client.get_job("123_7")
    assert isinstance(info, JobInfo)
    assert info.array_job_id == "123"
    assert info.array_task_id == "7"


async def test_get_job_rejects_array_suffix_when_profile_disallows():
    transport = FakeTransport()
    client = _client(transport, supports_array_jobs=False)
    with pytest.raises(InvalidJobSpec):
        await client.get_job("123_7")
    assert transport.runs == []
