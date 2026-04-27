"""Tests for slurmly.models.JobSpec validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slurmly import JobSpec
from slurmly.models import RenderedJob, SubmittedJob


# --- name ------------------------------------------------------------------


def test_jobspec_name_required():
    with pytest.raises(ValidationError):
        JobSpec(command=["echo", "hi"])  # type: ignore[call-arg]


def test_jobspec_name_pattern():
    JobSpec(name="train-model_v2.1", command=["x"])
    with pytest.raises(ValidationError):
        JobSpec(name="bad name", command=["x"])
    with pytest.raises(ValidationError):
        JobSpec(name="", command=["x"])


# --- command vs command_template ------------------------------------------


def test_jobspec_requires_one_of_command_or_template():
    with pytest.raises(ValidationError) as ei:
        JobSpec(name="t")
    assert "command" in str(ei.value)


def test_jobspec_rejects_both_command_and_template():
    with pytest.raises(ValidationError) as ei:
        JobSpec(name="t", command=["a"], command_template="a")
    assert "both" in str(ei.value)


def test_jobspec_rejects_empty_command_list():
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=[])


def test_jobspec_rejects_empty_string_in_command():
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=["python", ""])


def test_command_template_is_preserved_verbatim():
    """Core must NOT perform placeholder substitution on command_template."""
    tpl = "python infer.py --shard ${SLURM_ARRAY_TASK_ID} --run-id run_123"
    spec = JobSpec(name="t", command_template=tpl)
    assert spec.command_template == tpl
    assert "${SLURM_ARRAY_TASK_ID}" in spec.command_template


# --- memory / time_limit ---------------------------------------------------


@pytest.mark.parametrize("mem", ["512", "512M", "32G", "1T", "100K"])
def test_memory_accepts(mem):
    JobSpec(name="t", command=["x"], memory=mem)


@pytest.mark.parametrize("mem", ["32 GB", "32GB", "32g", "MB", "", "-32"])
def test_memory_rejects(mem):
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=["x"], memory=mem)


@pytest.mark.parametrize(
    "tl",
    ["30", "30:00", "01:30:00", "1-12", "1-12:30", "1-12:30:00"],
)
def test_time_limit_accepts(tl):
    JobSpec(name="t", command=["x"], time_limit=tl)


@pytest.mark.parametrize("tl", ["1.5h", "30m", "30 min", "1d", ""])
def test_time_limit_rejects(tl):
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=["x"], time_limit=tl)


# --- env keys --------------------------------------------------------------


def test_env_key_pattern():
    JobSpec(name="t", command=["x"], env={"WANDB_MODE": "offline", "_X": "1"})
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=["x"], env={"1BAD": "x"})
    with pytest.raises(ValidationError):
        JobSpec(name="t", command=["x"], env={"BAD-KEY": "x"})


# --- array spec model-level rules (Phase 4) -------------------------------


def test_array_requires_command_template_not_command():
    """shlex.join would single-quote ${SLURM_ARRAY_TASK_ID}, breaking arrays silently."""
    with pytest.raises(ValidationError) as ei:
        JobSpec(name="t", command=["python", "x.py"], array="1-10")
    msg = str(ei.value)
    assert "command_template" in msg
    assert "array" in msg


def test_array_with_command_template_accepted():
    spec = JobSpec(
        name="t",
        command_template="python x.py --shard ${SLURM_ARRAY_TASK_ID}",
        array="0-9",
    )
    assert spec.array == "0-9"


def test_array_spec_validation_rejects_garbage():
    with pytest.raises(ValidationError):
        JobSpec(name="t", command_template="echo $X", array="not a range")


def test_array_spec_validation_accepts_with_concurrency():
    spec = JobSpec(name="t", command_template="echo $X", array="0-99%4")
    assert spec.array == "0-99%4"


# --- internal_job_id format ------------------------------------------------


def test_submitted_job_internal_id_must_match_pattern():
    kwargs = dict(
        slurm_job_id="123",
        remote_job_dir="/tmp/x",
        remote_script_path="/tmp/x/run.sh",
        stdout_path="/tmp/x/stdout.log",
        stderr_path="/tmp/x/stderr.log",
    )
    SubmittedJob(internal_job_id="slurmly-deadbeef", **kwargs)
    with pytest.raises(ValidationError):
        SubmittedJob(internal_job_id="not-our-id", **kwargs)
    with pytest.raises(ValidationError):
        SubmittedJob(internal_job_id="slurmly-NOTHEX!", **kwargs)


def test_rendered_job_internal_id_validated_too():
    kwargs = dict(
        remote_job_dir="/tmp/x",
        remote_script_path="/tmp/x/run.sh",
        stdout_path="/tmp/x/stdout.log",
        stderr_path="/tmp/x/stderr.log",
        script="#!/usr/bin/env bash\n",
    )
    RenderedJob(internal_job_id="slurmly-12345678", **kwargs)
    with pytest.raises(ValidationError):
        RenderedJob(internal_job_id="bogus", **kwargs)
