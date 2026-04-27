"""Phase 2 — fixture-driven parsers for squeue/sacct JSON and parsable2."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slurmly.slurm.parser import (
    parse_sacct_json,
    parse_sacct_json_str,
    parse_sacct_parsable2,
    parse_squeue_json,
    parse_squeue_json_str,
)


def _load_json(root: Path, name: str) -> dict:
    return json.loads((root / name).read_text())


def _load_text(root: Path, name: str) -> str:
    return (root / name).read_text()


# --- squeue ----------------------------------------------------------------


def test_squeue_pending_extracts_queue_reason(fixtures_root):
    payload = _load_json(fixtures_root, "squeue_pending.json")
    info = parse_squeue_json(payload, "16680653")
    assert info is not None
    assert info.lifecycle == "queued"
    assert info.raw_state == "PENDING"
    assert info.queue_reason == "Priority"
    assert info.source == "squeue"
    assert info.visibility == "visible"
    assert info.name == "slurmly-sleep"
    assert info.partition == "shared"
    assert info.account == "ees260008"
    assert info.user == "x-openghm"
    assert info.cluster == "anvil"


def test_squeue_running_no_queue_reason(fixtures_root):
    payload = _load_json(fixtures_root, "squeue_running.json")
    info = parse_squeue_json(payload, "16680653")
    assert info is not None
    assert info.lifecycle == "running"
    assert info.raw_state == "RUNNING"
    # `state_reason: "None"` must not leak through; queue_reason only fires when PENDING.
    assert info.queue_reason is None


def test_squeue_empty_returns_none(fixtures_root):
    payload = _load_json(fixtures_root, "squeue_empty.json")
    assert parse_squeue_json(payload, "999") is None


def test_squeue_explicit_cluster_argument_overrides_payload():
    payload = {"jobs": [{"job_id": 1, "cluster": "anvil", "job_state": ["RUNNING"]}]}
    info = parse_squeue_json(payload, "1", cluster="explicit")
    assert info.cluster == "explicit"


def test_squeue_unknown_state_marks_visibility_unknown():
    payload = {"jobs": [{"job_id": 5, "job_state": ["WIBBLE"]}]}
    info = parse_squeue_json(payload, "5")
    assert info.lifecycle == "unknown"
    assert info.visibility == "unknown"


def test_squeue_handles_scalar_job_state_field():
    """Earlier Slurm builds emit job_state as a bare string."""
    payload = {"jobs": [{"job_id": 7, "job_state": "RUNNING"}]}
    info = parse_squeue_json(payload, "7")
    assert info.lifecycle == "running"
    assert info.raw_state == "RUNNING"


def test_squeue_json_str_wrapper_handles_invalid_json():
    assert parse_squeue_json_str("not json", "1") is None
    assert parse_squeue_json_str("", "1") is None


# --- sacct (JSON) ----------------------------------------------------------


def test_sacct_completed_basic(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_completed.json")
    info = parse_sacct_json(payload, "16680647")
    assert info is not None
    assert info.lifecycle == "succeeded"
    assert info.raw_state == "COMPLETED"
    assert info.exit_code == "0:0"
    assert info.elapsed == "00:00:05"
    # epoch -> ISO 8601 UTC
    assert info.start_time and info.start_time.endswith("Z")
    assert info.end_time and info.end_time.endswith("Z")
    assert info.source == "sacct"
    assert info.visibility == "visible"


def test_sacct_failed_carries_exit_code(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_failed.json")
    info = parse_sacct_json(payload, "16680664")
    assert info.lifecycle == "failed"
    assert info.raw_state == "FAILED"
    assert info.exit_code == "42:0"


def test_sacct_cancelled(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_cancelled.json")
    info = parse_sacct_json(payload, "16680665")
    assert info.lifecycle == "cancelled"
    assert info.raw_state == "CANCELLED"


def test_sacct_timeout(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_timeout.json")
    info = parse_sacct_json(payload, "16680666")
    assert info.lifecycle == "timeout"
    assert info.raw_state == "TIMEOUT"


def test_sacct_oom(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_oom.json")
    info = parse_sacct_json(payload, "16680748")
    assert info.lifecycle == "failed"
    assert info.raw_state == "OUT_OF_MEMORY"


def test_sacct_with_steps_picks_parent_row(fixtures_root):
    payload = _load_json(fixtures_root, "sacct_with_steps.json")
    info = parse_sacct_json(payload, "16680647")
    assert info is not None
    # parent has the user-given name; .batch step is the literal "batch"
    assert info.name == "slurmly-success"
    # ensure raw is the parent row (has top-level `steps` field)
    assert "steps" in (info.raw or {})


def test_sacct_falls_back_to_batch_step_when_parent_missing():
    payload = {
        "jobs": [
            {"job_id": "9999.batch", "name": "batch", "state": {"current": ["COMPLETED"]}},
        ]
    }
    info = parse_sacct_json(payload, "9999")
    assert info is not None
    assert info.name == "batch"
    assert info.lifecycle == "succeeded"


def test_sacct_unknown_state_marks_visibility_unknown():
    payload = {"jobs": [{"job_id": 1, "state": {"current": ["MYSTERY"]}}]}
    info = parse_sacct_json(payload, "1")
    assert info.lifecycle == "unknown"
    assert info.visibility == "unknown"


def test_sacct_missing_job_id_returns_none():
    info = parse_sacct_json({"jobs": []}, "1")
    assert info is None


def test_sacct_handles_missing_optional_fields_defensively():
    # exit_code without signal block; time without start/end
    payload = {
        "jobs": [
            {
                "job_id": 42,
                "name": "x",
                "state": {"current": ["COMPLETED"]},
                "exit_code": {"return_code": {"number": 0}},
                "time": {"elapsed": 0},
            }
        ]
    }
    info = parse_sacct_json(payload, "42")
    assert info.exit_code == "0:0"
    assert info.elapsed == "00:00:00"
    assert info.start_time is None
    assert info.end_time is None


def test_sacct_json_str_wrapper_handles_invalid_json():
    assert parse_sacct_json_str("", "1") is None
    assert parse_sacct_json_str("garbage", "1") is None


# --- sacct (--parsable2) ---------------------------------------------------


def test_sacct_parsable2_picks_parent_row(fixtures_root):
    body = _load_text(fixtures_root, "sacct_parsable2_completed.txt")
    info = parse_sacct_parsable2(body, "16680647")
    assert info is not None
    assert info.lifecycle == "succeeded"
    assert info.raw_state == "COMPLETED"
    assert info.exit_code == "0:0"
    assert info.elapsed == "00:00:05"
    assert info.name == "slurmly-success"
    assert info.start_time == "2026-04-26T12:31:12"
    assert info.end_time == "2026-04-26T12:31:17"
    assert info.source == "sacct"


def test_sacct_parsable2_filters_step_rows(fixtures_root):
    body = _load_text(fixtures_root, "sacct_parsable2_completed.txt")
    info = parse_sacct_parsable2(body, "16680647")
    # parent row's JobName is the user name; step rows are "batch"/"extern"
    assert info.name == "slurmly-success"


def test_sacct_parsable2_falls_back_to_batch_step():
    body = (
        "JobID|JobName|State|ExitCode|Elapsed|Start|End|Reason\n"
        "12345.batch|batch|COMPLETED|0:0|00:00:01|2026-04-26T12:00:00|2026-04-26T12:00:01|\n"
    )
    info = parse_sacct_parsable2(body, "12345")
    assert info is not None
    assert info.name == "batch"
    assert info.lifecycle == "succeeded"


def test_sacct_parsable2_empty_returns_none():
    assert parse_sacct_parsable2("", "1") is None
    assert parse_sacct_parsable2("JobID|State\n", "1") is None  # header only


def test_sacct_parsable2_extracts_pending_reason():
    body = (
        "JobID|JobName|State|ExitCode|Elapsed|Start|End|Reason\n"
        "1|x|PENDING|0:0|00:00:00|Unknown|Unknown|Resources\n"
    )
    info = parse_sacct_parsable2(body, "1")
    assert info.lifecycle == "queued"
    assert info.queue_reason == "Resources"


def test_sacct_parsable2_cancelled_with_user_suffix_normalizes():
    body = (
        "JobID|JobName|State|ExitCode|Elapsed|Start|End|Reason\n"
        "1|x|CANCELLED by 1234567|0:0|00:00:00|2026-04-26T12:00:00|2026-04-26T12:00:00|\n"
    )
    info = parse_sacct_parsable2(body, "1")
    assert info.lifecycle == "cancelled"


# --- exit_code edge cases (signals, OOM-by-signal) -----------------------


def test_sacct_signal_exit_code_preserved_verbatim():
    """ExitCode '0:9' (signal 9 / SIGKILL) must round-trip through the parser."""
    payload = {
        "jobs": [
            {
                "job_id": 7,
                "name": "x",
                "state": {"current": ["FAILED"]},
                "exit_code": {
                    "return_code": {"number": 0},
                    "signal": {"id": {"number": 9}},
                },
            }
        ]
    }
    info = parse_sacct_json(payload, "7")
    assert info.exit_code == "0:9"
    assert info.lifecycle == "failed"


def test_sacct_parsable2_signal_exit_code_preserved():
    body = (
        "JobID|JobName|State|ExitCode|Elapsed|Start|End|Reason\n"
        "5|x|FAILED|0:9|00:00:01|2026-04-26T12:00:00|2026-04-26T12:00:01|\n"
    )
    info = parse_sacct_parsable2(body, "5")
    assert info.exit_code == "0:9"
    assert info.lifecycle == "failed"
