"""Phase 2 — pure builders for squeue/sacct shell commands."""

from __future__ import annotations

import pytest

from slurmly.slurm.commands import (
    build_sacct_json,
    build_sacct_parsable2,
    build_squeue_json,
)


def test_build_squeue_json_minimal():
    cmd = build_squeue_json("16680647")
    assert cmd == "squeue --json --jobs=16680647"


def test_build_squeue_json_with_cluster_suffix():
    cmd = build_squeue_json("16680647", cluster="anvil")
    assert "--clusters=anvil" in cmd
    assert "--jobs=16680647" in cmd


def test_build_sacct_json_includes_starttime():
    cmd = build_sacct_json("123", lookback_days=7)
    assert "sacct --json" in cmd
    assert "--starttime=now-7days" in cmd
    assert "--jobs=123" in cmd


def test_build_sacct_json_uses_configured_lookback():
    """sacct's default lookback drops jobs that crossed midnight; honor profile."""
    cmd = build_sacct_json("9", lookback_days=30)
    assert "--starttime=now-30days" in cmd


def test_build_sacct_json_with_cluster_suffix():
    cmd = build_sacct_json("123", lookback_days=7, cluster="anvil")
    assert "--clusters=anvil" in cmd
    assert "--starttime=now-7days" in cmd


def test_build_sacct_json_rejects_zero_lookback():
    with pytest.raises(ValueError):
        build_sacct_json("123", lookback_days=0)


def test_build_sacct_parsable2_format_and_jobs():
    cmd = build_sacct_parsable2("123", lookback_days=14)
    assert "sacct --parsable2" in cmd
    assert "--format=" in cmd
    assert "JobID,JobName,State,ExitCode,Elapsed,Start,End,Reason" in cmd
    assert "--starttime=now-14days" in cmd
    assert "--jobs=123" in cmd


def test_build_sacct_parsable2_with_cluster():
    cmd = build_sacct_parsable2("1", lookback_days=7, cluster="anvil")
    assert "--clusters=anvil" in cmd
