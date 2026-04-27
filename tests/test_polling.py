"""Phase 3 — polling helper (PollingPolicy + poll_job)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from slurmly import PollingPolicy, PollingResult, SlurmSSHClient
from slurmly.polling import (
    age_seconds,
    attention_threshold_reached,
    is_terminal,
    with_relative_seconds,
)
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport
from slurmly.models import JobInfo

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


def _client(transport: FakeTransport) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )


# --- PollingPolicy ---------------------------------------------------------


def test_policy_picks_fresh_interval_for_young_job():
    policy = PollingPolicy()
    assert policy.next_interval(age_seconds=10) == policy.fresh_submit_interval_seconds


def test_policy_picks_normal_interval_after_window():
    policy = PollingPolicy(fresh_submit_window_seconds=60)
    assert policy.next_interval(age_seconds=120) == policy.normal_interval_seconds


def test_policy_picks_long_running_interval_for_old_job():
    policy = PollingPolicy(long_running_after_seconds=600, long_running_interval_seconds=90)
    assert policy.next_interval(age_seconds=900) == 90


def test_attention_threshold_reached_after_window():
    policy = PollingPolicy(attention_after_seconds=60)
    submitted = datetime.now(timezone.utc) - timedelta(seconds=120)
    assert attention_threshold_reached(submitted, policy) is True


def test_attention_threshold_not_yet_reached():
    policy = PollingPolicy(attention_after_seconds=600)
    submitted = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert attention_threshold_reached(submitted, policy) is False


# --- is_terminal -----------------------------------------------------------


def test_is_terminal_true_for_succeeded():
    info = JobInfo(slurm_job_id="1", lifecycle="succeeded", source="sacct")
    assert is_terminal(info) is True


def test_is_terminal_false_for_running():
    info = JobInfo(slurm_job_id="1", lifecycle="running", source="squeue")
    assert is_terminal(info) is False


# --- poll_job --------------------------------------------------------------


async def test_poll_job_running_keeps_polling():
    transport = FakeTransport(
        canned_responses=[(0, (FIXTURES / "squeue_running.json").read_text(), "")]
    )
    client = _client(transport)
    result = await client.poll_job("16680653")
    assert isinstance(result, PollingResult)
    assert result.should_retry is True
    assert result.transient is False
    assert result.info is not None
    assert result.info.lifecycle == "running"
    assert result.next_interval_seconds is not None


async def test_poll_job_completed_stops_polling():
    transport = FakeTransport(
        canned_responses=[
            (0, (FIXTURES / "squeue_empty.json").read_text(), ""),
            (0, (FIXTURES / "sacct_completed.json").read_text(), ""),
        ]
    )
    client = _client(transport)
    result = await client.poll_job("16680647")
    assert result.should_retry is False
    assert result.info is not None
    assert result.info.lifecycle == "succeeded"
    assert result.next_interval_seconds is None


async def test_poll_job_transient_gap_marks_transient():
    transport = FakeTransport(
        canned_responses=[
            (0, '{"jobs": []}', ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = with_relative_seconds(10)
    result = await client.poll_job("9999", submitted_at=submitted_at)
    assert result.transient is True
    assert result.should_retry is True


async def test_poll_job_invalid_job_id_terminates_no_retry():
    """An array-id (rejected pre-flight) must not look transient to the poller."""
    transport = FakeTransport()
    client = _client(transport)
    result = await client.poll_job("12345_0")
    assert result.should_retry is False
    assert result.transient is False
    assert result.error_type == "InvalidJobSpec"
    assert transport.runs == []


async def test_poll_job_not_found_outside_grace_window_terminates():
    transport = FakeTransport(
        canned_responses=[
            (0, '{"jobs": []}', ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = with_relative_seconds(600)
    result = await client.poll_job("9999", submitted_at=submitted_at)
    assert result.should_retry is False
    assert result.error_type == "SlurmJobNotFound"
    assert result.info is None


# --- helpers --------------------------------------------------------------


def test_age_seconds_grows_with_offset():
    base = datetime.now(timezone.utc)
    older = base - timedelta(seconds=30)
    assert 29.5 < age_seconds(older, now=base) < 30.5


# --- grace window boundary -----------------------------------------------


async def test_get_job_just_inside_grace_window_marks_transient():
    """grace=120s. submitted_at = now - 119s should still be transient."""
    transport = FakeTransport(
        canned_responses=[
            (0, '{"jobs": []}', ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = datetime.now(timezone.utc) - timedelta(seconds=119)
    info = await client.get_job("9999", submitted_at=submitted_at)
    assert info.visibility == "transient_gap"


async def test_get_job_just_outside_grace_window_raises_not_found():
    """grace=120s. submitted_at = now - 121s must raise SlurmJobNotFound."""
    from slurmly.exceptions import SlurmJobNotFound

    transport = FakeTransport(
        canned_responses=[
            (0, '{"jobs": []}', ""),
            (0, '{"jobs": []}', ""),
        ]
    )
    client = _client(transport)
    submitted_at = datetime.now(timezone.utc) - timedelta(seconds=121)
    with pytest.raises(SlurmJobNotFound):
        await client.get_job("9999", submitted_at=submitted_at)


def test_policy_boundary_at_long_running_threshold():
    """At exactly long_running_after_seconds the long-running interval kicks in."""
    policy = PollingPolicy(
        long_running_after_seconds=600,
        long_running_interval_seconds=90,
        normal_interval_seconds=30,
        fresh_submit_window_seconds=60,
    )
    assert policy.next_interval(age_seconds=599) == 30
    assert policy.next_interval(age_seconds=600) == 90
