"""Storage-agnostic polling helper.

Core slurmly does not provide a scheduler. Applications run their own
poller (cron, systemd timer, k8s job, asyncio task) and use these helpers
to decide *how often* to poll a given job and *how to interpret* a single
poll result.

Locking is the application's responsibility. The recommended pattern is
`SELECT ... FOR UPDATE SKIP LOCKED` (see `dev/04_phase3_operational_hardening.md`
§9 for an example).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .models import JobInfo


class PollingPolicy(BaseModel):
    """Per-job poll-interval policy.

    `next_interval()` picks an interval based on how long the job has been
    around. Tunables are exposed so callers can tighten the loop for
    interactive UIs or relax it for nightly batches.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    fresh_submit_interval_seconds: int = Field(default=10, ge=1)
    fresh_submit_window_seconds: int = Field(default=120, ge=1)
    normal_interval_seconds: int = Field(default=30, ge=1)
    long_running_after_seconds: int = Field(default=1800, ge=1)
    long_running_interval_seconds: int = Field(default=60, ge=1)
    grace_window_seconds: int = Field(default=120, ge=1)
    attention_after_seconds: int = Field(default=300, ge=1)

    def next_interval(self, *, age_seconds: float) -> int:
        """Return the recommended seconds-to-wait before the next poll."""
        if age_seconds < self.fresh_submit_window_seconds:
            return self.fresh_submit_interval_seconds
        if age_seconds >= self.long_running_after_seconds:
            return self.long_running_interval_seconds
        return self.normal_interval_seconds


class PollingResult(BaseModel):
    """Outcome of a single poll attempt.

    `should_retry=False` indicates the job has reached a terminal lifecycle
    or hit a non-recoverable error; the caller should stop polling. `transient`
    is True when the gap is expected (within grace window) and the caller
    should retry without raising.
    """

    model_config = ConfigDict(extra="forbid")

    slurm_job_id: str
    info: JobInfo | None = None
    error: str | None = None
    error_type: str | None = None
    should_retry: bool = True
    transient: bool = False
    next_interval_seconds: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


_TERMINAL_LIFECYCLES = {"succeeded", "failed", "cancelled", "timeout"}


def is_terminal(info: JobInfo) -> bool:
    return info.lifecycle in _TERMINAL_LIFECYCLES


def age_seconds(submitted_at: datetime, *, now: datetime | None = None) -> float:
    n = now or datetime.now(timezone.utc)
    return (n - submitted_at).total_seconds()


def attention_threshold_reached(
    submitted_at: datetime,
    policy: PollingPolicy,
    *,
    now: datetime | None = None,
) -> bool:
    """Whether the job has been idle long enough to need human attention.

    Useful in the application's poller for switching from "this is fine"
    to "alert someone" without scheduling decisions leaking into core.
    """
    return age_seconds(submitted_at, now=now) > policy.attention_after_seconds


def with_relative_seconds(seconds: float, *, base: datetime | None = None) -> datetime:
    """Tiny helper for tests: produce a UTC timestamp `seconds` ago."""
    base = base or datetime.now(timezone.utc)
    return base - timedelta(seconds=seconds)


__all__ = [
    "PollingPolicy",
    "PollingResult",
    "is_terminal",
    "age_seconds",
    "attention_threshold_reached",
    "with_relative_seconds",
]
