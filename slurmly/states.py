"""Slurm raw state -> normalized lifecycle mapping.

The mapping is the authoritative source for `JobInfo.lifecycle` and is
referenced from dev/03_phase2_status_support.md §5. Extras (e.g. REQUEUED,
STAGE_OUT) are reasonable Slurm-vocabulary additions; the spec rows are kept
verbatim.
"""

from __future__ import annotations

from typing import Literal

Lifecycle = Literal[
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "timeout",
    "unknown",
]


_RAW_TO_LIFECYCLE: dict[str, Lifecycle] = {
    # spec §5 — queued
    "PENDING": "queued",
    "CONFIGURING": "queued",
    "SUSPENDED": "queued",
    "RESIZING": "queued",
    # extras (queued-shaped)
    "REQUEUED": "queued",
    "RESV_DEL_HOLD": "queued",
    "REQUEUE_FED": "queued",
    "REQUEUE_HOLD": "queued",
    # spec §5 — running
    "RUNNING": "running",
    "COMPLETING": "running",
    "STOPPED": "running",
    # extras (running-shaped)
    "STAGE_OUT": "running",
    "SIGNALING": "running",
    # spec §5 — succeeded
    "COMPLETED": "succeeded",
    # spec §5 — failed
    "FAILED": "failed",
    "BOOT_FAIL": "failed",
    "DEADLINE": "failed",
    "NODE_FAIL": "failed",
    "OUT_OF_MEMORY": "failed",
    "PREEMPTED": "failed",
    "REVOKED": "failed",
    "SPECIAL_EXIT": "failed",
    # spec §5 — cancelled
    "CANCELLED": "cancelled",
    # spec §5 — timeout
    "TIMEOUT": "timeout",
}


def normalize_state(raw: str | None) -> Lifecycle:
    """Map a Slurm raw state string to a normalized lifecycle value.

    `CANCELLED by <uid>` (sacct's verbose form) is reduced to `CANCELLED`.
    Unknown / empty inputs return `"unknown"`.
    """
    if not raw:
        return "unknown"
    head = raw.strip().split()[0].upper()
    return _RAW_TO_LIFECYCLE.get(head, "unknown")
