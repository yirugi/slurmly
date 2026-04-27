"""Lifecycle map regression tests."""

from __future__ import annotations

import pytest

from slurmly.states import normalize_state


@pytest.mark.parametrize(
    "raw,lifecycle",
    [
        # spec §5 verbatim
        ("PENDING", "queued"),
        ("CONFIGURING", "queued"),
        ("SUSPENDED", "queued"),
        ("RESIZING", "queued"),
        ("RUNNING", "running"),
        ("COMPLETING", "running"),
        ("STOPPED", "running"),
        ("COMPLETED", "succeeded"),
        ("FAILED", "failed"),
        ("BOOT_FAIL", "failed"),
        ("DEADLINE", "failed"),
        ("NODE_FAIL", "failed"),
        ("OUT_OF_MEMORY", "failed"),
        ("PREEMPTED", "failed"),
        ("REVOKED", "failed"),
        ("SPECIAL_EXIT", "failed"),
        ("CANCELLED", "cancelled"),
        ("TIMEOUT", "timeout"),
    ],
)
def test_spec_lifecycle_rows(raw, lifecycle):
    assert normalize_state(raw) == lifecycle


def test_cancelled_with_user_suffix_normalizes():
    assert normalize_state("CANCELLED by 1234567") == "cancelled"


def test_unknown_state_returns_unknown():
    assert normalize_state("WIBBLE") == "unknown"
    assert normalize_state(None) == "unknown"
    assert normalize_state("") == "unknown"
