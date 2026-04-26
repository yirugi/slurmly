"""Capability detection + CapabilityReport.

`detect_capabilities()` is **diagnostic only**. It probes the cluster for
features the rest of slurmly assumes (squeue --json, sacct --json,
sacct --parsable2, sftp upload), but it never mutates the active
`ClusterProfile`. Mismatches between observed capabilities and the
profile show up as warnings on the report.

This split is intentional: a flaky probe must not silently alter client
behavior, and tests in CI must not depend on whatever the live cluster
happens to report today.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CapabilityReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    supports_squeue_json: bool = False
    supports_sacct_json: bool = False
    supports_sacct_parsable2: bool = False
    supports_sftp: bool = False

    slurm_version: str | None = None
    warnings: list[str] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)


def reconcile_with_profile(
    report: CapabilityReport,
    *,
    profile_supports_squeue_json: bool,
    profile_supports_sacct_json: bool,
) -> list[str]:
    """Compare a report against ClusterProfile flags; return warnings.

    Pure helper so the warning text is testable without a live cluster.
    """
    warnings: list[str] = []
    if profile_supports_squeue_json and not report.supports_squeue_json:
        warnings.append(
            "ClusterProfile.supports_squeue_json=True but probe failed; "
            "consider setting it to False in user config"
        )
    if not profile_supports_squeue_json and report.supports_squeue_json:
        warnings.append(
            "ClusterProfile.supports_squeue_json=False but probe succeeded; "
            "you may be paying an unnecessary sacct round-trip"
        )
    if profile_supports_sacct_json and not report.supports_sacct_json:
        warnings.append(
            "ClusterProfile.supports_sacct_json=True but probe failed; "
            "fallback path (--parsable2) will be used at runtime"
        )
    return warnings


__all__ = ["CapabilityReport", "reconcile_with_profile"]
