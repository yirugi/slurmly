"""Phase 3 — detect_capabilities() + reconcile warnings."""

from __future__ import annotations

import json
from pathlib import Path

from slurmly import CapabilityReport, SlurmSSHClient
from slurmly.capabilities import reconcile_with_profile
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport

FIXTURES = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


def _client(transport: FakeTransport, **profile_kwargs) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        **profile_kwargs,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )


async def test_detect_capabilities_all_supported():
    squeue_payload = (FIXTURES / "squeue_running.json").read_text()
    sacct_payload = (FIXTURES / "sacct_completed.json").read_text()
    transport = FakeTransport(
        canned_responses=[
            (0, "slurm 25.11.1\n", ""),  # sinfo --version
            (0, squeue_payload, ""),  # squeue --json
            (0, sacct_payload, ""),  # sacct --json
            (0, "JobID|State\n", ""),  # sacct --parsable2
            (0, "", ""),  # mkdir for sftp probe
            (0, "", ""),  # rm -f probe
        ]
    )
    client = _client(transport)
    report = await client.detect_capabilities()
    assert isinstance(report, CapabilityReport)
    assert report.supports_squeue_json is True
    assert report.supports_sacct_json is True
    assert report.supports_sacct_parsable2 is True
    assert report.supports_sftp is True
    assert report.slurm_version == "slurm 25.11.1"
    # No reconcile warnings — profile defaults match probe.
    assert all("does not support" not in w for w in report.warnings)


async def test_detect_capabilities_squeue_missing_emits_warning():
    transport = FakeTransport(
        canned_responses=[
            (0, "slurm 22.05\n", ""),
            (1, "", "squeue: error: --json not supported\n"),
            (0, '{"jobs": []}', ""),
            (0, "", ""),
            (0, "", ""),
            (0, "", ""),
        ]
    )
    # Profile claims squeue JSON support, but the probe finds none.
    client = _client(transport, supports_squeue_json=True)
    report = await client.detect_capabilities()
    assert report.supports_squeue_json is False
    assert any("supports_squeue_json=True" in w for w in report.warnings)


async def test_detect_capabilities_does_not_mutate_profile():
    transport = FakeTransport(
        canned_responses=[
            (0, "v\n", ""),
            (1, "", "no\n"),
            (1, "", "no\n"),
            (1, "", "no\n"),
            (0, "", ""),
            (0, "", ""),
        ]
    )
    client = _client(transport, supports_squeue_json=True, supports_sacct_json=True)
    before = client.cluster_profile.model_dump()
    await client.detect_capabilities()
    assert client.cluster_profile.model_dump() == before


def test_reconcile_with_profile_warns_on_mismatch():
    report = CapabilityReport(supports_squeue_json=False, supports_sacct_json=False)
    warnings = reconcile_with_profile(
        report, profile_supports_squeue_json=True, profile_supports_sacct_json=True
    )
    assert any("supports_squeue_json=True" in w for w in warnings)
    assert any("supports_sacct_json=True" in w for w in warnings)


def test_reconcile_with_profile_quiet_when_aligned():
    report = CapabilityReport(supports_squeue_json=True, supports_sacct_json=True)
    warnings = reconcile_with_profile(
        report, profile_supports_squeue_json=True, profile_supports_sacct_json=True
    )
    assert warnings == []
