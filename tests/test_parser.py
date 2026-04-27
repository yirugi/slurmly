"""Tests for slurmly.slurm.parser using Phase 0 fixtures."""

from __future__ import annotations

import pytest

from slurmly.exceptions import SbatchParseError
from slurmly.slurm.parser import parse_sbatch_parsable


def test_parse_plain_jobid_from_fixture(fixtures_root):
    raw = (fixtures_root / "sbatch_success.txt").read_text()
    result = parse_sbatch_parsable(raw)
    assert result.slurm_job_id.isdigit()
    assert result.cluster is None


def test_parse_jobid_with_cluster_suffix(fixtures_root):
    raw = (fixtures_root / "sbatch_success_with_cluster.txt").read_text()
    result = parse_sbatch_parsable(raw)
    assert result.slurm_job_id.isdigit()
    assert result.cluster == "anvil"


def test_parse_tolerates_trailing_whitespace_and_blank_lines():
    result = parse_sbatch_parsable("\n\n  16680647   \n\n")
    assert result.slurm_job_id == "16680647"
    assert result.cluster is None


def test_parse_tolerates_warning_lines_before_jobid():
    raw = "Warning: weird thing\n16680647;anvil\n"
    result = parse_sbatch_parsable(raw)
    assert result.slurm_job_id == "16680647"
    assert result.cluster == "anvil"


@pytest.mark.parametrize("bad", ["", "   \n  \n", "not-a-number", ";anvil", "abc;anvil"])
def test_parse_rejects_unparsable(bad):
    with pytest.raises(SbatchParseError):
        parse_sbatch_parsable(bad)
