"""Tests for slurmly.paths."""

from __future__ import annotations

import pytest

from slurmly.exceptions import InvalidConfig
from slurmly.paths import (
    generate_internal_job_id,
    job_paths,
    render_remote_base_dir,
)
from slurmly.validators import is_valid_internal_job_id


def test_generate_internal_job_id_format():
    iid = generate_internal_job_id()
    assert is_valid_internal_job_id(iid)


def test_generate_internal_job_id_unique():
    ids = {generate_internal_job_id() for _ in range(100)}
    assert len(ids) == 100


def test_render_remote_base_dir_substitutes_username():
    rendered = render_remote_base_dir("/anvil/scratch/{username}/slurmly", username="x-foo")
    assert rendered == "/anvil/scratch/x-foo/slurmly"


def test_render_remote_base_dir_passes_through_other_braces():
    # Non-{username} braces survive unchanged (not str.format).
    rendered = render_remote_base_dir("/data/{username}/{project}", username="u")
    assert rendered == "/data/u/{project}"


def test_render_remote_base_dir_rejects_bad_username():
    with pytest.raises(InvalidConfig):
        render_remote_base_dir("/x/{username}", username="../etc/passwd")


def test_job_paths_layout():
    iid = "slurmly-aabbccdd"
    paths = job_paths(remote_base_dir="/scratch/u/slurmly", internal_job_id=iid)
    assert paths["job_dir"] == f"/scratch/u/slurmly/jobs/{iid}"
    assert paths["script"] == f"/scratch/u/slurmly/jobs/{iid}/run.sh"
    assert paths["metadata"] == f"/scratch/u/slurmly/jobs/{iid}/metadata.json"
    assert paths["stdout"] == f"/scratch/u/slurmly/jobs/{iid}/stdout.log"
    assert paths["stderr"] == f"/scratch/u/slurmly/jobs/{iid}/stderr.log"


def test_job_paths_rejects_bad_internal_id():
    with pytest.raises(InvalidConfig):
        job_paths(remote_base_dir="/x", internal_job_id="not-our-id")
