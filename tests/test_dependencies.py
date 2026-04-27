"""Phase 4 — typed JobDependency model and renderer integration."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slurmly import JobDependency, JobSpec
from slurmly.paths import job_paths
from slurmly.profiles import ClusterProfile
from slurmly.scripts.renderer import render_script


def _render(spec: JobSpec) -> str:
    profile = ClusterProfile(
        name="t",
        default_partition="shared",
        allowed_partitions=["shared"],
    )
    paths = job_paths(remote_base_dir="/scratch/u", internal_job_id="slurmly-aaaaaaaa")
    return render_script(
        spec=spec,
        profile=profile,
        execution_profile=None,
        stdout_path=paths["stdout"],
        stderr_path=paths["stderr"],
        remote_job_dir=paths["job_dir"],
    )


def test_typed_dependency_renders_to_canonical_string():
    dep = JobDependency(type="afterok", job_ids=["123", "456"])
    assert dep.render() == "afterok:123:456"


def test_typed_dependency_singleton_renders_bare():
    assert JobDependency(type="singleton").render() == "singleton"


def test_typed_dependency_singleton_rejects_ids():
    with pytest.raises(ValidationError):
        JobDependency(type="singleton", job_ids=["1"])


def test_typed_dependency_non_singleton_requires_ids():
    with pytest.raises(ValidationError):
        JobDependency(type="afterok", job_ids=[])


def test_typed_dependency_rejects_non_numeric_ids():
    with pytest.raises(ValidationError):
        JobDependency(type="afterok", job_ids=["abc"])


def test_renderer_emits_typed_dependency():
    spec = JobSpec(
        name="t",
        command=["echo", "hi"],
        partition="shared",
        account="a1",
        dependency=JobDependency(type="afterok", job_ids=["111", "222"]),
    )
    script = _render(spec)
    assert "#SBATCH --dependency=afterok:111:222" in script


def test_renderer_passes_string_dependency_through_for_or_chains():
    """Slurm's `?` OR-chain isn't expressible in the typed form; raw strings work."""
    spec = JobSpec(
        name="t",
        command=["echo", "hi"],
        partition="shared",
        account="a1",
        dependency="afterok:111?afternotok:222",
    )
    script = _render(spec)
    assert "#SBATCH --dependency=afterok:111?afternotok:222" in script


def test_renderer_preserves_raw_string_and_comma_lists():
    """A comma-separated raw dependency list must be passed through verbatim."""
    spec = JobSpec(
        name="t",
        command=["echo", "hi"],
        partition="shared",
        account="a1",
        dependency="afterok:111,afterany:222",
    )
    script = _render(spec)
    assert "#SBATCH --dependency=afterok:111,afterany:222" in script


def test_typed_dependency_singleton_does_not_render_ids_field():
    spec = JobSpec(
        name="t",
        command=["echo", "hi"],
        partition="shared",
        account="a1",
        dependency=JobDependency(type="singleton"),
    )
    script = _render(spec)
    assert "#SBATCH --dependency=singleton" in script
    assert ":" not in script.split("--dependency=", 1)[1].split("\n", 1)[0]
