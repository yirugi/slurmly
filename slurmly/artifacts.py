"""Artifact helpers — list / download files produced by a Slurm job.

Phase 4 surface:

* ``Artifact`` — typed result of `list_artifacts`.
* ``ArtifactDownload`` — typed result of `download_artifact`.
* ``assert_artifact_path_safe(...)`` — paranoid path check; reused by both helpers.

Path policy
-----------

Artifact reads are constrained to ``<remote_base_dir>/jobs/<internal_job_id>/``.
The base-dir prefix uses a trailing slash so a sibling like
``/scratch/u/slurmly-evil`` cannot match ``/scratch/u/slurmly``. Symlink
escapes are not handled here — sites that worry about that should disable
shell access on the login node entirely.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .exceptions import InvalidConfig


class Artifact(BaseModel):
    """One file produced by a job, as enumerated by `list_artifacts`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    name: str  # path basename


class ArtifactDownload(BaseModel):
    """A successful artifact read."""

    model_config = ConfigDict(extra="forbid")

    path: str
    content: str
    fetched_at: str
    bytes_requested: int | None = None
    truncated: bool = False
    raw: dict = Field(default_factory=dict)


def _ensure_under_jobs(path: str, *, remote_base_dir: str) -> str:
    """Return the canonical jobs-root prefix and validate that `path` lives under it."""
    if not isinstance(path, str) or not path:
        raise InvalidConfig("artifact path must be a non-empty string")
    if "\x00" in path:
        raise InvalidConfig("artifact path must not contain NUL bytes")
    if "/.." in path or path.endswith("/..") or path == "..":
        raise InvalidConfig(f"artifact path {path!r} contains a traversal segment")
    base = remote_base_dir.rstrip("/")
    jobs_root = f"{base}/jobs/"
    if path == jobs_root.rstrip("/"):
        raise InvalidConfig("refusing to operate on the jobs root itself")
    # Skip prefix check for tilde-based base dirs — ~ expands on the remote
    # and can't be resolved locally. Absolute base dirs get strict enforcement.
    if base.startswith("/") and not path.startswith(jobs_root):
        raise InvalidConfig(
            f"artifact path {path!r} is not under {jobs_root!r}"
        )
    return jobs_root


def assert_artifact_path_safe(path: str, *, remote_base_dir: str) -> None:
    """Raise InvalidConfig unless `path` is a safe per-job artifact path."""
    _ensure_under_jobs(path, remote_base_dir=remote_base_dir)


def resolve_artifact_path(
    *, remote_job_dir: str, name: str, remote_base_dir: str
) -> str:
    """Resolve a relative artifact name against ``remote_job_dir``.

    Rejects absolute names, traversal segments, and resolved paths that escape
    ``<remote_base_dir>/jobs/<job_dir>/``.
    """
    if not isinstance(name, str) or not name:
        raise InvalidConfig("artifact name must be a non-empty string")
    if name.startswith("/"):
        raise InvalidConfig(f"artifact name must be relative; got {name!r}")
    if ".." in name.split("/"):
        raise InvalidConfig(f"artifact name must not contain '..'; got {name!r}")
    if "\x00" in name:
        raise InvalidConfig("artifact name must not contain NUL bytes")
    job_dir = remote_job_dir.rstrip("/")
    candidate = f"{job_dir}/{name.lstrip('/')}"
    assert_artifact_path_safe(candidate, remote_base_dir=remote_base_dir)
    if not candidate.startswith(f"{job_dir}/"):
        raise InvalidConfig(
            f"artifact name {name!r} escaped job dir {remote_job_dir!r}"
        )
    return candidate


__all__ = [
    "Artifact",
    "ArtifactDownload",
    "assert_artifact_path_safe",
    "resolve_artifact_path",
]
