"""Remote-directory cleanup helpers.

These functions emit shell commands and validate paths. The actual
execution lives in `SlurmSSHClient` (see `cleanup_remote_job_dir`,
`plan_cleanup`, `cleanup`).

Path safety
-----------
Every path passed to a cleanup-side `rm -rf` is validated against the
client's `remote_base_dir`. The check requires the candidate to start
with `<remote_base_dir>/jobs/` (with the trailing slash, so
`/scratch/u/slurmly-evil` does not match `/scratch/u/slurmly`). Empty,
absolute-root, and `..`-bearing paths are rejected.
"""

from __future__ import annotations

import shlex
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .exceptions import InvalidConfig


class CleanupCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    age_days: float | None = None
    reason: str | None = None


class CleanupPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    remote_base_dir: str
    older_than_days: float | None = None
    candidates: list[CleanupCandidate] = Field(default_factory=list)
    raw: dict[str, Any] = Field(default_factory=dict)


class CleanupResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    removed: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    dry_run: bool = False


def jobs_root(remote_base_dir: str) -> str:
    """Return `<remote_base_dir>/jobs` with no trailing slash."""
    return remote_base_dir.rstrip("/") + "/jobs"


def jobs_root_prefix(remote_base_dir: str) -> str:
    """Return the prefix every cleanup target must start with.

    The trailing slash is required: a target like `<base>/jobs-old` must
    NOT be considered "inside `<base>/jobs`".
    """
    return jobs_root(remote_base_dir) + "/"


def assert_safe_to_remove(path: str, *, remote_base_dir: str) -> None:
    """Raise InvalidConfig if `path` isn't safely under `<base>/jobs/`."""
    if not isinstance(path, str) or not path:
        raise InvalidConfig("cleanup path must be a non-empty string")
    if ".." in path.split("/"):
        raise InvalidConfig(f"cleanup path contains '..': {path!r}")
    if path in {"/", ""}:
        raise InvalidConfig(f"cleanup path is unsafe: {path!r}")
    # Tilde-based base dirs expand on the remote; skip prefix enforcement.
    base = remote_base_dir.rstrip("/")
    if base.startswith("~"):
        return
    prefix = jobs_root_prefix(remote_base_dir)
    if not path.startswith(prefix):
        raise InvalidConfig(
            f"cleanup path {path!r} is not under remote_base_dir/jobs/ "
            f"({prefix!r}); refusing to remove"
        )
    # Reject anything that's exactly the prefix (we never delete the jobs
    # root itself) or has a trailing slash that would broaden the match.
    tail = path[len(prefix):]
    if not tail or tail.startswith("/"):
        raise InvalidConfig(f"cleanup path {path!r} resolves to the jobs root")


def build_rm_rf(path: str) -> str:
    return f"rm -rf -- {shlex.quote(path)}"


def build_find_stale(remote_base_dir: str, *, older_than_days: float) -> str:
    """Emit a `find` command that lists job dirs older than N days.

    Output: one absolute path per line. Uses `-mindepth 1 -maxdepth 1`
    so we only list immediate children of `<base>/jobs/` (one level deep
    by design — slurmly's per-job directories live exactly there).

    Internally we convert ``older_than_days`` to minutes and use
    ``find -mmin +<N>`` so fractional days work — ``older_than_days=0.5``
    matches dirs older than 12 hours, and ``0.001`` is a useful smoke
    threshold (~86 seconds). ``find -mtime +N`` only honors integer days
    and silently rounds down, which surprises callers passing 0 or 0.5.
    """
    if older_than_days < 0:
        raise InvalidConfig("older_than_days must be >= 0")
    root = jobs_root(remote_base_dir)
    minutes = max(0, int(older_than_days * 24 * 60))
    return (
        f"find {shlex.quote(root)} -mindepth 1 -maxdepth 1 -type d "
        f"-mmin +{minutes} -print"
    )


__all__ = [
    "CleanupCandidate",
    "CleanupPlan",
    "CleanupResult",
    "jobs_root",
    "jobs_root_prefix",
    "assert_safe_to_remove",
    "build_rm_rf",
    "build_find_stale",
]
