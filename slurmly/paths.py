"""Remote path helpers and internal job id generator.

Path policy
-----------

Per-job layout (under the user-supplied `remote_base_dir`):

    <remote_base_dir>/
      jobs/
        <internal_job_id>/
          run.sh
          metadata.json
          stdout.log
          stderr.log

`internal_job_id` is the only opaque component slurmly inserts; it has the
shape `slurmly-<8 hex>` (matched by validators.is_valid_internal_job_id).
"""

from __future__ import annotations

import secrets

from .exceptions import InvalidConfig
from .validators import is_valid_internal_job_id, is_valid_username


SCRIPT_NAME = "run.sh"
METADATA_NAME = "metadata.json"
STDOUT_NAME = "stdout.log"
STDERR_NAME = "stderr.log"

# Array-job log filenames carry %A (parent job id) and %a (task id) so each
# task writes its own file. The returned path is a Slurm-side template, not a
# resolvable file — `tail_stdout/stderr` on the parent is undefined for arrays.
STDOUT_NAME_ARRAY = "stdout-%A_%a.log"
STDERR_NAME_ARRAY = "stderr-%A_%a.log"


def generate_internal_job_id() -> str:
    """Return a fresh `slurmly-<8 hex>` id."""
    return f"slurmly-{secrets.token_hex(4)}"


def render_remote_base_dir(template: str, *, username: str) -> str:
    """Substitute `{username}` (and only `{username}`) into a base-dir template.

    Other `{...}` tokens are passed through as literal text — `str.format` is
    deliberately avoided so user-controlled remote_base_dir values can't trigger
    arbitrary attribute lookups.
    """
    if not is_valid_username(username):
        raise InvalidConfig(
            f"username {username!r} contains characters not allowed in a remote path"
        )
    return template.replace("{username}", username)


def join_remote(*parts: str) -> str:
    """POSIX path join that does not let a later absolute path nuke earlier parts."""
    if not parts:
        return ""
    out = parts[0].rstrip("/")
    for p in parts[1:]:
        if not p:
            continue
        out = f"{out}/{p.lstrip('/')}"
    return out


def job_paths(
    *,
    remote_base_dir: str,
    internal_job_id: str,
    is_array: bool = False,
) -> dict[str, str]:
    """Return the canonical per-job paths.

    Keys: `job_dir`, `script`, `metadata`, `stdout`, `stderr`.

    When `is_array=True`, `stdout` and `stderr` use Slurm `%A_%a` filename
    patterns so per-task logs don't clobber each other.
    """
    if not is_valid_internal_job_id(internal_job_id):
        raise InvalidConfig(
            f"internal_job_id {internal_job_id!r} is not in the slurmly-<8hex> form"
        )
    if not remote_base_dir:
        raise InvalidConfig("remote_base_dir must be a non-empty path")

    job_dir = join_remote(remote_base_dir, "jobs", internal_job_id)
    stdout = STDOUT_NAME_ARRAY if is_array else STDOUT_NAME
    stderr = STDERR_NAME_ARRAY if is_array else STDERR_NAME
    return {
        "job_dir": job_dir,
        "script": join_remote(job_dir, SCRIPT_NAME),
        "metadata": join_remote(job_dir, METADATA_NAME),
        "stdout": join_remote(job_dir, stdout),
        "stderr": join_remote(job_dir, stderr),
    }
