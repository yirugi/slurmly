"""Pure builders for the Slurm shell commands slurmly issues.

These functions produce strings that will be passed to SSHTransport.run.
Building them as pure functions makes them straightforward to test in
isolation and to assert on in fake-transport tests.
"""

from __future__ import annotations

import shlex


def build_mkdir_p(path: str) -> str:
    return f"mkdir -p {shlex.quote(path)}"


def build_submit(*, remote_job_dir: str, script_filename: str = "run.sh") -> str:
    """`cd <job_dir> && sbatch --parsable <script>`.

    Phase 1 does not pass `--clusters` automatically; callers (Phase 2+) can
    add a per-cluster variant if a multi-cluster federation is configured.
    """
    return (
        f"cd {shlex.quote(remote_job_dir)} && "
        f"sbatch --parsable {shlex.quote(script_filename)}"
    )


def build_scancel(slurm_job_id: str, *, signal: str | None = None, cluster: str | None = None) -> str:
    parts = ["scancel"]
    if signal:
        parts.append(f"--signal={shlex.quote(signal)}")
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(shlex.quote(slurm_job_id))
    return " ".join(parts)


def build_squeue_json(slurm_job_id: str, *, cluster: str | None = None) -> str:
    """`squeue --json --jobs=<id> [--clusters=<name>]`."""
    parts = ["squeue", "--json"]
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(f"--jobs={shlex.quote(slurm_job_id)}")
    return " ".join(parts)


def build_squeue_json_many(
    slurm_job_ids: list[str], *, cluster: str | None = None
) -> str:
    """`squeue --json --jobs=a,b,c [--clusters=<name>]`.

    Slurm accepts a comma-separated id list. Caller is responsible for chunking
    so the joined argument doesn't blow past the cluster's command-length limit.
    """
    if not slurm_job_ids:
        raise ValueError("slurm_job_ids must not be empty")
    joined = ",".join(slurm_job_ids)
    parts = ["squeue", "--json"]
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(f"--jobs={shlex.quote(joined)}")
    return " ".join(parts)


def build_sacct_json(
    slurm_job_id: str,
    *,
    lookback_days: int,
    cluster: str | None = None,
) -> str:
    """`sacct --json --starttime=now-Ndays --jobs=<id> [--clusters=<name>]`.

    `--starttime` is mandatory: sacct's default lookback can drop jobs that
    started before midnight, which would silently return "no rows".
    """
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    parts = ["sacct", "--json", f"--starttime=now-{int(lookback_days)}days"]
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(f"--jobs={shlex.quote(slurm_job_id)}")
    return " ".join(parts)


def build_sacct_json_many(
    slurm_job_ids: list[str],
    *,
    lookback_days: int,
    cluster: str | None = None,
) -> str:
    """`sacct --json --starttime=now-Ndays --jobs=a,b,c [--clusters=<name>]`."""
    if not slurm_job_ids:
        raise ValueError("slurm_job_ids must not be empty")
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    joined = ",".join(slurm_job_ids)
    parts = ["sacct", "--json", f"--starttime=now-{int(lookback_days)}days"]
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(f"--jobs={shlex.quote(joined)}")
    return " ".join(parts)


_SACCT_PARSABLE2_FORMAT = (
    "JobID,JobName,State,ExitCode,Elapsed,Start,End,Reason,Partition,Account,User,QOS"
)


def build_sacct_parsable2(
    slurm_job_id: str,
    *,
    lookback_days: int,
    cluster: str | None = None,
) -> str:
    """`sacct --parsable2 --format=... --starttime=now-Ndays --jobs=<id>`.

    The `--parsable2` form is the fallback for clusters whose Slurm build
    doesn't emit JSON (`ClusterProfile.supports_sacct_json = False`).
    """
    if lookback_days < 1:
        raise ValueError("lookback_days must be >= 1")
    parts = [
        "sacct",
        "--parsable2",
        f"--format={_SACCT_PARSABLE2_FORMAT}",
        f"--starttime=now-{int(lookback_days)}days",
    ]
    if cluster:
        parts.append(f"--clusters={shlex.quote(cluster)}")
    parts.append(f"--jobs={shlex.quote(slurm_job_id)}")
    return " ".join(parts)


def build_find_files(root: str, *, pattern: str = "*") -> str:
    """`find <root> -type f -name <pattern> -print`.

    Used by the artifact helper. Caller validates `root` against
    `remote_base_dir/jobs/` before invoking.
    """
    return (
        f"find {shlex.quote(root)} -type f -name {shlex.quote(pattern)} -print"
    )


def build_tail(path: str, *, lines: int) -> str:
    """`test -f <path> && tail -n <N> <path>`.

    `test -f` short-circuits with a non-zero exit when the file is missing,
    which the client interprets as `exists=False` instead of an error.
    """
    return f"test -f {shlex.quote(path)} && tail -n {int(lines)} {shlex.quote(path)}"
