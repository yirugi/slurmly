"""Parsers for Slurm CLI output.

Phase 1: `sbatch --parsable` parser.
Phase 2: `squeue --json`, `sacct --json`, and `sacct --parsable2` parsers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..exceptions import SbatchParseError
from ..models import JobInfo
from ..states import normalize_state

_PARSABLE_LINE_RE = re.compile(r"^\d+(?:;[A-Za-z0-9_.\-]+)?$")


@dataclass(frozen=True)
class ParsedSbatch:
    """Output shape of `sbatch --parsable [--clusters=...]`.

    Without `--clusters`: stdout is `<jobid>\\n`.
    With `--clusters`: stdout is `<jobid>;<clustername>\\n`.

    sbatch may emit warning lines on stderr while still succeeding; the parser
    only consumes stdout.
    """

    slurm_job_id: str
    cluster: str | None


def parse_sbatch_parsable(stdout: str) -> ParsedSbatch:
    """Parse the single output line from `sbatch --parsable`.

    Tolerates: trailing whitespace/newlines, leading blank lines.
    Raises SbatchParseError if no usable token is found.
    """
    if stdout is None:
        raise SbatchParseError(raw_stdout="")

    # Scan every non-empty line; pick the first one that matches the
    # sbatch --parsable shape. Tolerates login-banner / warning / wrapper
    # output appearing on either side of the real result.
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line or not _PARSABLE_LINE_RE.match(line):
            continue
        if ";" in line:
            head, _, tail = line.partition(";")
            return ParsedSbatch(slurm_job_id=head, cluster=tail or None)
        return ParsedSbatch(slurm_job_id=line, cluster=None)

    raise SbatchParseError(raw_stdout=stdout)


# --- shared field helpers --------------------------------------------------


def _first_or_none(value: Any) -> Any:
    """Slurm 25 emits some scalar-shaped fields as single-element lists.

    `job_state: ["PENDING"]`, `state.current: ["COMPLETED"]`, etc. Use this
    helper everywhere we read those fields so the parser is robust to either
    shape.
    """
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _clean_str(value: Any) -> str | None:
    """Treat empty / whitespace / Slurm's literal "None" sentinel as missing."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    s = value.strip()
    if not s or s == "None":
        return None
    return s


def _epoch_to_iso(value: Any) -> str | None:
    """Slurm JSON reports times as integer epoch seconds; 0 means unset."""
    if value is None:
        return None
    if isinstance(value, dict):
        # Slurm 25 occasionally wraps numbers as {"set": bool, "number": int, ...}
        if not value.get("set", True):
            return None
        value = value.get("number")
    if value is None:
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _seconds_to_hms(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        if not value.get("set", True):
            return None
        value = value.get("number")
    try:
        s = int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    if s is None or s < 0:
        return None
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _read_set_number(value: Any) -> int | None:
    """Read Slurm 25's `{set: bool, infinite: bool, number: int}` numeric wrappers.

    Returns the integer when the field is meaningfully set, else None.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if not value.get("set"):
            return None
        n = value.get("number")
        if n is None:
            return None
        try:
            return int(n)
        except (TypeError, ValueError):
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_exit_code(ec: Any) -> str | None:
    """sacct JSON exit_code -> "<rc>:<sig>" matching the parsable2 form."""
    if not isinstance(ec, dict):
        return None
    rc = ec.get("return_code")
    if isinstance(rc, dict):
        rc_n = rc.get("number")
    else:
        rc_n = rc
    sig = ec.get("signal", {})
    sig_n: Any = 0
    if isinstance(sig, dict):
        sig_id = sig.get("id")
        if isinstance(sig_id, dict):
            sig_n = sig_id.get("number", 0)
        elif sig_id is not None:
            sig_n = sig_id
    if rc_n is None and sig_n in (None, 0):
        return None
    return f"{int(rc_n or 0)}:{int(sig_n or 0)}"


# --- squeue ----------------------------------------------------------------


def _squeue_row_id(row: dict) -> str:
    """Return the canonical id for a squeue row.

    Plain jobs: `<job_id>`.
    Array tasks: `<array_job_id>_<array_task_id>` (when both are meaningfully set).
    """
    arr_parent = _read_set_number(row.get("array_job_id"))
    arr_task = _read_set_number(row.get("array_task_id"))
    if arr_parent and arr_task is not None:
        return f"{arr_parent}_{arr_task}"
    return str(row.get("job_id"))


def _select_job(jobs: list[dict], slurm_job_id: str) -> dict | None:
    """Pick the row matching `slurm_job_id` (plain or `<parent>_<task>`).

    For plain ids we accept either: a row whose top-level `job_id` matches,
    or — when the user asked for the parent only — any task row that points
    back to it via `array_job_id`.
    """
    target = str(slurm_job_id)
    for j in jobs:
        if _squeue_row_id(j) == target:
            return j
        if str(j.get("job_id")) == target:
            return j
    if "_" not in target:
        # Parent-only query against an array: squeue lists tasks individually,
        # so we return the first matching task as a representative. Callers
        # who care about per-task state should query `<parent>_<task>` ids
        # directly. Polling a parent id is consequently best-effort: a
        # terminal task can mask still-running siblings.
        for j in jobs:
            arr_parent = _read_set_number(j.get("array_job_id"))
            if arr_parent and str(arr_parent) == target:
                return j
    return None


def parse_squeue_json(
    payload: dict,
    slurm_job_id: str,
    *,
    cluster: str | None = None,
) -> JobInfo | None:
    """Build a JobInfo from `squeue --json` output, or None if absent.

    Returns None when the job isn't in the squeue snapshot — that's the
    normal handoff signal to fall back to sacct.
    """
    jobs = (payload or {}).get("jobs") or []
    if not jobs:
        return None

    selected = _select_job(jobs, slurm_job_id)
    if selected is None:
        return None

    raw_state = _clean_str(_first_or_none(selected.get("job_state")))
    lifecycle = normalize_state(raw_state)

    queue_reason = None
    if raw_state and raw_state.upper() == "PENDING":
        queue_reason = _clean_str(selected.get("state_reason"))

    arr_parent = _read_set_number(selected.get("array_job_id"))
    arr_task = _read_set_number(selected.get("array_task_id"))

    return JobInfo(
        slurm_job_id=str(slurm_job_id),
        cluster=cluster or _clean_str(selected.get("cluster")),
        lifecycle=lifecycle,
        raw_state=raw_state,
        name=_clean_str(selected.get("name")),
        user=_clean_str(selected.get("user_name")),
        partition=_clean_str(selected.get("partition")),
        account=_clean_str(selected.get("account")),
        qos=_clean_str(selected.get("qos")),
        queue_reason=queue_reason,
        array_job_id=str(arr_parent) if arr_parent else None,
        array_task_id=str(arr_task) if arr_task is not None and arr_parent else None,
        source="squeue",
        visibility="visible" if lifecycle != "unknown" else "unknown",
        raw=selected,
    )


# --- sacct (JSON) ----------------------------------------------------------


def _sacct_row_id(row: dict) -> str:
    """Return the canonical id for a sacct JSON row, expanding array tasks."""
    rid = str(row.get("job_id"))
    array_block = row.get("array")
    if isinstance(array_block, dict):
        parent = _read_set_number(array_block.get("job_id"))
        task = _read_set_number(array_block.get("task_id"))
        if parent and task is not None:
            return f"{parent}_{task}"
    return rid


def _find_parent_or_batch(
    jobs: list[dict],
    slurm_job_id: str,
) -> dict | None:
    """sacct returns parent + step rows; prefer the parent.

    Real Slurm fixtures keep step rows nested under `jobs[*].steps[*]`, so
    `jobs` itself usually contains only parent rows. But some Slurm versions
    flatten the list into top-level rows like `123.batch`. Handle both. For
    array jobs, the canonical id is built from the row's `array` block.
    """
    target = str(slurm_job_id)
    parent = None
    batch_step = None
    for row in jobs:
        rid = str(row.get("job_id"))
        canonical = _sacct_row_id(row)
        if rid == target or canonical == target:
            parent = row
        elif rid == f"{target}.batch":
            batch_step = row
    if parent is None and "_" not in target:
        # Fallback: a parent-only query may match the first task row.
        for row in jobs:
            block = row.get("array")
            if isinstance(block, dict):
                p = _read_set_number(block.get("job_id"))
                if p and str(p) == target:
                    parent = row
                    break
    return parent or batch_step


def parse_sacct_json(
    payload: dict,
    slurm_job_id: str,
    *,
    cluster: str | None = None,
) -> JobInfo | None:
    """Build a JobInfo from `sacct --json` output, or None if absent."""
    jobs = (payload or {}).get("jobs") or []
    if not jobs:
        return None

    selected = _find_parent_or_batch(jobs, slurm_job_id)
    if selected is None:
        return None

    state = selected.get("state") or {}
    if isinstance(state, dict):
        raw_state = _clean_str(_first_or_none(state.get("current")))
    else:
        raw_state = _clean_str(state)
    lifecycle = normalize_state(raw_state)

    time_block = selected.get("time") or {}

    arr_parent = None
    arr_task = None
    array_block = selected.get("array")
    if isinstance(array_block, dict):
        p = _read_set_number(array_block.get("job_id"))
        t = _read_set_number(array_block.get("task_id"))
        if p:
            arr_parent = p
            arr_task = t

    return JobInfo(
        slurm_job_id=str(slurm_job_id),
        cluster=cluster or _clean_str(selected.get("cluster")),
        lifecycle=lifecycle,
        raw_state=raw_state,
        name=_clean_str(selected.get("name")),
        user=_clean_str(selected.get("user")),
        partition=_clean_str(selected.get("partition")),
        account=_clean_str(selected.get("account")),
        qos=_clean_str(selected.get("qos")),
        exit_code=_format_exit_code(selected.get("exit_code")),
        elapsed=_seconds_to_hms(time_block.get("elapsed")),
        start_time=_epoch_to_iso(time_block.get("start")),
        end_time=_epoch_to_iso(time_block.get("end")),
        array_job_id=str(arr_parent) if arr_parent else None,
        array_task_id=str(arr_task) if arr_task is not None and arr_parent else None,
        source="sacct",
        visibility="visible" if lifecycle != "unknown" else "unknown",
        raw=selected,
    )


# --- sacct (--parsable2 fallback) ------------------------------------------


def parse_sacct_parsable2(
    stdout: str,
    slurm_job_id: str,
    *,
    cluster: str | None = None,
) -> JobInfo | None:
    """Parse sacct's pipe-delimited parsable2 output.

    Format expected: header row + one or more data rows. The parent row has
    `JobID` exactly equal to the requested job id; step rows append `.batch`,
    `.extern`, `.0`, etc. Parent wins; `.batch` is the fallback.
    """
    if not stdout or not stdout.strip():
        return None

    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    header = lines[0].split("|")
    rows: list[dict[str, str]] = []
    for ln in lines[1:]:
        cells = ln.split("|")
        if len(cells) < len(header):
            cells = cells + [""] * (len(header) - len(cells))
        rows.append({k: v for k, v in zip(header, cells)})

    target = str(slurm_job_id)
    parent = next((r for r in rows if r.get("JobID") == target), None)
    fallback = next((r for r in rows if r.get("JobID") == f"{target}.batch"), None)
    selected = parent or fallback
    if selected is None:
        return None

    raw_state = _clean_str(selected.get("State"))
    lifecycle = normalize_state(raw_state)

    queue_reason = None
    if raw_state and raw_state.upper() == "PENDING":
        queue_reason = _clean_str(selected.get("Reason"))

    return JobInfo(
        slurm_job_id=target,
        cluster=cluster,
        lifecycle=lifecycle,
        raw_state=raw_state,
        name=_clean_str(selected.get("JobName")),
        user=_clean_str(selected.get("User")),
        partition=_clean_str(selected.get("Partition")),
        account=_clean_str(selected.get("Account")),
        qos=_clean_str(selected.get("QOS")),
        queue_reason=queue_reason,
        exit_code=_clean_str(selected.get("ExitCode")),
        elapsed=_clean_str(selected.get("Elapsed")),
        start_time=_clean_str(selected.get("Start")),
        end_time=_clean_str(selected.get("End")),
        source="sacct",
        visibility="visible" if lifecycle != "unknown" else "unknown",
        raw=dict(selected),
    )


def parse_squeue_json_all(
    payload: dict,
    *,
    cluster: str | None = None,
) -> dict[str, JobInfo]:
    """Parse every job row in a squeue JSON payload, keyed by canonical id.

    For array tasks the key is `<parent>_<task>`; for plain jobs it is the
    Slurm job id. Used by the batch `get_jobs` path.
    """
    out: dict[str, JobInfo] = {}
    jobs = (payload or {}).get("jobs") or []
    for row in jobs:
        rid = _squeue_row_id(row)
        info = parse_squeue_json({"jobs": [row]}, rid, cluster=cluster)
        if info is not None:
            out[rid] = info
    return out


def parse_sacct_json_all(
    payload: dict,
    *,
    cluster: str | None = None,
) -> dict[str, JobInfo]:
    """Parse every job row in a sacct JSON payload, keyed by canonical id."""
    out: dict[str, JobInfo] = {}
    jobs = (payload or {}).get("jobs") or []
    for row in jobs:
        rid = _sacct_row_id(row)
        if rid.endswith(".batch") or rid.endswith(".extern"):
            continue
        info = parse_sacct_json({"jobs": [row]}, rid, cluster=cluster)
        if info is not None:
            out[rid] = info
    return out


def parse_squeue_json_str(stdout: str, slurm_job_id: str, *, cluster: str | None = None) -> JobInfo | None:
    """Convenience wrapper: parses raw stdout into JSON and dispatches."""
    if not stdout or not stdout.strip():
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parse_squeue_json(payload, slurm_job_id, cluster=cluster)


def parse_sacct_json_str(stdout: str, slurm_job_id: str, *, cluster: str | None = None) -> JobInfo | None:
    """Convenience wrapper: parses raw stdout into JSON and dispatches."""
    if not stdout or not stdout.strip():
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parse_sacct_json(payload, slurm_job_id, cluster=cluster)
