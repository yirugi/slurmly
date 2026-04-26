"""Core typed domain models for slurmly.

JobSpec       - what the user submits.
SubmittedJob  - what `submit()` returns.
RenderedJob   - what `render_only()` returns.
LogChunk      - what `tail_stdout/stderr` returns.
JobInfo       - what `get_job()` returns (Phase 2).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .states import Lifecycle
from .validators import (
    is_valid_array_spec,
    is_valid_env_key,
    is_valid_internal_job_id,
    is_valid_job_name,
    is_valid_memory,
    is_valid_plain_job_id,
    is_valid_time_limit,
)

DependencyType = Literal[
    "after",
    "afterany",
    "afterok",
    "afternotok",
    "aftercorr",
    "singleton",
]

JobInfoSource = Literal["squeue", "sacct", "none"]
JobVisibility = Literal["visible", "transient_gap", "not_found", "unknown"]


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _Open(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- JobDependency (Phase 4) ----------------------------------------------


class JobDependency(_Frozen):
    """Typed Slurm job dependency.

    Renders to Slurm's ``--dependency=<type>:<id>[:<id>...]`` AND-list form.
    For OR-chains (``?`` separators), the ``singleton`` type, or other rare
    forms not expressible here, pass a raw string via ``JobSpec.dependency``.
    """

    type: DependencyType
    job_ids: list[str] = Field(default_factory=list)

    @field_validator("job_ids")
    @classmethod
    def _v_ids(cls, v: list[str]) -> list[str]:
        for jid in v:
            if not isinstance(jid, str) or not is_valid_plain_job_id(jid):
                raise ValueError(
                    f"job_ids entries must be plain numeric job ids; got {jid!r}"
                )
        return v

    @model_validator(mode="after")
    def _v_singleton_or_ids(self) -> JobDependency:
        if self.type == "singleton":
            if self.job_ids:
                raise ValueError("dependency type 'singleton' takes no job_ids")
        else:
            if not self.job_ids:
                raise ValueError(
                    f"dependency type {self.type!r} requires at least one job id"
                )
        return self

    def render(self) -> str:
        if self.type == "singleton":
            return "singleton"
        return f"{self.type}:" + ":".join(self.job_ids)


# --- JobSpec ---------------------------------------------------------------


class JobSpec(_Open):
    """Specification for a Slurm job submission."""

    name: str
    command: list[str] | None = None
    command_template: str | None = None

    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    nodes: int | None = Field(default=None, ge=1)
    ntasks: int | None = Field(default=None, ge=1)
    cpus_per_task: int | None = Field(default=None, ge=1)
    memory: str | None = None

    gpus: int | None = Field(default=None, ge=0)
    gpu_type: str | None = None

    time_limit: str | None = None
    working_dir: str | None = None
    env: dict[str, str] = Field(default_factory=dict)

    array: str | None = None
    dependency: str | JobDependency | None = None

    execution_profile: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)

    # --- field validators ---

    @field_validator("name")
    @classmethod
    def _v_name(cls, v: str) -> str:
        if not is_valid_job_name(v):
            raise ValueError(
                "JobSpec.name must match [A-Za-z0-9._-]{1,128}"
            )
        return v

    @field_validator("memory")
    @classmethod
    def _v_memory(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not is_valid_memory(v):
            raise ValueError(
                "memory must be like '512', '512M', '32G', '1T'"
            )
        return v

    @field_validator("time_limit")
    @classmethod
    def _v_time(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not is_valid_time_limit(v):
            raise ValueError(
                "time_limit must be a Slurm --time form, "
                "e.g. '30', '30:00', '01:30:00', '1-12:00:00'"
            )
        return v

    @field_validator("env")
    @classmethod
    def _v_env(cls, v: dict[str, str]) -> dict[str, str]:
        for key in v:
            if not is_valid_env_key(key):
                raise ValueError(
                    f"env key {key!r} is not a valid POSIX env name"
                )
        return v

    @field_validator("command")
    @classmethod
    def _v_command(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        if len(v) == 0:
            raise ValueError("command must not be empty")
        for i, part in enumerate(v):
            if not isinstance(part, str) or part == "":
                raise ValueError(
                    f"command[{i}] must be a non-empty string"
                )
        return v

    @field_validator("array")
    @classmethod
    def _v_array(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not is_valid_array_spec(v):
            raise ValueError(
                "array must be a Slurm --array spec, e.g. '0-9', '0-9%2', "
                "'1,3,5', '0-99:2'"
            )
        return v

    @field_validator("dependency")
    @classmethod
    def _v_dependency(cls, v: str | JobDependency | None) -> str | JobDependency | None:
        if v is None or isinstance(v, JobDependency):
            return v
        s = v.strip()
        if not s:
            raise ValueError("dependency string must not be empty")
        return s

    # --- model-level validation ---

    @model_validator(mode="after")
    def _v_command_xor_template(self) -> JobSpec:
        has_command = self.command is not None
        has_template = self.command_template is not None
        if has_command and has_template:
            raise ValueError(
                "JobSpec accepts either `command` or `command_template`, not both"
            )
        if not has_command and not has_template:
            raise ValueError(
                "JobSpec requires one of `command` or `command_template`"
            )
        if has_template and not (self.command_template or "").strip():
            raise ValueError("command_template must not be empty")
        return self

    @model_validator(mode="after")
    def _v_array_requires_template(self) -> JobSpec:
        # Array jobs need ${SLURM_ARRAY_TASK_ID} expansion in the shell. With
        # `command: list[str]`, the renderer calls shlex.join() and the variable
        # gets single-quoted into a literal — which silently breaks the job.
        # Force callers to use `command_template` (a verbatim shell string)
        # when an array spec is set.
        if self.array is not None and self.command is not None:
            raise ValueError(
                "array jobs require `command_template` (not `command`); "
                "shlex.join() would single-quote ${SLURM_ARRAY_TASK_ID}"
            )
        return self


# --- SubmittedJob ----------------------------------------------------------


class SubmittedJob(_Open):
    """Result of a successful `submit()` call.

    `submit_stdout`/`submit_stderr` are kept as raw strings so that callers can
    reconcile state if the parser failed but a job actually entered the queue.
    """

    internal_job_id: str
    slurm_job_id: str
    cluster: str | None = None
    remote_job_dir: str
    remote_script_path: str
    stdout_path: str
    stderr_path: str
    submitted_at: str | None = None

    submit_stdout: str = ""
    submit_stderr: str = ""

    @field_validator("internal_job_id")
    @classmethod
    def _v_iid(cls, v: str) -> str:
        if not is_valid_internal_job_id(v):
            raise ValueError(
                "internal_job_id must match 'slurmly-<8 hex>'"
            )
        return v


# --- RenderedJob -----------------------------------------------------------


class RenderedJob(_Open):
    """Output of `render_only()` — produced without contacting the cluster."""

    internal_job_id: str
    remote_job_dir: str
    remote_script_path: str
    stdout_path: str
    stderr_path: str
    script: str

    @field_validator("internal_job_id")
    @classmethod
    def _v_iid(cls, v: str) -> str:
        if not is_valid_internal_job_id(v):
            raise ValueError(
                "internal_job_id must match 'slurmly-<8 hex>'"
            )
        return v


# --- LogChunk --------------------------------------------------------------


class LogChunk(_Open):
    """A piece of stdout/stderr fetched from the cluster."""

    path: str
    content: str
    exists: bool = True
    lines_requested: int | None = None
    bytes_requested: int | None = None
    fetched_at: str
    note: str | None = None


# --- JobInfo ---------------------------------------------------------------


class JobInfo(_Open):
    """Status snapshot of a Slurm job.

    Fields not reported by the chosen source remain `None`. `raw` carries the
    selected backend payload (squeue or sacct row) so callers can dig deeper.

    `visibility` distinguishes:
      - "visible"        : a real backend match was found
      - "transient_gap"  : within submit grace window, neither squeue nor
                           sacct reports the job yet (treat as "unknown")
      - "not_found"      : explicitly absent from both backends
                           (callers normally see this as a raised
                           SlurmJobNotFound; the value is reserved for cases
                           where a not_found JobInfo is materialized)
      - "unknown"        : raw_state didn't map to a known lifecycle
    """

    slurm_job_id: str
    cluster: str | None = None
    lifecycle: Lifecycle
    raw_state: str | None = None

    name: str | None = None
    user: str | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    queue_reason: str | None = None
    exit_code: str | None = None
    elapsed: str | None = None
    start_time: str | None = None
    end_time: str | None = None

    array_job_id: str | None = None
    array_task_id: str | None = None

    source: JobInfoSource
    visibility: JobVisibility = "visible"
    raw: dict[str, Any] | None = None


# --- CancelResult ----------------------------------------------------------


class CancelResult(_Open):
    """Result of a `scancel` call. Phase 1 only reports whether the command
    succeeded; full cross-check against squeue/sacct lives in Phase 2."""

    slurm_job_id: str
    cluster: str | None = None
    signal: str | None = None
    requested_at: str
    raw_stdout: str = ""
    raw_stderr: str = ""
