# API Reference

Every public symbol exported from `slurmly` (see `slurmly/__init__.py`).

```python
from slurmly import (
    SlurmSSHClient,
    JobSpec, SubmittedJob, RenderedJob, LogChunk, CancelResult,
    JobInfo, JobInfoSource, JobVisibility, Lifecycle,
    JobDependency, DependencyType,
    Artifact, ArtifactDownload,
    ClusterProfile, ExecutionProfile, ClusterPreset,
    CapabilityReport,
    CleanupCandidate, CleanupPlan, CleanupResult,
    PollingPolicy, PollingResult,
    SlurmlyHooks,
    CommandStartedEvent, CommandFinishedEvent,
    SubmitSucceededEvent, SubmitFailedEvent,
    StatusCheckedEvent, CancelRequestedEvent,
    get_preset, list_presets,
    SlurmlyError, InvalidConfig, InvalidJobSpec, UnknownPreset,
    SSHTransportError, SbatchError, SbatchParseError,
    ScancelError, SlurmJobNotFound,
)
```

---

## `SlurmSSHClient`

The single entry point. All SSH-side state lives here.

### Constructor

```python
SlurmSSHClient(
    *,
    transport: SSHTransport,
    remote_base_dir: str = "~/slurmly",
    cluster_profile: ClusterProfile | None = None,
    execution_profiles: dict[str, ExecutionProfile] | None = None,
    account: str | None = None,
    hooks: SlurmlyHooks | None = None,
    max_concurrent_commands: int = 8,
)
```

| Parameter                 | Type                                | Description |
|---------------------------|-------------------------------------|-------------|
| `transport`               | `SSHTransport`                      | SSH transport (real `AsyncSSHTransport` or test `FakeTransport`). Required. |
| `remote_base_dir`         | `str`                               | Path on the cluster used as the parent of all per-job working directories. Each job gets `<remote_base_dir>/jobs/slurmly-<8hex>/`. Defaults to `~/slurmly`. Absolute paths enable strict cleanup/artifact path enforcement. |
| `cluster_profile`         | `ClusterProfile \| None`            | Cluster behavior knobs (allowed partitions, JSON support flags, GPU directive style). Defaults to `ClusterProfile()` (no allow-list restrictions, JSON support assumed). |
| `execution_profiles`      | `dict[str, ExecutionProfile]`       | Named preambles (module loads, env activation) referenced by `JobSpec.execution_profile`. |
| `account`                 | `str \| None`                       | Slurm `--account=` injected into `JobSpec` when the spec doesn't set its own. Falls back to `cluster_profile.default_account`. |
| `hooks`                   | `SlurmlyHooks \| None`              | Observability hook sink. Defaults to a no-op. |
| `max_concurrent_commands` | `int` (`>=1`)                       | Semaphore limit on concurrent SSH commands per client. Default 8. |

### Convenience factory: `connect`

```python
@classmethod
SlurmSSHClient.connect(
    *,
    host: str,
    username: str,
    remote_base_dir: str = "~/slurmly",
    key_path: str | None = None,
    port: int = 22,
    known_hosts_path: str | None = None,
    accept_unknown_hosts: bool = True,
    cluster_profile: ClusterProfile | None = None,
    execution_profiles: dict[str, ExecutionProfile] | None = None,
    account: str | None = None,
    hooks: SlurmlyHooks | None = None,
    max_concurrent_commands: int = 8,
) -> SlurmSSHClient
```

Builds an `SSHConfig` + `AsyncSSHTransport` from kwargs and forwards everything else to
the regular constructor. Use this when you don't need proxy jumps, custom timeouts, or
heredoc uploads — for those, construct `SSHConfig` + `AsyncSSHTransport` explicitly.

Properties (read-only):

- `cluster_profile: ClusterProfile`
- `remote_base_dir: str`
- `hooks: SlurmlyHooks`
- `max_concurrent_commands: int`

### Alternate constructor

```python
@classmethod
SlurmSSHClient.from_config(
    path: str,
    *,
    transport: SSHTransport | None = None,
    hooks: SlurmlyHooks | None = None,
) -> SlurmSSHClient
```

Build a client from a config file. Format detected by extension (`.yaml`, `.yml`, `.toml`,
`.json`). See [configuration.md](configuration.md) for the schema.

### Lifecycle

```python
async def close() -> None
async def __aenter__() -> SlurmSSHClient
async def __aexit__(...) -> None
```

Use as an async context manager:

```python
async with SlurmSSHClient.from_config("slurmly.yaml") as client:
    ...
```

---

### `submit(spec)`

```python
async def submit(spec: JobSpec) -> SubmittedJob
```

Render the batch script, validate against `cluster_profile`, upload to
`<remote_base_dir>/jobs/<id>/run.sh`, and submit with `sbatch --parsable`.

**Idempotency.** Submission is **not** idempotent. A dropped SSH connection during the
`sbatch` round trip raises `SbatchError` rather than silently re-submitting. Reconciliation
(querying `squeue --name=<job_name>`, etc.) is the caller's job. The raw sbatch output is
preserved on `SubmittedJob.submit_stdout` / `submit_stderr` to help.

**Returns:** `SubmittedJob` (see below).

**Raises:** `InvalidJobSpec`, `InvalidConfig`, `SbatchError`, `SbatchParseError`,
`SSHTransportError`.

---

### `render_only(spec)`

```python
def render_only(spec: JobSpec) -> RenderedJob
```

Produce the full rendered batch script and the paths it *would* be uploaded to, without
contacting the cluster. Same validation as `submit`. Useful for tests, dry runs, and CI.

---

### `cancel(slurm_job_id, *, signal=None, cluster=None)`

```python
async def cancel(
    slurm_job_id: str,
    *,
    signal: str | None = None,
    cluster: str | None = None,
) -> CancelResult
```

Issue `scancel` against the job. With `signal=` (e.g., `"TERM"`, `"USR1"`), sends the named
signal instead of the default cancel. Idempotent in the sense that already-terminated jobs
do not raise.

**Raises:** `ScancelError`, `SSHTransportError`.

---

### `tail_stdout(job, *, lines=200)` / `tail_stderr(job, *, lines=200)`

```python
async def tail_stdout(job: SubmittedJob, *, lines: int = 200) -> LogChunk
async def tail_stderr(job: SubmittedJob, *, lines: int = 200) -> LogChunk
```

Run `tail -n <lines>` against the job's stdout/stderr file. Returns a `LogChunk` whose
`exists=False` and `content=""` when the file has not been written yet (job still queued or
log buffered).

---

### `get_job(slurm_job_id, *, cluster=None, submitted_at=None)`

```python
async def get_job(
    slurm_job_id: str,
    *,
    cluster: str | None = None,
    submitted_at: datetime | str | None = None,
) -> JobInfo
```

Resolve a single job's status using a multi-source strategy:

1. Query `squeue --json --jobs=<id>` if `cluster_profile.supports_squeue_json=True`.
2. If the job is not in `squeue`, query `sacct --json --jobs=<id> --starttime=...`.
3. If still missing:
   - Within `cluster_profile.submit_grace_window_seconds` of `submitted_at` →
     return a `JobInfo` with `lifecycle="unknown"` and `visibility="transient_gap"`.
   - Otherwise → raise `SlurmJobNotFound`.

Pass `submitted_at` (the value from `SubmittedJob.submitted_at`) to enable the grace window.

**Raises:** `SlurmJobNotFound`, `SSHTransportError`.

---

### `get_jobs(slurm_job_ids, *, cluster=None)`

```python
async def get_jobs(
    slurm_job_ids: list[str],
    *,
    cluster: str | None = None,
) -> dict[str, JobInfo]
```

Batched lookup. For each chunk of up to 200 ids, issues:

- one `squeue --json --jobs=a,b,c,...`,
- and one `sacct --json --jobs=<missing>,...` for ids `squeue` did not return.

Falls through to per-id `sacct --parsable2` when `supports_sacct_json=False`. Skips the
`squeue` leg when `supports_squeue_json=False`. Dedupes ids; preserves submission order in
the returned dict. **Missing ids are absent from the result — no errors are raised** for ids
the cluster does not know about.

---

### `poll_job(...)`

```python
async def poll_job(
    slurm_job_id: str,
    *,
    cluster: str | None = None,
    submitted_at: datetime | str | None = None,
    policy: PollingPolicy | None = None,
) -> PollingResult
```

Single-step poll. Wraps `get_job`, classifies the outcome (terminal / retriable /
transient gap / not-found), and recommends the next polling interval based on `policy` and
job age.

The application owns the loop. `slurmly` does not run a scheduler.

---

### `detect_capabilities()`

```python
async def detect_capabilities() -> CapabilityReport
```

Probe the cluster for runtime support: `squeue --json`, `sacct --json`,
`sacct --parsable2`, SFTP. Diagnostic only — **never mutates `ClusterProfile`**. Each probe
runs independently; one failure does not abort others. Mismatches between observed
behavior and `cluster_profile` flags surface as `CapabilityReport.warnings`.

---

### `cleanup_remote_job_dir(job, *, dry_run=False)`

```python
async def cleanup_remote_job_dir(
    job: SubmittedJob,
    *,
    dry_run: bool = False,
) -> CleanupResult
```

Remove a single job's working directory. Path is validated against
`<remote_base_dir>/jobs/` (rejects traversal, sibling-prefix collisions, the jobs root
itself, and absolute roots).

---

### `plan_cleanup(*, older_than_days)`

```python
async def plan_cleanup(*, older_than_days: float) -> CleanupPlan
```

Enumerate working directories whose mtime is older than `older_than_days` (fractional
days supported — internally translated to `find -mmin +<minutes>`). Returns a
`CleanupPlan` of candidates the caller can inspect/filter before execution.

---

### `cleanup(plan, *, dry_run=False)`

```python
async def cleanup(plan: CleanupPlan, *, dry_run: bool = False) -> CleanupResult
```

Execute (or dry-run) a plan. Each candidate is **re-validated** at execution time, so a
plan from a different `remote_base_dir` is rejected even if it looked safe at planning
time.

---

### `list_artifacts(job, *, pattern="*")`

```python
async def list_artifacts(
    job: SubmittedJob,
    *,
    pattern: str = "*",
) -> list[Artifact]
```

List files under `job.remote_job_dir` matching the glob `pattern`. The implementation runs
`find <job_dir> -type f -name <pattern>`; results outside `<remote_base_dir>/jobs/` are
filtered post-hoc as a safety net.

---

### `download_artifact(job, name, *, max_bytes=None)`

```python
async def download_artifact(
    job: SubmittedJob,
    name: str,
    *,
    max_bytes: int | None = None,
) -> ArtifactDownload
```

Read an artifact by name **relative to the job directory**. Absolute paths and `..`
segments are rejected. `max_bytes` truncates the read; `ArtifactDownload.truncated`
reflects whether the cap was hit.

---

### Other introspection

```python
def list_execution_profiles(self) -> list[str]
```

Names of execution profiles known to this client, sorted.

---

## Models

All models use Pydantic v2 with `extra="forbid"`. Most are frozen — mutate by constructing
a new instance.

### `JobSpec`

```python
class JobSpec(BaseModel):
    name: str                                    # [A-Za-z0-9._-]{1,128}

    # Exactly one of:
    command: list[str] | None = None             # shell-quoted via shlex.join
    command_template: str | None = None          # emitted verbatim

    # Slurm directives (all optional)
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    # Resources
    nodes: int | None = None                     # >= 1
    ntasks: int | None = None                    # >= 1
    cpus_per_task: int | None = None             # >= 1
    memory: str | None = None                    # "512", "512M", "32G", "1T"
    gpus: int | None = None                      # >= 0
    gpu_type: str | None = None                  # e.g. "a100"

    # Scheduling
    time_limit: str | None = None                # "30", "30:00", "01:30:00", "1-12:00:00"
    working_dir: str | None = None
    env: dict[str, str] = {}                     # POSIX env names

    # Advanced
    array: str | None = None                     # "0-99", "0-99%10", "1,3,5"
    dependency: str | JobDependency | None = None

    # Metadata
    execution_profile: str | None = None
    metadata: dict[str, str] = {}
```

**Cross-field rules:**

- Exactly one of `command` or `command_template` is required.
- When `array` is set, `command_template` is **required** — `command: list[str]` is
  rejected because `shlex.join` would single-quote `${SLURM_ARRAY_TASK_ID}` and prevent
  shell expansion.
- All `env` keys must be valid POSIX env names (`[A-Za-z_][A-Za-z0-9_]*`).
- Cluster-side validation (allowed partitions/accounts/qos, GPU directive support) runs at
  `submit` and `render_only` time, not on construction.

### `SubmittedJob`

```python
class SubmittedJob(BaseModel):
    internal_job_id: str                # slurmly-<8hex>
    slurm_job_id: str
    cluster: str | None = None
    remote_job_dir: str
    remote_script_path: str
    stdout_path: str
    stderr_path: str
    submitted_at: str | None = None     # ISO 8601, "...Z"

    submit_stdout: str = ""             # raw sbatch output, kept for reconciliation
    submit_stderr: str = ""
```

### `RenderedJob`

```python
class RenderedJob(BaseModel):
    internal_job_id: str
    remote_job_dir: str
    remote_script_path: str
    stdout_path: str
    stderr_path: str
    script: str                         # full rendered #!/bin/bash batch script
```

### `JobInfo`

```python
class JobInfo(BaseModel):
    slurm_job_id: str
    cluster: str | None = None
    lifecycle: Lifecycle                # see states.Lifecycle
    raw_state: str | None = None        # original Slurm state, e.g. "PENDING", "CANCELLED by 12345"

    name: str | None = None
    user: str | None = None
    partition: str | None = None
    account: str | None = None
    qos: str | None = None

    queue_reason: str | None = None     # "Resources", "Priority", ...
    exit_code: str | None = None        # "0:0", "42:0", "0:9", ...
    elapsed: str | None = None          # "HH:MM:SS"
    start_time: str | None = None       # ISO 8601
    end_time: str | None = None         # ISO 8601

    array_job_id: str | None = None     # parent id when this is an array task
    array_task_id: str | None = None    # task id when this is an array task

    source: JobInfoSource               # "squeue" | "sacct" | "none"
    visibility: JobVisibility = "visible"   # "visible" | "transient_gap" | "not_found" | "unknown"
    raw: dict[str, Any] | None = None       # backend payload for debugging
```

### `LogChunk`

```python
class LogChunk(BaseModel):
    path: str
    content: str
    exists: bool = True                 # False when the log file is not yet on the cluster
    lines_requested: int | None = None
    bytes_requested: int | None = None
    fetched_at: str                     # ISO 8601 with Z
    note: str | None = None
```

### `CancelResult`

```python
class CancelResult(BaseModel):
    slurm_job_id: str
    cluster: str | None = None
    signal: str | None = None
    requested_at: str
    raw_stdout: str = ""
    raw_stderr: str = ""
```

### `JobDependency`

```python
class JobDependency(BaseModel):
    type: DependencyType                # "after" | "afterany" | "afterok"
                                        # | "afternotok" | "aftercorr" | "singleton"
    job_ids: list[str] = []             # plain numeric ids only (no array suffixes)

    def render(self) -> str:
        # "singleton" or "<type>:<id>:<id>:..."
```

Renders the AND-list form. For OR-chains (Slurm's `?` syntax) and other exotic
expressions, pass `JobSpec.dependency` as a raw string instead.

### Type aliases

```python
DependencyType = Literal[
    "after", "afterany", "afterok", "afternotok", "aftercorr", "singleton",
]
JobInfoSource = Literal["squeue", "sacct", "none"]
JobVisibility = Literal["visible", "transient_gap", "not_found", "unknown"]
Lifecycle = Literal[
    "queued", "running", "succeeded", "failed", "cancelled", "timeout", "unknown",
]
```

---

## Profiles

### `ClusterProfile`

Per-cluster behavior knobs.

```python
class ClusterProfile(BaseModel):
    name: str = "default"

    # GPU directive style
    gpu_directive: Literal["gpus", "gres", "gres_typed", "gpus_per_node"] = "gpus"
    default_gpu_type: str | None = None

    # Slurm defaults
    default_partition: str | None = None
    default_account: str | None = None
    default_qos: str | None = None

    # Allowed values (empty list = no restriction)
    allowed_partitions: list[str] = []
    allowed_accounts: list[str] = []
    allowed_qos: list[str] = []

    # Backend support
    supports_squeue_json: bool = True
    supports_sacct_json: bool = True
    supports_parsable2_fallback: bool = True

    # sacct lookback window (in days)
    sacct_lookback_days: int = 7        # >= 1

    # Working-directory mode
    working_dir_mode: Literal["script_cd", "sbatch_chdir"] = "script_cd"

    # Advanced features
    supports_array_jobs: bool = False

    # Operational tolerances
    submit_grace_window_seconds: int = 120
    not_found_attention_after_seconds: int = 300
```

**`gpu_directive` styles**

- `gpus` → `#SBATCH --gpus=N` (Slurm ≥ 21.08).
- `gres` → `#SBATCH --gres=gpu:N`.
- `gres_typed` → `#SBATCH --gres=gpu:<type>:N` (requires `default_gpu_type` or `gpu_type`).
- `gpus_per_node` → `#SBATCH --gpus-per-node=N` (Anvil's recommended form).

**`working_dir_mode`**

- `script_cd` (default) → emit `cd "<dir>"` at the top of the script body.
- `sbatch_chdir` → emit `#SBATCH --chdir=<dir>`.

### `ExecutionProfile`

```python
class ExecutionProfile(BaseModel):
    name: str
    preamble: list[str] = []            # shell lines, emitted verbatim
    env: dict[str, str] = {}            # exported into the job environment
```

Lines in `preamble` are not quoted or modified — they go into the rendered script as-is.
Use them for `module load`, venv activation, conda env switches, `set -e`/`set -u`, etc.

### `ClusterPreset`

```python
class ClusterPreset(BaseModel):
    name: str
    aliases: list[str] = []             # e.g., ["anvil", "rcac_anvil"]
    description: str | None = None
    source_urls: list[str] = []         # citations to official docs
    last_reviewed: str | None = None    # YYYY-MM-DD

    ssh_host: str | None = None
    remote_base_dir_template: str | None = None     # "{username}" placeholder allowed

    cluster_profile: ClusterProfile
    execution_profiles: dict[str, ExecutionProfile] = {}
```

---

## Hooks

`SlurmlyHooks` is the observability sink. Subclass and override the methods you care
about; defaults are no-ops.

```python
class SlurmlyHooks:
    async def on_command_started(self, event: CommandStartedEvent) -> None: ...
    async def on_command_finished(self, event: CommandFinishedEvent) -> None: ...
    async def on_submit_succeeded(self, event: SubmitSucceededEvent) -> None: ...
    async def on_submit_failed(self, event: SubmitFailedEvent) -> None: ...
    async def on_status_checked(self, event: StatusCheckedEvent) -> None: ...
    async def on_cancel_requested(self, event: CancelRequestedEvent) -> None: ...
```

### Event payloads

```python
class CommandStartedEvent:
    operation: str                      # "sbatch" | "squeue" | "sacct" | "scancel" | "tail" | "find" | "rm" | "read"
    command: str                        # full shell command, post-shlex-quote
    retry_safe: bool = False            # True for read-only ops
    started_at: str                     # ISO 8601 with Z
    metadata: dict[str, Any] = {}

class CommandFinishedEvent:
    operation: str
    command: str
    exit_status: int
    duration_seconds: float
    finished_at: str
    error: str | None = None            # exception class + message when raised
    metadata: dict[str, Any] = {}

class SubmitSucceededEvent:
    internal_job_id: str
    slurm_job_id: str
    cluster: str | None = None
    submitted_at: str
    metadata: dict[str, Any] = {}

class SubmitFailedEvent:
    internal_job_id: str | None = None
    error_type: str                     # exception class name
    error_message: str
    failed_at: str
    metadata: dict[str, Any] = {}

class StatusCheckedEvent:
    slurm_job_id: str
    source: str                         # "squeue" | "sacct" | "none"
    lifecycle: str
    visibility: str
    checked_at: str

class CancelRequestedEvent:
    slurm_job_id: str
    cluster: str | None = None
    signal: str | None = None
    requested_at: str
```

Hook methods may raise — exceptions propagate. If you need fault isolation, swallow
exceptions in your subclass.

---

## Polling

### `PollingPolicy`

```python
class PollingPolicy(BaseModel):
    fresh_submit_interval_seconds: int = 10
    fresh_submit_window_seconds: int = 120
    normal_interval_seconds: int = 30
    long_running_after_seconds: int = 1800       # 30 minutes
    long_running_interval_seconds: int = 60
    grace_window_seconds: int = 120
    attention_after_seconds: int = 300

    def next_interval(self, *, age_seconds: float) -> int: ...
```

Intervals scale with job age:

```
[0 ... fresh_submit_window_seconds)             → fresh_submit_interval_seconds
[fresh_submit_window ... long_running_after)    → normal_interval_seconds
[long_running_after ... ∞)                      → long_running_interval_seconds
```

### `PollingResult`

```python
class PollingResult(BaseModel):
    slurm_job_id: str
    info: JobInfo | None = None
    error: str | None = None
    error_type: str | None = None
    should_retry: bool = True           # False for terminal or unrecoverable
    transient: bool = False             # True = expected gap, retry without alerting
    next_interval_seconds: int | None = None
    metadata: dict[str, Any] = {}
```

### Helpers

```python
def is_terminal(info: JobInfo) -> bool
def age_seconds(submitted_at: datetime, *, now: datetime | None = None) -> float
def attention_threshold_reached(
    submitted_at: datetime,
    policy: PollingPolicy,
    *,
    now: datetime | None = None,
) -> bool
```

---

## Cleanup

```python
class CleanupCandidate(BaseModel):
    path: str
    age_days: float | None = None
    reason: str | None = None

class CleanupPlan(BaseModel):
    remote_base_dir: str
    older_than_days: float | None = None
    candidates: list[CleanupCandidate] = []
    raw: dict[str, Any] = {}

class CleanupResult(BaseModel):
    removed: list[str] = []
    skipped: list[str] = []             # safety-rejected at execute time
    errors: list[str] = []              # remote rm failures
    dry_run: bool = False
```

Path validation rules (raise `InvalidConfig`):

- Empty / `"/"`: rejected.
- Contains `..`: rejected.
- Does not start with `<remote_base_dir>/jobs/` (note the trailing slash —
  `/scratch/u/slurmly-evil/...` does not match `/scratch/u/slurmly`): rejected.
  This prefix check applies only when `remote_base_dir` is an absolute path.
  Tilde-based base dirs (`~/slurmly`) skip the prefix check (remote `~` can't be
  resolved locally) but still enforce the `..` and empty-path rules.
- Resolves exactly to the jobs root: rejected.

---

## Artifacts

```python
class Artifact(BaseModel):
    path: str                           # absolute path on the cluster
    name: str                           # basename

class ArtifactDownload(BaseModel):
    path: str
    content: str                        # bytes decoded as UTF-8
    fetched_at: str
    bytes_requested: int | None = None
    truncated: bool = False             # True if max_bytes was hit
    raw: dict = {}
```

Artifact path validation matches cleanup's, with one extra rule for
`download_artifact(name=...)`: `name` must be relative (no leading `/`) and free of `..`.

---

## Capabilities

```python
class CapabilityReport(BaseModel):
    supports_squeue_json: bool = False
    supports_sacct_json: bool = False
    supports_sacct_parsable2: bool = False
    supports_sftp: bool = False
    slurm_version: str | None = None    # output of `sinfo --version`
    warnings: list[str] = []            # flagged mismatches with the active profile
    raw: dict[str, Any] = {}            # raw probe outputs for debugging
```

`detect_capabilities()` fills this. `slurmly` never auto-applies the result to
`ClusterProfile` — apply changes yourself if you want to act on warnings.

---

## Presets

```python
def list_presets() -> list[str]                 # canonical names only (no aliases)
def get_preset(name_or_alias: str) -> ClusterPreset
```

`get_preset` raises `UnknownPreset` if the lookup fails. The current registry contains
`purdue_anvil` (aliases: `anvil`, `rcac_anvil`).

See [anvil.md](anvil.md) for full details on the Anvil preset.

---

## Lifecycle states

`Lifecycle` (from `slurmly.states`) maps Slurm raw states onto seven values:

| Lifecycle    | Slurm raw states (subset) |
|--------------|----------------------------|
| `queued`     | PENDING, CONFIGURING, SUSPENDED, RESIZING, REQUEUED, REVOKED, SPECIAL_EXIT |
| `running`    | RUNNING, COMPLETING, STOPPED, STAGE_OUT, SIGNALING |
| `succeeded`  | COMPLETED |
| `failed`     | FAILED, BOOT_FAIL, DEADLINE, NODE_FAIL, OUT_OF_MEMORY, PREEMPTED |
| `cancelled`  | CANCELLED, CANCELLED by `<uid>` |
| `timeout`    | TIMEOUT |
| `unknown`    | unrecognized / missing |

Helper:

```python
def normalize_state(raw: str | None) -> Lifecycle
```

---

## Exceptions

```
SlurmlyError                            (base)
├── InvalidConfig                       config invalid / incomplete
│   └── UnknownPreset                   preset name/alias not found
├── InvalidJobSpec                      JobSpec validation failed at submit time
├── SSHTransportError                   SSH-level failures
├── RemoteCommandError                  command exited non-zero
│   ├── SbatchError                     sbatch invocation failed
│   └── ScancelError                    scancel invocation failed
├── SbatchParseError                    sbatch ok but --parsable output unreadable
├── SlurmJobNotFound                    not in squeue or sacct (past grace window)
└── TransientStatusGap                  inside grace window, not yet visible
```

`RemoteCommandError` (and its subclasses) carry `command`, `exit_status`, `stdout`,
`stderr` for diagnostics.

---

## In-memory testing

`slurmly.testing.FakeTransport` implements the `SSHTransport` protocol with canned
responses. See [cookbook.md](cookbook.md#testing-with-faketransport) for usage.

```python
from slurmly.testing import FakeTransport

transport = FakeTransport(canned_responses=[(0, "12345\n", "")])
client = SlurmSSHClient(
    transport=transport,
    cluster_profile=ClusterProfile(default_partition="shared", allowed_partitions=["shared"]),
    remote_base_dir="/scratch/u/slurmly",
    account="acct1",
)
```
