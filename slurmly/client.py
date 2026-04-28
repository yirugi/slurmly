"""SlurmSSHClient — public entry point.

Phase 1 surface:
    - submit(spec) -> SubmittedJob
    - cancel(job_id, signal=None, cluster=None) -> CancelResult
    - tail_stdout(job, lines=200) -> LogChunk
    - tail_stderr(job, lines=200) -> LogChunk
    - render_only(spec) -> RenderedJob
    - close()
    - from_config(path) -> SlurmSSHClient

Phase 2 surface:
    - get_job(slurm_job_id, cluster=None, submitted_at=None) -> JobInfo
    - get_jobs([job_id, ...], cluster=None) -> dict[str, JobInfo]

Phase 3 surface:
    - detect_capabilities() -> CapabilityReport
    - cleanup_remote_job_dir(job)
    - plan_cleanup(older_than_days=N) -> CleanupPlan
    - cleanup(plan, dry_run=False) -> CleanupResult
    - poll_job(slurm_job_id, ..., policy=None) -> PollingResult
    - hooks: SlurmlyHooks
    - max_concurrent_commands: asyncio.Semaphore on every transport call
"""

from __future__ import annotations

import asyncio
import json
import shlex
import time
from datetime import datetime, timezone
from typing import Any

from .artifacts import (
    Artifact,
    ArtifactDownload,
    assert_artifact_path_safe,
    resolve_artifact_path,
)
from .capabilities import CapabilityReport, reconcile_with_profile
from .cleanup import (
    CleanupCandidate,
    CleanupPlan,
    CleanupResult,
    assert_safe_to_remove,
    build_find_stale,
    build_rm_rf,
)
from .exceptions import (
    InvalidConfig,
    InvalidJobSpec,
    SbatchError,
    SlurmJobNotFound,
    SlurmlyError,
)
from .hooks import (
    CancelRequestedEvent,
    CommandFinishedEvent,
    CommandStartedEvent,
    SlurmlyHooks,
    StatusCheckedEvent,
    SubmitFailedEvent,
    SubmitSucceededEvent,
)
from .models import CancelResult, JobInfo, JobSpec, LogChunk, RenderedJob, SubmittedJob
from .paths import generate_internal_job_id, job_paths
from .polling import PollingPolicy, PollingResult, age_seconds, is_terminal
from .profiles import ClusterProfile, ExecutionProfile
from .scripts.renderer import render_script, validate_against_profile
from .slurm.commands import (
    build_find_files,
    build_mkdir_p,
    build_sacct_json,
    build_sacct_json_many,
    build_sacct_parsable2,
    build_scancel,
    build_squeue_json,
    build_squeue_json_many,
    build_submit,
    build_tail,
)
from .slurm.parser import (
    parse_sacct_json_all,
    parse_sacct_json_str,
    parse_sacct_parsable2,
    parse_sbatch_parsable,
    parse_squeue_json_all,
    parse_squeue_json_str,
)
from .ssh import CommandResult, SSHTransport
from .validators import is_valid_array_job_id, is_valid_plain_job_id

# Cap on how many job ids go into a single squeue/sacct invocation. Slurm
# accepts a generous list, but the joined argument inflates command-line
# length quickly; chunking keeps us well clear of common ARG_MAX limits.
_BATCH_GET_JOBS_CHUNK = 200


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SlurmSSHClient:
    """Typed Slurm SSH client.

    Construct directly when the caller already has a transport and resolved
    profiles. Use `SlurmSSHClient.from_config(path)` for the typical YAML/TOML
    config path.
    """

    def __init__(
        self,
        *,
        transport: SSHTransport,
        remote_base_dir: str = "~/slurmly",
        cluster_profile: ClusterProfile | None = None,
        execution_profiles: dict[str, ExecutionProfile] | None = None,
        account: str | None = None,
        hooks: SlurmlyHooks | None = None,
        max_concurrent_commands: int = 8,
    ) -> None:
        if not remote_base_dir:
            raise InvalidConfig("remote_base_dir must be a non-empty path")
        if max_concurrent_commands < 1:
            raise InvalidConfig("max_concurrent_commands must be >= 1")
        self._transport = transport
        self._cluster_profile = cluster_profile or ClusterProfile()
        self._remote_base_dir = remote_base_dir
        self._execution_profiles: dict[str, ExecutionProfile] = dict(execution_profiles or {})
        self._account = account
        self._hooks = hooks or SlurmlyHooks()
        self._semaphore = asyncio.Semaphore(max_concurrent_commands)
        self._max_concurrent_commands = max_concurrent_commands

    # --- introspection (read-only) ---

    @property
    def cluster_profile(self) -> ClusterProfile:
        return self._cluster_profile

    @property
    def remote_base_dir(self) -> str:
        return self._remote_base_dir

    @property
    def hooks(self) -> SlurmlyHooks:
        return self._hooks

    @property
    def max_concurrent_commands(self) -> int:
        return self._max_concurrent_commands

    def list_execution_profiles(self) -> list[str]:
        return sorted(self._execution_profiles.keys())

    # --- alternate constructor ---

    @classmethod
    def from_config(
        cls,
        path: str,
        *,
        transport: SSHTransport | None = None,
        hooks: SlurmlyHooks | None = None,
    ) -> "SlurmSSHClient":
        """Load a slurmly config file and build a client.

        File format is detected from the extension: `.yaml`/`.yml`, `.toml`,
        or `.json`. Pass `transport=` to override the default asyncssh-backed
        transport (useful in tests). Pass `hooks=` to wire observability.
        """
        from .config import build_client_from_config

        return build_client_from_config(
            path, transport_override=transport, hooks=hooks
        )

    @classmethod
    def connect(
        cls,
        *,
        host: str,
        username: str,
        remote_base_dir: str = "~/slurmly",
        key_path: str | None = None,
        key_content: str | None = None,
        port: int = 22,
        known_hosts_path: str | None = None,
        accept_unknown_hosts: bool = True,
        cluster_profile: ClusterProfile | None = None,
        execution_profiles: dict[str, ExecutionProfile] | None = None,
        account: str | None = None,
        hooks: SlurmlyHooks | None = None,
        max_concurrent_commands: int = 8,
    ) -> "SlurmSSHClient":
        """Convenience factory: build SSHConfig + AsyncSSHTransport from kwargs.

        For full control (proxy_jump, custom timeouts, heredoc upload, …)
        construct `SSHConfig` + `AsyncSSHTransport` explicitly and pass them
        to the regular constructor.
        """
        from .ssh.transport import AsyncSSHTransport, SSHConfig

        transport = AsyncSSHTransport(SSHConfig(
            host=host,
            username=username,
            port=port,
            key_path=key_path,
            key_content=key_content,
            known_hosts_path=known_hosts_path,
            accept_unknown_hosts=accept_unknown_hosts,
        ))
        return cls(
            transport=transport,
            remote_base_dir=remote_base_dir,
            cluster_profile=cluster_profile,
            execution_profiles=execution_profiles,
            account=account,
            hooks=hooks,
            max_concurrent_commands=max_concurrent_commands,
        )

    # --- lifecycle ---

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> "SlurmSSHClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # --- centralized transport call (semaphore + hooks) ---

    async def _run(
        self,
        command: str,
        *,
        operation: str,
        check: bool = False,
        retry_safe: bool = False,
        timeout: float | None = None,
    ) -> CommandResult:
        """Single-entry wrapper around `transport.run`.

        Every SSH command issued by the client must go through here so the
        concurrency semaphore and hook events fire uniformly.
        """
        started_at = _utc_now_iso()
        t0 = time.monotonic()
        await self._hooks.on_command_started(
            CommandStartedEvent(
                operation=operation,
                command=command,
                retry_safe=retry_safe,
                started_at=started_at,
            )
        )
        async with self._semaphore:
            try:
                result = await self._transport.run(
                    command, timeout=timeout, check=check, retry_safe=retry_safe
                )
            except Exception as e:
                duration = time.monotonic() - t0
                await self._hooks.on_command_finished(
                    CommandFinishedEvent(
                        operation=operation,
                        command=command,
                        exit_status=-1,
                        duration_seconds=duration,
                        finished_at=_utc_now_iso(),
                        error=f"{type(e).__name__}: {e}",
                    )
                )
                raise
        duration = time.monotonic() - t0
        await self._hooks.on_command_finished(
            CommandFinishedEvent(
                operation=operation,
                command=command,
                exit_status=result.exit_status,
                duration_seconds=duration,
                finished_at=_utc_now_iso(),
            )
        )
        return result

    async def _upload(self, path: str, content: str, *, mode: int = 0o700) -> None:
        # Uploads gate on the same semaphore as commands but intentionally do
        # not emit `command_started/finished` hooks: there is no shell command
        # to log, and SFTP/heredoc behave differently enough that synthesizing
        # one would muddy hook semantics. Submit/cancel/status hooks already
        # cover the user-visible operations that include uploads.
        async with self._semaphore:
            await self._transport.upload_text(path, content, mode=mode)

    # --- core ops ---

    def render_only(self, spec: JobSpec) -> RenderedJob:
        """Render `run.sh` and the per-job paths without touching the cluster.

        Profile validation runs first (same checks as `submit`), so this also
        works as a dry-run validator.
        """
        spec = self._apply_account_default(spec)
        validate_against_profile(spec, self._cluster_profile)

        internal_job_id = generate_internal_job_id()
        paths = job_paths(
            remote_base_dir=self._remote_base_dir,
            internal_job_id=internal_job_id,
            is_array=spec.array is not None,
        )
        execution_profile = self._resolve_execution_profile(spec.execution_profile)
        script = render_script(
            spec=spec,
            profile=self._cluster_profile,
            execution_profile=execution_profile,
            stdout_path=paths["stdout"],
            stderr_path=paths["stderr"],
            remote_job_dir=paths["job_dir"],
        )
        return RenderedJob(
            internal_job_id=internal_job_id,
            remote_job_dir=paths["job_dir"],
            remote_script_path=paths["script"],
            stdout_path=paths["stdout"],
            stderr_path=paths["stderr"],
            script=script,
        )

    async def submit(self, spec: JobSpec) -> SubmittedJob:
        """Render the job, upload it, run sbatch, return a SubmittedJob.

        sbatch is non-idempotent: the run is **not** retry_safe, so a dropped
        connection during sbatch surfaces as an SSHTransportError to the caller
        rather than silently re-submitting.
        """
        internal_job_id: str | None = None
        try:
            spec = self._apply_account_default(spec)
            validate_against_profile(spec, self._cluster_profile)

            internal_job_id = generate_internal_job_id()
            paths = job_paths(
                remote_base_dir=self._remote_base_dir,
                internal_job_id=internal_job_id,
                is_array=spec.array is not None,
            )

            execution_profile = self._resolve_execution_profile(spec.execution_profile)

            script = render_script(
                spec=spec,
                profile=self._cluster_profile,
                execution_profile=execution_profile,
                stdout_path=paths["stdout"],
                stderr_path=paths["stderr"],
                remote_job_dir=paths["job_dir"],
            )

            await self._run(
                build_mkdir_p(paths["job_dir"]),
                operation="mkdir",
                check=True,
                retry_safe=True,
            )

            metadata_doc = _job_metadata(
                internal_job_id=internal_job_id,
                spec=spec,
                paths=paths,
            )
            await self._upload(
                paths["metadata"],
                json.dumps(metadata_doc, indent=2, sort_keys=True),
                mode=0o600,
            )
            await self._upload(paths["script"], script, mode=0o700)

            submit_cmd = build_submit(remote_job_dir=paths["job_dir"])
            result = await self._run(
                submit_cmd,
                operation="sbatch",
                check=False,
                retry_safe=False,
            )
            if result.exit_status != 0:
                raise SbatchError(
                    command=submit_cmd,
                    exit_status=result.exit_status,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )

            parsed = parse_sbatch_parsable(result.stdout)
            submitted_at = _utc_now_iso()

            job = SubmittedJob(
                internal_job_id=internal_job_id,
                slurm_job_id=parsed.slurm_job_id,
                cluster=parsed.cluster,
                remote_job_dir=paths["job_dir"],
                remote_script_path=paths["script"],
                stdout_path=paths["stdout"],
                stderr_path=paths["stderr"],
                submitted_at=submitted_at,
                submit_stdout=result.stdout,
                submit_stderr=result.stderr,
            )
            await self._hooks.on_submit_succeeded(
                SubmitSucceededEvent(
                    internal_job_id=job.internal_job_id,
                    slurm_job_id=job.slurm_job_id,
                    cluster=job.cluster,
                    submitted_at=submitted_at,
                )
            )
            return job
        except SlurmlyError as e:
            await self._hooks.on_submit_failed(
                SubmitFailedEvent(
                    internal_job_id=internal_job_id,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    failed_at=_utc_now_iso(),
                )
            )
            raise

    async def cancel(
        self,
        slurm_job_id: str,
        *,
        signal: str | None = None,
        cluster: str | None = None,
    ) -> CancelResult:
        cmd = build_scancel(slurm_job_id, signal=signal, cluster=cluster)
        await self._hooks.on_cancel_requested(
            CancelRequestedEvent(
                slurm_job_id=slurm_job_id,
                cluster=cluster,
                signal=signal,
                requested_at=_utc_now_iso(),
            )
        )
        # scancel exits 0 even when the job is already gone; non-zero is a real error.
        # Cancel is non-idempotent in the strict sense (signal delivery can race with
        # job exit), but it's safe to re-issue against an already-terminated job, so
        # we allow retry_safe=True here.
        result = await self._run(cmd, operation="scancel", check=True, retry_safe=True)
        return CancelResult(
            slurm_job_id=slurm_job_id,
            cluster=cluster,
            signal=signal,
            requested_at=_utc_now_iso(),
            raw_stdout=result.stdout,
            raw_stderr=result.stderr,
        )

    async def get_job(
        self,
        slurm_job_id: str,
        *,
        cluster: str | None = None,
        submitted_at: datetime | str | None = None,
    ) -> JobInfo:
        """Return a JobInfo snapshot for `slurm_job_id`.

        Strategy:
          1. Query `squeue --json` (or skip when ClusterProfile.supports_squeue_json is False).
          2. If squeue has no row, query `sacct --json --starttime=now-Ndays`
             (or `sacct --parsable2` when supports_sacct_json is False).
          3. If both come up empty:
             - within the submit grace window -> JobInfo(visibility="transient_gap")
             - otherwise -> raise SlurmJobNotFound

        `submitted_at` may be a datetime or an ISO 8601 string (e.g. the value
        on `SubmittedJob.submitted_at`); used only for the grace-window check.
        Array job ids (`<id>_<task>`) are explicitly rejected — array support
        lands in Phase 4.
        """
        self._validate_job_id(slurm_job_id)

        if self._cluster_profile.supports_squeue_json:
            squeue_info = await self._squeue_lookup(slurm_job_id, cluster=cluster)
            if squeue_info is not None:
                await self._emit_status(squeue_info)
                return squeue_info

        sacct_info = await self._sacct_lookup(slurm_job_id, cluster=cluster)
        if sacct_info is not None:
            await self._emit_status(sacct_info)
            return sacct_info

        info = self._handle_missing(slurm_job_id, cluster=cluster, submitted_at=submitted_at)
        await self._emit_status(info)
        return info

    async def get_jobs(
        self,
        slurm_job_ids: list[str],
        *,
        cluster: str | None = None,
    ) -> dict[str, JobInfo]:
        """Batched best-effort lookup. Missing ids are simply absent.

        Issues at most one `squeue --jobs=a,b,c` and (for ids not visible in
        squeue) one `sacct --json --jobs=a,b,c` per chunk of
        ``_BATCH_GET_JOBS_CHUNK`` ids. When the cluster profile sets
        ``supports_sacct_json=False``, sacct is queried per-id via the
        parsable2 fallback. When ``supports_squeue_json=False``, the squeue
        leg is skipped entirely.

        Each id is validated against the profile (array suffixes only when
        ``supports_array_jobs=True``). Invalid ids surface as
        ``InvalidJobSpec``.
        """
        for jid in slurm_job_ids:
            self._validate_job_id(jid)
        # Preserve order while deduplicating.
        unique_ids: list[str] = []
        seen: set[str] = set()
        for jid in slurm_job_ids:
            if jid not in seen:
                seen.add(jid)
                unique_ids.append(jid)
        if not unique_ids:
            return {}

        out: dict[str, JobInfo] = {}
        for chunk in _chunks(unique_ids, _BATCH_GET_JOBS_CHUNK):
            await self._batch_lookup_chunk(chunk, cluster=cluster, out=out)
        return out

    async def _batch_lookup_chunk(
        self,
        chunk: list[str],
        *,
        cluster: str | None,
        out: dict[str, JobInfo],
    ) -> None:
        squeue_rows: dict[str, JobInfo] = {}
        if self._cluster_profile.supports_squeue_json:
            cmd = build_squeue_json_many(chunk, cluster=cluster)
            r = await self._run(cmd, operation="squeue", check=False, retry_safe=True)
            if r.exit_status == 0 and r.stdout.strip():
                try:
                    payload = json.loads(r.stdout)
                except json.JSONDecodeError:
                    payload = {}
                squeue_rows = parse_squeue_json_all(payload, cluster=cluster)
        # Match ids to canonical row keys. A bare parent id may come back as
        # one or more `<parent>_<task>` rows; record the first such match
        # under the parent id so the caller's lookup table behaves intuitively.
        for jid in chunk:
            info = squeue_rows.get(jid)
            if info is None and "_" not in jid:
                for key, row in squeue_rows.items():
                    if key.split("_", 1)[0] == jid:
                        info = row.model_copy(update={"slurm_job_id": jid})
                        break
            if info is not None:
                await self._emit_status(info)
                out[jid] = info

        missing = [jid for jid in chunk if jid not in out]
        if not missing:
            return

        if self._cluster_profile.supports_sacct_json:
            cmd = build_sacct_json_many(
                missing,
                lookback_days=self._cluster_profile.sacct_lookback_days,
                cluster=cluster,
            )
            r = await self._run(cmd, operation="sacct", check=False, retry_safe=True)
            if r.exit_status == 0 and r.stdout.strip():
                try:
                    payload = json.loads(r.stdout)
                except json.JSONDecodeError:
                    payload = {}
                sacct_rows = parse_sacct_json_all(payload, cluster=cluster)
                for jid in missing:
                    info = sacct_rows.get(jid)
                    if info is None and "_" not in jid:
                        for key, row in sacct_rows.items():
                            if key.split("_", 1)[0] == jid:
                                info = row.model_copy(update={"slurm_job_id": jid})
                                break
                    if info is not None:
                        await self._emit_status(info)
                        out[jid] = info
            return

        # parsable2 fallback: per-id (no batch form).
        for jid in missing:
            info = await self._sacct_lookup(jid, cluster=cluster)
            if info is not None:
                await self._emit_status(info)
                out[jid] = info

    async def _squeue_lookup(
        self,
        slurm_job_id: str,
        *,
        cluster: str | None,
    ) -> JobInfo | None:
        cmd = build_squeue_json(slurm_job_id, cluster=cluster)
        # squeue exits non-zero when the id isn't in the active queue on some
        # builds; treat any non-zero as "not in squeue" rather than an error.
        result = await self._run(cmd, operation="squeue", check=False, retry_safe=True)
        if result.exit_status != 0:
            return None
        return parse_squeue_json_str(result.stdout, slurm_job_id, cluster=cluster)

    async def _sacct_lookup(
        self,
        slurm_job_id: str,
        *,
        cluster: str | None,
    ) -> JobInfo | None:
        if self._cluster_profile.supports_sacct_json:
            cmd = build_sacct_json(
                slurm_job_id,
                lookback_days=self._cluster_profile.sacct_lookback_days,
                cluster=cluster,
            )
            result = await self._run(cmd, operation="sacct", check=False, retry_safe=True)
            if result.exit_status != 0:
                return None
            return parse_sacct_json_str(result.stdout, slurm_job_id, cluster=cluster)

        # fallback: --parsable2
        cmd = build_sacct_parsable2(
            slurm_job_id,
            lookback_days=self._cluster_profile.sacct_lookback_days,
            cluster=cluster,
        )
        result = await self._run(cmd, operation="sacct", check=False, retry_safe=True)
        if result.exit_status != 0:
            return None
        return parse_sacct_parsable2(result.stdout, slurm_job_id, cluster=cluster)

    def _validate_job_id(self, slurm_job_id: str) -> None:
        """Validate `slurm_job_id` against the active ClusterProfile.

        - Plain numeric ids (``"12345"``) are always accepted.
        - Array task suffixes (``"12345_3"``) and parent-with-glob (``"12345_*"``)
          are only accepted when the cluster profile sets
          ``supports_array_jobs=True``.
        """
        if not isinstance(slurm_job_id, str):
            raise InvalidJobSpec("slurm_job_id must be a string")
        if self._cluster_profile.supports_array_jobs:
            if not is_valid_array_job_id(slurm_job_id):
                raise InvalidJobSpec(
                    f"job id {slurm_job_id!r} is not a Slurm job id "
                    f"(plain or `<id>_<task>`)"
                )
        else:
            if not is_valid_plain_job_id(slurm_job_id):
                raise InvalidJobSpec(
                    f"job id {slurm_job_id!r} is not a plain numeric Slurm job id; "
                    "set ClusterProfile.supports_array_jobs=True to accept array suffixes"
                )

    def _handle_missing(
        self,
        slurm_job_id: str,
        *,
        cluster: str | None,
        submitted_at: datetime | str | None,
    ) -> JobInfo:
        grace = self._cluster_profile.submit_grace_window_seconds
        ts = _parse_submitted_at(submitted_at)
        if ts is not None:
            elapsed = (datetime.now(timezone.utc) - ts).total_seconds()
            if elapsed < grace:
                return JobInfo(
                    slurm_job_id=str(slurm_job_id),
                    cluster=cluster,
                    lifecycle="unknown",
                    source="none",
                    visibility="transient_gap",
                )
        raise SlurmJobNotFound(
            f"slurm_job_id {slurm_job_id!r} was not found in squeue or sacct "
            f"(lookback={self._cluster_profile.sacct_lookback_days}d)"
        )

    async def _emit_status(self, info: JobInfo) -> None:
        await self._hooks.on_status_checked(
            StatusCheckedEvent(
                slurm_job_id=info.slurm_job_id,
                source=info.source,
                lifecycle=info.lifecycle,
                visibility=info.visibility,
                checked_at=_utc_now_iso(),
            )
        )

    async def tail_stdout(self, job: SubmittedJob, *, lines: int = 200) -> LogChunk:
        return await self._tail(job.stdout_path, lines=lines)

    async def tail_stderr(self, job: SubmittedJob, *, lines: int = 200) -> LogChunk:
        return await self._tail(job.stderr_path, lines=lines)

    async def _tail(self, path: str, *, lines: int) -> LogChunk:
        cmd = build_tail(path, lines=lines)
        result = await self._run(cmd, operation="tail", check=False, retry_safe=True)
        # `test -f X && tail` exits non-zero (typically 1) when X doesn't exist;
        # we interpret any non-zero exit as a missing file.
        if result.exit_status != 0:
            return LogChunk(
                path=path,
                content="",
                exists=False,
                lines_requested=lines,
                fetched_at=_utc_now_iso(),
                note="file not yet present on the cluster",
            )
        return LogChunk(
            path=path,
            content=result.stdout,
            exists=True,
            lines_requested=lines,
            fetched_at=_utc_now_iso(),
        )

    # --- Phase 3: capability detection ---

    async def detect_capabilities(self) -> CapabilityReport:
        """Probe the cluster for runtime features.

        Diagnostic only — never mutates ClusterProfile. Each probe runs
        independently; one failing probe does not abort the others.
        """
        raw: dict[str, Any] = {}
        warnings: list[str] = []

        version = await self._probe_version(raw)
        squeue_ok = await self._probe_squeue_json(raw)
        sacct_ok = await self._probe_sacct_json(raw)
        parsable2_ok = await self._probe_sacct_parsable2(raw)
        sftp_ok = await self._probe_sftp(raw, warnings)

        report = CapabilityReport(
            supports_squeue_json=squeue_ok,
            supports_sacct_json=sacct_ok,
            supports_sacct_parsable2=parsable2_ok,
            supports_sftp=sftp_ok,
            slurm_version=version,
            warnings=warnings,
            raw=raw,
        )
        report.warnings.extend(
            reconcile_with_profile(
                report,
                profile_supports_squeue_json=self._cluster_profile.supports_squeue_json,
                profile_supports_sacct_json=self._cluster_profile.supports_sacct_json,
            )
        )
        return report

    async def _probe_version(self, raw: dict[str, Any]) -> str | None:
        try:
            r = await self._run(
                "sinfo --version",
                operation="probe.sinfo_version",
                check=False,
                retry_safe=True,
            )
        except Exception as e:
            raw["sinfo_version_error"] = str(e)
            return None
        if r.exit_status != 0:
            raw["sinfo_version_exit"] = r.exit_status
            return None
        text = r.stdout.strip()
        raw["sinfo_version"] = text
        return text or None

    async def _probe_squeue_json(self, raw: dict[str, Any]) -> bool:
        # Scope the probe to the SSH user's own jobs. Cluster-wide squeue can
        # produce megabytes of JSON on a busy site, making the probe needlessly
        # slow (and prone to timing out behind a 60s command budget).
        try:
            r = await self._run(
                'squeue --json --me 2>/dev/null || squeue --json -u "$USER"',
                operation="probe.squeue_json",
                check=False,
                retry_safe=True,
            )
        except Exception as e:
            raw["squeue_json_error"] = str(e)
            return False
        raw["squeue_json_exit"] = r.exit_status
        if r.exit_status != 0 or not r.stdout.strip():
            return False
        try:
            json.loads(r.stdout)
        except json.JSONDecodeError:
            return False
        return True

    async def _probe_sacct_json(self, raw: dict[str, Any]) -> bool:
        try:
            r = await self._run(
                "sacct --json --starttime=now-1hours --noheader",
                operation="probe.sacct_json",
                check=False,
                retry_safe=True,
            )
        except Exception as e:
            raw["sacct_json_error"] = str(e)
            return False
        raw["sacct_json_exit"] = r.exit_status
        if r.exit_status != 0 or not r.stdout.strip():
            return False
        try:
            json.loads(r.stdout)
        except json.JSONDecodeError:
            return False
        return True

    async def _probe_sacct_parsable2(self, raw: dict[str, Any]) -> bool:
        try:
            r = await self._run(
                "sacct --parsable2 --starttime=now-1hours --noheader --format=JobID,State",
                operation="probe.sacct_parsable2",
                check=False,
                retry_safe=True,
            )
        except Exception as e:
            raw["sacct_parsable2_error"] = str(e)
            return False
        raw["sacct_parsable2_exit"] = r.exit_status
        return r.exit_status == 0

    async def _probe_sftp(self, raw: dict[str, Any], warnings: list[str]) -> bool:
        # SFTP probe is best-effort: write a tiny marker, read it back,
        # delete it. Done under remote_base_dir/jobs/.probe so we stay inside
        # a path the user already trusts.
        marker_dir = f"{self._remote_base_dir.rstrip('/')}/jobs"
        probe_path = f"{marker_dir}/.slurmly_sftp_probe"
        try:
            await self._run(
                build_mkdir_p(marker_dir),
                operation="probe.mkdir",
                check=True,
                retry_safe=True,
            )
            await self._upload(probe_path, "probe\n", mode=0o600)
        except Exception as e:
            raw["sftp_error"] = str(e)
            warnings.append(f"sftp upload probe failed: {e}")
            return False
        # Cleanup is best-effort; failure here doesn't change the answer.
        try:
            await self._run(
                f"rm -f {shlex.quote(probe_path)}",
                operation="probe.cleanup",
                check=False,
                retry_safe=True,
            )
        except Exception:
            pass
        raw["sftp_probe_path"] = probe_path
        return True

    # --- Phase 3: cleanup ---

    async def cleanup_remote_job_dir(
        self, job: SubmittedJob, *, dry_run: bool = False
    ) -> CleanupResult:
        """Remove a single job's remote directory.

        The path is validated against `remote_base_dir/jobs/`. `dry_run=True`
        returns the would-be operation without touching the cluster.
        """
        path = job.remote_job_dir
        result = CleanupResult(dry_run=dry_run)
        try:
            assert_safe_to_remove(path, remote_base_dir=self._remote_base_dir)
        except InvalidConfig as e:
            result.errors.append(f"{path}: {e}")
            return result
        if dry_run:
            result.removed.append(path)
            return result
        try:
            await self._run(
                build_rm_rf(path), operation="cleanup", check=True, retry_safe=True
            )
            result.removed.append(path)
        except Exception as e:
            result.errors.append(f"{path}: {e}")
        return result

    async def plan_cleanup(self, *, older_than_days: float) -> CleanupPlan:
        """Enumerate per-job directories older than `older_than_days`.

        Uses `find -mindepth 1 -maxdepth 1 -type d -mtime +N` under
        `<remote_base_dir>/jobs/`. Output is one absolute path per line.
        """
        cmd = build_find_stale(
            self._remote_base_dir, older_than_days=older_than_days
        )
        result = await self._run(cmd, operation="cleanup.plan", check=False, retry_safe=True)
        candidates: list[CleanupCandidate] = []
        if result.exit_status == 0:
            for line in result.stdout.splitlines():
                p = line.strip()
                if not p:
                    continue
                try:
                    assert_safe_to_remove(p, remote_base_dir=self._remote_base_dir)
                except InvalidConfig:
                    continue
                candidates.append(
                    CleanupCandidate(path=p, reason=f">{int(older_than_days)}d old")
                )
        return CleanupPlan(
            remote_base_dir=self._remote_base_dir,
            older_than_days=older_than_days,
            candidates=candidates,
            raw={"exit_status": result.exit_status, "stderr": result.stderr},
        )

    async def cleanup(
        self, plan: CleanupPlan, *, dry_run: bool = False
    ) -> CleanupResult:
        """Execute (or dry-run) a CleanupPlan.

        Each candidate is validated again at execution time — a plan stale
        from a different remote_base_dir cannot accidentally widen scope.
        """
        result = CleanupResult(dry_run=dry_run)
        for candidate in plan.candidates:
            try:
                assert_safe_to_remove(
                    candidate.path, remote_base_dir=self._remote_base_dir
                )
            except InvalidConfig as e:
                result.skipped.append(f"{candidate.path}: {e}")
                continue
            if dry_run:
                result.removed.append(candidate.path)
                continue
            try:
                await self._run(
                    build_rm_rf(candidate.path),
                    operation="cleanup",
                    check=True,
                    retry_safe=True,
                )
                result.removed.append(candidate.path)
            except Exception as e:
                result.errors.append(f"{candidate.path}: {e}")
        return result

    # --- Phase 4: artifacts ---

    async def list_artifacts(
        self,
        job: SubmittedJob,
        *,
        pattern: str = "*",
    ) -> list[Artifact]:
        """List files produced under ``job.remote_job_dir`` matching ``pattern``.

        Implementation: ``find <job_dir> -type f -name <pattern>``. The job
        directory must live under ``<remote_base_dir>/jobs/`` — otherwise
        InvalidConfig is raised before the cluster is touched. Symlinks that
        point outside the job dir are not protected against here.
        """
        assert_artifact_path_safe(job.remote_job_dir, remote_base_dir=self._remote_base_dir)
        cmd = build_find_files(job.remote_job_dir, pattern=pattern)
        result = await self._run(
            cmd, operation="artifact.list", check=False, retry_safe=True
        )
        artifacts: list[Artifact] = []
        if result.exit_status != 0:
            return artifacts
        for line in result.stdout.splitlines():
            path = line.strip()
            if not path:
                continue
            try:
                assert_artifact_path_safe(path, remote_base_dir=self._remote_base_dir)
            except InvalidConfig:
                continue
            artifacts.append(Artifact(path=path, name=path.rsplit("/", 1)[-1]))
        return artifacts

    async def download_artifact(
        self,
        job: SubmittedJob,
        name: str,
        *,
        max_bytes: int | None = None,
    ) -> ArtifactDownload:
        """Read a file inside ``job.remote_job_dir`` and return its contents.

        ``name`` is a *relative* path beneath the job directory; absolute
        paths and ``..`` segments are rejected. ``max_bytes`` caps the read
        size at the transport layer; the returned ``truncated`` flag reflects
        whether the cap was hit (best-effort: equality of returned size and
        cap implies truncation).
        """
        path = resolve_artifact_path(
            remote_job_dir=job.remote_job_dir,
            name=name,
            remote_base_dir=self._remote_base_dir,
        )
        # A missing file surfaces as SSHTransportError from the transport's
        # SFTP wrapper (or FileNotFoundError from FakeTransport in tests).
        # We deliberately don't translate either: callers checking artifact
        # availability should prefer `list_artifacts(job, pattern=name)`,
        # which returns an empty list rather than raising.
        async with self._semaphore:
            data = await self._transport.read_text(path, max_bytes=max_bytes)
        truncated = (
            max_bytes is not None and len(data) >= max_bytes
        )
        return ArtifactDownload(
            path=path,
            content=data,
            fetched_at=_utc_now_iso(),
            bytes_requested=max_bytes,
            truncated=truncated,
        )

    # --- Phase 3: polling ---

    async def poll_job(
        self,
        slurm_job_id: str,
        *,
        cluster: str | None = None,
        submitted_at: datetime | str | None = None,
        policy: PollingPolicy | None = None,
    ) -> PollingResult:
        """Single poll step — wraps `get_job` with transient/terminal classification.

        Callers run their own scheduler; this helper just turns one observation
        into actionable signal: should we keep polling, and if so, when next?
        """
        policy = policy or PollingPolicy()
        try:
            info = await self.get_job(
                slurm_job_id, cluster=cluster, submitted_at=submitted_at
            )
        except (SlurmJobNotFound, InvalidJobSpec) as e:
            # Permanent errors: not-found past the grace window, malformed
            # job ids (e.g. array suffixes). A poller must NOT retry these.
            return PollingResult(
                slurm_job_id=str(slurm_job_id),
                error=str(e),
                error_type=type(e).__name__,
                should_retry=False,
                transient=False,
            )
        except SlurmlyError as e:
            return PollingResult(
                slurm_job_id=str(slurm_job_id),
                error=str(e),
                error_type=type(e).__name__,
                should_retry=True,
                transient=True,
            )

        ts = _parse_submitted_at(submitted_at)
        age = age_seconds(ts) if ts is not None else 0.0
        next_int = policy.next_interval(age_seconds=age)

        if info.visibility == "transient_gap":
            return PollingResult(
                slurm_job_id=str(slurm_job_id),
                info=info,
                should_retry=True,
                transient=True,
                next_interval_seconds=policy.fresh_submit_interval_seconds,
            )
        if is_terminal(info):
            return PollingResult(
                slurm_job_id=str(slurm_job_id),
                info=info,
                should_retry=False,
                transient=False,
                next_interval_seconds=None,
            )
        return PollingResult(
            slurm_job_id=str(slurm_job_id),
            info=info,
            should_retry=True,
            transient=False,
            next_interval_seconds=next_int,
        )

    # --- internals ---

    def _apply_account_default(self, spec: JobSpec) -> JobSpec:
        """Inject `slurm.account` from config when the spec didn't set one.

        Spec §8.1: `account` must be present in either config or JobSpec; this
        is the place that materializes that requirement at submit/render time.
        """
        if spec.account is not None:
            return spec
        account = self._account or self._cluster_profile.default_account
        if account is None:
            raise InvalidConfig(
                "no account is set: provide JobSpec.account, "
                "SlurmSSHClient(account=...), slurm.account in config, "
                "or ClusterProfile.default_account"
            )
        return spec.model_copy(update={"account": account})

    def _resolve_execution_profile(self, name: str | None) -> ExecutionProfile | None:
        if name is None:
            return None
        if name not in self._execution_profiles:
            raise InvalidConfig(
                f"execution_profile {name!r} is not defined; "
                f"known: {sorted(self._execution_profiles)}"
            )
        return self._execution_profiles[name]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("chunk size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_submitted_at(value: datetime | str | None) -> datetime | None:
    """Accept either a datetime or an ISO 8601 string (with `Z` suffix).

    `SubmittedJob.submitted_at` is an ISO string ending in `Z`; Python <3.11's
    `datetime.fromisoformat` doesn't accept the `Z` shorthand, so normalize
    to `+00:00` first.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _job_metadata(*, internal_job_id: str, spec: JobSpec, paths: dict[str, str]) -> dict[str, Any]:
    return {
        "internal_job_id": internal_job_id,
        "submitted_at": _utc_now_iso(),
        "paths": paths,
        "spec": spec.model_dump(exclude_none=True),
    }
