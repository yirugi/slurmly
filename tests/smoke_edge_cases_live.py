"""Edge-case live smoke test against Purdue Anvil.

Each case is independent: a failure in one does not abort the rest.
Every submitted directory is cleaned up at the end (best-effort even on
exception). Final summary prints PASS / FAIL / SKIP per case.

Quick cases (each <60s):
  1. OOM kill              -> lifecycle=failed, raw_state=OUT_OF_MEMORY, exit_code='0:9'
  2. Invalid partition     -> SbatchError with "Invalid partition" in stderr
  3. Invalid account       -> SbatchError with account-related stderr
  4. Cancel terminated job -> no exception
  5. Download missing file -> SSHTransportError
  6. List artifacts no match -> empty list
  7. afternotok dependency -> dep job runs after first job fails
  8. heredoc upload parity -> same submit works with upload_method='heredoc'

Long case (~5 min):
  9. Array job 0-9%2 (sleep 60 each) — polling adaptation + per-task artifacts
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from slurmly import (  # noqa: E402
    JobDependency,
    JobSpec,
    PollingPolicy,
    SlurmSSHClient,
    SubmittedJob,
    get_preset,
)
from slurmly.exceptions import (  # noqa: E402
    SbatchError,
    SlurmJobNotFound,
    SSHTransportError,
)
from slurmly.ssh.transport import AsyncSSHTransport, SSHConfig  # noqa: E402
from slurmly.testing import FakeTransport  # noqa: E402  (not used; placeholder)


HOME = os.path.expanduser("~")
USERNAME = "x-openghm"
KEY_PATH = f"{HOME}/.ssh/openghm_anvil_ed25519"
KNOWN_HOSTS_PATH = f"{HOME}/.ssh/known_hosts"
ACCOUNT = "ees260008"
REMOTE_BASE_DIR = f"/anvil/scratch/{USERNAME}/slurmly_edge_smoke"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _section(label: str) -> None:
    print(f"\n=== [{_now()}] {label} ===")


def _make_client(*, upload_method: str = "sftp", max_concurrent: int = 4) -> SlurmSSHClient:
    preset = get_preset("purdue_anvil")
    transport = AsyncSSHTransport(SSHConfig(
        host="login05.anvil.rcac.purdue.edu",
        username=USERNAME,
        key_path=KEY_PATH,
        known_hosts_path=KNOWN_HOSTS_PATH,
        connect_timeout_seconds=15.0,
        command_timeout_seconds=60.0,
        upload_method=upload_method,
    ))
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=preset.cluster_profile,
        remote_base_dir=REMOTE_BASE_DIR,
        execution_profiles=preset.execution_profiles,
        account=ACCOUNT,
        max_concurrent_commands=max_concurrent,
    )


async def _wait_terminal(
    client: SlurmSSHClient,
    slurm_job_id: str,
    submitted_at: str | None,
    *,
    label: str,
    max_iters: int = 240,
    policy: PollingPolicy | None = None,
    progress_every: int = 6,
) -> str:
    """Poll until terminal. Print one line every `progress_every` iterations."""
    policy = policy or PollingPolicy(
        fresh_submit_interval_seconds=3,
        fresh_submit_window_seconds=60,
        normal_interval_seconds=8,
    )
    last_lifecycle = "unknown"
    for i in range(max_iters):
        res = await client.poll_job(
            slurm_job_id, submitted_at=submitted_at, policy=policy
        )
        lifecycle = res.info.lifecycle if res.info else "no-info"
        last_lifecycle = lifecycle
        if i % progress_every == 0 or not res.should_retry:
            print(
                f"[{_now()}] {label} {slurm_job_id} "
                f"transient={res.transient} lifecycle={lifecycle} "
                f"interval={res.next_interval_seconds}"
            )
        if not res.should_retry:
            return last_lifecycle
        await asyncio.sleep(res.next_interval_seconds or 5)
    raise SystemExit(f"timed out waiting for {slurm_job_id}")


# --- per-case runners ----------------------------------------------------

# Each case: (name, async fn) -> "PASS" | "FAIL: ..." | "SKIP: ..."
# The fn appends any submitted SubmittedJob to `submitted` for cleanup.


async def case_oom(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    spec = JobSpec(
        name="edge-oom",
        command=["python3", "-c",
                 "x = bytearray(500 * 1024 * 1024); print(len(x))"],
        partition="shared",
        time_limit="00:02:00",
        memory="50M",
        cpus_per_task=1,
    )
    job = await client.submit(spec)
    submitted.append(job)
    lc = await _wait_terminal(client, job.slurm_job_id, job.submitted_at,
                              label="oom", max_iters=120)
    info = await client.get_job(job.slurm_job_id, submitted_at=job.submitted_at)
    if lc != "failed":
        return f"FAIL: expected lifecycle=failed, got {lc}"
    raw = info.raw_state or ""
    # Anvil reports OOM as raw_state="OUT_OF_MEMORY" (sometimes "FAILED" + signal=9).
    if raw not in ("OUT_OF_MEMORY", "FAILED"):
        return f"FAIL: expected raw_state OUT_OF_MEMORY/FAILED, got {raw!r}"
    if info.exit_code and not (info.exit_code.endswith(":9") or "OUT_OF_MEMORY" in raw):
        return f"FAIL: exit_code={info.exit_code} raw_state={raw} — neither indicates OOM"
    return f"PASS (raw_state={raw}, exit_code={info.exit_code})"


async def case_invalid_partition(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    # Bypass the client-side allow-list by overriding partition validation:
    # We submit through a freshly-built profile that allows "__nope__".
    from slurmly.profiles import ClusterProfile
    bad_profile = ClusterProfile(
        name="bad",
        default_partition="__nope__",
        allowed_partitions=["__nope__"],
        gpu_directive="gpus_per_node",
        sacct_lookback_days=14,
    )
    bad_client = SlurmSSHClient(
        transport=client._transport,
        cluster_profile=bad_profile,
        remote_base_dir=REMOTE_BASE_DIR,
        account=ACCOUNT,
    )
    spec = JobSpec(
        name="edge-bad-partition",
        command=["echo", "hi"],
        partition="__nope__",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
    )
    try:
        job = await bad_client.submit(spec)
        submitted.append(job)
        return f"FAIL: expected SbatchError, got slurm_job_id={job.slurm_job_id}"
    except SbatchError as e:
        if "partition" not in (e.stderr or "").lower():
            return f"FAIL: SbatchError but stderr lacks 'partition': {e.stderr!r}"
        return f"PASS (stderr: {e.stderr.strip()[:120]!r})"


async def case_invalid_account(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    spec = JobSpec(
        name="edge-bad-account",
        command=["echo", "hi"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
        account="__bogus_acct__",
    )
    try:
        job = await client.submit(spec)
        submitted.append(job)
        return f"FAIL: expected SbatchError, got slurm_job_id={job.slurm_job_id}"
    except SbatchError as e:
        msg = (e.stderr or "").lower()
        if "account" not in msg and "association" not in msg:
            return f"FAIL: stderr lacks 'account': {e.stderr!r}"
        return f"PASS (stderr: {e.stderr.strip()[:120]!r})"


async def case_cancel_terminated(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    spec = JobSpec(
        name="edge-cancel-terminated",
        command=["echo", "fast"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
    )
    job = await client.submit(spec)
    submitted.append(job)
    await _wait_terminal(client, job.slurm_job_id, job.submitted_at,
                         label="cancel-term", max_iters=60)
    # Now scancel the already-completed job — must not raise.
    try:
        result = await client.cancel(job.slurm_job_id)
    except Exception as e:
        return f"FAIL: scancel raised {type(e).__name__}: {e}"
    return f"PASS (raw_stderr={result.raw_stderr.strip()[:80]!r})"


async def case_download_missing(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    spec = JobSpec(
        name="edge-dl-missing",
        command=["echo", "hi"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
    )
    job = await client.submit(spec)
    submitted.append(job)
    await _wait_terminal(client, job.slurm_job_id, job.submitted_at,
                         label="dl-missing", max_iters=60)
    try:
        await client.download_artifact(job, "this_does_not_exist.txt")
    except SSHTransportError as e:
        return f"PASS (got SSHTransportError: {str(e)[:100]!r})"
    except Exception as e:
        return f"FAIL: expected SSHTransportError, got {type(e).__name__}: {e}"
    return "FAIL: expected an exception, got success"


async def case_list_no_match(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    spec = JobSpec(
        name="edge-list-empty",
        command=["echo", "hi"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
    )
    job = await client.submit(spec)
    submitted.append(job)
    await _wait_terminal(client, job.slurm_job_id, job.submitted_at,
                         label="list-empty", max_iters=60)
    arts = await client.list_artifacts(job, pattern="*.does_not_exist")
    if arts:
        return f"FAIL: expected empty, got {[a.name for a in arts]}"
    return "PASS"


async def case_afternotok_dependency(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    """A: exit 1 (failed). B: afternotok:A. B should run to completion."""
    a_spec = JobSpec(
        name="edge-failer",
        command=["bash", "-c", "exit 1"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
    )
    a = await client.submit(a_spec)
    submitted.append(a)
    b_spec = JobSpec(
        name="edge-afternotok",
        command=["echo", "B ran because A failed"],
        partition="shared",
        time_limit="00:01:00",
        memory="128M",
        cpus_per_task=1,
        dependency=JobDependency(type="afternotok", job_ids=[a.slurm_job_id]),
    )
    b = await client.submit(b_spec)
    submitted.append(b)
    a_lc = await _wait_terminal(client, a.slurm_job_id, a.submitted_at,
                                label="A(fail)", max_iters=120)
    b_lc = await _wait_terminal(client, b.slurm_job_id, b.submitted_at,
                                label="B(afternotok)", max_iters=180)
    if a_lc != "failed":
        return f"FAIL: A expected failed, got {a_lc}"
    if b_lc != "succeeded":
        return f"FAIL: B (afternotok) expected succeeded, got {b_lc}"
    return "PASS"


async def case_heredoc_upload(submitted: list[SubmittedJob]) -> str:
    """Build a fresh client with upload_method='heredoc' and verify submit works."""
    heredoc_client = _make_client(upload_method="heredoc", max_concurrent=2)
    try:
        spec = JobSpec(
            name="edge-heredoc",
            command=["bash", "-c", "echo 'heredoc upload works' && echo \"with quote 'x' and \\$VAR literal\""],
            partition="shared",
            time_limit="00:01:00",
            memory="128M",
            cpus_per_task=1,
        )
        job = await heredoc_client.submit(spec)
        submitted.append(job)
        lc = await _wait_terminal(heredoc_client, job.slurm_job_id, job.submitted_at,
                                  label="heredoc", max_iters=60)
        if lc != "succeeded":
            return f"FAIL: expected succeeded, got {lc}"
        out = await heredoc_client.tail_stdout(job, lines=20)
        if "heredoc upload works" not in out.content:
            return f"FAIL: stdout missing expected text: {out.content!r}"
        if "with quote 'x'" not in out.content:
            return f"FAIL: special chars not preserved: {out.content!r}"
        return "PASS"
    finally:
        await heredoc_client.close()


async def case_long_array_5min(client: SlurmSSHClient, submitted: list[SubmittedJob]) -> str:
    """Array 0-9%2, sleep 60 each — ~5 min wallclock."""
    spec = JobSpec(
        name="edge-long-array",
        command_template="echo \"task=$SLURM_ARRAY_TASK_ID start=$(date +%s)\" && sleep 60 && echo \"task=$SLURM_ARRAY_TASK_ID done\"",
        array="0-9%2",
        partition="shared",
        time_limit="00:03:00",
        memory="256M",
        cpus_per_task=1,
    )
    parent = await client.submit(spec)
    submitted.append(parent)
    print(f"  parent slurm_job_id = {parent.slurm_job_id}")

    # Poll the parent until ALL tasks are terminal. Use a long-running policy
    # so we can verify interval adaptation across the run.
    policy = PollingPolicy(
        fresh_submit_interval_seconds=4,
        fresh_submit_window_seconds=30,
        normal_interval_seconds=15,
        long_running_after_seconds=180,
        long_running_interval_seconds=30,
    )
    seen_intervals: set[int] = set()
    task_ids = [f"{parent.slurm_job_id}_{i}" for i in range(10)]
    deadline = asyncio.get_event_loop().time() + 600  # 10-min cap
    last_states: dict[str, str] = {}

    while asyncio.get_event_loop().time() < deadline:
        # Use parent for the polling-interval verification (it picks up the
        # array's overall state).
        res = await client.poll_job(parent.slurm_job_id, submitted_at=parent.submitted_at,
                                    policy=policy)
        if res.next_interval_seconds is not None:
            seen_intervals.add(res.next_interval_seconds)

        # Now check each task individually via batched call.
        infos = await client.get_jobs(task_ids)
        for tid in task_ids:
            info = infos.get(tid)
            if info is None:
                last_states[tid] = "missing"
            else:
                last_states[tid] = info.lifecycle
        n_done = sum(1 for v in last_states.values()
                     if v in ("succeeded", "failed", "cancelled", "timeout"))
        n_running = sum(1 for v in last_states.values() if v == "running")
        n_pending = sum(1 for v in last_states.values() if v == "queued")
        print(f"[{_now()}] long-array done={n_done}/10 running={n_running} "
              f"pending={n_pending} interval={res.next_interval_seconds}")
        if n_done == 10:
            break
        await asyncio.sleep(min(20, res.next_interval_seconds or 15))
    else:
        return f"FAIL: timed out, last_states={last_states}"

    # Verify per-task artifacts exist.
    arts = await client.list_artifacts(parent, pattern="stdout-*")
    stdout_names = sorted(a.name for a in arts)
    print(f"  per-task stdout files ({len(stdout_names)}): {stdout_names}")
    if len(stdout_names) != 10:
        return f"FAIL: expected 10 per-task stdout files, got {len(stdout_names)}"

    # Verify task 5 stdout contains its task id.
    sample_name = next((n for n in stdout_names if "_5" in n), stdout_names[5])
    sample = await client.download_artifact(parent, sample_name)
    if "task=5" not in sample.content:
        return f"FAIL: task-5 stdout missing 'task=5': {sample.content!r}"

    # Verify polling adapted: at least 'fresh' (4s) and one 'normal' or longer interval seen.
    if 4 not in seen_intervals:
        return f"FAIL: never used fresh interval (4s); saw {seen_intervals}"
    if not any(s >= 15 for s in seen_intervals):
        return f"FAIL: never used normal/long interval; saw {seen_intervals}"

    # Final batch must report all 10 succeeded.
    final = await client.get_jobs(task_ids)
    not_ok = {tid: info.lifecycle for tid, info in final.items()
              if info.lifecycle != "succeeded"}
    if not_ok:
        return f"FAIL: not-succeeded tasks: {not_ok}"
    return f"PASS (intervals={sorted(seen_intervals)})"


# --- main runner ---------------------------------------------------------


async def main() -> int:
    client = _make_client()
    submitted: list[SubmittedJob] = []
    results: list[tuple[str, str]] = []

    cases = [
        ("1. OOM kill", lambda: case_oom(client, submitted)),
        ("2. Invalid partition", lambda: case_invalid_partition(client, submitted)),
        ("3. Invalid account", lambda: case_invalid_account(client, submitted)),
        ("4. Cancel terminated", lambda: case_cancel_terminated(client, submitted)),
        ("5. Download missing", lambda: case_download_missing(client, submitted)),
        ("6. List no match", lambda: case_list_no_match(client, submitted)),
        ("7. afternotok dependency", lambda: case_afternotok_dependency(client, submitted)),
        ("8. heredoc upload parity", lambda: case_heredoc_upload(submitted)),
        ("9. Long-running array (~5 min)", lambda: case_long_array_5min(client, submitted)),
    ]

    try:
        for label, fn in cases:
            _section(label)
            try:
                outcome = await fn()
            except Exception as e:
                outcome = f"FAIL (uncaught): {type(e).__name__}: {e}"
            print(f"  -> {outcome}")
            results.append((label, outcome))

        # ---- summary ---------------------------------------------------
        _section("SUMMARY")
        n_pass = sum(1 for _, r in results if r.startswith("PASS"))
        n_fail = sum(1 for _, r in results if r.startswith("FAIL"))
        n_skip = sum(1 for _, r in results if r.startswith("SKIP"))
        for label, outcome in results:
            mark = "✓" if outcome.startswith("PASS") else ("✗" if outcome.startswith("FAIL") else "○")
            print(f"  {mark} {label}: {outcome}")
        print(f"\n  totals: PASS={n_pass} FAIL={n_fail} SKIP={n_skip}")
        return 0 if n_fail == 0 else 1
    finally:
        # Always attempt cleanup.
        _section("cleanup all submitted jobs")
        for j in submitted:
            try:
                try:
                    await client.cancel(j.slurm_job_id)
                except (SlurmJobNotFound, Exception):
                    pass
                res = await client.cleanup_remote_job_dir(j)
                if res.errors:
                    print(f"  partial: {j.remote_job_dir}: {res.errors}")
                else:
                    print(f"  cleaned: {j.remote_job_dir}")
            except Exception as ce:
                print(f"  CLEANUP FAILED for {j.remote_job_dir}: {ce}")
        await client.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
