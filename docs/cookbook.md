# Cookbook

End-to-end recipes. Every example assumes you've built `client` from a config file —
see [configuration.md](configuration.md) and [anvil.md](anvil.md).

---

## 1. Submit and wait

```python
import asyncio
from slurmly import SlurmSSHClient, JobSpec
from slurmly.polling import PollingPolicy, is_terminal

async def run_to_completion():
    async with SlurmSSHClient.from_config("slurmly.yaml") as client:
        job = await client.submit(
            JobSpec(
                name="hello",
                command_template="echo hello && sleep 5 && echo bye",
                partition="shared",
                cpus_per_task=1,
                memory="256M",
                time_limit="00:02:00",
            )
        )
        policy = PollingPolicy(fresh_submit_interval_seconds=3, normal_interval_seconds=10)

        while True:
            result = await client.poll_job(
                job.slurm_job_id,
                submitted_at=job.submitted_at,
                policy=policy,
            )
            if result.info and is_terminal(result.info):
                print(f"done: {result.info.lifecycle} (exit_code={result.info.exit_code})")
                break
            if not result.should_retry:
                print(f"unrecoverable: {result.error}")
                break
            await asyncio.sleep(result.next_interval_seconds or 10)

asyncio.run(run_to_completion())
```

The application owns the loop. `slurmly` does not run a scheduler.

---

## 2. Cancel a running job

```python
result = await client.cancel(job.slurm_job_id)
print(result.requested_at, result.raw_stdout, result.raw_stderr)
```

Send a non-default signal:

```python
await client.cancel(job.slurm_job_id, signal="USR1")
```

Already-terminated jobs do not raise — `scancel` is treated as idempotent.

---

## 3. Tail logs

```python
out = await client.tail_stdout(job, lines=200)
err = await client.tail_stderr(job, lines=200)

if not out.exists:
    print("stdout not yet present on the cluster")
else:
    print(out.content)
```

`tail_stdout` is safe to call before the job has started; `LogChunk.exists=False`
just signals "file not yet there".

---

## 4. Render only (no SSH, no submit)

```python
rendered = client.render_only(JobSpec(
    name="dryrun", command=["python", "train.py"],
    partition="shared", cpus_per_task=4, memory="4G",
    time_limit="00:30:00",
))
print(rendered.script)
```

Same validation as `submit`, but no remote calls. Useful in CI to assert
specs render the way you expect.

---

## 5. Job arrays

```python
job = await client.submit(JobSpec(
    name="batch-infer",
    command_template="python infer.py --shard ${SLURM_ARRAY_TASK_ID}",
    array="0-99%10",                       # 100 tasks, 10 concurrent
    partition="shared",
    cpus_per_task=1,
    memory="512M",
    time_limit="00:05:00",
))
print(job.slurm_job_id)                    # parent id, e.g. "16685309"
```

Rules:

- `array` requires `command_template`. `command: list[str]` is rejected because
  `shlex.join` would single-quote `${SLURM_ARRAY_TASK_ID}`.
- The cluster profile must have `supports_array_jobs=True` (the Anvil preset does).
- Per-task logs are written to `stdout-<parent>_<task>.log` etc. under the job
  directory.

Query a single task:

```python
task_info = await client.get_job(f"{job.slurm_job_id}_3")
print(task_info.array_job_id, task_info.array_task_id, task_info.lifecycle)
```

---

## 6. Typed dependencies

Run a follower only after the parent succeeds:

```python
from slurmly import JobDependency

follower = await client.submit(JobSpec(
    name="aggregate",
    command_template="python aggregate.py",
    dependency=JobDependency(type="afterok", job_ids=[job.slurm_job_id]),
    partition="shared",
    cpus_per_task=1,
    memory="256M",
    time_limit="00:05:00",
))
```

`JobDependency` produces AND-list dependencies (`afterok:1:2:3`). For OR-chains
(Slurm's `?` syntax) and other exotic forms, pass a raw string:

```python
JobSpec(..., dependency="afterok:123?afterany:456")
```

`singleton` dependencies need no ids:

```python
JobDependency(type="singleton")
```

---

## 7. Batched status query

When you have a long list of job ids:

```python
ids = [job.slurm_job_id for job in submitted_jobs]
status_map = await client.get_jobs(ids)

for jid, info in status_map.items():
    print(jid, info.lifecycle, info.exit_code)
```

Internally, this issues one `squeue --jobs=a,b,c` and (for ids `squeue` did not
return) one `sacct --jobs=...` per chunk of ~200 ids. **Missing ids are absent**
from the result — no exceptions.

---

## 8. Artifacts

```python
files = await client.list_artifacts(job, pattern="*.json")
for art in files:
    print(art.name, art.path)

metrics = await client.download_artifact(job, "metrics.json", max_bytes=1_000_000)
print(metrics.truncated, len(metrics.content))
```

`name` must be relative to the job directory. Absolute paths and `..` segments
are rejected.

---

## 9. Plan and execute cleanup

Remove a single job's working directory:

```python
result = await client.cleanup_remote_job_dir(job)              # actually deletes
result = await client.cleanup_remote_job_dir(job, dry_run=True)  # just lists
```

Sweep stale directories:

```python
plan = await client.plan_cleanup(older_than_days=14)
print(f"{len(plan.candidates)} candidates")

# Inspect / filter before executing
risky = [c for c in plan.candidates if c.path.endswith("-do-not-delete")]
plan.candidates = [c for c in plan.candidates if c not in risky]

result = await client.cleanup(plan, dry_run=True)
print(result.removed, result.skipped, result.errors)
```

`older_than_days` is a `float`. `0.5` means "older than 12 hours"; `0.001` is
useful for smoke-testing (~86 seconds). Internally the implementation uses
`find -mmin +<minutes>`.

Each path is **re-validated at execute time** against the client's
`remote_base_dir`, so a plan from a different base is rejected.

---

## 10. Capability detection

Use `detect_capabilities()` once on startup to learn what the cluster actually
supports:

```python
report = await client.detect_capabilities()
print(report.slurm_version)
print(report.supports_squeue_json, report.supports_sacct_json)
for w in report.warnings:
    print("warning:", w)
```

`slurmly` never auto-applies the report to `ClusterProfile`. If the cluster
disagrees with your profile (e.g., `supports_sacct_json=True` in profile, but the
probe shows it's broken), fix the config yourself.

---

## 11. Observability hooks

Wire metrics or structured logging by subclassing `SlurmlyHooks`:

```python
import logging
import time
from slurmly.hooks import (
    SlurmlyHooks,
    CommandStartedEvent, CommandFinishedEvent,
    SubmitSucceededEvent, SubmitFailedEvent,
    StatusCheckedEvent, CancelRequestedEvent,
)

log = logging.getLogger("slurmly")

class LoggingHooks(SlurmlyHooks):
    async def on_command_started(self, event: CommandStartedEvent) -> None:
        log.info("ssh.start op=%s retry_safe=%s", event.operation, event.retry_safe)

    async def on_command_finished(self, event: CommandFinishedEvent) -> None:
        log.info(
            "ssh.done op=%s exit=%s dur=%.3fs error=%s",
            event.operation, event.exit_status, event.duration_seconds, event.error,
        )

    async def on_submit_succeeded(self, event: SubmitSucceededEvent) -> None:
        log.info("submit.ok internal=%s slurm=%s", event.internal_job_id, event.slurm_job_id)

    async def on_submit_failed(self, event: SubmitFailedEvent) -> None:
        log.warning("submit.fail %s: %s", event.error_type, event.error_message)

    async def on_status_checked(self, event: StatusCheckedEvent) -> None:
        log.debug("status %s lifecycle=%s source=%s", event.slurm_job_id, event.lifecycle, event.source)

    async def on_cancel_requested(self, event: CancelRequestedEvent) -> None:
        log.info("cancel %s signal=%s", event.slurm_job_id, event.signal)

client = SlurmSSHClient.from_config("slurmly.yaml", hooks=LoggingHooks())
```

Hooks are async. Exceptions propagate — swallow them in your subclass if you
need fault isolation.

---

## 12. Testing with `FakeTransport`

The `slurmly.testing.FakeTransport` is a drop-in `SSHTransport` that records every
call and returns canned responses. No SSH, no cluster — fast unit tests.

```python
import pytest
from slurmly import SlurmSSHClient, JobSpec, ClusterProfile
from slurmly.testing import FakeTransport

@pytest.mark.asyncio
async def test_submit_records_sbatch():
    transport = FakeTransport(canned_responses=[
        # for sbatch --parsable
        (0, "12345\n", ""),
    ])
    client = SlurmSSHClient(
        transport=transport,
        cluster_profile=ClusterProfile(
            default_partition="shared",
            allowed_partitions=["shared"],
        ),
        remote_base_dir="/scratch/u/slurmly",
        account="acct1",
    )

    job = await client.submit(JobSpec(
        name="t",
        command_template="echo ok",
        partition="shared",
        cpus_per_task=1,
        memory="256M",
        time_limit="00:01:00",
    ))

    assert job.slurm_job_id == "12345"
    # FakeTransport recorded the sbatch invocation
    assert any("sbatch --parsable" in call.command for call in transport.runs)
    # And the upload of the rendered script
    assert any(u.path.endswith("/run.sh") for u in transport.uploads)
```

You can also drive it with a callable for command-dependent responses:

```python
def handler(command: str):
    if command.startswith("sbatch"):
        return (0, "999\n", "")
    if command.startswith("squeue"):
        return (0, '{"jobs":[]}', "")
    return (0, "", "")

transport = FakeTransport(handler=handler)
```

`FakeTransport` exposes:

- `runs: list[RecordedCall]` — every `run()` call (command, timeout, check, retry_safe)
- `uploads: list[RecordedUpload]` — every `upload_text()` call (path, content, mode)
- `reads: list[str]` — every `read_text()` path
- `files: dict[str, str]` — in-memory file store backing `read_text` (and populated by `upload_text`)

---

## 13. Manual reconciliation after a network blip

If `submit` raises mid-flight you don't know whether the job was created. Use
the raw sbatch output preserved on `SubmittedJob` (when you have one) or query
by name:

```python
import asyncio
from slurmly import SbatchError

try:
    job = await client.submit(spec)
except SbatchError as e:
    # Use the job name as the reconciliation key — squeue can filter by name.
    cmd = f"squeue --me --name={spec.name} --json --noheader"
    # Run via your transport directly, or just resubmit if your job is idempotent.
    ...
```

Submission is intentionally non-idempotent: silently re-submitting on connection
loss is a worse failure mode than surfacing the error.

---

## 14. Combining everything: an Anvil end-to-end

This is what `dev/smoke_full_live.py` does at a high level — submit a few jobs,
poll, get logs, exercise arrays/dependencies, batch-status, artifacts, and
cleanup:

```python
import asyncio
from slurmly import SlurmSSHClient, JobSpec, JobDependency
from slurmly.polling import is_terminal

async def main():
    async with SlurmSSHClient.from_config("slurmly.yaml") as client:
        # 1. Submit two jobs
        a = await client.submit(JobSpec(
            name="a", command_template="sleep 3 && echo a-done",
            partition="shared", cpus_per_task=1, memory="256M",
            time_limit="00:01:00",
        ))
        b = await client.submit(JobSpec(
            name="b-after-a", command_template="echo b-after-a",
            dependency=JobDependency(type="afterok", job_ids=[a.slurm_job_id]),
            partition="shared", cpus_per_task=1, memory="256M",
            time_limit="00:01:00",
        ))

        # 2. Wait for both via batched status
        while True:
            statuses = await client.get_jobs([a.slurm_job_id, b.slurm_job_id])
            if all(is_terminal(info) for info in statuses.values()):
                break
            await asyncio.sleep(5)

        # 3. Inspect outputs
        for job in (a, b):
            logs = await client.tail_stdout(job, lines=20)
            print(f"{job.slurm_job_id}:\n{logs.content}")

        # 4. List + download artifacts
        for job in (a, b):
            arts = await client.list_artifacts(job, pattern="*")
            print(job.slurm_job_id, [a.name for a in arts])

        # 5. Cleanup
        for job in (a, b):
            await client.cleanup_remote_job_dir(job)

asyncio.run(main())
```
