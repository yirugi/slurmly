"""End-to-end lifecycle scenarios driven by FakeTransport.

These exercise the chain submit → poll → tail → cleanup (plus arrays,
dependencies, cancel, and get_jobs) in a single FakeTransport per test.
The point is to catch sequencing/ordering bugs that per-method unit tests
can miss.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from slurmly import (
    JobDependency,
    JobSpec,
    PollingPolicy,
    SlurmSSHClient,
    SubmittedJob,
)
from slurmly.exceptions import InvalidConfig
from slurmly.polling import is_terminal
from slurmly.profiles import ClusterProfile
from slurmly.testing import FakeTransport


BASE = "/scratch/u/slurmly"


def _client(transport: FakeTransport, **kwargs) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="test",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
        submit_grace_window_seconds=120,
        supports_array_jobs=kwargs.pop("supports_array_jobs", False),
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir=BASE,
        account="acct1",
        **kwargs,
    )


def _running_payload(job_id: int) -> str:
    return json.dumps({
        "jobs": [{
            "job_id": job_id,
            "name": "x",
            "user_name": "u",
            "partition": "shared",
            "account": "acct1",
            "qos": "cpu",
            "job_state": ["RUNNING"],
            "array_job_id": {"set": True, "infinite": False, "number": 0},
            "array_task_id": {"set": False, "infinite": False, "number": 0},
        }]
    })


def _sacct_completed(job_id: int, raw_state: str = "COMPLETED") -> str:
    return json.dumps({
        "jobs": [{
            "job_id": job_id,
            "name": "x",
            "state": {"current": [raw_state]},
            "exit_code": {"return_code": {"number": 0}},
            "time": {"elapsed": 5},
        }]
    })


# --- 1. Single job full lifecycle ---------------------------------------


async def test_full_lifecycle_submit_poll_tail_cleanup():
    """submit → poll-running → poll-completed → tail → list_artifacts → cleanup.

    Verifies the whole pipeline composes correctly and order-sensitive
    operations (mkdir before sbatch, sbatch before poll, etc.) line up.
    """
    sbatch_id = 16680647
    job_dir_holder = {"path": None}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            # Capture the job dir we just created.
            target = cmd[len("mkdir -p "):].strip().strip("'")
            job_dir_holder["path"] = target
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            return (0, f"{sbatch_id}\n", "")
        if "squeue --json" in cmd:
            return (0, _running_payload(sbatch_id), "")
        if "sacct --json" in cmd:
            return (0, _sacct_completed(sbatch_id), "")
        if "tail" in cmd and "test -f" in cmd:
            return (0, "hello\nbye\n", "")
        if cmd.startswith("find "):  # list_artifacts
            assert job_dir_holder["path"]
            return (0, f"{job_dir_holder['path']}/run.sh\n{job_dir_holder['path']}/stdout.log\n", "")
        if cmd.startswith("rm -rf "):
            return (0, "", "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    job = await client.submit(JobSpec(name="t", command=["echo", "hi"], partition="shared"))
    assert job.slurm_job_id == str(sbatch_id)

    # Poll until terminal.
    seen_running = False
    seen_terminal = False
    for _ in range(5):
        result = await client.poll_job(
            job.slurm_job_id, submitted_at=job.submitted_at,
            policy=PollingPolicy(fresh_submit_interval_seconds=1, normal_interval_seconds=1),
        )
        assert result.error is None
        if result.info and result.info.lifecycle == "running":
            seen_running = True
            # Force-flip the handler so the next squeue returns empty -> sacct succeeds.
            def handler2(cmd: str):
                if "squeue --json" in cmd:
                    return (0, '{"jobs": []}', "")
                return handler(cmd)
            transport.handler = handler2
            continue
        if result.info and is_terminal(result.info):
            seen_terminal = True
            break
    assert seen_running and seen_terminal

    # Tail.
    log = await client.tail_stdout(job)
    assert log.exists is True
    assert "hello" in log.content

    # List + cleanup.
    arts = await client.list_artifacts(job)
    assert {a.name for a in arts} == {"run.sh", "stdout.log"}
    cleanup = await client.cleanup_remote_job_dir(job)
    assert cleanup.errors == []
    assert cleanup.removed == [job.remote_job_dir]


# --- 2. Dependency chain ------------------------------------------------


async def test_dependency_chain_a_then_b():
    """A submit → B (afterok:A) submit → script for B contains the typed dep."""
    state = {"submit_count": 0}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            state["submit_count"] += 1
            return (0, f"{1000 + state['submit_count']}\n", "")
        if "squeue --json" in cmd:
            return (0, '{"jobs": []}', "")
        if "sacct --json" in cmd:
            # Find which job id is being asked about.
            for jid in ("1001", "1002"):
                if f"jobs={jid}" in cmd or f"--jobs={jid}" in cmd:
                    return (0, _sacct_completed(int(jid)), "")
            return (0, '{"jobs": []}', "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    a = await client.submit(JobSpec(name="a", command=["echo", "a"], partition="shared"))
    b = await client.submit(JobSpec(
        name="b", command=["echo", "b"], partition="shared",
        dependency=JobDependency(type="afterok", job_ids=[a.slurm_job_id]),
    ))
    assert a.slurm_job_id == "1001"
    assert b.slurm_job_id == "1002"

    # The rendered script for B must carry --dependency=afterok:1001
    scripts = [u for u in transport.uploads if u.path.endswith("/run.sh")]
    assert len(scripts) == 2
    assert "--dependency" not in scripts[0].content
    assert "#SBATCH --dependency=afterok:1001" in scripts[1].content

    # Both reach terminal via sacct.
    for j in (a, b):
        info = await client.get_job(j.slurm_job_id, submitted_at=j.submitted_at)
        assert info.lifecycle == "succeeded"


# --- 3. Cancel mid-flight ------------------------------------------------


async def test_cancel_mid_flight_then_terminal_via_sacct():
    """submit → poll(running) → cancel → poll(cancelled via sacct) → cleanup."""
    state = {"cancelled": False}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            return (0, "777\n", "")
        if "scancel" in cmd:
            state["cancelled"] = True
            return (0, "", "")
        if "squeue --json" in cmd:
            if state["cancelled"]:
                return (0, '{"jobs": []}', "")
            return (0, _running_payload(777), "")
        if "sacct --json" in cmd:
            return (0, _sacct_completed(777, raw_state="CANCELLED"), "")
        if cmd.startswith("rm -rf "):
            return (0, "", "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    job = await client.submit(JobSpec(name="t", command=["sleep", "60"], partition="shared"))
    info = await client.get_job(job.slurm_job_id, submitted_at=job.submitted_at)
    assert info.lifecycle == "running"

    await client.cancel(job.slurm_job_id)
    info = await client.get_job(job.slurm_job_id, submitted_at=job.submitted_at)
    assert info.lifecycle == "cancelled"
    cleanup = await client.cleanup_remote_job_dir(job)
    assert cleanup.errors == []


# --- 4. get_jobs batch lifecycle ---------------------------------------


async def test_get_jobs_lifecycle_three_submits_then_one_batch_query():
    """Three independent submits, then one get_jobs call resolves all."""
    state = {"counter": 100}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            state["counter"] += 1
            return (0, f"{state['counter']}\n", "")
        if "squeue --json" in cmd:
            # Return all three as RUNNING in one shot (assume same chunk).
            jobs = []
            for i in (101, 102, 103):
                if str(i) in cmd:
                    jobs.append(json.loads(_running_payload(i))["jobs"][0])
            return (0, json.dumps({"jobs": jobs}), "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    jobs: list[SubmittedJob] = []
    for n in ("a", "b", "c"):
        jobs.append(await client.submit(JobSpec(
            name=n, command=["echo", n], partition="shared",
        )))
    out = await client.get_jobs([j.slurm_job_id for j in jobs])
    assert set(out.keys()) == {"101", "102", "103"}
    assert all(info.lifecycle == "running" for info in out.values())
    # One squeue call (all three in a single chunk).
    squeue_calls = [r for r in transport.runs if "squeue --json" in r.command]
    assert len(squeue_calls) == 1


# --- 5. Submit failure → second resubmit succeeds ----------------------


async def test_submit_failure_then_resubmit_succeeds():
    """SbatchError on first attempt; same spec succeeds on retry."""
    from slurmly.exceptions import SbatchError

    state = {"first": True}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            if state["first"]:
                state["first"] = False
                return (1, "", "Invalid partition\n")
            return (0, "999\n", "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    spec = JobSpec(name="t", command=["x"], partition="shared")
    try:
        await client.submit(spec)
        assert False, "expected SbatchError"
    except SbatchError as e:
        assert "Invalid partition" in str(e)
    job = await client.submit(spec)
    assert job.slurm_job_id == "999"


# --- 6. Polling grace-window scenario ----------------------------------


async def test_polling_transient_then_visible():
    """Job invisible right after submit (transient_gap), then squeue picks it up."""
    state = {"calls": 0}

    def handler(cmd: str):
        if cmd.startswith("mkdir -p "):
            return (0, "", "")
        if "sbatch --parsable" in cmd:
            return (0, "55\n", "")
        if "squeue --json" in cmd:
            state["calls"] += 1
            if state["calls"] <= 1:
                return (0, '{"jobs": []}', "")
            return (0, _running_payload(55), "")
        if "sacct --json" in cmd:
            return (0, '{"jobs": []}', "")
        return (0, "", "")

    transport = FakeTransport(handler=handler)
    client = _client(transport)
    job = await client.submit(JobSpec(name="t", command=["x"], partition="shared"))
    # First poll: gap is transient, within grace window.
    r1 = await client.poll_job(job.slurm_job_id, submitted_at=job.submitted_at)
    assert r1.transient is True
    assert r1.should_retry is True
    # Second poll: squeue now responds with running.
    r2 = await client.poll_job(job.slurm_job_id, submitted_at=job.submitted_at)
    assert r2.transient is False
    assert r2.info.lifecycle == "running"


# --- 7. Cleanup plan from a different remote_base_dir is rejected -------


async def test_cleanup_plan_from_different_base_is_rejected_at_execute():
    """Plan paths re-validated at execute time — different base = skipped."""
    from slurmly import CleanupCandidate, CleanupPlan

    plan = CleanupPlan(
        remote_base_dir="/elsewhere",
        older_than_days=7,
        candidates=[CleanupCandidate(path="/elsewhere/jobs/slurmly-x")],
    )
    transport = FakeTransport()
    client = _client(transport)
    result = await client.cleanup(plan)
    assert result.skipped and "not under remote_base_dir" in result.skipped[0]
    assert result.removed == []
    assert transport.runs == []
