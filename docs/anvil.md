# Purdue RCAC Anvil Guide

This page covers Anvil-specific setup: SSH, allocation, scratch path, partitions,
modules, and common pitfalls. Last reviewed against the
[official Anvil docs](https://docs.rcac.purdue.edu/userguides/anvil/jobs/) on
**2026-04-26**.

The built-in preset (`cluster.preset: purdue_anvil`) seeds the rest — this guide
explains what the preset assumes and what you still have to provide.

---

## Cluster facts (Phase 0 verified)

| Property               | Value                                                   |
|------------------------|---------------------------------------------------------|
| Slurm version          | `25.11.1` (`--json` natively supported)                 |
| Login pool             | `login01..login05.anvil.rcac.purdue.edu`                |
| Default QoS            | `cpu` (cannot run jobs in `gpu`, `gpu-debug`, `ai` partitions) |
| `/home`                | NFS, small per-user quota — **do not use for job I/O**  |
| Scratch                | Exposed as `$SCRATCH` (per RCAC convention). Anvil's scratch lives on GPFS. |
| SFTP                   | Available                                               |
| `squeue --json`        | Works                                                   |
| `sacct --json`         | Works (Anvil purges sacct quickly for old jobs — 14-day lookback recommended) |

---

## SSH setup

Set up Anvil SSH access following the
[RCAC instructions](https://www.rcac.purdue.edu/knowledge/anvil/access/login).
Anvil uses ACCESS-issued credentials; usernames are typically `x-<gateway-id>`.

Once you can `ssh` to Anvil from a terminal, verify the basics from your shell before
pointing `slurmly` at it:

```bash
ssh anvil 'sinfo --version && sacctmgr show assoc user=$USER -P'
```

`slurmly` connects via `asyncssh` and does not read `~/.ssh/config`. Pass the same
host/username/key you used above directly into `SSHConfig` (or your YAML).

> **Login pool note.** Anvil distributes logins across `login01..login05`. Pick one
> per session and stick with it (so `asyncssh` connection reuse works); rotate
> across sessions to spread load.

---

## Allocation and accounts

You need:

1. An ACCESS allocation that includes Anvil. Allocation IDs look like
   `abc123456`. Set this as `slurm.account`.
2. A QoS that matches the allocation. Most CPU allocations carry `cpu` only; GPU
   allocations carry `gpu`/`gpu-debug`. The slurmly preset does **not** set a
   default QoS — Slurm picks the allocation default.

Discover what your account has:

```bash
ssh anvil 'sacctmgr show assoc user=$USER -P'
```

The `Partition`/`QOS` columns tell you what you can submit to.

> **CPU-only allocation reality:** if your allocation is `QoS=cpu`, the `gpu`,
> `gpu-debug`, and `ai` partitions will reject your jobs at submit time (`sbatch`
> exits non-zero). `slurmly` surfaces the error verbatim in `SbatchError.stderr`.
> The preset still lists those partitions in `allowed_partitions` because they
> exist — `allowed_partitions` is a client-side allow-list, not an entitlement
> check.

---

## Scratch and working directory

Anvil exposes the per-user scratch area as the `$SCRATCH` environment variable
(per the standard RCAC convention). On the cluster shell:

```bash
$ ssh anvil 'echo $SCRATCH'
/anvil/scratch/x-myuser
```

Per-job working directories live under `<remote_base_dir>/jobs/`. Use a directory
inside `$SCRATCH` — for example `$SCRATCH/slurmly`.

`slurmly` does **not** expand shell variables on the client side: the path is used
in client-side path-validation (string comparison) and in literal SFTP/find
commands. Resolve `$SCRATCH` once and put the literal path in your config:

```bash
ssh anvil 'echo $SCRATCH/slurmly'         # → /anvil/scratch/x-myuser/slurmly
```

```python
remote_base_dir="/anvil/scratch/x-myuser/slurmly"
```

The preset ships a `remote_base_dir_template` of `/anvil/scratch/{username}/slurmly`,
which the YAML loader renders with the SSH `username` so YAML users can rely on it.
Set `slurm.remote_base_dir` (or pass `remote_base_dir=` directly) to override.

> Don't use `/home/<username>` for job I/O — Anvil's home is NFS-backed and quota-limited.

---

## Partitions

Anvil partitions tracked by the preset:

| Partition   | Purpose                                                  | QoS needed |
|-------------|----------------------------------------------------------|------------|
| `debug`     | Quick interactive debugging (limited time and nodes)     | `cpu`      |
| `shared`    | **Default.** Shared nodes, suitable for short CPU jobs    | `cpu`      |
| `standard`  | Whole-node CPU jobs                                       | `cpu`      |
| `wholenode` | Exclusive whole-node jobs                                 | `cpu`      |
| `wide`      | Multi-node CPU jobs                                       | `cpu`      |
| `highmem`   | Large-memory CPU nodes                                    | `cpu`      |
| `profiling` | Profiling/instrumentation                                 | `cpu`      |
| `gpu`       | GPU compute                                              | `gpu`      |
| `gpu-debug` | Quick GPU debugging                                       | `gpu`      |
| `ai`        | AI-specialized GPU partition                              | `gpu`/`ai` |

Always check current limits with `sinfo -o '%P %a %l %D %t'` from the login node
— time limits and node counts change.

---

## GPU directive

Anvil's docs use `--gpus-per-node=N`. The preset selects the matching directive
style:

```python
ClusterProfile(gpu_directive="gpus_per_node", ...)
```

So a `JobSpec` like:

```python
JobSpec(name="train", command=["python", "train.py"], gpus=2, partition="gpu",
        time_limit="01:00:00", account="<your_gpu_allocation>")
```

renders to:

```bash
#SBATCH --gpus-per-node=2
```

If you have a typed GPU constraint (e.g., specific A100 model), use
`gpu_type="..."` and switch to `gres_typed` in your `cluster_profile` override.

---

## Module trees

Anvil ships **modtree** as a meta-loader. The preset includes four execution
profiles you can `extends:`:

- `anvil_cpu_modules` → `module load modtree/cpu`
- `anvil_gpu_modules` → `module load modtree/gpu`
- `anvil_ngc` → `module load modtree/gpu` + `module load ngc`
- `anvil_biocontainers` → `module load modtree/cpu` + `module load biocontainers`

A typical project profile:

```yaml
execution_profiles:
  my_project_cpu:
    extends: anvil_cpu_modules
    preamble:
      - module purge
      - module load modtree/cpu
      - module load python
      - source $HOME/.venv/bin/activate
      - module list
    env:
      OMP_NUM_THREADS: "8"
```

Reference it from a `JobSpec`:

```python
JobSpec(name="train", command=["python", "train.py"],
        partition="shared", cpus_per_task=8, time_limit="01:00:00",
        execution_profile="my_project_cpu")
```

---

## Full working config

Replace `x-myuser` with your ACCESS username and `abc123456` with your allocation
ID. The preset supplies `ssh.host` (`anvil.rcac.purdue.edu`) and the
`remote_base_dir_template` (`/anvil/scratch/{username}/slurmly`), so the YAML can
be very short:

```yaml
cluster:
  preset: purdue_anvil

ssh:
  username: x-myuser
  key_path: ~/.ssh/anvil_ed25519
  max_concurrent_commands: 4

slurm:
  account: abc123456
  # remote_base_dir defaults to /anvil/scratch/x-myuser/slurmly via the preset template

execution_profiles:
  my_project_cpu:
    extends: anvil_cpu_modules
    preamble:
      - module purge
      - module load modtree/cpu
      - module list
```

Or in code, using the preset for `cluster_profile` + `execution_profiles`:

```python
from slurmly import SlurmSSHClient, get_preset

preset = get_preset("purdue_anvil")
client = SlurmSSHClient.connect(
    host=preset.ssh_host,
    username="x-myuser",
    key_path="~/.ssh/anvil_ed25519",
    remote_base_dir="/anvil/scratch/x-myuser/slurmly",
    account="abc123456",
    cluster_profile=preset.cluster_profile,
    execution_profiles=preset.execution_profiles,
)
```

Runnable example (sized for `QoS=cpu`):

```python
import asyncio
from slurmly import SlurmSSHClient, JobSpec
from slurmly.polling import is_terminal

async def main():
    async with SlurmSSHClient.from_config("slurmly.yaml") as client:
        job = await client.submit(
            JobSpec(
                name="anvil-smoke",
                command_template='echo "ok from $HOSTNAME at $(date -Iseconds)"',
                partition="shared",
                cpus_per_task=1,
                memory="256M",
                time_limit="00:01:00",
                execution_profile="my_project_cpu",
            )
        )
        print(f"submitted slurm_job_id={job.slurm_job_id}")
        print(f"working dir: {job.remote_job_dir}")

        while True:
            info = await client.get_job(job.slurm_job_id, submitted_at=job.submitted_at)
            print(f"  state={info.lifecycle} raw={info.raw_state}")
            if is_terminal(info):
                break
            await asyncio.sleep(5)

        out = await client.tail_stdout(job, lines=20)
        print("--- stdout ---")
        print(out.content)

        # Clean up the working directory
        await client.cleanup_remote_job_dir(job)

asyncio.run(main())
```

---

## Operational notes

- **Keep `max_concurrent_commands` low.** Anvil's per-user SSH session limit is
  conservative. Eight concurrent commands is rarely useful and risks getting
  rate-limited. Three or four is typically enough.
- **Be polite with `sacct`.** `sacct --starttime=now-14days` against a busy
  cluster is not free. The preset uses 14 days because Anvil purges history fast,
  but if you only ever query newly-submitted jobs, lower it.
- **No detection of user limits.** `slurmly` does not query your allocation's
  remaining SU. Plan accordingly — the cluster will surface SU exhaustion as an
  `sbatch` error.
- **Login-node etiquette.** All commands run on the login node, including `tail`
  on log files. Use `tail_stdout(lines=200)` rather than streaming entire log
  files.

---

## Troubleshooting

| Symptom                                                        | Likely cause / fix |
|----------------------------------------------------------------|--------------------|
| `SbatchError: ... Invalid account ...`                         | `slurm.account` mismatch with what `sacctmgr show assoc` reports. |
| `SbatchError: ... Invalid partition ...`                       | Partition name typo or out-of-allow-list at the cluster (`QoS` mismatch with partition). |
| `SlurmJobNotFound` shortly after submit                        | Past `submit_grace_window_seconds` and not visible to `squeue`/`sacct`. Try widening `sacct_lookback_days` and the grace window. |
| `JobInfo.lifecycle="failed"`, `exit_code="0:9"`                | Job killed by signal 9, often OOM. Check `sacct --format=ReqMem,MaxRSS`. |
| `JobInfo.lifecycle="timeout"`                                  | Job hit `time_limit`. Either raise `time_limit` or split work. |
| `SSHTransportError: ... Connection lost`                       | Anvil session limit hit, or network blip. Lower `max_concurrent_commands`. |
| `InvalidConfig: cleanup path ... not under remote_base_dir/jobs/` | `SubmittedJob` was constructed manually with an unrelated path. Always use what `submit` returns. |
