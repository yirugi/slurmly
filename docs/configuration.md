# Configuration

`slurmly` can be configured two ways:

1. **In code** — pass `SSHConfig`, `ClusterProfile`, etc. to `SlurmSSHClient(...)`.
   Best for backends (FastAPI, Celery, …) that already get secrets and settings from
   environment / secret stores.
2. **From a file** — call `SlurmSSHClient.from_config(path)` to load YAML, TOML, or JSON.
   Best for CLI tools and scripts.

In-code construction is the canonical surface; the file loader is a thin wrapper. Anything
the loader can do, you can do directly in code.

---

## Code-first (minimal)

```python
from slurmly import SlurmSSHClient

client = SlurmSSHClient.connect(
    host="login.example.edu",
    username="myuser",
    key_path="~/.ssh/cluster_ed25519",
    account="my_allocation",
)
```

`SlurmSSHClient.connect(...)` is a thin factory that builds an `SSHConfig` +
`AsyncSSHTransport` from kwargs and uses default `ClusterProfile()` (no allowed-list
restrictions). For full control — proxy jumps, custom timeouts, heredoc uploads,
restricted partition lists, named execution profiles — pass everything to the
regular constructor:

```python
from slurmly import SlurmSSHClient, ClusterProfile, ExecutionProfile
from slurmly.ssh.transport import AsyncSSHTransport, SSHConfig

client = SlurmSSHClient(
    transport=AsyncSSHTransport(SSHConfig(
        host="login.example.edu",
        username="myuser",
        key_path="~/.ssh/cluster_ed25519",
        proxy_jump="alice@bastion.example.com",
    )),
    remote_base_dir="/scratch/myuser/slurmly",
    cluster_profile=ClusterProfile(
        default_partition="shared",
        allowed_partitions=["shared", "debug", "gpu"],
    ),
    account="my_allocation",
    execution_profiles={
        "py311": ExecutionProfile(
            name="py311",
            preamble=["module purge", "module load python/3.11"],
        ),
    },
)
```

### `SSHConfig` fields

| Field                       | Required | Default              | Notes |
|-----------------------------|----------|----------------------|-------|
| `host`                      | yes      | —                    | Login node hostname. |
| `username`                  | yes      | —                    | Cluster username. |
| `key_path`                  | no¹      | —                    | **Private key** (no `.pub` extension). `~` and `~user` are expanded at connect time; absolute paths are passed through. |
| `port`                      | no       | `22`                 | |
| `known_hosts_path`          | no       | system default²      | When unset, asyncssh uses `~/.ssh/known_hosts` and `/etc/ssh/ssh_known_hosts`. Backend deployments without those files should provide an explicit path. |
| `accept_unknown_hosts`      | no       | `true`               | When `true`, host key verification is disabled (asyncssh `known_hosts=None`). Set to `false` to enforce strict verification via `known_hosts_path` or the system default. |
| `connect_timeout_seconds`   | no       | `10.0`               | TCP connect timeout. |
| `command_timeout_seconds`   | no       | `30.0`               | Per-command soft cap. |
| `keepalive_interval_seconds`| no       | `30.0`               | SSH keepalive cadence. |
| `keepalive_count_max`       | no       | `3`                  | Drops connection after N missed keepalives. |
| `upload_method`             | no       | `"sftp"`             | `"heredoc"` for clusters with SFTP disabled. |
| `proxy_jump`                | no       | —                    | `"user@host:port"`; mutually exclusive with `proxy_command`. |
| `proxy_command`             | no       | —                    | e.g. `"ssh -W %h:%p user@bastion"`. |

¹ Required in practice unless an SSH agent or other asyncssh-supported credential source is
available. Most users pass it.
² Setting `known_hosts_path=None` is **not** the same as omitting the field — the default
applies only when omitted; explicit `None` may disable host-key verification depending on
asyncssh version. Don't pass `None` unless you understand that.

### `ClusterProfile` fields

Per-cluster behavior knobs. Slurm calls these "partitions" (not "queues") — the term
matches `--partition=` and `sinfo`.

| Field                                | Default      | Notes |
|--------------------------------------|--------------|-------|
| `name`                               | `"default"`  | Free-form label. |
| `default_partition`                  | `None`       | Used when `JobSpec.partition` is unset. |
| `allowed_partitions`                 | `[]`         | Empty = no restriction. Otherwise `JobSpec.partition` must match. |
| `default_account` / `allowed_accounts` | `None` / `[]` | Same pattern for account. |
| `default_qos` / `allowed_qos`        | `None` / `[]` | Same pattern for QoS. |
| `gpu_directive`                      | `"gpus"`     | One of `gpus`, `gres`, `gres_typed`, `gpus_per_node`. |
| `default_gpu_type`                   | `None`       | Used with `gres_typed`. |
| `supports_squeue_json`               | `True`       | Set `False` to fall back to parsable formats. |
| `supports_sacct_json`                | `True`       | |
| `supports_parsable2_fallback`        | `True`       | |
| `sacct_lookback_days`                | `7`          | History window for `sacct --starttime=`. |
| `working_dir_mode`                   | `"script_cd"` | Or `"sbatch_chdir"`. |
| `supports_array_jobs`                | `False`      | |
| `submit_grace_window_seconds`        | `120`        | Window where a freshly-submitted job is allowed to be invisible to `squeue`/`sacct`. |
| `not_found_attention_after_seconds`  | `300`        | Operator-attention threshold. |

### `remote_base_dir`

Optional. Per-job working directories land at `<remote_base_dir>/jobs/slurmly-<8hex>/`.
Defaults to `~/slurmly` when not set (the remote shell expands `~` on the cluster).

For cleanup and artifact path-safety checks, **absolute paths give strict enforcement** —
slurmly can verify that `rm -rf` and file reads stay inside `<base>/jobs/`. With a
tilde-based path (the default), those checks are relaxed to `..` and empty-path rejection
only, since the remote `~` can't be resolved locally.

Most HPC sites have small NFS-backed home quotas and ask users to keep job I/O on scratch.
If your cluster has a dedicated scratch area, prefer an explicit absolute path:

```python
remote_base_dir="/scratch/myuser/slurmly"    # or /anvil/scratch/x-myuser/slurmly, etc.
```

Shell variables (`$HOME`, `$SCRATCH`) are **not** expanded by slurmly. Resolve them once
with `ssh <host> 'echo $SCRATCH'` and put the literal result in your config.

### `ExecutionProfile`

Reusable preamble + env applied to the rendered batch script before the user command:

```python
ExecutionProfile(
    name="pytorch_gpu",
    preamble=[
        "module purge",
        "module load cuda",
        "source ~/.venv/bin/activate",
    ],
    env={"CUDA_VISIBLE_DEVICES": "0,1"},
)
```

`preamble` lines are emitted verbatim — they are not quoted or modified.

---

## Loading from a file

```python
async with SlurmSSHClient.from_config("slurmly.yaml") as client:
    ...
```

Format detected by extension:

| Extension      | Format | Requirement                    |
|----------------|--------|--------------------------------|
| `.yaml`, `.yml`| YAML   | PyYAML (bundled)               |
| `.toml`        | TOML   | stdlib `tomllib` (Python 3.11+)|
| `.json`        | JSON   | stdlib                         |

### Top-level shape

```yaml
cluster:                      # optional — pulls a built-in preset
  preset: purdue_anvil

ssh:                          # required
  host: ...
  username: ...
  key_path: ...               # private key path (not .pub)

slurm:                        # optional section
  account: ...
  remote_base_dir: ...        # optional; defaults to ~/slurmly

cluster_profile:              # optional overrides for any ClusterProfile field
  ...

execution_profiles:           # optional named preambles
  my_profile:
    extends: anvil_cpu_modules
    preamble: [...]
    env: {...}
```

### Merge precedence

```
built-in preset
  < config-file values
  < explicit constructor arguments (transport, hooks)
  < runtime JobSpec  (applied inside SlurmSSHClient.submit / render_only)
```

Each level overrides the one above for any field it specifies.

### `cluster.preset`

Looks up a `ClusterPreset` by name or alias. Currently recognized: `purdue_anvil`
(aliases `anvil`, `rcac_anvil`). The preset seeds:

- `cluster_profile` defaults
- `execution_profiles` defaults
- `ssh.host` (only if you don't set one)
- `remote_base_dir` (via `remote_base_dir_template`, only if not set explicitly)

If `cluster.preset` is omitted, you must provide `ssh.host` and (typically) a
`cluster_profile` block. `slurm.remote_base_dir` defaults to `~/slurmly` when omitted.

### `ssh` section

Same fields as `SSHConfig` above. Plus one client-level setting:

```yaml
ssh:
  host: login.example.edu
  username: myuser
  key_path: ~/.ssh/cluster_ed25519
  max_concurrent_commands: 8           # client-level semaphore (default 8, >= 1)
```

Mapping form for `proxy_jump`:

```yaml
ssh:
  proxy_jump:
    host: bastion.example.com
    username: alice
    port: 22
```

If you need a different SSH key for the bastion, use `proxy_command` instead — the
mapping form does not yet support per-bastion `key_path`.

### `slurm` section

| Field             | Required | Default               | Notes |
|-------------------|----------|-----------------------|-------|
| `account`         | no       | `None`                | Forwarded to the client as `account=`. Injected into `JobSpec` when the spec doesn't set its own. |
| `remote_base_dir` | no       | `~/slurmly`           | Remote path for per-job working directories. See below. |

### `cluster_profile` overrides

Any field of `ClusterProfile` can be overridden:

```yaml
cluster_profile:
  default_partition: shared
  allowed_partitions: [shared, debug, gpu]
  sacct_lookback_days: 14
  supports_array_jobs: true
```

### `execution_profiles` section

```yaml
execution_profiles:
  my_project_cpu:
    extends: anvil_cpu_modules            # optional, must name a known profile
    preamble:                              # if set, replaces inherited preamble
      - module purge
      - module load python/3.11
      - source ~/.venv/bin/activate
    env:                                   # shallow-merged on top of inherited env
      OMP_NUM_THREADS: "8"
```

Rules:

- `extends` must name a profile defined either in this file or in the active preset.
- If `preamble:` is present, it replaces the inherited list. If absent and `extends` is
  set, the inherited preamble is preserved.
- `env:` is shallow-merged (user keys win) over the inherited env.

### Worked example

A non-preset config that wires everything explicitly:

```yaml
ssh:
  host: login.example.edu
  username: myuser
  key_path: ~/.ssh/cluster_ed25519
  max_concurrent_commands: 4

slurm:
  account: my_allocation
  remote_base_dir: /scratch/myuser/slurmly

cluster_profile:
  default_partition: shared
  allowed_partitions: [shared, debug, standard, gpu]
  gpu_directive: gpus
  sacct_lookback_days: 14
  supports_array_jobs: true

execution_profiles:
  my_project_cpu:
    preamble:
      - module purge
      - module load python/3.11
      - source ~/.venv/bin/activate
    env:
      OMP_NUM_THREADS: "8"
```

