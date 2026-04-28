# slurmly

**SSH-only Slurm client library for Python.**

`slurmly` controls Slurm clusters by running the standard CLI (`sbatch`, `squeue`, `sacct`,
`scancel`) over SSH against a login node. No REST API, no PySlurm, no C bindings —
if you can `ssh` to the login node, you can drive jobs from Python.

- Fully typed (Pydantic v2), `async`-first, framework-agnostic.
- First-class support for job arrays, dependencies, batched status, artifacts, cleanup, and capability detection.
- Observable via typed hooks; testable with an in-memory `FakeTransport` (no SSH or cluster required).

---

## Installation

```bash
pip install slurmly
```

Requires Python 3.11+. Runtime dependencies: `asyncssh>=2.14`, `pydantic>=2.5`, `PyYAML>=6.0`.

---

## Quickstart

```python
import asyncio
from slurmly import SlurmSSHClient, JobSpec

async def main():
    async with SlurmSSHClient.connect(
        host="login.example.edu",
        username="myuser",
        key_path="~/.ssh/cluster_ed25519",       # absolute paths work too
        account="my_allocation",
    ) as client:
        job = await client.submit(JobSpec(
            name="hello",
            command_template="echo hi from $HOSTNAME",
            partition="shared",
            time_limit="00:01:00",
        ))
        info = await client.get_job(job.slurm_job_id, submitted_at=job.submitted_at)
        print(job.slurm_job_id, info.lifecycle)

asyncio.run(main())
```

That's the whole API. For polling loops, log tailing, arrays, dependencies, artifacts,
and cleanup, see [docs/cookbook.md](docs/cookbook.md).

### Loading from a config file (optional)

If you'd rather keep configuration out of code, `SlurmSSHClient.from_config(path)` reads
YAML, TOML, or JSON. See [docs/configuration.md](docs/configuration.md) for the full
schema. Minimal `slurmly.yaml`:

```yaml
ssh:
  host: login.example.edu
  username: myuser
  key_path: ~/.ssh/cluster_ed25519   # private key (not .pub)

slurm:
  account: my_allocation
  # remote_base_dir defaults to ~/slurmly when omitted
```

```python
async with SlurmSSHClient.from_config("slurmly.yaml") as client:
    ...
```

---

## Features

| Capability                     | Entry point                                                        |
|--------------------------------|--------------------------------------------------------------------|
| Submit a job                   | `client.submit(spec)`                                              |
| Render-only (no SSH)           | `client.render_only(spec)`                                         |
| Cancel / signal                | `client.cancel(job_id, signal=...)`                                |
| Tail stdout / stderr           | `client.tail_stdout(job)`, `client.tail_stderr(job)`               |
| Get a single status            | `client.get_job(job_id, submitted_at=...)`                         |
| Get many statuses (batched)    | `client.get_jobs([id1, id2, ...])`                                 |
| Polling helpers                | `client.poll_job(...)`, `PollingPolicy`, `is_terminal(...)`        |
| Capability probe               | `client.detect_capabilities()`                                     |
| Job arrays                     | `JobSpec(array="0-99%10", command_template=...)`                   |
| Typed dependencies             | `JobDependency(type="afterok", job_ids=[...])`                     |
| List / download artifacts      | `client.list_artifacts(job)`, `client.download_artifact(job, ...)` |
| Plan / execute remote cleanup  | `client.plan_cleanup(...)`, `client.cleanup(plan)`                 |
| Observability hooks            | Subclass `SlurmlyHooks`, pass to constructor                       |
| In-memory testing              | `from slurmly.testing import FakeTransport`                        |

---

## Documentation

| Topic                                | File                                            |
|--------------------------------------|-------------------------------------------------|
| API reference                        | [docs/api-reference.md](docs/api-reference.md)  |
| Configuration files (YAML/TOML/JSON) | [docs/configuration.md](docs/configuration.md)  |
| Purdue Anvil guide                   | [docs/anvil.md](docs/anvil.md)                  |
| Cookbook (patterns & recipes)        | [docs/cookbook.md](docs/cookbook.md)            |

---

## License

MIT.
