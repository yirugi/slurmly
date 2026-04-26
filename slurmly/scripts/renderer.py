"""Slurm batch script renderer.

The renderer is a pure function: given a JobSpec + profiles + the per-job
paths it returns the exact bytes that will be uploaded as `run.sh`. It does
not touch SSH or the filesystem.

Output ordering (mandated by overview §7.4):

    1. shebang
    2. #SBATCH directives
    3. blank line
    4. set -euo pipefail
    5. execution profile preamble
    6. env exports
    7. working directory handling
    8. user command
"""

from __future__ import annotations

import shlex
from typing import Iterable

from ..exceptions import InvalidJobSpec
from ..models import JobDependency, JobSpec
from ..profiles import ClusterProfile, ExecutionProfile, GpuDirectiveMode

SHEBANG = "#!/usr/bin/env bash"
SHELL_OPTS = "set -euo pipefail"


class ProfileValidationError(InvalidJobSpec):
    """A JobSpec is incompatible with the active ClusterProfile."""


# --- profile-dependent validation ------------------------------------------


def validate_against_profile(spec: JobSpec, profile: ClusterProfile) -> None:
    """Check JobSpec against ClusterProfile constraints.

    MUST be called before any remote SSH operation. Raises ProfileValidationError
    on incompatibility.
    """
    # array jobs
    if spec.array is not None and not profile.supports_array_jobs:
        raise ProfileValidationError(
            "array jobs are not enabled for this cluster profile"
        )

    # allowlist checks
    _check_allow("partition", spec.partition, profile.allowed_partitions)
    _check_allow("account", spec.account, profile.allowed_accounts)
    _check_allow("qos", spec.qos, profile.allowed_qos)

    # gpu / gpu_type compatibility
    if spec.gpu_type is not None:
        if profile.gpu_directive != "gres_typed":
            raise ProfileValidationError(
                f"gpu_type is only valid when ClusterProfile.gpu_directive is "
                f"'gres_typed' (got {profile.gpu_directive!r}). For typed GPUs, "
                f"override the profile or omit gpu_type."
            )
        if not spec.gpus or spec.gpus < 1:
            raise ProfileValidationError(
                "gpu_type requires gpus >= 1"
            )

    if spec.gpus is not None and spec.gpus > 0 and spec.gpus and profile.gpu_directive == "gres_typed":
        # If the profile is typed but no gpu_type is provided, use the default.
        if spec.gpu_type is None and profile.default_gpu_type is None:
            raise ProfileValidationError(
                "ClusterProfile.gpu_directive='gres_typed' requires either "
                "JobSpec.gpu_type or ClusterProfile.default_gpu_type"
            )

    # working_dir + sbatch_chdir basic shape
    if spec.working_dir is not None and not spec.working_dir.startswith("/"):
        raise ProfileValidationError(
            f"working_dir must be an absolute path; got {spec.working_dir!r}"
        )


def _check_allow(name: str, value: str | None, allowed: list[str]) -> None:
    if value is None or not allowed:
        return
    if value not in allowed:
        raise ProfileValidationError(
            f"{name} {value!r} is not in the cluster profile allowlist {allowed}"
        )


# --- rendering -------------------------------------------------------------


def render_script(
    *,
    spec: JobSpec,
    profile: ClusterProfile,
    execution_profile: ExecutionProfile | None,
    stdout_path: str,
    stderr_path: str,
    remote_job_dir: str,
) -> str:
    """Return the full text of `run.sh`.

    Caller is responsible for having validated `spec` against `profile` (via
    `validate_against_profile`) and for resolving `execution_profile` from the
    spec's `execution_profile` name.
    """
    sbatch_lines = list(
        _sbatch_directives(
            spec=spec,
            profile=profile,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            remote_job_dir=remote_job_dir,
        )
    )

    body_lines: list[str] = [SHELL_OPTS]

    if execution_profile is not None:
        for line in execution_profile.preamble:
            body_lines.append(line)

    # env exports: spec env wins over execution_profile env (spec is more specific).
    merged_env: dict[str, str] = {}
    if execution_profile is not None:
        merged_env.update(execution_profile.env)
    merged_env.update(spec.env)
    for k, v in merged_env.items():
        body_lines.append(f"export {k}={shlex.quote(v)}")

    # working directory handling (only emits anything in script_cd mode)
    if profile.working_dir_mode == "script_cd" and spec.working_dir is not None:
        body_lines.append(f"cd {shlex.quote(spec.working_dir)}")

    # user command
    body_lines.append(_render_command(spec))

    return "\n".join([SHEBANG, *sbatch_lines, "", *body_lines, ""])


def _render_command(spec: JobSpec) -> str:
    if spec.command is not None:
        return shlex.join(spec.command)
    # command_template is trusted: emitted verbatim (no substitution).
    assert spec.command_template is not None  # model_validator guarantees one of two
    return spec.command_template


def _sbatch_directives(
    *,
    spec: JobSpec,
    profile: ClusterProfile,
    stdout_path: str,
    stderr_path: str,
    remote_job_dir: str,
) -> Iterable[str]:
    """Yield `#SBATCH ...` lines in the canonical order."""
    yield f"#SBATCH --job-name={spec.name}"

    partition = spec.partition or profile.default_partition
    if partition:
        yield f"#SBATCH --partition={partition}"

    account = spec.account or profile.default_account
    if account:
        yield f"#SBATCH --account={account}"

    qos = spec.qos or profile.default_qos
    if qos:
        yield f"#SBATCH --qos={qos}"

    if spec.nodes is not None:
        yield f"#SBATCH --nodes={spec.nodes}"
    if spec.ntasks is not None:
        yield f"#SBATCH --ntasks={spec.ntasks}"
    if spec.cpus_per_task is not None:
        yield f"#SBATCH --cpus-per-task={spec.cpus_per_task}"
    if spec.memory is not None:
        yield f"#SBATCH --mem={spec.memory}"
    if spec.time_limit is not None:
        yield f"#SBATCH --time={spec.time_limit}"

    # GPU directive — only emit if gpus is requested.
    if spec.gpus is not None and spec.gpus > 0:
        yield _render_gpu_directive(
            mode=profile.gpu_directive,
            gpus=spec.gpus,
            gpu_type=spec.gpu_type or profile.default_gpu_type,
        )

    if spec.array is not None:
        yield f"#SBATCH --array={spec.array}"

    if spec.dependency is not None:
        yield f"#SBATCH --dependency={_render_dependency(spec.dependency)}"

    # working dir directive
    if profile.working_dir_mode == "sbatch_chdir":
        chdir = spec.working_dir or remote_job_dir
        yield f"#SBATCH --chdir={chdir}"

    # output / error are always emitted.
    yield f"#SBATCH --output={stdout_path}"
    yield f"#SBATCH --error={stderr_path}"


def _render_dependency(value: str | JobDependency) -> str:
    if isinstance(value, JobDependency):
        return value.render()
    return value


def _render_gpu_directive(
    *,
    mode: GpuDirectiveMode,
    gpus: int,
    gpu_type: str | None,
) -> str:
    if mode == "gpus":
        return f"#SBATCH --gpus={gpus}"
    if mode == "gres":
        return f"#SBATCH --gres=gpu:{gpus}"
    if mode == "gres_typed":
        if not gpu_type:
            # validate_against_profile should have caught this
            raise ProfileValidationError(
                "gres_typed requires gpu_type or ClusterProfile.default_gpu_type"
            )
        return f"#SBATCH --gres=gpu:{gpu_type}:{gpus}"
    if mode == "gpus_per_node":
        return f"#SBATCH --gpus-per-node={gpus}"
    raise ProfileValidationError(f"unknown gpu_directive mode {mode!r}")
