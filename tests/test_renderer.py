"""Tests for the script renderer and profile-dependent validation."""

from __future__ import annotations

import pytest

from slurmly import JobSpec
from slurmly.profiles import ClusterProfile, ExecutionProfile
from slurmly.scripts.renderer import (
    ProfileValidationError,
    render_script,
    validate_against_profile,
)


def _basic_paths():
    return {
        "stdout_path": "/scratch/u/jobs/slurmly-aabbccdd/stdout.log",
        "stderr_path": "/scratch/u/jobs/slurmly-aabbccdd/stderr.log",
        "remote_job_dir": "/scratch/u/jobs/slurmly-aabbccdd",
    }


# --- ordering --------------------------------------------------------------


def test_sbatch_directives_come_before_shell_options():
    spec = JobSpec(name="t", command=["echo", "hi"], partition="shared")
    profile = ClusterProfile(default_account="a")
    spec = spec.model_copy(update={"account": "a"})
    script = render_script(
        spec=spec,
        profile=profile,
        execution_profile=None,
        **_basic_paths(),
    )
    lines = script.splitlines()
    assert lines[0] == "#!/usr/bin/env bash"
    sbatch_indices = [i for i, ln in enumerate(lines) if ln.startswith("#SBATCH")]
    set_idx = lines.index("set -euo pipefail")
    assert sbatch_indices, "no #SBATCH directives emitted"
    assert max(sbatch_indices) < set_idx
    # blank line between #SBATCH block and body
    assert lines[max(sbatch_indices) + 1] == ""


def test_output_and_error_directives_always_present():
    spec = JobSpec(name="t", command=["x"], account="a")
    profile = ClusterProfile()
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "#SBATCH --output=/scratch/u/jobs/slurmly-aabbccdd/stdout.log" in script
    assert "#SBATCH --error=/scratch/u/jobs/slurmly-aabbccdd/stderr.log" in script


# --- gpu directive rendering ----------------------------------------------


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("gpus", "#SBATCH --gpus=2"),
        ("gres", "#SBATCH --gres=gpu:2"),
        ("gpus_per_node", "#SBATCH --gpus-per-node=2"),
    ],
)
def test_gpu_directive_modes(mode, expected):
    spec = JobSpec(name="t", command=["x"], account="a", gpus=2)
    profile = ClusterProfile(gpu_directive=mode)
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert expected in script


def test_gres_typed_renders_with_gpu_type():
    spec = JobSpec(name="t", command=["x"], account="a", gpus=2, gpu_type="a100")
    profile = ClusterProfile(gpu_directive="gres_typed")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "#SBATCH --gres=gpu:a100:2" in script


def test_gpus_zero_emits_no_gpu_directive():
    spec = JobSpec(name="t", command=["x"], account="a", gpus=0)
    profile = ClusterProfile(gpu_directive="gpus")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "--gpus" not in script
    assert "--gres" not in script


# --- working_dir handling --------------------------------------------------


def test_script_cd_with_working_dir_emits_body_cd():
    spec = JobSpec(name="t", command=["x"], account="a", working_dir="/work/here")
    profile = ClusterProfile(working_dir_mode="script_cd")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "cd /work/here" in script
    assert "#SBATCH --chdir" not in script


def test_script_cd_without_working_dir_emits_no_cd():
    spec = JobSpec(name="t", command=["x"], account="a")
    profile = ClusterProfile(working_dir_mode="script_cd")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    body_after_blank = script.split("\n\n", 1)[1]
    assert "cd " not in body_after_blank


def test_sbatch_chdir_with_working_dir_emits_directive_no_body_cd():
    spec = JobSpec(name="t", command=["x"], account="a", working_dir="/work/here")
    profile = ClusterProfile(working_dir_mode="sbatch_chdir")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "#SBATCH --chdir=/work/here" in script
    body_after_blank = script.split("\n\n", 1)[1]
    assert "cd " not in body_after_blank


def test_sbatch_chdir_without_working_dir_falls_back_to_remote_job_dir():
    spec = JobSpec(name="t", command=["x"], account="a")
    profile = ClusterProfile(working_dir_mode="sbatch_chdir")
    script = render_script(
        spec=spec, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "#SBATCH --chdir=/scratch/u/jobs/slurmly-aabbccdd" in script


# --- execution profile env merge ------------------------------------------


def test_execution_profile_env_merges_below_spec_env():
    """spec.env wins over execution_profile.env for the same key."""
    spec = JobSpec(name="t", command=["x"], account="a", env={"MODE": "spec"})
    ep = ExecutionProfile(name="ep", env={"MODE": "profile", "EXTRA": "1"})
    profile = ClusterProfile()
    script = render_script(
        spec=spec, profile=profile, execution_profile=ep, **_basic_paths()
    )
    assert "export MODE=spec" in script
    assert "export EXTRA=1" in script


def test_execution_profile_preamble_emitted_in_order():
    spec = JobSpec(name="t", command=["x"], account="a")
    ep = ExecutionProfile(
        name="ep",
        preamble=["module purge", "module load modtree/cpu"],
    )
    profile = ClusterProfile()
    script = render_script(
        spec=spec, profile=profile, execution_profile=ep, **_basic_paths()
    )
    purge_idx = script.index("module purge")
    load_idx = script.index("module load modtree/cpu")
    assert purge_idx < load_idx


# --- command_template emitted verbatim ------------------------------------


def test_command_template_emitted_without_substitution():
    spec = JobSpec(
        name="t",
        command_template="python x.py --shard ${SLURM_ARRAY_TASK_ID}",
        account="a",
    )
    script = render_script(
        spec=spec,
        profile=ClusterProfile(),
        execution_profile=None,
        **_basic_paths(),
    )
    assert "python x.py --shard ${SLURM_ARRAY_TASK_ID}" in script


def test_command_list_uses_shlex_join():
    spec = JobSpec(name="t", command=["python", "x.py", "--name", "with space"], account="a")
    script = render_script(
        spec=spec, profile=ClusterProfile(), execution_profile=None, **_basic_paths()
    )
    assert "python x.py --name 'with space'" in script


# --- profile-dependent validation -----------------------------------------


def test_validate_partition_allowlist():
    spec = JobSpec(name="t", command=["x"], account="a", partition="nope")
    profile = ClusterProfile(allowed_partitions=["shared", "debug"])
    with pytest.raises(ProfileValidationError):
        validate_against_profile(spec, profile)


def test_validate_account_allowlist():
    spec = JobSpec(name="t", command=["x"], account="other")
    profile = ClusterProfile(allowed_accounts=["a", "b"])
    with pytest.raises(ProfileValidationError):
        validate_against_profile(spec, profile)


def test_validate_gpu_type_requires_gres_typed():
    spec = JobSpec(name="t", command=["x"], account="a", gpus=1, gpu_type="a100")
    profile = ClusterProfile(gpu_directive="gpus")
    with pytest.raises(ProfileValidationError):
        validate_against_profile(spec, profile)


def test_validate_gpu_type_with_gres_typed_ok():
    spec = JobSpec(name="t", command=["x"], account="a", gpus=1, gpu_type="a100")
    profile = ClusterProfile(gpu_directive="gres_typed")
    validate_against_profile(spec, profile)  # no raise


def test_validate_working_dir_must_be_absolute():
    spec = JobSpec(name="t", command=["x"], account="a", working_dir="relative/path")
    profile = ClusterProfile()
    with pytest.raises(ProfileValidationError):
        validate_against_profile(spec, profile)


# --- end-to-end body ordering pin -----------------------------------------


def test_full_body_ordering_preamble_env_workdir_command():
    """Pin §9 ordering: preamble -> env exports -> working_dir cd -> command."""
    from slurmly.profiles import ExecutionProfile

    spec = JobSpec(
        name="t",
        command=["python", "train.py"],
        account="a",
        working_dir="/work/here",
        env={"WANDB_MODE": "offline"},
    )
    ep = ExecutionProfile(name="ep", preamble=["module purge", "module load cuda"])
    profile = ClusterProfile(working_dir_mode="script_cd")
    script = render_script(
        spec=spec, profile=profile, execution_profile=ep, **_basic_paths()
    )
    purge = script.index("module purge")
    export = script.index("export WANDB_MODE")
    cd = script.index("cd /work/here")
    cmd = script.index("python train.py")
    assert purge < export < cd < cmd


# --- anvil preset → renderer integration ---------------------------------


def test_anvil_preset_partition_renders_and_disallowed_rejects():
    """Drive the anvil preset's ClusterProfile through the full pipeline."""
    from slurmly import get_preset

    profile = get_preset("purdue_anvil").cluster_profile

    ok = JobSpec(name="t", command=["x"], account="acct", partition="ai")
    validate_against_profile(ok, profile)
    script = render_script(
        spec=ok, profile=profile, execution_profile=None, **_basic_paths()
    )
    assert "#SBATCH --partition=ai" in script

    bad = JobSpec(name="t", command=["x"], account="acct", partition="not_a_partition")
    with pytest.raises(ProfileValidationError):
        validate_against_profile(bad, profile)
