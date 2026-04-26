"""Built-in cluster preset registry."""

from __future__ import annotations

from .exceptions import UnknownPreset
from .profiles import ClusterPreset, ClusterProfile, ExecutionProfile

# --- purdue_anvil ----------------------------------------------------------
#
# Values reflect Phase 0 cluster_notes/purdue_anvil.md (last_reviewed
# 2026-04-26):
#   - remote_base_dir_template uses /anvil/scratch (GPFS), not /scratch
#   - allowed_partitions includes `standard` and `profiling`
#   - sacct_lookback_days bumped to 14 (Anvil purges sacct fast for older jobs)
#   - gpu_directive `gpus_per_node` is doc-derived; Phase 0 collector account
#     had QoS=cpu only and could not run a real GPU job to confirm.

PURDUE_ANVIL_PRESET = ClusterPreset(
    name="purdue_anvil",
    aliases=["anvil", "rcac_anvil", "purdue-anvil"],
    description="Purdue RCAC Anvil Slurm cluster preset (community-maintained)",
    source_urls=[
        "https://docs.rcac.purdue.edu/userguides/anvil/jobs/",
        "https://rcac.purdue.edu/knowledge/anvil/run/partitions",
        "https://rcac.purdue.edu/knowledge/anvil/run/examples/slurm/gpu?all=true",
    ],
    last_reviewed="2026-04-26",
    ssh_host="anvil.rcac.purdue.edu",
    remote_base_dir_template="/anvil/scratch/{username}/slurmly",
    cluster_profile=ClusterProfile(
        name="purdue_anvil",
        gpu_directive="gpus_per_node",
        default_partition="shared",
        allowed_partitions=[
            "debug",
            "gpu-debug",
            "wholenode",
            "wide",
            "shared",
            "standard",
            "highmem",
            "profiling",
            "gpu",
            "ai",
        ],
        supports_squeue_json=True,
        supports_sacct_json=True,
        supports_parsable2_fallback=True,
        sacct_lookback_days=14,
        working_dir_mode="script_cd",
        supports_array_jobs=True,
    ),
    execution_profiles={
        "anvil_cpu_modules": ExecutionProfile(
            name="anvil_cpu_modules",
            preamble=[
                "module purge",
                "module load modtree/cpu",
                "module list",
            ],
        ),
        "anvil_gpu_modules": ExecutionProfile(
            name="anvil_gpu_modules",
            preamble=[
                "module purge",
                "module load modtree/gpu",
                "module list",
            ],
        ),
        "anvil_ngc": ExecutionProfile(
            name="anvil_ngc",
            preamble=[
                "module purge",
                "module load modtree/gpu",
                "module load ngc",
                "module list",
            ],
        ),
        "anvil_biocontainers": ExecutionProfile(
            name="anvil_biocontainers",
            preamble=[
                "module purge",
                "module load modtree/cpu",
                "module load biocontainers",
                "module list",
            ],
        ),
    },
)


# --- registry --------------------------------------------------------------

_PRESETS: dict[str, ClusterPreset] = {
    PURDUE_ANVIL_PRESET.name: PURDUE_ANVIL_PRESET,
}

_ALIASES: dict[str, str] = {
    alias: PURDUE_ANVIL_PRESET.name for alias in PURDUE_ANVIL_PRESET.aliases
}


def list_presets() -> list[str]:
    """Return canonical preset names (aliases excluded)."""
    return sorted(_PRESETS.keys())


def get_preset(name_or_alias: str) -> ClusterPreset:
    """Look up a preset by canonical name or alias.

    Raises UnknownPreset if not found.
    """
    if name_or_alias in _PRESETS:
        return _PRESETS[name_or_alias]
    canonical = _ALIASES.get(name_or_alias)
    if canonical is None:
        raise UnknownPreset(
            f"unknown preset {name_or_alias!r}; "
            f"known: {', '.join(list_presets())}"
        )
    return _PRESETS[canonical]
