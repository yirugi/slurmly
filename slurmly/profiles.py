"""Cluster / execution / preset profile types."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

GpuDirectiveMode = Literal["gpus", "gres", "gres_typed", "gpus_per_node"]
WorkingDirMode = Literal["script_cd", "sbatch_chdir"]
UploadMethod = Literal["sftp", "heredoc"]


class ClusterProfile(BaseModel):
    """Per-cluster behavior knobs.

    Built-in presets ship with a default ClusterProfile, but every field can
    be overridden in user config.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = "default"

    gpu_directive: GpuDirectiveMode = "gpus"
    default_gpu_type: str | None = None

    default_partition: str | None = None
    default_account: str | None = None
    default_qos: str | None = None

    allowed_partitions: list[str] = Field(default_factory=list)
    allowed_accounts: list[str] = Field(default_factory=list)
    allowed_qos: list[str] = Field(default_factory=list)

    supports_squeue_json: bool = True
    supports_sacct_json: bool = True
    supports_parsable2_fallback: bool = True

    sacct_lookback_days: int = 7
    working_dir_mode: WorkingDirMode = "script_cd"
    supports_array_jobs: bool = False

    submit_grace_window_seconds: int = 120
    not_found_attention_after_seconds: int = 300


class ExecutionProfile(BaseModel):
    """Reusable preamble + env applied to a job script before the user command.

    `preamble` lines are emitted verbatim; they come from trusted config so
    the renderer does not quote or escape them.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    preamble: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class ClusterPreset(BaseModel):
    """A bundled, documentation-derived starting point for a known cluster.

    Presets carry only public, non-user-specific information (host, partitions,
    GPU convention, common module preambles). Username, key path, allocation
    name, and scratch path must come from user config.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    aliases: list[str] = Field(default_factory=list)
    description: str | None = None
    source_urls: list[str] = Field(default_factory=list)
    last_reviewed: str | None = None

    ssh_host: str | None = None
    remote_base_dir_template: str | None = None
    cluster_profile: ClusterProfile
    execution_profiles: dict[str, ExecutionProfile] = Field(default_factory=dict)
