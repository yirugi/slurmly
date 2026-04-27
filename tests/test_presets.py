"""Tests for the preset registry and the purdue_anvil preset."""

from __future__ import annotations

import pytest

from slurmly import get_preset, list_presets
from slurmly.exceptions import UnknownPreset


def test_list_presets_contains_purdue_anvil():
    assert "purdue_anvil" in list_presets()


@pytest.mark.parametrize("name", ["purdue_anvil", "anvil", "rcac_anvil", "purdue-anvil"])
def test_get_preset_resolves_aliases(name):
    p = get_preset(name)
    assert p.name == "purdue_anvil"


def test_get_preset_unknown_raises():
    with pytest.raises(UnknownPreset):
        get_preset("does-not-exist")


# --- value assertions ------------------------------------------------------


def test_anvil_remote_base_dir_template_uses_anvil_scratch():
    p = get_preset("purdue_anvil")
    assert p.remote_base_dir_template == "/anvil/scratch/{username}/slurmly"


def test_anvil_default_gpu_directive_is_gpus_per_node():
    p = get_preset("purdue_anvil")
    assert p.cluster_profile.gpu_directive == "gpus_per_node"


def test_anvil_partition_allowlist_includes_phase0_corrections():
    p = get_preset("purdue_anvil")
    parts = p.cluster_profile.allowed_partitions
    # Phase 0 noted these were missing from the original draft.
    assert "standard" in parts
    assert "profiling" in parts
    assert "shared" in parts
    assert "gpu" in parts


def test_anvil_does_not_carry_user_specific_values():
    p = get_preset("purdue_anvil")
    # Presets must NOT carry account, username, key path, allocation, scratch dir, etc.
    assert p.cluster_profile.default_account is None
    # default_partition is policy-acceptable; username/account etc. live in user config.


def test_anvil_execution_profiles_present():
    p = get_preset("purdue_anvil")
    for name in ["anvil_cpu_modules", "anvil_gpu_modules", "anvil_ngc", "anvil_biocontainers"]:
        assert name in p.execution_profiles
