"""Tests for the config loader."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from slurmly import SlurmSSHClient
from slurmly.exceptions import InvalidConfig
from slurmly.testing import FakeTransport


def _write(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body))
    return path


# --- preset-driven Anvil config -------------------------------------------


def test_anvil_preset_config_resolves_required_overrides(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil

        ssh:
          username: x-openghm
          key_path: /run/secrets/anvil_ed25519

        slurm:
          account: ees260008
          remote_base_dir: /anvil/scratch/x-openghm/slurmly
        """,
    )
    transport = FakeTransport()
    client = SlurmSSHClient.from_config(str(cfg), transport=transport)
    assert client.remote_base_dir == "/anvil/scratch/x-openghm/slurmly"
    assert client.cluster_profile.gpu_directive == "gpus_per_node"
    assert "shared" in client.cluster_profile.allowed_partitions
    assert "anvil_cpu_modules" in client.list_execution_profiles()


def test_remote_base_dir_can_be_filled_from_preset_template(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil

        ssh:
          username: x-openghm
          key_path: /run/secrets/anvil_ed25519

        slurm:
          account: ees260008
        """,
    )
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert client.remote_base_dir == "/anvil/scratch/x-openghm/slurmly"


def test_anvil_preset_config_overrides_sacct_lookback(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil

        ssh:
          username: x-openghm
          key_path: /k

        slurm:
          account: ees260008

        cluster_profile:
          sacct_lookback_days: 30
        """,
    )
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert client.cluster_profile.sacct_lookback_days == 30


def test_execution_profile_extends_preset_profile(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil

        ssh:
          username: x-openghm
          key_path: /k

        slurm:
          account: ees260008

        execution_profiles:
          my_project_cpu:
            extends: anvil_cpu_modules
        """,
    )
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert "my_project_cpu" in client.list_execution_profiles()


# --- validation -----------------------------------------------------------


def test_missing_username_raises(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          key_path: /k
        slurm:
          account: a
          remote_base_dir: /scratch/u
        """,
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_missing_remote_base_dir_without_template_raises(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        ssh:
          host: x.example.com
          username: u
          key_path: /k
        slurm:
          account: a
        cluster_profile:
          name: bare
        """,
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_proxy_jump_and_proxy_command_mutual_exclusion(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
          proxy_jump: bastion
          proxy_command: 'nc -X 5 proxy 22'
        slurm:
          account: a
          remote_base_dir: /scratch/u
        """,
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_unknown_preset_raises(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: not-a-real-cluster
        ssh:
          username: u
          key_path: /k
        slurm:
          account: a
          remote_base_dir: /scratch/u
        """,
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_heredoc_upload_method_accepted_in_phase3(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
          upload_method: heredoc
        slurm:
          account: a
        """,
    )
    # Phase 3 wires heredoc upload through stdin pipe; the YAML loader must
    # now accept it without raising.
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert client is not None


def test_invalid_upload_method_rejected(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
          upload_method: nope
        slurm:
          account: a
        """,
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_unsupported_extension_raises(tmp_path):
    cfg = tmp_path / "slurmly.cfg"
    cfg.write_text("anything")
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


# --- tilde expansion (key_path / known_hosts_path) -----------------------


async def test_tilde_in_key_path_is_expanded_at_connect_time(monkeypatch):
    """SSHConfig stores the literal `~/...`; AsyncSSHTransport expands it at connect."""
    import os
    import sys

    from slurmly.ssh.transport import AsyncSSHTransport, SSHConfig

    cfg = SSHConfig(host="h", username="u", key_path="~/.ssh/test_key",
                    known_hosts_path="~/known")
    assert cfg.key_path == "~/.ssh/test_key"
    assert cfg.known_hosts_path == "~/known"

    transport = AsyncSSHTransport(cfg)
    captured: dict = {}

    class _StubAsyncssh:
        @staticmethod
        async def connect(**kwargs):
            captured.update(kwargs)

            class _C:
                def close(self):
                    pass

                async def wait_closed(self):
                    pass

            return _C()

    monkeypatch.setitem(sys.modules, "asyncssh", _StubAsyncssh)
    await transport._ensure_conn()
    assert captured["client_keys"] == [os.path.expanduser("~/.ssh/test_key")]
    assert captured["known_hosts"] == os.path.expanduser("~/known")
    assert "~" not in captured["client_keys"][0]
    assert "~" not in captured["known_hosts"]


# --- execution_profiles: extends chain & circular -----------------------


def test_execution_profile_extends_two_level_chain_in_same_file(tmp_path):
    """A extends B extends C: A inherits C's preamble + B's overlay + own overlay."""
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
        slurm:
          account: a

        execution_profiles:
          c_base:
            preamble:
              - module purge
              - module load c-base
            env:
              C_VAR: c
          b_mid:
            extends: c_base
            preamble:
              - module load b-mid
            env:
              B_VAR: b
          a_top:
            extends: b_mid
            preamble:
              - module load a-top
            env:
              A_VAR: a
        """,
    )
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    a = client._execution_profiles["a_top"]
    # `preamble:` in `a_top` REPLACES inherited preamble (per docs).
    assert a.preamble == ["module load a-top"]
    # env is shallow-merged across the chain.
    assert a.env == {"C_VAR": "c", "B_VAR": "b", "A_VAR": "a"}


def test_execution_profile_extends_unknown_raises(tmp_path):
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
        slurm:
          account: a
        execution_profiles:
          orphan:
            extends: does_not_exist
        """,
    )
    with pytest.raises(InvalidConfig, match="extends"):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


def test_execution_profile_circular_extends_unresolvable(tmp_path):
    """Forward-references fail because profiles are resolved in declaration order."""
    cfg = _write(
        tmp_path / "slurmly.yaml",
        """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
        slurm:
          account: a
        execution_profiles:
          first:
            extends: second
          second:
            extends: first
        """,
    )
    with pytest.raises(InvalidConfig, match="extends"):
        SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())


# --- TOML / JSON parity --------------------------------------------------


_PARITY_BODY = {
    "yaml": """
        cluster:
          preset: purdue_anvil
        ssh:
          username: u
          key_path: /k
        slurm:
          account: a1
          remote_base_dir: /scratch/u/slurmly
        cluster_profile:
          sacct_lookback_days: 21
    """,
    "toml": """
[cluster]
preset = "purdue_anvil"

[ssh]
username = "u"
key_path = "/k"

[slurm]
account = "a1"
remote_base_dir = "/scratch/u/slurmly"

[cluster_profile]
sacct_lookback_days = 21
    """,
    "json": json.dumps({
        "cluster": {"preset": "purdue_anvil"},
        "ssh": {"username": "u", "key_path": "/k"},
        "slurm": {"account": "a1", "remote_base_dir": "/scratch/u/slurmly"},
        "cluster_profile": {"sacct_lookback_days": 21},
    }),
}


def test_toml_config_parity_with_yaml(tmp_path):
    cfg = tmp_path / "slurmly.toml"
    cfg.write_text(textwrap.dedent(_PARITY_BODY["toml"]))
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert client.remote_base_dir == "/scratch/u/slurmly"
    assert client.cluster_profile.sacct_lookback_days == 21
    assert "shared" in client.cluster_profile.allowed_partitions


def test_json_config_parity_with_yaml(tmp_path):
    cfg = tmp_path / "slurmly.json"
    cfg.write_text(_PARITY_BODY["json"])
    client = SlurmSSHClient.from_config(str(cfg), transport=FakeTransport())
    assert client.remote_base_dir == "/scratch/u/slurmly"
    assert client.cluster_profile.sacct_lookback_days == 21


def test_config_file_not_found_raises():
    with pytest.raises(InvalidConfig, match="not found"):
        SlurmSSHClient.from_config("/no/such/file.yaml", transport=FakeTransport())
