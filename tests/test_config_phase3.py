"""Phase 3 — config loader changes (proxy_jump mapping, max_concurrent_commands)."""

from __future__ import annotations

import pytest

from slurmly import InvalidConfig, SlurmSSHClient
from slurmly.testing import FakeTransport


def _write(path, body: str) -> str:
    path.write_text(body)
    return str(path)


def test_structured_proxy_jump_string_form_passthrough(tmp_path):
    cfg = _write(
        tmp_path / "c.yaml",
        "cluster:\n"
        "  preset: purdue_anvil\n"
        "slurm:\n"
        "  account: a\n"
        "ssh:\n"
        "  username: u\n"
        "  key_path: /k\n"
        "  proxy_jump: bastion@host:2222\n",
    )
    client = SlurmSSHClient.from_config(cfg, transport=FakeTransport())
    assert client is not None


def test_structured_proxy_jump_mapping_form_canonicalizes(tmp_path):
    cfg = _write(
        tmp_path / "c.yaml",
        "cluster:\n"
        "  preset: purdue_anvil\n"
        "slurm:\n"
        "  account: a\n"
        "ssh:\n"
        "  username: u\n"
        "  key_path: /k\n"
        "  proxy_jump:\n"
        "    host: bastion.example.com\n"
        "    username: svc\n"
        "    port: 2222\n",
    )
    client = SlurmSSHClient.from_config(cfg, transport=FakeTransport())
    assert client is not None


def test_structured_proxy_jump_with_key_path_rejected(tmp_path):
    cfg = _write(
        tmp_path / "c.yaml",
        "cluster:\n"
        "  preset: purdue_anvil\n"
        "slurm:\n"
        "  account: a\n"
        "ssh:\n"
        "  username: u\n"
        "  key_path: /k\n"
        "  proxy_jump:\n"
        "    host: bastion.example.com\n"
        "    username: svc\n"
        "    key_path: /secrets/bastion_key\n",
    )
    with pytest.raises(InvalidConfig) as ei:
        SlurmSSHClient.from_config(cfg, transport=FakeTransport())
    assert "proxy_jump.key_path" in str(ei.value)


def test_max_concurrent_commands_from_config(tmp_path):
    cfg = _write(
        tmp_path / "c.yaml",
        "cluster:\n"
        "  preset: purdue_anvil\n"
        "slurm:\n"
        "  account: a\n"
        "ssh:\n"
        "  username: u\n"
        "  key_path: /k\n"
        "  max_concurrent_commands: 4\n",
    )
    client = SlurmSSHClient.from_config(cfg, transport=FakeTransport())
    assert client.max_concurrent_commands == 4


def test_max_concurrent_commands_must_be_positive(tmp_path):
    cfg = _write(
        tmp_path / "c.yaml",
        "cluster:\n"
        "  preset: purdue_anvil\n"
        "slurm:\n"
        "  account: a\n"
        "ssh:\n"
        "  username: u\n"
        "  key_path: /k\n"
        "  max_concurrent_commands: 0\n",
    )
    with pytest.raises(InvalidConfig):
        SlurmSSHClient.from_config(cfg, transport=FakeTransport())
