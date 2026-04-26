"""Config-file loader for SlurmSSHClient.

File format is detected from the path suffix:

    .yaml / .yml -> YAML (requires PyYAML, install with `slurmly[yaml]`)
    .toml        -> TOML (stdlib `tomllib`)
    .json        -> JSON (stdlib)

Merge precedence:

    built-in preset
      < config file values
      < explicit constructor arguments (transport_override only at this level)

Runtime JobSpec wins last, applied inside SlurmSSHClient.submit/render_only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .exceptions import InvalidConfig
from .paths import render_remote_base_dir
from .presets import get_preset
from .profiles import ClusterPreset, ClusterProfile, ExecutionProfile
from .ssh import SSHTransport
from .ssh.transport import AsyncSSHTransport, SSHConfig

_VALID_UPLOAD_METHODS = {"sftp", "heredoc"}


def build_client_from_config(
    path: str,
    *,
    transport_override: SSHTransport | None = None,
    hooks: "object | None" = None,
):  # noqa: ANN201 — return type is SlurmSSHClient (forward ref to avoid cycle)
    """Parse `path` and return a fully-built SlurmSSHClient."""
    from .client import SlurmSSHClient

    raw = _load_file(Path(path))
    if not isinstance(raw, dict):
        raise InvalidConfig(f"top-level config must be a mapping, got {type(raw).__name__}")

    preset = _resolve_preset(raw)
    cluster_profile = _build_cluster_profile(raw, preset)
    execution_profiles = _build_execution_profiles(raw, preset)
    ssh_config = _build_ssh_config(raw, preset)
    remote_base_dir = _resolve_remote_base_dir(raw, preset, ssh_config.username)
    default_account = _resolve_default_account(raw)
    max_concurrent = _resolve_max_concurrent_commands(raw)

    transport = transport_override or AsyncSSHTransport(ssh_config)

    kwargs: dict[str, Any] = dict(
        transport=transport,
        cluster_profile=cluster_profile,
        remote_base_dir=remote_base_dir,
        execution_profiles=execution_profiles,
        default_account=default_account,
        max_concurrent_commands=max_concurrent,
    )
    if hooks is not None:
        kwargs["hooks"] = hooks
    return SlurmSSHClient(**kwargs)


# --- file loading ----------------------------------------------------------


def _load_file(path: Path) -> Any:
    if not path.exists():
        raise InvalidConfig(f"config file not found: {path}")
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError as e:
            raise InvalidConfig(
                "PyYAML is required to read YAML configs (pip install slurmly[yaml])"
            ) from e
        return yaml.safe_load(text)
    if suffix == ".toml":
        import tomllib

        return tomllib.loads(text)
    if suffix == ".json":
        return json.loads(text)
    raise InvalidConfig(
        f"unsupported config extension {suffix!r}; use .yaml/.yml/.toml/.json"
    )


# --- preset resolution -----------------------------------------------------


def _resolve_preset(raw: dict[str, Any]) -> ClusterPreset | None:
    cluster_section = raw.get("cluster") or {}
    preset_name = cluster_section.get("preset") if isinstance(cluster_section, dict) else None
    if preset_name is None:
        return None
    return get_preset(preset_name)


# --- cluster_profile -------------------------------------------------------


def _build_cluster_profile(
    raw: dict[str, Any],
    preset: ClusterPreset | None,
) -> ClusterProfile:
    base: dict[str, Any] = {}
    if preset is not None:
        base = preset.cluster_profile.model_dump()
    overrides = raw.get("cluster_profile") or {}
    if not isinstance(overrides, dict):
        raise InvalidConfig("cluster_profile must be a mapping")
    base.update(overrides)
    try:
        return ClusterProfile.model_validate(base)
    except Exception as e:
        raise InvalidConfig(f"invalid cluster_profile: {e}") from e


# --- execution profiles ----------------------------------------------------


def _build_execution_profiles(
    raw: dict[str, Any],
    preset: ClusterPreset | None,
) -> dict[str, ExecutionProfile]:
    """Merge preset execution profiles with user-defined ones.

    User entries may use `extends: <preset_profile_name>` to inherit preamble
    and env from a preset profile. The user's own keys override.
    """
    profiles: dict[str, ExecutionProfile] = {}
    if preset is not None:
        for name, ep in preset.execution_profiles.items():
            profiles[name] = ep

    user_section = raw.get("execution_profiles") or {}
    if not isinstance(user_section, dict):
        raise InvalidConfig("execution_profiles must be a mapping")

    for name, body in user_section.items():
        if not isinstance(body, dict):
            raise InvalidConfig(f"execution_profiles.{name} must be a mapping")

        extends = body.get("extends")
        merged: dict[str, Any] = {"name": name, "preamble": [], "env": {}}
        if extends:
            base = profiles.get(extends) or (
                preset.execution_profiles.get(extends) if preset is not None else None
            )
            if base is None:
                raise InvalidConfig(
                    f"execution_profiles.{name}.extends={extends!r} not found"
                )
            merged["preamble"] = list(base.preamble)
            merged["env"] = dict(base.env)

        if "preamble" in body:
            merged["preamble"] = list(body["preamble"])
        if "env" in body:
            merged["env"] = {**merged["env"], **body["env"]}

        profiles[name] = ExecutionProfile.model_validate(merged)

    return profiles


# --- SSH config ------------------------------------------------------------


def _build_ssh_config(
    raw: dict[str, Any],
    preset: ClusterPreset | None,
) -> SSHConfig:
    section = raw.get("ssh") or {}
    if not isinstance(section, dict):
        raise InvalidConfig("ssh must be a mapping")

    host = section.get("host") or (preset.ssh_host if preset is not None else None)
    if not host:
        raise InvalidConfig("ssh.host is required (no preset provided one)")

    username = section.get("username")
    if not username:
        raise InvalidConfig("ssh.username is required")
    key_path = section.get("key_path")
    if not key_path:
        raise InvalidConfig("ssh.key_path is required")

    proxy_jump = _normalize_proxy_jump(section.get("proxy_jump"))
    proxy_command = section.get("proxy_command")
    if proxy_jump and proxy_command:
        raise InvalidConfig("ssh.proxy_jump and ssh.proxy_command are mutually exclusive")
    upload_method = section.get("upload_method", "sftp")
    if upload_method not in _VALID_UPLOAD_METHODS:
        raise InvalidConfig(
            f"ssh.upload_method must be one of {sorted(_VALID_UPLOAD_METHODS)}; "
            f"got {upload_method!r}"
        )

    return SSHConfig(
        host=host,
        port=int(section.get("port", 22)),
        username=username,
        key_path=key_path,
        known_hosts_path=section.get("known_hosts_path"),
        connect_timeout_seconds=float(section.get("connect_timeout_seconds", 10.0)),
        command_timeout_seconds=float(section.get("command_timeout_seconds", 30.0)),
        keepalive_interval_seconds=float(section.get("keepalive_interval_seconds", 30.0)),
        keepalive_count_max=int(section.get("keepalive_count_max", 3)),
        proxy_jump=proxy_jump,
        proxy_command=section.get("proxy_command"),
        upload_method=upload_method,
    )


def _normalize_proxy_jump(value: Any) -> str | None:
    """Accept proxy_jump as either a string or a structured mapping.

    String form is passed through (asyncssh's `tunnel` parses it).
    Mapping form `{host, username?, port?, key_path?}` is converted to a
    canonical `[user@]host[:port]` string. `key_path` in the mapping is not
    yet supported (would require pre-creating a bastion connection); when
    set, we raise InvalidConfig pointing the user at proxy_command.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        host = value.get("host")
        if not host:
            raise InvalidConfig("ssh.proxy_jump.host is required")
        if value.get("key_path"):
            raise InvalidConfig(
                "ssh.proxy_jump.key_path is not yet supported; use ssh.proxy_command "
                "(e.g. proxy_command: 'ssh -W %h:%p -i /path/to/bastion_key user@bastion') "
                "to authenticate to the bastion with a different key"
            )
        username = value.get("username")
        port = value.get("port")
        canonical = host
        if username:
            canonical = f"{username}@{canonical}"
        if port:
            canonical = f"{canonical}:{int(port)}"
        return canonical
    raise InvalidConfig(
        f"ssh.proxy_jump must be a string or a mapping, got {type(value).__name__}"
    )


def _resolve_max_concurrent_commands(raw: dict[str, Any]) -> int:
    section = raw.get("ssh") or {}
    if not isinstance(section, dict):
        return 8
    value = section.get("max_concurrent_commands", 8)
    try:
        n = int(value)
    except (TypeError, ValueError) as e:
        raise InvalidConfig(
            f"ssh.max_concurrent_commands must be an integer; got {value!r}"
        ) from e
    if n < 1:
        raise InvalidConfig("ssh.max_concurrent_commands must be >= 1")
    return n


# --- remote_base_dir -------------------------------------------------------


def _resolve_remote_base_dir(
    raw: dict[str, Any],
    preset: ClusterPreset | None,
    username: str,
) -> str:
    """Resolve the working-tree root in this order:

        slurm.remote_base_dir > top-level remote_base_dir > preset template
    """
    slurm_section = raw.get("slurm") or {}
    if isinstance(slurm_section, dict) and slurm_section.get("remote_base_dir"):
        return str(slurm_section["remote_base_dir"])
    if raw.get("remote_base_dir"):
        return str(raw["remote_base_dir"])
    if preset is not None and preset.remote_base_dir_template:
        return render_remote_base_dir(preset.remote_base_dir_template, username=username)
    raise InvalidConfig(
        "remote_base_dir is required: set slurm.remote_base_dir, "
        "top-level remote_base_dir, or use a preset with a remote_base_dir_template"
    )


# --- account ---------------------------------------------------------------


def _resolve_default_account(raw: dict[str, Any]) -> str | None:
    slurm_section = raw.get("slurm") or {}
    if isinstance(slurm_section, dict):
        return slurm_section.get("account")
    return None


