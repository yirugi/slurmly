"""asyncssh-backed implementation of SSHTransport.

A single AsyncSSHTransport instance wraps one persistent SSH connection,
opened lazily on first use. asyncssh is imported lazily so unit tests that
only touch the fake transport don't need it installed.

Phase 3 additions:
  - `retry_safe=True` opt-in retry on connection loss for read-only commands.
  - `upload_method='heredoc'` pipes content via stdin (`cat > path`) instead
    of SFTP, for sites that disable SFTP. NUL bytes are rejected.
"""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass

from ..exceptions import RemoteCommandError, SSHTransportError
from . import CommandResult


@dataclass(frozen=True)
class SSHConfig:
    host: str
    username: str
    port: int = 22
    key_path: str | None = None
    known_hosts_path: str | None = None
    connect_timeout_seconds: float = 10.0
    command_timeout_seconds: float = 30.0
    keepalive_interval_seconds: float = 30.0
    keepalive_count_max: int = 3
    proxy_jump: str | None = None
    proxy_command: str | None = None
    upload_method: str = "sftp"  # "sftp" | "heredoc"


_RECONNECT_HINTS = (
    "ChannelOpenError",
    "ConnectionLost",
    "ConnectionResetError",
    "Connection reset",
    "Broken pipe",
    "EOF",
)


def _looks_like_connection_loss(exc: BaseException) -> bool:
    msg = f"{type(exc).__name__}: {exc}"
    return any(hint in msg for hint in _RECONNECT_HINTS)


class AsyncSSHTransport:
    def __init__(self, config: SSHConfig) -> None:
        if config.proxy_jump and config.proxy_command:
            raise SSHTransportError(
                "ssh.proxy_jump and ssh.proxy_command are mutually exclusive"
            )
        if config.upload_method not in {"sftp", "heredoc"}:
            raise SSHTransportError(
                f"upload_method must be 'sftp' or 'heredoc', got {config.upload_method!r}"
            )
        self._config = config
        self._conn = None  # type: ignore[var-annotated]
        self._asyncssh = None  # type: ignore[var-annotated]

    # --- connection management ---

    async def _ensure_conn(self):  # noqa: ANN202
        if self._conn is not None:
            return self._conn
        try:
            import asyncssh  # type: ignore[import-not-found]
        except ImportError as e:  # pragma: no cover
            raise SSHTransportError(
                "asyncssh is required to use AsyncSSHTransport (pip install slurmly[ssh])"
            ) from e
        self._asyncssh = asyncssh

        cfg = self._config
        connect_kwargs: dict = {
            "host": cfg.host,
            "port": cfg.port,
            "username": cfg.username,
            "keepalive_interval": cfg.keepalive_interval_seconds,
            "keepalive_count_max": cfg.keepalive_count_max,
        }
        if cfg.key_path:
            connect_kwargs["client_keys"] = [os.path.expanduser(cfg.key_path)]
        if cfg.known_hosts_path:
            connect_kwargs["known_hosts"] = os.path.expanduser(cfg.known_hosts_path)
        if cfg.proxy_command:
            connect_kwargs["proxy_command"] = cfg.proxy_command
        if cfg.proxy_jump:
            # asyncssh's `tunnel` accepts an existing connection or a
            # destination string parsed by parse_target_spec.
            connect_kwargs["tunnel"] = cfg.proxy_jump

        try:
            self._conn = await asyncssh.connect(**connect_kwargs)
        except Exception as e:
            raise SSHTransportError(f"failed to connect to {cfg.host}: {e}") from e
        return self._conn

    async def _drop_connection(self) -> None:
        """Close + forget the active connection so the next call reconnects."""
        if self._conn is None:
            return
        try:
            self._conn.close()
            await self._conn.wait_closed()
        except Exception:
            pass
        finally:
            self._conn = None

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            await self._conn.wait_closed()
            self._conn = None

    # --- public API ---

    async def run(
        self,
        command: str,
        *,
        timeout: float | None = None,
        check: bool = True,
        retry_safe: bool = False,
    ) -> CommandResult:
        try:
            return await self._run_once(command, timeout=timeout, check=check)
        except SSHTransportError as e:
            if not retry_safe:
                raise
            cause = e.__cause__ or e
            if not _looks_like_connection_loss(cause):
                raise
            # Drop the broken connection and retry exactly once.
            await self._drop_connection()
            return await self._run_once(command, timeout=timeout, check=check)

    async def _run_once(
        self,
        command: str,
        *,
        timeout: float | None,
        check: bool,
    ) -> CommandResult:
        conn = await self._ensure_conn()
        effective_timeout = timeout if timeout is not None else self._config.command_timeout_seconds
        try:
            proc = await conn.run(command, check=False, timeout=effective_timeout)
        except Exception as e:
            raise SSHTransportError(f"failed to run command: {e}") from e

        result = CommandResult(
            command=command,
            exit_status=int(proc.exit_status) if proc.exit_status is not None else -1,
            stdout=_to_str(proc.stdout),
            stderr=_to_str(proc.stderr),
        )
        if check and result.exit_status != 0:
            raise RemoteCommandError(
                command=command,
                exit_status=result.exit_status,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return result

    async def upload_text(self, path: str, content: str, *, mode: int = 0o700) -> None:
        if self._config.upload_method == "heredoc":
            return await self._upload_text_heredoc(path, content, mode=mode)
        return await self._upload_text_sftp(path, content, mode=mode)

    async def _upload_text_sftp(self, path: str, content: str, *, mode: int) -> None:
        conn = await self._ensure_conn()
        try:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open(path, "w") as f:
                    await f.write(content)
                await sftp.chmod(path, mode)
        except Exception as e:
            raise SSHTransportError(f"sftp upload to {path} failed: {e}") from e

    async def _upload_text_heredoc(self, path: str, content: str, *, mode: int) -> None:
        if "\x00" in content:
            raise SSHTransportError(
                "heredoc upload cannot transfer NUL bytes; switch to SFTP for binary content"
            )
        # `cat > path` with stdin pipe sidesteps ARG_MAX and shell escaping.
        # The trailing chmod runs in the same shell so the upload remains atomic
        # from the caller's perspective.
        cmd = f"cat > {shlex.quote(path)} && chmod {mode:o} {shlex.quote(path)}"
        conn = await self._ensure_conn()
        try:
            proc = await conn.run(
                cmd,
                check=False,
                timeout=self._config.command_timeout_seconds,
                input=content,
            )
        except Exception as e:
            raise SSHTransportError(f"heredoc upload to {path} failed: {e}") from e
        if proc.exit_status not in (0, None):
            raise SSHTransportError(
                f"heredoc upload to {path} failed with exit "
                f"{int(proc.exit_status)}: {_to_str(proc.stderr).strip()[:300]}"
            )

    async def read_text(self, path: str, *, max_bytes: int | None = None) -> str:
        conn = await self._ensure_conn()
        try:
            async with conn.start_sftp_client() as sftp:
                async with sftp.open(path, "r") as f:
                    data = await f.read(max_bytes) if max_bytes else await f.read()
        except Exception as e:
            raise SSHTransportError(f"sftp read of {path} failed: {e}") from e
        return _to_str(data)


def _to_str(data: object) -> str:
    if data is None:
        return ""
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    return str(data)


__all__ = ["AsyncSSHTransport", "SSHConfig"]
