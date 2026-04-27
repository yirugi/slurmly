"""Phase 3 — AsyncSSHTransport reconnect + heredoc + proxy_jump."""

from __future__ import annotations

import pytest

from slurmly.exceptions import SSHTransportError
from slurmly.ssh.transport import AsyncSSHTransport, SSHConfig, _looks_like_connection_loss


def _cfg(**overrides) -> SSHConfig:
    base = dict(host="h", username="u", key_path="/k")
    base.update(overrides)
    return SSHConfig(**base)


def test_proxy_jump_and_proxy_command_mutually_exclusive():
    cfg = _cfg(proxy_jump="bastion", proxy_command="ssh -W %h:%p user@bastion")
    with pytest.raises(SSHTransportError):
        AsyncSSHTransport(cfg)


def test_invalid_upload_method_rejected():
    with pytest.raises(SSHTransportError):
        AsyncSSHTransport(_cfg(upload_method="nope"))


def test_connection_loss_classifier_matches_known_hints():
    assert _looks_like_connection_loss(ConnectionResetError("reset"))
    # asyncssh-style names appear in str(type(e)) + str(e)
    err = type("ChannelOpenError", (Exception,), {})("oops")
    assert _looks_like_connection_loss(err)


def test_connection_loss_classifier_ignores_unrelated_errors():
    assert _looks_like_connection_loss(ValueError("nope")) is False


# --- retry_safe runtime behavior via a mock conn -------------------------


class _MockProc:
    def __init__(self, exit_status: int = 0, stdout: str = "ok\n", stderr: str = ""):
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr


class _BrokenThenOkConn:
    """First conn.run raises connection-loss; reconnect yields a fresh one that succeeds."""

    def __init__(self) -> None:
        self.calls = 0

    async def run(self, command, *, check=False, timeout=None, **kwargs):
        self.calls += 1
        raise ConnectionResetError("Broken pipe")

    def close(self):
        pass

    async def wait_closed(self):
        pass


class _OkConn:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, command, *, check=False, timeout=None, **kwargs):
        self.calls += 1
        return _MockProc()

    def close(self):
        pass

    async def wait_closed(self):
        pass


async def test_retry_safe_reconnects_on_connection_loss(monkeypatch):
    transport = AsyncSSHTransport(_cfg())
    conn1 = _BrokenThenOkConn()
    conn2 = _OkConn()
    sequence = [conn1, conn2]

    async def fake_ensure_conn():
        if transport._conn is not None:
            return transport._conn
        transport._conn = sequence.pop(0)
        return transport._conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)
    result = await transport.run("echo hi", retry_safe=True)
    assert result.exit_status == 0
    assert conn1.calls == 1
    assert conn2.calls == 1


async def test_not_retry_safe_propagates_connection_loss(monkeypatch):
    transport = AsyncSSHTransport(_cfg())
    conn = _BrokenThenOkConn()

    async def fake_ensure_conn():
        if transport._conn is not None:
            return transport._conn
        transport._conn = conn
        return transport._conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)
    with pytest.raises(SSHTransportError):
        await transport.run("sbatch run.sh", retry_safe=False)
    # Only one attempt — no reconnect
    assert conn.calls == 1


# --- heredoc upload ------------------------------------------------------


class _RecordingConn:
    """Capture conn.run kwargs for heredoc upload assertions."""

    def __init__(self) -> None:
        self.last_command: str | None = None
        self.last_input: str | None = None

    async def run(self, command, *, check=False, timeout=None, input=None, **kwargs):
        self.last_command = command
        self.last_input = input
        return _MockProc()

    def close(self):
        pass

    async def wait_closed(self):
        pass


async def test_heredoc_upload_pipes_content_via_stdin(monkeypatch):
    transport = AsyncSSHTransport(_cfg(upload_method="heredoc"))
    conn = _RecordingConn()

    async def fake_ensure_conn():
        transport._conn = conn
        return conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)
    await transport.upload_text("/scratch/run.sh", "#!/bin/sh\necho hi\n", mode=0o755)
    assert conn.last_input == "#!/bin/sh\necho hi\n"
    assert "cat > /scratch/run.sh" in conn.last_command
    assert "chmod 755 /scratch/run.sh" in conn.last_command


async def test_heredoc_upload_rejects_nul_bytes():
    transport = AsyncSSHTransport(_cfg(upload_method="heredoc"))
    with pytest.raises(SSHTransportError):
        await transport.upload_text("/scratch/run.sh", "binary\x00content")


# --- heredoc with adversarial content (relies on stdin pipe, not shell) ---


async def test_heredoc_upload_passes_special_chars_via_stdin(monkeypatch):
    """Newlines, quotes, backslashes, and a literal 'EOF' line must round-trip
    because content goes via stdin (`input=`), not via shell-quoted args."""
    transport = AsyncSSHTransport(_cfg(upload_method="heredoc"))
    conn = _RecordingConn()

    async def fake_ensure_conn():
        transport._conn = conn
        return conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)

    payload = (
        '#!/bin/sh\n'
        "echo 'single quote'\n"
        'echo "double quote"\n'
        'echo back\\slash\n'
        'EOF\n'
        '__END__\n'
    )
    await transport.upload_text("/scratch/run.sh", payload, mode=0o700)
    assert conn.last_input == payload
    # The shell command itself must NOT contain the payload (proves no shell escape).
    assert "single quote" not in conn.last_command
    assert "double quote" not in conn.last_command


async def test_heredoc_upload_quotes_path_with_spaces(monkeypatch):
    transport = AsyncSSHTransport(_cfg(upload_method="heredoc"))
    conn = _RecordingConn()

    async def fake_ensure_conn():
        transport._conn = conn
        return conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)
    await transport.upload_text("/scratch/with space/run.sh", "x", mode=0o755)
    assert "'/scratch/with space/run.sh'" in conn.last_command


async def test_heredoc_upload_handles_large_payload(monkeypatch):
    """A 1 MB payload should still go through stdin without ARG_MAX issues."""
    transport = AsyncSSHTransport(_cfg(upload_method="heredoc"))
    conn = _RecordingConn()

    async def fake_ensure_conn():
        transport._conn = conn
        return conn

    monkeypatch.setattr(transport, "_ensure_conn", fake_ensure_conn)
    big = "x" * 1_000_000
    await transport.upload_text("/scratch/big.txt", big, mode=0o600)
    assert conn.last_input == big
    assert len(conn.last_command) < 200  # the shell command stays small
