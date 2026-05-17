"""SSH transport layer.

The Protocol + CommandResult live here at module level so the testing fake can
depend on them without importing the asyncssh-backed implementation (which is
loaded lazily from `transport.py`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class CommandResult:
    command: str
    exit_status: int
    stdout: str
    stderr: str


class SSHTransport(Protocol):
    async def run(
        self,
        command: str,
        *,
        timeout: float | None = None,
        check: bool = True,
        retry_safe: bool = False,
    ) -> CommandResult: ...

    async def upload_text(self, path: str, content: str, *, mode: int = 0o700) -> None: ...

    async def read_text(self, path: str, *, max_bytes: int | None = None) -> str: ...

    async def read_bytes(
        self, path: str, *, offset: int = 0, length: int | None = None
    ) -> bytes: ...

    async def stat_size(self, path: str) -> int | None: ...

    async def download(self, remote_path: str, local_path: str) -> int: ...

    async def upload_file(
        self, local_path: str, remote_path: str, *, mode: int = 0o600
    ) -> None: ...

    async def makedirs(self, path: str, *, mode: int = 0o700) -> None: ...

    async def close(self) -> None: ...


__all__ = ["CommandResult", "SSHTransport"]
