"""In-process fake SSH transport for unit tests.

Records every `run`, `upload_text`, and `read_text` call. Behavior is driven
by either:

* a callable handler `(command: str) -> CommandResult` (or tuple), or
* a list of canned responses popped in order, or
* defaults that succeed silently.

Aim: tests can construct a SlurmSSHClient with a FakeTransport, drive
`submit()`, then assert on the recorded sequence of remote calls and on the
final SubmittedJob.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Union

from ..exceptions import RemoteCommandError
from ..ssh import CommandResult


@dataclass
class RecordedCall:
    command: str
    timeout: float | None
    check: bool
    retry_safe: bool = False


@dataclass
class RecordedUpload:
    path: str
    content: str
    mode: int


CommandHandler = Callable[[str], Union[CommandResult, "tuple[int, str, str]"]]


@dataclass
class FakeTransport:
    """A configurable, in-memory SSHTransport implementation."""

    # Handler called for every `run`. If None, returns a clean (0, "", "")
    # result unless an entry is found in `canned_responses` first.
    handler: CommandHandler | None = None

    # Optional FIFO list of canned responses, consumed before the handler.
    # Entries can be CommandResult or (exit, stdout, stderr) tuples.
    canned_responses: list[CommandResult | tuple[int, str, str]] = field(default_factory=list)

    # Recorded interactions.
    runs: list[RecordedCall] = field(default_factory=list)
    uploads: list[RecordedUpload] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)

    # Optional in-memory file store: path -> content. `read_text` reads from
    # here; `upload_text` writes here.
    files: dict[str, str] = field(default_factory=dict)

    closed: bool = False

    async def run(
        self,
        command: str,
        *,
        timeout: float | None = None,
        check: bool = True,
        retry_safe: bool = False,
    ) -> CommandResult:
        self.runs.append(
            RecordedCall(
                command=command, timeout=timeout, check=check, retry_safe=retry_safe
            )
        )
        result = self._next_result(command)
        if check and result.exit_status != 0:
            raise RemoteCommandError(
                command=command,
                exit_status=result.exit_status,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return result

    def _next_result(self, command: str) -> CommandResult:
        if self.canned_responses:
            head = self.canned_responses.pop(0)
            return _coerce_result(command, head)
        if self.handler is not None:
            return _coerce_result(command, self.handler(command))
        return CommandResult(command=command, exit_status=0, stdout="", stderr="")

    async def upload_text(self, path: str, content: str, *, mode: int = 0o700) -> None:
        self.uploads.append(RecordedUpload(path=path, content=content, mode=mode))
        self.files[path] = content

    async def read_text(self, path: str, *, max_bytes: int | None = None) -> str:
        self.reads.append(path)
        if path not in self.files:
            raise FileNotFoundError(path)
        data = self.files[path]
        if max_bytes is not None:
            return data[:max_bytes]
        return data

    async def close(self) -> None:
        self.closed = True


def _coerce_result(
    command: str,
    raw: CommandResult | tuple[int, str, str],
) -> CommandResult:
    if isinstance(raw, CommandResult):
        return raw
    exit_status, stdout, stderr = raw
    return CommandResult(
        command=command, exit_status=exit_status, stdout=stdout, stderr=stderr
    )


__all__ = ["FakeTransport", "RecordedCall", "RecordedUpload"]
