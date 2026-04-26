"""Observability hook interface.

Core slurmly does not depend on any logging / metrics backend. Instead it
emits typed events through a `SlurmlyHooks` instance. The default is a no-op;
applications wire the hook to their preferred sink (logging, structlog,
OpenTelemetry, Prometheus, audit log, etc.).

Event handlers are async — runtime is `await`ed in the client's hot path, so
implementations should be cheap (offload heavy work to a queue).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CommandStartedEvent(_Event):
    operation: str
    command: str
    retry_safe: bool = False
    started_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class CommandFinishedEvent(_Event):
    operation: str
    command: str
    exit_status: int
    duration_seconds: float
    finished_at: str
    error: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SubmitSucceededEvent(_Event):
    internal_job_id: str
    slurm_job_id: str
    cluster: str | None = None
    submitted_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class SubmitFailedEvent(_Event):
    internal_job_id: str | None = None
    error_type: str
    error_message: str
    failed_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class StatusCheckedEvent(_Event):
    slurm_job_id: str
    source: str
    lifecycle: str
    visibility: str
    checked_at: str


class CancelRequestedEvent(_Event):
    slurm_job_id: str
    cluster: str | None = None
    signal: str | None = None
    requested_at: str


class SlurmlyHooks:
    """Default no-op implementation.

    Subclass and override the methods you care about. All methods are async
    and accept exactly one event object.
    """

    async def on_command_started(self, event: CommandStartedEvent) -> None:
        return None

    async def on_command_finished(self, event: CommandFinishedEvent) -> None:
        return None

    async def on_submit_succeeded(self, event: SubmitSucceededEvent) -> None:
        return None

    async def on_submit_failed(self, event: SubmitFailedEvent) -> None:
        return None

    async def on_status_checked(self, event: StatusCheckedEvent) -> None:
        return None

    async def on_cancel_requested(self, event: CancelRequestedEvent) -> None:
        return None


__all__ = [
    "SlurmlyHooks",
    "CommandStartedEvent",
    "CommandFinishedEvent",
    "SubmitSucceededEvent",
    "SubmitFailedEvent",
    "StatusCheckedEvent",
    "CancelRequestedEvent",
]
