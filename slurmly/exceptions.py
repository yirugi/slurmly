"""Exception hierarchy for slurmly."""

from __future__ import annotations


class SlurmlyError(Exception):
    """Base class for all slurmly errors."""


class InvalidConfig(SlurmlyError):
    """Configuration is invalid or incomplete."""


class InvalidJobSpec(SlurmlyError):
    """JobSpec failed validation (shape, regex, profile compatibility)."""


class UnknownPreset(InvalidConfig):
    """Cluster preset name/alias not in registry."""


class SSHTransportError(SlurmlyError):
    """Wraps SSH-level failures (connect, exec, sftp)."""


class RemoteCommandError(SlurmlyError):
    """A remote command exited non-zero when failure was not expected."""

    def __init__(
        self,
        command: str,
        exit_status: int,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(
            f"command failed with exit {exit_status}: {command}\n"
            f"stderr: {stderr.strip()[:500]}"
        )


class SbatchError(RemoteCommandError):
    """sbatch invocation failed."""


class SbatchParseError(SlurmlyError):
    """sbatch succeeded but its --parsable output could not be parsed."""

    def __init__(self, raw_stdout: str, raw_stderr: str = "") -> None:
        self.raw_stdout = raw_stdout
        self.raw_stderr = raw_stderr
        super().__init__(
            f"could not parse sbatch --parsable output: {raw_stdout!r}"
        )


class ScancelError(RemoteCommandError):
    """scancel invocation failed."""


class SlurmJobNotFound(SlurmlyError):
    """Job id was not found in squeue or sacct."""


class TransientStatusGap(SlurmlyError):
    """Job is within submit grace window but not yet visible in squeue/sacct."""
