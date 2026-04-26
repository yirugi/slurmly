"""slurmly — SSH-only Slurm cluster client library."""

from .artifacts import Artifact, ArtifactDownload
from .capabilities import CapabilityReport
from .cleanup import CleanupCandidate, CleanupPlan, CleanupResult
from .client import SlurmSSHClient
from .exceptions import (
    InvalidConfig,
    InvalidJobSpec,
    SbatchError,
    SbatchParseError,
    ScancelError,
    SlurmJobNotFound,
    SlurmlyError,
    SSHTransportError,
    UnknownPreset,
)
from .hooks import (
    CancelRequestedEvent,
    CommandFinishedEvent,
    CommandStartedEvent,
    SlurmlyHooks,
    StatusCheckedEvent,
    SubmitFailedEvent,
    SubmitSucceededEvent,
)
from .models import (
    CancelResult,
    DependencyType,
    JobDependency,
    JobInfo,
    JobInfoSource,
    JobSpec,
    JobVisibility,
    LogChunk,
    RenderedJob,
    SubmittedJob,
)
from .polling import PollingPolicy, PollingResult
from .presets import get_preset, list_presets
from .profiles import ClusterPreset, ClusterProfile, ExecutionProfile
from .states import Lifecycle

__version__ = "0.4.0"

__all__ = [
    # client
    "SlurmSSHClient",
    # models
    "JobSpec",
    "SubmittedJob",
    "RenderedJob",
    "LogChunk",
    "CancelResult",
    "JobInfo",
    "JobInfoSource",
    "JobVisibility",
    "Lifecycle",
    # profiles
    "ClusterProfile",
    "ExecutionProfile",
    "ClusterPreset",
    # registry
    "get_preset",
    "list_presets",
    # phase 3
    "CapabilityReport",
    "CleanupCandidate",
    "CleanupPlan",
    "CleanupResult",
    "PollingPolicy",
    "PollingResult",
    "SlurmlyHooks",
    "CommandStartedEvent",
    "CommandFinishedEvent",
    "SubmitSucceededEvent",
    "SubmitFailedEvent",
    "StatusCheckedEvent",
    "CancelRequestedEvent",
    # phase 4
    "JobDependency",
    "DependencyType",
    "Artifact",
    "ArtifactDownload",
    # exceptions
    "SlurmlyError",
    "InvalidConfig",
    "InvalidJobSpec",
    "UnknownPreset",
    "SSHTransportError",
    "SbatchError",
    "SbatchParseError",
    "ScancelError",
    "SlurmJobNotFound",
    # version
    "__version__",
]
