"""Regex-based validators used by JobSpec and config.

These are kept as plain functions (not pydantic validators) so they can be
reused from config loaders, profile validation, and tests.
"""

from __future__ import annotations

import re

# --- Patterns ---------------------------------------------------------------

# Slurm --mem accepts integer + optional unit suffix (K|M|G|T).
# Examples: "512", "512M", "32G", "1T". Reject ambiguous forms like "32 GB".
_MEMORY_RE = re.compile(r"^\d+[KMGT]?$")

# Slurm --time accepts these forms:
#   minutes               -> "30"
#   minutes:seconds       -> "30:00"
#   hours:minutes:seconds -> "01:30:00"
#   days-hours            -> "1-12"
#   days-hours:minutes    -> "1-12:30"
#   days-hours:minutes:seconds -> "1-12:30:00"
_TIME_LIMIT_RE = re.compile(
    r"^(?:"
    r"\d+"
    r"|\d+:\d{1,2}"
    r"|\d+:\d{1,2}:\d{1,2}"
    r"|\d+-\d{1,2}"
    r"|\d+-\d{1,2}:\d{1,2}"
    r"|\d+-\d{1,2}:\d{1,2}:\d{1,2}"
    r")$"
)

# POSIX shell env var name: [A-Za-z_][A-Za-z0-9_]*
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# JobSpec.name: usable as Slurm --job-name and as a path fragment.
# Allow alnum, dash, underscore, dot. 1..128 chars.
_JOB_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

# internal_job_id: slurmly-<8 hex>  (generator + validator must agree)
_INTERNAL_JOB_ID_RE = re.compile(r"^slurmly-[0-9a-f]{8}$")

# Username: typical POSIX login plus ACCESS-style "x-foo" form.
_USERNAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9._-]{0,63}$")

# Slurm --array spec. Slurm's grammar:
#   N | N-M | N-M:S | N,M,P | combinations of those, optionally followed by %C
# Accept lists of items separated by commas, each item being:
#   <int> | <int>-<int> | <int>-<int>:<int>
# and an optional trailing "%<int>" for max-concurrent.
_ARRAY_ITEM = r"\d+(?:-\d+(?::\d+)?)?"
_ARRAY_SPEC_RE = re.compile(
    rf"^{_ARRAY_ITEM}(?:,{_ARRAY_ITEM})*(?:%\d+)?$"
)

# Slurm job id with optional array task suffix:
#   plain "12345" or array "12345_0" / "12345_3" / "12345_*"
_PLAIN_JOB_ID_RE = re.compile(r"^\d+$")
_ARRAY_JOB_ID_RE = re.compile(r"^\d+(?:_(?:\d+|\*))?$")


# --- Public helpers ---------------------------------------------------------


def is_valid_memory(value: str) -> bool:
    return bool(_MEMORY_RE.match(value))


def is_valid_time_limit(value: str) -> bool:
    return bool(_TIME_LIMIT_RE.match(value))


def is_valid_env_key(value: str) -> bool:
    return bool(_ENV_KEY_RE.match(value))


def is_valid_job_name(value: str) -> bool:
    return bool(_JOB_NAME_RE.match(value))


def is_valid_internal_job_id(value: str) -> bool:
    return bool(_INTERNAL_JOB_ID_RE.match(value))


def is_valid_username(value: str) -> bool:
    return bool(_USERNAME_RE.match(value))


def is_valid_array_spec(value: str) -> bool:
    return bool(_ARRAY_SPEC_RE.match(value))


def is_valid_plain_job_id(value: str) -> bool:
    return bool(_PLAIN_JOB_ID_RE.match(value))


def is_valid_array_job_id(value: str) -> bool:
    """Plain id OR `<id>_<task>` / `<id>_*`."""
    return bool(_ARRAY_JOB_ID_RE.match(value))
