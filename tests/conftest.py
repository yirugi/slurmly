from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "slurm" / "purdue_anvil"


@pytest.fixture
def fixtures_root() -> Path:
    return FIXTURES_ROOT
