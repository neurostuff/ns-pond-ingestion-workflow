import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent / "cassettes")


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "filter_headers": ["authorization", "x-api-key"],
    }
