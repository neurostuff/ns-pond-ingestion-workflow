import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent / "cassettes")


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "filter_headers": [
            "authorization",
            "x-api-key",
            "x-els-apikey",
            "x-els-insttoken",
        ],
        "filter_query_parameters": [
            ("api_key", "DUMMY"),
        ],
        "record_mode": "once",
    }
