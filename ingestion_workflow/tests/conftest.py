import json
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.tests import upload_body, wiley_tdm


@pytest.fixture(scope="session")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent / "cassettes")


def pytest_recording_configure(config, vcr):
    vcr.register_matcher("wiley_token", wiley_tdm.same_token)
    vcr.register_matcher("upload_body", upload_body.same_body_but_input_fingerprints)


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "filter_headers": [
            "authorization",
            "x-api-key",
            "x-els-apikey",
            "x-els-insttoken",
            ("wiley-tdm-client-token", wiley_tdm.REPLAY_TOKEN),
        ],
        "filter_query_parameters": ["api_key"],
        "record_mode": "once",
        "ignore_hosts": ["localhost"],
    }


@pytest.fixture(scope="session")
def manifest_identifiers() -> Identifiers:
    """Load the first 1000 manifest entries into Identifiers."""
    manifest_path = Path(__file__).parent / "data" / "manifests" / "index.json"
    with manifest_path.open(encoding="utf-8") as manifest_file:
        manifest_data = json.load(manifest_file)

    identifiers: list[Identifier] = []
    for neurostore_id, trio in list(manifest_data.items())[:1000]:
        pmid, doi, pmcid = trio
        identifiers.append(
            Identifier(
                neurostore=neurostore_id,
                pmid=pmid,
                doi=doi,
                pmcid=pmcid,
            )
        )

    return Identifiers(identifiers)


@pytest.fixture(scope="session")
def semantic_scholar_api_key() -> str:
    """Return Semantic Scholar API key or skip tests if missing."""
    load_dotenv()
    api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
    if not api_key:
        pytest.skip("SEMANTIC_SCHOLAR_API_KEY not configured")
    return api_key
