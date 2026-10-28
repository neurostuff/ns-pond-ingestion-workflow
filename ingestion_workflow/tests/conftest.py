import json
import os
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from dotenv import load_dotenv
from ingestion_workflow.catalog.evidence import Evidence
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.tests import cassette_secrets, upload_body, wiley_tdm
from ingestion_workflow.tests import neurostore as neurostore_mock


@pytest.fixture(scope="session")
def vcr_cassette_dir() -> str:
    return str(Path(__file__).parent / "cassettes")


def pytest_recording_configure(config, vcr):
    vcr.register_matcher("wiley_token", wiley_tdm.same_token)
    vcr.register_matcher("upload_body", upload_body.same_body_but_input_fingerprints)


@pytest.fixture(scope="session")
def vcr_config():
    return {
        "before_record_response": cassette_secrets.scrub_response,
        "filter_headers": [
            *cassette_secrets.SECRET_REQUEST_HEADERS,
            ("wiley-tdm-client-token", wiley_tdm.REPLAY_TOKEN),
        ],
        "filter_query_parameters": [
            *cassette_secrets.SECRET_QUERY_PARAMETERS,
            *((name, cassette_secrets.REPLAY_EMAIL) for name in cassette_secrets.EMAIL_QUERY_PARAMETERS),
        ],
        "record_mode": "once",
        "ignore_hosts": ["localhost", neurostore_mock.HOST],
    }


@pytest.fixture()
def neurostore(monkeypatch):
    """neurostore's ingester as its contract has it, at `neurostore_mock.URL`, with
    NEUROSTORE_TOKEN set to its admin token. An ingester takes it as its `transport`, or
    gets it from `from_settings`. The test fails on any request or answer the contract
    does not allow, even one the code under test caught."""
    monkeypatch.setenv("NEUROSTORE_TOKEN", neurostore_mock.TOKEN)
    server = neurostore_mock.Neurostore()
    server.install(monkeypatch)
    yield server
    server.check()


@pytest.fixture()
def pubmed():
    with PubMedClient(email=cassette_secrets.REPLAY_EMAIL) as client:
        yield client


@pytest.fixture()
def evidence(pubmed):
    """NCBI and the registries, asked with the placeholder email and answered from the
    test's cassette, so what a test registers is checked against recorded answers."""
    registry = DoiRegistry()
    yield Evidence(pubmed, registry, CrossrefClient(cassette_secrets.REPLAY_EMAIL))
    registry.close()


#: Loopback is where a test starts its own server, and the mock neurostore's host is served
#: in-process; the Hub is where docling's weights come from, a cache CI fills in its own step
#: and never a cassette.
IGNORED_HOSTS = ["localhost", "127.0.0.1", neurostore_mock.HOST, "huggingface.co"]


def pytest_collection_modifyitems(items):
    """A test that takes `evidence` asks NCBI and the registries, so it has a cassette;
    only IGNORED_HOSTS are left out of it."""
    for item in items:
        if "evidence" in item.fixturenames and item.get_closest_marker("vcr") is None:
            item.add_marker(pytest.mark.vcr(ignore_hosts=IGNORED_HOSTS))


@pytest.fixture(scope="session", autouse=True)
def readabilipy_node_modules():
    """readabilipy runs `npm install` on its first use, inside site-packages. xdist workers
    reaching that together corrupt each other's node_modules, so one installs it and the
    rest wait."""
    from filelock import FileLock
    from readabilipy.simple_json import have_node

    with FileLock(Path(tempfile.gettempdir()) / "readabilipy-npm-install.lock"):
        have_node()


@pytest.fixture(autouse=True)
def no_ambient_email(monkeypatch):
    """Settings read PUBMED_EMAIL, then EMAIL, so a developer's own address would change
    which requests a test makes; a test that wants one sets it."""
    monkeypatch.delenv("PUBMED_EMAIL", raising=False)
    monkeypatch.delenv("EMAIL", raising=False)


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


class _Provider(BaseHTTPRequestHandler):
    statuses: list
    asked: list
    body: bytes

    def _answer(self):
        self.asked.append(time.monotonic())
        status = self.statuses[min(len(self.asked), len(self.statuses)) - 1]
        self.send_response(status)
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    do_GET = do_PUT = do_POST = _answer

    def log_message(self, *args):
        pass


@pytest.fixture
def provider():
    """A provider on localhost that answers each request with the next of `statuses` (the
    last one from then on) and `body`, and notes when each was asked."""
    handler = type("Handler", (_Provider,), {
        "statuses": [200], "asked": [],
        "body": b'{"message": {"items": []}, "email": [], "results": [], "resultsFound": 0}'})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handler.url = f"http://127.0.0.1:{server.server_port}/"
    yield handler
    server.shutdown()
    thread.join()
    server.server_close()
