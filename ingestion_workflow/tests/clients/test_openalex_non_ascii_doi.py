"""A DOI with a non-ASCII cased letter is one DOI from the client to the stages that read its answer.

Served by a local HTTP server that answers the way OpenAlex does (the work's DOI
as a resolver URL, in the case it was registered in), because no recorded answer
exists for such a DOI.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages.notices import NoticesStage
from ingestion_workflow.services import reference_lists as L
from ingestion_workflow.tests.services.test_corrections import _ctx

REGISTERED = "10.26321/ÁBC.Def"
CANONICAL = Identifier(doi=REGISTERED).doi
WORK = {
    "doi": "https://doi.org/" + REGISTERED,
    "ids": {"doi": "https://doi.org/" + REGISTERED, "pmid": "https://pubmed.ncbi.nlm.nih.gov/42"},
    "is_retracted": True,
    "publication_year": 2011,
    "authorships": [{"author": {"display_name": "Jane A. Smith"}}],
}


#: Every DOI the server was asked for, as it was sent.
ASKED = []


class _OpenAlexHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        asked = parse_qs(urlparse(self.path).query).get("filter", [""])[0].removeprefix("doi:").split("|")
        ASKED.extend(asked)
        known = [WORK] if any(a.lower() == REGISTERED.lower() for a in asked) else []
        body = json.dumps({"results": known}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def client():
    server = HTTPServer(("127.0.0.1", 0), _OpenAlexHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = OpenAlexClient(email="test@example.com")
    client.BASE_URL = f"http://127.0.0.1:{server.server_port}"
    yield client
    server.shutdown()
    server.server_close()


def test_the_answers_are_keyed_by_the_canonical_doi(client):
    assert client.get_retractions([CANONICAL]) == {CANONICAL: True}
    assert set(client.get_cited_works([CANONICAL])) == {CANONICAL}


def test_metadata_is_asked_for_and_keyed_by_the_canonical_doi(client):
    ASKED.clear()
    identifier = Identifier(doi=REGISTERED)
    found = client.get_metadata([identifier])
    assert ASKED == [CANONICAL]
    assert found[identifier.slug].publication_year == 2011


def test_a_retraction_openalex_alone_reports_reaches_the_notice(client):
    stage = NoticesStage(Settings())
    stage._openalex = client
    stage._client = SimpleNamespace(get_notices=lambda pmids: {})
    ref = SimpleNamespace(id="a1", identifier=Identifier(doi=REGISTERED))

    [out] = stage.execute(_ctx(), [Work(ref=ref, source="", fingerprint="f", upstream=None)])

    assert out.summary["retracted"] and out.summary["retraction"]["source"] == "openalex"


def test_openalex_fills_a_reference_whose_doi_has_a_non_ascii_letter(client):
    refs = L.from_crossref({"reference": [{"DOI": REGISTERED}]})

    assert L.enrich(refs, client.get_cited_works(L.cited(refs))) == 1
    assert refs[0]["authors"] == ["Smith"] and refs[0]["year"] == 2011 and refs[0]["pmid"] == "42"
