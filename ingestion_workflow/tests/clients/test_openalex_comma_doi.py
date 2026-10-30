"""A cited DOI holding a comma costs only its own lookup, not its batch's.

Crossref deposits carry DOIs like 10.1080/08,959,420.2016.1161693. In a `doi:a|b`
filter the comma starts a new filter, and OpenAlex answered 400 for the batch: on
2026-10-30 that left 1,498 reference lists without names. Served by a local server
that answers as OpenAlex does, since a recorded 400 names no work.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.models.ids import Identifier

COMMA = "10.1080/08,959,420.2016.1161693"
PLAIN = "10.1002/hbm.24703"


def _work(doi, year):
    return {"doi": "https://doi.org/" + doi, "ids": {"doi": "https://doi.org/" + doi},
            "is_retracted": False, "publication_year": year, "title": "t",
            "authorships": [{"author": {"display_name": "Jane A. Smith"}}]}


WORKS = {COMMA: _work(COMMA, 2016), PLAIN: _work(PLAIN, 2019)}
#: Each request as (filter or None, single work's DOI or None).
ASKED = []


class _OpenAlexHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        single = unquote(url.path).partition("/works/doi:")[2] or None
        query = parse_qs(url.query).get("filter", [None])[0]
        ASKED.append((query, single))
        if single:
            work = WORKS.get(single)
            self._answer(200 if work else 404, work or {"error": "not found"})
        elif any(":" not in part for part in query.split(",")):
            # each comma-separated part must be its own `field:value` filter
            self._answer(400, {"error": "Invalid query parameters error."})
        else:
            asked = query.removeprefix("doi:").split("|")
            self._answer(200, {"results": [WORKS[d] for d in asked if d in WORKS]})

    def _answer(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def client():
    server = HTTPServer(("127.0.0.1", 0), _OpenAlexHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = OpenAlexClient(email="test@example.com")
    client.BASE_URL = f"http://127.0.0.1:{server.server_port}"
    ASKED.clear()
    yield client
    server.shutdown()
    server.server_close()


def test_a_comma_doi_is_asked_alone_and_its_batch_is_answered(client):
    found = client.get_cited_works([PLAIN, COMMA])

    assert set(found) == {PLAIN, COMMA}
    assert (f"doi:{PLAIN}", None) in ASKED and (None, COMMA) in ASKED
    assert not any(q and COMMA in q for q, _ in ASKED)


def test_retractions_and_metadata_ask_a_comma_doi_alone_too(client):
    assert client.get_retractions([PLAIN, COMMA]) == {PLAIN: False, COMMA: False}
    found = client.get_metadata([Identifier(doi=PLAIN), Identifier(doi=COMMA)])
    assert len(found) == 2
    assert not any(q and COMMA in q for q, _ in ASKED)


def test_a_batch_of_comma_dois_alone_sends_no_filter(client):
    client.get_cited_works([COMMA])

    assert ASKED == [(None, COMMA)]
