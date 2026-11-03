"""The data-source matcher reads its aliases from the repo's dictionary and writes
paper_entities.tsv keyed by neurostore's data-source ids."""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

SCRIPTS = Path(__file__).resolve().parents[3] / "scripts" / "data_sources"
SAMPLES = Path(__file__).resolve().parents[1] / "data" / "sample_outputs"
sys.path.insert(0, str(SCRIPTS))

import match  # noqa: E402


@pytest.fixture(scope="module")
def rows():
    return match.read_dictionary_rows()


def test_dictionary_ids_are_unique_and_parents_exist(rows):
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids))
    assert {p for r in rows for p in match.split(r["hosted_by"])} <= set(ids)


def paper(tmp_path, text):
    d = tmp_path / "pond" / "p1"
    (d / "processed" / "pubget").mkdir(parents=True, exist_ok=True)
    (d / "processed" / "pubget" / "text.txt").write_text(text)
    (d / "identifiers.json").write_text(json.dumps({"pmid": "1", "doi": "10.1/x"}))
    return d


def test_context_acronym_needs_a_dataset_word(tmp_path, rows):
    match.init(rows)
    found = match.search(str(paper(tmp_path, "Methods\nWe thank DIAN.\n")))
    assert not found["rows"]
    found = match.search(str(paper(tmp_path, "Methods\nWe used DIAN data.\n")))
    assert [(r["entity"], r["status"]) for r in found["rows"]] == [("dian", "used")]


def test_writes_paper_entities_for_link_matches(tmp_path):
    out = tmp_path / "out"
    subprocess.run(
        [sys.executable, "-I", str(SCRIPTS / "match.py"), str(out), str(SAMPLES)],
        check=True,
        capture_output=True,
    )
    with open(out / "paper_entities.tsv", newline="") as f:
        found = list(csv.DictReader(f, delimiter="\t"))
    assert [(r["paper"], r["pmid"], r["doi"], r["entity"]) for r in found] == [
        ("6dYthnbocsU4", "33524583", "10.1016/j.neuroimage.2021.117808", "neurovault")
    ]


def test_missing_from_neurostore_names_ids_the_api_lacks(rows):
    # the shape of neurostore's list response
    served = [{"id": r["id"]} for r in rows if r["id"] != "ukb"]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            page = int(parse_qs(urlparse(self.path).query)["page"][0])
            body = {
                "results": served[(page - 1) * 1000 : page * 1000],
                "metadata": {"total_count": len(served)},
            }
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        api = f"http://127.0.0.1:{server.server_port}/api"
        assert match.missing_from_neurostore(rows, api) == ["ukb"]
    finally:
        server.shutdown()
