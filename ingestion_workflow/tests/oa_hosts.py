"""One local HTTP server playing OpenAlex, Semantic Scholar and every file host.

The PDF source runs for real against it, through real `requests`; only the
network is local.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import requests
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pdf_extractor import PdfExtractor

PDF = b"%PDF-1.4\n%%EOF\n"
HTML = b"<html><body>Read this article on our site</body></html>"
EMAIL = "pipeline@example.org"


class OaHosts:
    def __init__(self) -> None:
        self.works: list[dict] = []
        self.s2: dict[str, str] = {}  # "DOI:..." -> openAccessPdf url
        self.files: dict[str, list[tuple[int, bytes, dict]]] = {}
        self.openalex_status = 200
        self.openalex_failing_types: set[str] = set()
        self.fetched: list[str] = []
        self.openalex_queries: list[dict] = []
        self.s2_queries: list[list[str]] = []
        hosts = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, headers=None):
                self.send_response(status)
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parts = urlsplit(self.path)
                if parts.path == "/works":
                    query = parse_qs(parts.query)
                    hosts.openalex_queries.append(query)
                    id_type, values = query["filter"][0].split(":", 1)
                    if hosts.openalex_status != 200 or id_type in hosts.openalex_failing_types:
                        return self._send(500 if hosts.openalex_status == 200 else
                                          hosts.openalex_status, b"{}")
                    wanted = values.split("|")
                    results = [w for w in hosts.works if _id_of(w, id_type) in wanted]
                    return self._send(200, json.dumps({"results": results}).encode())
                hosts.fetched.append(parts.path)
                queue = hosts.files.get(parts.path)
                if not queue:
                    return self._send(404, b"not found")
                status, body, headers = queue.pop(0) if len(queue) > 1 else queue[0]
                return self._send(status, body, headers)

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                ids = json.loads(self.rfile.read(length))["ids"]
                hosts.s2_queries.append(ids)
                records = [
                    {"title": "t", "openAccessPdf": {"url": hosts.s2[i]}} if i in hosts.s2
                    else None
                    for i in ids
                ]
                return self._send(200, json.dumps(records).encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def serve(self, path: str, *responses) -> str:
        self.files[path] = [r if len(r) == 3 else (*r, {}) for r in responses]
        return self.url + path

    def work(self, doi: str | None, *locations: dict, best: dict | None = None,
             pmid: str | None = None) -> None:
        ids = {}
        if doi:
            ids["doi"] = f"https://doi.org/{doi}"
        if pmid:
            ids["pmid"] = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}"
        self.works.append({"ids": ids, "best_oa_location": best, "locations": list(locations)})

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _id_of(work: dict, id_type: str) -> str | None:
    value = work["ids"].get(id_type)
    return value.rsplit("doi.org/" if id_type == "doi" else "/", 1)[1] if value else None


def location(pdf_url=None, kind="journal", landing=None, version="publishedVersion", name=None):
    """An OpenAlex location as the API returns it."""
    return {
        "is_oa": True,
        "pdf_url": pdf_url,
        "landing_page_url": landing,
        "version": version,
        "source": {"type": kind, "display_name": name or kind},
    }


def pdf_source(tmp_path, session=None, **overrides) -> PdfExtractor:
    """A PDF source asking the local hosts, with its sleeps recorded in `.slept`."""
    settings = {
        "cache_root": tmp_path / "cache",
        "data_root": tmp_path / "data",
        "pdf_cache_root": tmp_path / "pdfs",
        "max_workers": 1,
        "openalex_email": EMAIL,
        "openalex_api_key": None,
        "semantic_scholar_api_key": "s2-key",
        **overrides,
    }
    extractor = PdfExtractor(settings=Settings(**settings), session=session or requests.Session())
    extractor.slept = []
    now = [1000.0]

    def sleep(delay):
        extractor.slept.append(delay)
        now[0] += delay

    extractor._sleep = sleep
    extractor._clock = lambda: now[0]
    return extractor
