import http.server
import threading
import urllib.request
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pytest
import vcr
import yaml
from ingestion_workflow.tests import cassette_secrets, wiley_tdm

CASSETTES = Path(__file__).parent / "cassettes"
SECRET_NAME_PARTS = ("key", "token", "auth", "secret", "cookie", "password")
# What a cassette may hold under a credential's name: the placeholder, or nothing.
PLACEHOLDERS = {
    "wiley-tdm-client-token": wiley_tdm.REPLAY_TOKEN,
    **cassette_secrets.ECHOED_KEY_HEADERS,
}
# Not credentials: CORS lists of header names, rate-limit counters, a search cursor.
NOT_CREDENTIALS = ("access-control-", "x-ratelimit")
CURSOR_PARAMETERS = {"token"}


def _looks_secret(name: str) -> bool:
    name = name.lower()
    return any(part in name for part in SECRET_NAME_PARTS) and not name.startswith(NOT_CREDENTIALS)


def _unfiltered(cassette: Path) -> list[str]:
    found = []
    for interaction in yaml.safe_load(cassette.read_text())["interactions"]:
        request, response = interaction["request"], interaction["response"]
        for where, headers in (("request", request["headers"]), ("response", response["headers"])):
            for name, values in headers.items():
                if _looks_secret(name) and list(values) != [PLACEHOLDERS.get(name.lower())]:
                    found.append(f"{where} header {name}")
        for name, _ in parse_qsl(urlsplit(request["uri"]).query):
            if _looks_secret(name) and name not in CURSOR_PARAMETERS:
                found.append(f"query parameter {name}")
    return found


@pytest.mark.parametrize("cassette", sorted(CASSETTES.rglob("*.yaml")), ids=lambda p: p.name)
def test_a_cassette_holds_no_credential_but_its_placeholder(cassette):
    assert _unfiltered(cassette) == []


class _EchoesTheKey(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("x-els-apikey", self.headers["x-els-apikey"] or "")
        self.send_header("Set-Cookie", "session=abc")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def test_a_recording_replaces_a_key_the_host_echoes_and_drops_what_was_sent(vcr_config, tmp_path):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _EchoesTheKey)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    live_key = "0123456789abcdef0123456789abcdef"
    cassette = tmp_path / "recorded.yaml"
    recorder = vcr.VCR(**{k: v for k, v in vcr_config.items() if k != "ignore_hosts"})
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_port}/article?apiKey={live_key}&api_key={live_key}&x=1",
        headers={"x-els-apikey": live_key, "x-api-key": live_key, "Authorization": live_key},
    )
    try:
        with recorder.use_cassette(str(cassette)):
            urllib.request.urlopen(request).close()
    finally:
        server.shutdown()

    assert live_key not in cassette.read_text()
    assert _unfiltered(cassette) == []
    recorded = yaml.safe_load(cassette.read_text())["interactions"][0]
    echoed = recorded["response"]["headers"]["x-els-apikey"]
    assert echoed == [cassette_secrets.REPLAY_ELSEVIER_KEY]
    assert recorded["request"]["uri"].endswith("/article?x=1")
