"""Uploading a parse through neurostore's study_schema ingester, against a stub server.

The stub answers the way neurostore's ingester does in its own tests
(store/backend/neurostore/tests/test_study_schema_ingest.py): admin only, keyed by the
parse's analysis keys, a re-upload of the same parse creates nothing, an original
re-parse that reads an analysis from other cells of the same table under the same name
aliases it, and a stored parse that is no longer current is refused with NOT_CURRENT.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from ingestion_workflow.catalog import Status
from ingestion_workflow.config import NeurostoreEnv, Settings
from ingestion_workflow.services import neurostore_ingest as ni
from ingestion_workflow.tests.services.test_paper_parse import _written
from study_schema import keys

ADMIN = "admin-token-5f2c9e"
USER = "user-token-8a1d"


class _Neurostore:
    """The ingester's observable behaviour, in memory."""

    def __init__(self) -> None:
        self.requests = []
        #: Answers given, in order, before the upload is handled: (status, body, headers),
        #: or ("sleep", seconds) to answer only after the client has given up.
        self.script = []
        self.papers = {}

    def handle(self, headers, body):
        self.requests.append((headers, body))
        if self.script:
            step = self.script.pop(0)
            if step[0] == "sleep":
                time.sleep(step[1])
                return 200, {}, {}
            return step
        auth = headers.get("Authorization")
        if auth == f"Bearer {USER}":
            return 403, _error(403, "Only an admin can upload study_schema artifacts."), {}
        if auth != f"Bearer {ADMIN}":
            return 401, _error(401, "Unauthorized"), {}
        upload = json.loads(body)
        parse = upload.get("coordinate_parse")
        if parse is None:
            return 400, _error(400, "An upload needs a coordinate_parse, a record, or both."), {}
        ids = parse["header"]["identifiers"]
        base_study_id = upload.get("base_study_id") or ids.get("neurostore_base_study_id")
        paper = self.papers.setdefault(
            base_study_id,
            {"study_id": f"study-{len(self.papers)}", "current": None, "stored": set(),
             "analyses": {}},
        )
        parse_id = parse["parse_id"]
        if paper["current"] not in (None, parse_id) and parse_id in paper["stored"]:
            return 422, _error(422, "This parse is stored and is no longer the paper's "
                               "current parse.", code="NOT_CURRENT"), {}
        held = paper["analyses"]
        uploaded = {a["key"]: _place(a) for a in parse["analyses"]}
        dropped = {k: v for k, v in held.items() if k not in uploaded}
        aliased = [
            {"from": old, "to": new}
            for new, (table, name, cells) in uploaded.items()
            if new not in held
            for old, (o_table, o_name, o_cells) in dropped.items()
            if (o_table, o_name) == (table, name) and o_cells & cells
        ]
        paper["stored"].add(parse_id)
        paper["current"] = parse_id
        paper["analyses"] = uploaded
        return 200, {
            "kind": "parse",
            "base_study_id": base_study_id,
            "study_id": paper["study_id"],
            "parse_id": parse_id,
            "skeleton": {
                "created": sum(k not in held for k in uploaded),
                "updated": sum(k in held for k in uploaded),
                "superseded": len(dropped),
                "omitted_analyses": len(parse.get("omitted_analyses") or []),
                "aliased": aliased,
                "orphaned_claims": [],
            },
            "set_aside_resolved": [],
            "provenance": {"parse": {"run_id": upload.get("run_id")}},
        }, {}


def _place(analysis):
    cells = frozenset((c["row"], c["column_group"]) for c in analysis.get("cells") or [])
    return analysis.get("table_id"), keys.normalize_name(analysis["name"]), cells


def _error(status, detail, code=None):
    error = {"status": status, "title": "Error", "detail": detail, "timestamp": "t",
             "request_id": "r"}
    if code:
        error["errors"] = [{"field": "coordinate_parse/parse_id", "code": code}]
    return error


@pytest.fixture()
def neurostore():
    stub = _Neurostore()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 - http.server's name
            body = self.rfile.read(int(self.headers["Content-Length"]))
            status, payload, headers = stub.handle(dict(self.headers), body)
            data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
            try:
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except OSError:
                pass  # the client timed out and hung up

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    stub.url = f"http://127.0.0.1:{server.server_address[1]}"
    yield stub
    server.shutdown()
    server.server_close()


@pytest.fixture()
def study(tmp_path):
    _, _, summary = _written(tmp_path)
    return summary["dir"].parent


@pytest.fixture()
def token(monkeypatch, tmp_path):
    # read_token falls back to ./.env; a checkout's own .env must not leak into a test.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NEUROSTORE_TOKEN", ADMIN)
    return ADMIN


def _ingester(neurostore, sleeps=None, **kwargs):
    kwargs.setdefault("backoff", 1.0)
    return ni.NeurostoreIngester(
        neurostore.url, sleep=(sleeps.append if sleeps is not None else lambda s: None),
        **kwargs,
    )


def _artifact(study):
    return json.loads((study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text())


def test_a_parse_is_posted_as_sync_wrote_it_and_its_summary_recorded(neurostore, study, token):
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    headers, body = neurostore.requests[0]
    assert headers["Authorization"] == f"Bearer {ADMIN}"
    sent = json.loads(body)
    assert sent["coordinate_parse"] == json.loads(
        (study / "parse" / "coordinate_parse.json").read_text()
    )
    assert sent["parsed_paper"] == json.loads((study / "parse" / "parsed_paper.json").read_text())
    assert artifact == _artifact(study)
    assert artifact["status"] == "uploaded"
    assert ni.Result(artifact["status"]).catalog_status is Status.OK
    assert artifact["http_status"] == 200
    assert artifact["parse_id"] == sent["coordinate_parse"]["parse_id"]
    assert artifact["payload_sha256"] == hashlib.sha256(body).hexdigest()
    assert artifact["checks"][:2] == ["model", "check_paper"]
    summary = artifact["summary"]
    assert summary["kind"] == "parse"
    assert summary["study_id"] == "study-0"
    assert summary["skeleton"]["created"] == len(sent["coordinate_parse"]["analyses"]) > 0
    assert ADMIN not in (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text()


def test_reposting_the_same_parse_creates_nothing(neurostore, study, token):
    with _ingester(neurostore) as ingester:
        first = ingester.upload(study)
        again = ingester.upload(study)

    n = first["summary"]["skeleton"]["created"]
    assert again["status"] == "uploaded"
    assert again["summary"]["study_id"] == first["summary"]["study_id"]
    assert again["summary"]["skeleton"] == {
        **first["summary"]["skeleton"], "created": 0, "updated": n
    }
    assert neurostore.requests[0][1] == neurostore.requests[1][1]


def _reparse(study):
    """The parse re-read with one more cell under one analysis, as a new parse_id."""
    path = study / "parse" / "coordinate_parse.json"
    parse = json.loads(path.read_text())
    old = parse["parse_id"]
    analysis = next(a for a in parse["analyses"] if a["name"] == "Houses > Faces")
    before = analysis["key"]
    analysis["cells"].append({"row": analysis["cells"][0]["row"] - 1, "column_group": 0})
    analysis["key"] = keys.table_key(
        analysis["table_id"], [(c["row"], c["column_group"]) for c in analysis["cells"]],
        analysis["name"],
    )
    parse["parse_id"] = hashlib.sha256(json.dumps(parse["analyses"]).encode()).hexdigest()
    path.write_text(json.dumps(parse))
    return old, before, analysis["key"]


def test_a_reparse_reading_other_cells_records_the_alias(neurostore, study, token):
    with _ingester(neurostore) as ingester:
        ingester.upload(study)
        old_parse, before, after = _reparse(study)
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    assert artifact["parse_id"] != old_parse
    assert artifact["summary"]["skeleton"]["aliased"] == [{"from": before, "to": after}]
    assert artifact["summary"]["skeleton"]["created"] == 1


@pytest.mark.parametrize("status", [400, 404, 422])
def test_a_refusal_is_recorded_loudly_and_never_retried(neurostore, study, token, status, caplog):
    neurostore.script = [(status, _error(status, "The coordinate-parse does not validate "
                                         "against study_schema.", code="SCHEMA"), {})]
    sleeps = []
    with _ingester(neurostore, sleeps) as ingester, caplog.at_level(logging.WARNING):
        artifact = ingester.upload(study)

    assert len(neurostore.requests) == 1
    assert sleeps == []
    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["http_status"] == status
    assert artifact["error"]["detail"].startswith("The coordinate-parse does not validate")
    assert artifact["error"]["errors"] == [
        {"field": "coordinate_parse/parse_id", "code": "SCHEMA"}
    ]
    assert "does not validate" in artifact["error"]["body_excerpt"]
    assert "refused" in caplog.text


def test_replaying_a_superseded_parse_is_refused_not_retried(neurostore, study, token):
    with _ingester(neurostore) as ingester:
        ingester.upload(study)
        original = (study / "parse" / "coordinate_parse.json").read_text()
        _reparse(study)
        ingester.upload(study)
        (study / "parse" / "coordinate_parse.json").write_text(original)
        artifact = ingester.upload(study)

    assert len(neurostore.requests) == 3
    assert artifact["status"] == "refused"
    assert artifact["error"]["errors"][0]["code"] == "NOT_CURRENT"


@pytest.mark.parametrize("token_value, status", [("wrong-token", 401), (USER, 403)])
def test_a_refused_token_stops_the_run(neurostore, study, monkeypatch, tmp_path, token_value,
                                       status, caplog):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NEUROSTORE_TOKEN", token_value)
    sleeps = []
    with _ingester(neurostore, sleeps) as ingester, caplog.at_level(logging.DEBUG):
        with pytest.raises(ni.NeurostoreAuthError) as stopped:
            ingester.upload(study)

    assert len(neurostore.requests) == 1
    assert sleeps == []
    assert "NEUROSTORE_TOKEN" in str(stopped.value)
    assert token_value not in str(stopped.value)
    assert token_value not in caplog.text
    artifact = _artifact(study)
    assert artifact["status"] == "unauthorized"
    assert artifact["http_status"] == status


def test_a_missing_token_stops_before_anything_is_sent(neurostore, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    with pytest.raises(ni.NeurostoreAuthError, match="NEUROSTORE_TOKEN"):
        _ingester(neurostore)
    assert neurostore.requests == []


@pytest.mark.parametrize("transient", [503, 502, 429])
def test_a_transient_error_is_retried_with_backoff_then_succeeds(neurostore, study, token,
                                                                 transient):
    neurostore.script = [(transient, b"busy", {"Retry-After": "5"}), (500, b"oops", {})]
    sleeps = []
    with _ingester(neurostore, sleeps) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    assert [a.get("http_status") for a in artifact["attempts"]] == [transient, 500, 200]
    # Retry-After is honoured when it asks for longer than the backoff.
    assert sleeps == [5.0, 2.0]
    assert len(neurostore.requests) == 3
    assert "error" not in artifact


def test_timeouts_on_every_attempt_are_recorded_as_a_block(neurostore, study, token):
    neurostore.script = [("sleep", 1.0)] * 3
    sleeps = []
    with _ingester(neurostore, sleeps, timeout=0.2, retries=2) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "blocked"
    assert ni.Result(artifact["status"]).catalog_status is Status.FAILED
    assert [a["error"] for a in artifact["attempts"]] == ["timeout"] * 3
    assert artifact["error"] == {"detail": "timeout"}
    assert sleeps == [1.0, 2.0]
    assert _artifact(study) == artifact


def test_a_5xx_on_every_attempt_is_a_block_with_the_last_body(neurostore, study, token):
    neurostore.script = [(503, b"down", {})] * 3
    with _ingester(neurostore, retries=2) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "blocked"
    assert artifact["http_status"] == 503
    assert artifact["error"]["body_excerpt"] == "down"
    assert len(neurostore.requests) == 3


def test_a_dry_run_writes_the_exact_payload_and_sends_nothing(neurostore, study, monkeypatch,
                                                              tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    artifact = ni.NeurostoreIngester(neurostore.url, dry_run=True).upload(study, run_id="run-7")

    assert neurostore.requests == []
    assert artifact["status"] == "dry_run"
    payload = (study / ni.UPLOAD_DIR / ni.PAYLOAD_NAME).read_bytes()
    assert artifact["payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert json.loads(payload) == {
        "coordinate_parse": json.loads((study / "parse" / "coordinate_parse.json").read_text()),
        "parsed_paper": json.loads((study / "parse" / "parsed_paper.json").read_text()),
        "run_id": "run-7",
    }


def test_the_dry_run_payload_is_the_body_a_real_upload_sends(neurostore, study, token):
    ni.NeurostoreIngester(neurostore.url, dry_run=True).upload(study)
    with _ingester(neurostore) as ingester:
        ingester.upload(study)
    assert neurostore.requests[0][1] == (study / ni.UPLOAD_DIR / ni.PAYLOAD_NAME).read_bytes()


def test_a_parse_whose_key_is_not_its_cells_is_not_sent(neurostore, study, token):
    path = study / "parse" / "coordinate_parse.json"
    parse = json.loads(path.read_text())
    parse["analyses"][0]["key"] = parse["analyses"][0]["table_id"] + "#000000000000"
    path.write_text(json.dumps(parse))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert neurostore.requests == []
    assert artifact["status"] == "invalid"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert any("is not the key its cells give" in p for p in artifact["error"]["problems"])


def test_a_parse_that_fails_the_model_is_not_sent(neurostore, study, token):
    path = study / "parse" / "coordinate_parse.json"
    parse = json.loads(path.read_text())
    del parse["analyses"][0]["role"]
    path.write_text(json.dumps(parse))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert neurostore.requests == []
    assert artifact["status"] == "invalid"
    assert any("analyses/0/role" in p for p in artifact["error"]["problems"])


def test_a_paper_sync_wrote_no_parse_for_is_recorded_missing(neurostore, tmp_path, token):
    study = tmp_path / "pond" / "noparse"
    study.mkdir(parents=True)
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)
    assert neurostore.requests == []
    assert artifact["status"] == "missing"
    assert _artifact(study)["status"] == "missing"


def test_a_token_the_server_echoes_is_redacted_from_the_artifact(neurostore, study, token):
    neurostore.script = [
        (400, {"detail": f"bad header Bearer {ADMIN}", "errors": [{"authorization": "x"}]}, {})
    ]
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    text = (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text()
    assert ADMIN not in text
    assert artifact["error"]["detail"] == f"bad header Bearer {ni.REDACTED}"
    assert artifact["error"]["errors"] == [{"authorization": ni.REDACTED}]


def test_the_api_url_follows_neurostore_env_unless_set():
    assert ni.api_url(Settings(neurostore_env=NeurostoreEnv.STAGING)) == (
        "https://staging.neurostore.xyz"
    )
    assert ni.api_url(Settings(neurostore_api_url="http://localhost:81/")) == (
        "http://localhost:81"
    )
    with pytest.raises(ValueError, match="neurostore_api_url"):
        ni.api_url(Settings(neurostore_env=NeurostoreEnv.DEV))


def test_from_settings_reads_the_upload_fields(neurostore, study, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("UPLOAD_TOKEN", ADMIN)
    settings = Settings(
        neurostore_api_url=neurostore.url,
        neurostore_token_env="UPLOAD_TOKEN",
        neurostore_upload_parsed_paper=False,
        neurostore_upload_retries=0,
    )
    neurostore.script = [(503, b"down", {})]
    with ni.NeurostoreIngester.from_settings(settings) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "blocked"
    assert len(neurostore.requests) == 1
    assert "parsed_paper" not in json.loads(neurostore.requests[0][1])
    assert artifact["endpoint"] == neurostore.url + "/api/study-schema-uploads/"
