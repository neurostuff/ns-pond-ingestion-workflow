"""Uploading a parse through neurostore's study_schema ingester.

The network tests replay cassettes recorded against a private, local neurostore (master
with the content-key ingester, its own Postgres and Redis, an admin and a non-admin
user), never against staging or production. Requests match on their body too, so a
replay passes only if the client sends exactly the bytes it sent when recorded.

To record one again, start that neurostore at `LOCAL` with the base study `BASE`, set
NEUROSTORE_TOKEN to its admin token (to a non-admin token for the 403 case, to a token it
does not know for the 401 case), delete the cassette and run the test. The cassettes
keep no Authorization header.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
from pathlib import Path

import httpx
import pytest
from ingestion_workflow.catalog import Status
from ingestion_workflow.config import Settings
from ingestion_workflow.services import neurostore_ingest as ni
from ingestion_workflow.tests.services.test_paper_parse import BASE, _written

#: The private neurostore the cassettes were recorded against.
LOCAL = "http://127.0.0.1:28084"
#: What a replay sends as the token: the cassettes do not keep the header.
REPLAY_TOKEN = "replay-token-5f2c9e"
CASSETTES = Path(__file__).parents[1] / "cassettes" / "neurostore_ingest"


@pytest.fixture(scope="module")
def vcr_cassette_dir():
    return str(CASSETTES)


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": ["authorization", "cookie", "set-cookie"],
        "record_mode": "once",
        "match_on": ["method", "scheme", "host", "port", "path", "query", "body"],
    }


@pytest.fixture()
def study(tmp_path):
    _, _, summary = _written(tmp_path)
    return summary["dir"].parent


@pytest.fixture()
def token(monkeypatch):
    value = os.environ.get("NEUROSTORE_TOKEN") or REPLAY_TOKEN
    monkeypatch.setenv("NEUROSTORE_TOKEN", value)
    return value


def _ingester(url=LOCAL, sleeps=None, **kwargs):
    return ni.NeurostoreIngester(
        url, sleep=(sleeps.append if sleeps is not None else lambda s: None), **kwargs
    )


def _artifact(study):
    return json.loads((study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text())


def _files(study):
    return {name: json.loads((study / "parse" / f"{name}.json").read_text())
            for name in ("coordinate_parse", "parsed_paper")}


def _edit(study, name, change):
    path = study / "parse" / f"{name}.json"
    document = json.loads(path.read_text())
    change(document)
    path.write_text(json.dumps(document))


def _closed_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.vcr
def test_a_parse_and_its_parsed_paper_are_posted_as_sync_wrote_them(study, token, vcr):
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    (request,) = vcr.requests
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.body) == _files(study)
    assert artifact["status"] == "uploaded"
    assert ni.Result(artifact["status"]).catalog_status is Status.OK
    summary = artifact["summary"]
    parse_id = _files(study)["coordinate_parse"]["parse_id"]
    assert (summary["kind"], summary["base_study_id"], summary["parse_id"]) == (
        "parse", BASE, parse_id)
    assert summary["study_id"] and "skeleton" in summary
    assert artifact["checks"] == ["model", "check_paper", "json_schema"]
    assert artifact["endpoint"] == LOCAL + ni.UPLOAD_PATH
    assert _artifact(study) == artifact
    assert artifact["last_uploaded"][artifact["endpoint"]]["summary"] == summary


@pytest.mark.vcr
def test_reposting_the_same_parse_changes_nothing(study, token):
    with _ingester() as ingester:
        first = ingester.upload(study)
        again = ingester.upload(study)

    assert (first["status"], again["status"]) == ("uploaded", "uploaded")
    assert again["summary"]["study_id"] == first["summary"]["study_id"]
    assert again["summary"]["skeleton"]["created"] == 0
    assert [h["status"] for h in _artifact(study)["history"]] == ["uploaded", "uploaded"]


def _new_paper(study):
    """The fixture paper as one neurostore has no base study for, with its own ids."""
    ids = {"pmid": "31000001", "pmcid": None, "doi": "10.5555/ingest.new.31000001",
           "neurostore_base_study_id": None}
    for name in ("coordinate_parse", "parsed_paper"):
        _edit(study, name, lambda d: d["header"]["identifiers"].update(ids))


@pytest.mark.vcr
def test_a_paper_neurostore_does_not_hold_gets_a_base_study_from_its_parsed_paper(
    study, token, vcr
):
    _new_paper(study)
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    created = artifact["summary"]["base_study_id"]
    assert created and created != BASE
    # The ingester took the base study's bibliography from the ParsedPaper.
    found = httpx.get(f"{LOCAL}/api/base-studies/{created}").json()
    assert (found["name"], found["pmid"], found["doi"]) == (
        "Faces and houses", "31000001", "10.5555/ingest.new.31000001")
    assert "parsed_paper" in json.loads(vcr.requests[0].body)


@pytest.mark.vcr
def test_a_paper_s_corrections_go_up_in_its_parsed_paper(study, token, vcr):
    """Neurostore marks a paper retracted from these; it is never asked to delete one."""
    retraction = {"kind": "retraction", "pmid": "39999999", "doi": None}
    _edit(study, "parsed_paper", lambda d: d["bibliography"].update(corrections=[retraction]))
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    sent = json.loads(vcr.requests[0].body)
    assert sent["parsed_paper"]["bibliography"]["corrections"] == [retraction]
    assert [r.method for r in vcr.requests] == ["POST"]


@pytest.mark.vcr
def test_an_unknown_base_study_is_the_paper_s_refusal_not_a_stop(study, token):
    _edit(study, "coordinate_parse", lambda d: d["header"]["identifiers"].update(
        neurostore_base_study_id="doesnotexist1"))
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["detail"] == "Record doesnotexist1 not found in BaseStudy"
    assert [a["http_status"] for a in artifact["attempts"]] == [404]


@pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query"],
                 record_mode="none")
@pytest.mark.parametrize("code", ["KEY_MISMATCH", "DUPLICATE_KEY"])
def test_a_key_the_ingester_refuses_is_the_paper_s_failure(study, token, code):
    """Neurostore refuses a parse whose analysis keys its study_schema does not derive
    (KEY_MISMATCH) or that keys two analyses alike (DUPLICATE_KEY). This client checks the
    same keys with its own study_schema before sending, so the refusal reaches it only
    when the two pins derive keys differently. The cassettes are neurostore's answers to
    such parses (the fixture's first key changed; its first analysis sent twice)."""
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["codes"] == [code]
    assert artifact["error"]["detail"] == (
        "Analysis keys must be derived from their name, space and points.")
    assert len(artifact["attempts"]) == 1


@pytest.mark.vcr
@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_token_stops_the_upload_and_writes_nothing(study, token, status):
    with _ingester() as ingester, pytest.raises(ni.NeurostoreAuthError) as stopped:
        ingester.upload(study)

    assert f"with {status}" in str(stopped.value)
    assert token not in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


@pytest.mark.vcr
def test_an_api_url_that_is_not_the_base_url_stops_the_upload(study, token):
    """`/api` belongs to the upload path, so a base URL ending in it reaches no route."""
    with _ingester(LOCAL + "/api") as ingester, pytest.raises(ni.NeurostoreConfigError):
        ingester.upload(study)
    assert not (study / ni.UPLOAD_DIR).exists()


def _response(status, body=b"", headers=None):
    return httpx.Response(status, content=body, headers=headers or {},
                          request=httpx.Request("POST", LOCAL + ni.UPLOAD_PATH))


@pytest.mark.parametrize("status, headers, body, stop", [
    (301, {"Location": "https://staging.neurostore.xyz/api/"}, b"", ni.NeurostoreConfigError),
    (307, {"Location": "/api/study-schema-uploads/"}, b"", ni.NeurostoreConfigError),
    (404, {}, b'{"detail": "Not Found"}', ni.NeurostoreConfigError),
    (405, {}, b'{"detail": "Method Not Allowed"}', ni.NeurostoreConfigError),
    (415, {}, b'{"detail": "Unsupported Media Type"}', ni.NeurostoreConfigError),
    (401, {}, b'{"detail": "Provided token is not valid"}', ni.NeurostoreAuthError),
    (404, {}, b'{"detail": "Record x not found in BaseStudy"}', None),
    (422, {}, b'{"errors": [{"code": "KEY_MISMATCH"}]}', None),
    (400, {}, b'{"detail": "Additional properties"}', None),
])
def test_which_statuses_mean_every_paper_would_fail(token, status, headers, body, stop):
    """A proxy or a wrong URL answers these, which the local neurostore cannot be made to."""
    with _ingester() as ingester:
        found = ingester._stop(_response(status, body, headers), status)
    assert type(found) is stop if stop else found is None


@pytest.mark.parametrize("body, problem, foreign", [
    (lambda parse: b"<html>maintenance</html>", "not JSON", False),
    (lambda parse: {"kind": "parse", "parse_id": "someone-else", "base_study_id": BASE},
     "not this one", False),
    (lambda parse: {"kind": "record", "parse_id": "p"}, "not the ingester's summary", False),
    (lambda parse: {"kind": "parse", "parse_id": parse["parse_id"]}, "names no base study",
     False),
    (lambda parse: {"kind": "parse", "parse_id": parse["parse_id"],
                    "base_study_id": "otherBase001"}, "not '22tHjbNRU8t2'", True),
])
def test_a_2xx_that_is_not_this_parse_s_summary_is_not_an_upload(study, token, body, problem,
                                                                  foreign):
    parse = _files(study)["coordinate_parse"]
    answer = body(parse)
    with _ingester() as ingester:
        summary, found, filed_elsewhere = ingester._summary(
            _response(200, answer if isinstance(answer, bytes) else json.dumps(answer).encode()),
            parse)
    assert summary is None and problem in found and filed_elsewhere is foreign


@pytest.mark.vcr(record_mode="none")
def test_a_summary_of_another_base_study_is_refused_not_retried(study, token):
    """Its base study would be learned as the article's: the same body gets the same
    answer, so it is the paper's refusal, recorded at once."""
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["detail"] == (
        "the summary is of base study 'otherBase001', not '22tHjbNRU8t2'")
    assert "summary" not in artifact and sleeps == []
    assert [a["http_status"] for a in artifact["attempts"]] == [200]


@pytest.mark.vcr(record_mode="none")
def test_a_2xx_that_is_not_a_summary_blocks_the_paper_after_its_tries(study, token):
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
        assert not ingester.down
    assert artifact["status"] == "blocked"
    assert ni.Result(artifact["status"]).catalog_status is Status.FAILED
    assert artifact["error"]["detail"] == (
        "the 2xx body is not the ingester's summary of a parse upload")
    assert [a["http_status"] for a in artifact["attempts"]] == [200] * (ni.RETRIES + 1)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]


@pytest.mark.vcr(record_mode="none")
@pytest.mark.parametrize("asked, waited", [(7, 7.0), (600, ni.MAX_DELAY)], ids=["7", "600"])
def test_a_429_is_retried_after_the_delay_it_asks_for(study, token, asked, waited):
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded"
    assert [a["http_status"] for a in artifact["attempts"]] == [429, 200]
    assert sleeps == [waited]


@pytest.mark.vcr(record_mode="none")
def test_a_408_is_retried_like_a_timeout(study, token):
    """A proxy that timed the request out says nothing about the paper: refusing it for
    good would never send the paper again."""
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded"
    assert [a["http_status"] for a in artifact["attempts"]] == [408, 200]
    assert sleeps == [ni.BACKOFF]


@pytest.mark.vcr(record_mode="none")
@pytest.mark.parametrize("status", [502, 503])
def test_a_proxy_with_nothing_behind_it_is_down(study, token, status):
    """The outage the stop is for: a proxy answers every try and the health check itself."""
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable, match="nor its health check"):
            ingester.upload(study)
        assert ingester.down
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert not (study / ni.UPLOAD_DIR).exists()


@pytest.mark.vcr(record_mode="none")
def test_a_redirect_that_names_the_token_stops_without_it(study, monkeypatch):
    # The composed Location carries the replay token, so the test sends that one.
    monkeypatch.setenv("NEUROSTORE_TOKEN", REPLAY_TOKEN)
    with _ingester() as ingester, pytest.raises(ni.NeurostoreConfigError) as stopped:
        ingester.upload(study)
    assert "redirects (302)" in str(stopped.value)
    assert REPLAY_TOKEN not in str(stopped.value) and ni.REDACTED in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_down_api_stops_the_upload_after_its_tries_and_writes_nothing(study, token):
    sleeps = []
    url = f"http://127.0.0.1:{_closed_port()}"
    with _ingester(url, sleeps) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable, match="nor its health check"):
            ingester.upload(study)
        assert sleeps == [2.0, 4.0, 8.0, 16.0]
        assert ingester.down
        # Down for the rest of the run: the next paper is not tried at all.
        with pytest.raises(ni.NeurostoreUnavailable):
            ingester.upload(study)
        assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert not (study / ni.UPLOAD_DIR).exists()


@pytest.mark.vcr
def test_an_api_that_is_up_but_fails_on_one_paper_fails_that_paper_only(study, token):
    """Recorded with the local neurostore's database stopped: every POST answers 500, and
    the ingester's route, which needs no database to route a GET, still answers 405."""
    sleeps = []
    with _ingester(sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
        assert not ingester.down

    assert artifact["status"] == "blocked"
    assert ni.Result(artifact["status"]).catalog_status is Status.FAILED
    assert [a["http_status"] for a in artifact["attempts"]] == [500] * (ni.RETRIES + 1)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert _artifact(study)["status"] == "blocked"


@pytest.mark.vcr
def test_an_api_whose_route_answers_otherwise_is_down(token):
    """Only the ingester's own 405 to a GET says it is served: a server without the route
    answers 404 (as here, at a path the local neurostore does not serve)."""
    with _ingester() as up, _ingester(LOCAL + "/nothing-here") as elsewhere:
        assert (up._healthy(), elsewhere._healthy()) == (True, False)


@pytest.mark.vcr
def test_a_later_failure_or_dry_run_keeps_the_upload_on_record(study, token, tmp_path):
    with _ingester() as ingester:
        uploaded = ingester.upload(study)
        _edit(study, "coordinate_parse", lambda d: d["header"]["identifiers"].update(
            neurostore_base_study_id="doesnotexist2"))
        ingester.upload(study)
    before = (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text()
    root = tmp_path / "dry"
    dry = ni.NeurostoreIngester(LOCAL, dry_run=True, dry_run_root=root).upload(study)

    assert (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text() == before
    assert dry["status"] == "dry_run"
    assert ni.Result(dry["status"]).catalog_status is None
    assert json.loads((root / study.name / ni.DRY_RUN_NAME).read_text()) == dry
    artifact = _artifact(study)
    assert artifact["status"] == "refused"
    kept = artifact["last_uploaded"][uploaded["endpoint"]]
    assert (kept["summary"], kept["parse_id"]) == (uploaded["summary"], uploaded["parse_id"])
    assert [h["status"] for h in artifact["history"]] == ["uploaded", "refused"]


@pytest.mark.vcr(record_mode="none")
def test_a_dry_run_writes_the_exact_payload_and_sends_nothing(study, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    root = tmp_path / "dry"
    artifact = ni.NeurostoreIngester(LOCAL, dry_run=True, dry_run_root=root).upload(study)

    assert artifact["status"] == "dry_run"
    payload = (root / study.name / ni.PAYLOAD_NAME).read_bytes()
    assert artifact["payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert json.loads(payload) == _files(study)
    assert not (study / ni.UPLOAD_DIR).exists()


def test_the_dry_run_payload_is_the_body_a_real_upload_sends(study, tmp_path):
    """The real upload's body is in the first cassette; this fixture's parse is the same."""
    root = tmp_path / "dry"
    ni.NeurostoreIngester(LOCAL, dry_run=True, dry_run_root=root).upload(study)
    import yaml

    cassette = yaml.safe_load(
        (CASSETTES / "test_a_parse_and_its_parsed_paper_are_posted_as_sync_wrote_them.yaml")
        .read_text())
    sent = cassette["interactions"][0]["request"]["body"]
    assert sent.encode() == (root / study.name / ni.PAYLOAD_NAME).read_bytes()


@pytest.mark.vcr(record_mode="none")
@pytest.mark.parametrize("change, problem", [
    (lambda d: d["analyses"][0].update(key=d["analyses"][0]["key"][:-4] + "0000"),
     "is not the key its content gives"),
    (lambda d: d["analyses"].append(d["analyses"][0]), "has two analyses keyed"),
    (lambda d: d["analyses"][0].pop("role"), "analyses/0/role"),
])
def test_a_parse_that_fails_its_checks_here_is_not_sent(study, token, change, problem):
    _edit(study, "coordinate_parse", change)
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "invalid"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert any(problem in p for p in artifact["error"]["problems"])


@pytest.mark.vcr(record_mode="none")
def test_a_parse_only_the_json_schema_refuses_is_not_sent(study, token):
    """The model reads a coordinate written as a string as a number; the ingester's own
    check, study_schema's JSON Schema, does not, so neither does this one."""
    _edit(study, "coordinate_parse", lambda d: d["analyses"][0]["points"][0].update(
        coordinates=[str(c) for c in d["analyses"][0]["points"][0]["coordinates"]]))
    with _ingester() as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "invalid"
    problems = artifact["error"]["problems"]
    assert problems and all(p.startswith("coordinate-parse/analyses/0/points/0/coordinates/")
                            for p in problems), problems


@pytest.mark.vcr(record_mode="none")
def test_a_paper_sync_wrote_no_parse_for_is_recorded_missing(tmp_path, token):
    study = tmp_path / "pond" / "noparse"
    study.mkdir(parents=True)
    with _ingester() as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "missing"
    assert _artifact(study)["status"] == "missing"


def test_the_upload_needs_its_api_url_and_reads_its_token_from_the_environment_only(
        monkeypatch, tmp_path):
    with pytest.raises(ni.NeurostoreConfigError, match="neurostore_api_url"):
        ni.api_url(Settings())
    assert not [name for name in Settings.model_fields if "token" in name]
    settings = Settings(neurostore_api_url=LOCAL + "/")
    monkeypatch.setenv("NEUROSTORE_TOKEN", REPLAY_TOKEN)
    with ni.NeurostoreIngester.from_settings(settings) as ingester:
        assert ingester.url == LOCAL + ni.UPLOAD_PATH
    # A token in ./.env is not read: the environment variable is the one source.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(f"NEUROSTORE_TOKEN={REPLAY_TOKEN}\n")
    monkeypatch.delenv("NEUROSTORE_TOKEN")
    with pytest.raises(ni.NeurostoreAuthError, match="no neurostore token"):
        ni.NeurostoreIngester.from_settings(settings)


def test_a_missing_or_malformed_token_stops_before_anything_is_sent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    with pytest.raises(ni.NeurostoreAuthError, match="no neurostore token"):
        _ingester()
    monkeypatch.setenv("NEUROSTORE_TOKEN", "half token")
    with pytest.raises(ni.NeurostoreAuthError) as stopped:
        _ingester()
    assert "half token" not in str(stopped.value)


@pytest.mark.vcr(str(CASSETTES / "test_a_parse_and_its_parsed_paper_are_posted_as_sync_wrote_them"
                     ".yaml"), record_mode="none")
def test_a_token_with_a_trailing_newline_is_stripped(study, token, monkeypatch):
    """It sends the first test's request, so it replays that cassette."""
    monkeypatch.setenv("NEUROSTORE_TOKEN", f" {token}\n")
    with _ingester() as ingester:
        assert ingester.upload(study)["status"] == "uploaded"


def test_a_request_that_cannot_be_sent_stops_without_the_httpx_error(study, token):
    sleeps = []
    with _ingester(LOCAL.removeprefix("http://"), sleeps) as ingester:
        with pytest.raises(ni.NeurostoreConfigError, match="cannot be sent") as stopped:
            ingester.upload(study)

    assert sleeps == []
    assert stopped.value.__context__ is None and stopped.value.__cause__ is None
    assert token not in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


def test_any_authorization_value_is_scrubbed_not_only_the_token():
    # The exact token is not always what an error prints: a pasted token's repr
    # carries escapes, and a proxy may echo some other credential.
    assert ni.scrub("Illegal header value b'Bearer other-secret\\n'", REPLAY_TOKEN) == (
        f"Illegal header value b'Bearer {ni.REDACTED}'"
    )
    assert "s3cr3t" not in ni.scrub('{"Authorization": "Bearer s3cr3t"}', REPLAY_TOKEN)
    assert "s3cr3t" not in ni.scrub("authorization=s3cr3t", None)
    assert ni.scrub(f"token {REPLAY_TOKEN} rejected", REPLAY_TOKEN) == (
        f"token {ni.REDACTED} rejected")


@pytest.mark.parametrize("before_cut", [5, 10, len(REPLAY_TOKEN) - 1])
def test_a_raw_token_echoed_across_the_excerpt_cut_leaves_no_part_behind(token, before_cut):
    """A proxy or a debug page can echo the raw token; neurostore's own errors do not."""
    echo = b"x" * (ni.BODY_EXCERPT - before_cut) + token.encode() + b" and more"
    with _ingester() as ingester:
        error = ingester._error(_response(422, echo))
    assert token[:before_cut] not in json.dumps(error)
    assert len(error["body_excerpt"]) == ni.BODY_EXCERPT


def test_a_raw_token_echoed_in_an_error_field_is_redacted(token):
    body = json.dumps({"detail": f"token {token} rejected", "title": token,
                       "errors": [{"authorization": "x"}]}).encode()
    with _ingester() as ingester:
        error = ingester._error(_response(400, body))
    assert token not in json.dumps(error)
    assert error["detail"] == f"token {ni.REDACTED} rejected"
    assert (error["title"], error["errors"]) == (ni.REDACTED, [{"authorization": ni.REDACTED}])


@pytest.fixture()
def http_debug(caplog):
    """DEBUG from the HTTP loggers, which `configure_logging` pins at WARNING for any
    test in the same process that ran it first."""
    for name in ni._HTTP_LOGGERS:
        caplog.set_level(logging.DEBUG, logger=name)
    caplog.set_level(logging.DEBUG)
    return caplog


def test_a_token_in_a_transport_error_is_kept_out_of_the_artifact_and_logs(
    study, token, http_debug
):
    """httpcore traces a refused header at DEBUG with the header in it."""
    url = f"http://127.0.0.1:{_closed_port()}"
    with _ingester(url) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable) as stopped:
            ingester.upload(study)
        logging.getLogger("httpcore.http11").debug("Illegal header value b'Bearer %s'", token)
        logging.getLogger("httpx").debug("sent Authorization: Bearer %s", token)

    assert token not in str(stopped.value)
    assert token not in http_debug.text
    assert "Illegal header value b'Bearer [redacted]'" in http_debug.text


#: Every logger httpx and httpcore print a request or a header on.
HTTP_STACK = ("httpx", "httpcore", "httpcore.connection", "httpcore.http11", "httpcore.http2",
              "httpcore.proxy", "httpcore.socks")


@pytest.mark.parametrize("name", HTTP_STACK)
def test_every_http_logger_is_scrubbed(token, http_debug, name):
    http_debug.set_level(logging.DEBUG, logger=name)
    with _ingester():
        logging.getLogger(name).debug("sent Authorization: Bearer %s (%s)", "other-secret", token)
    assert "other-secret" not in http_debug.text and token not in http_debug.text
    assert f"Bearer {ni.REDACTED}" in http_debug.text


def test_closing_an_ingester_drops_its_token_and_keeps_scrubbing_bearer_values(
    token, http_debug
):
    # Relative counts: an ingester another test in this process left open holds it too.
    held = ni._SCRUB.tokens.count(token)
    other = _ingester()
    with _ingester():
        assert ni._SCRUB.tokens.count(token) == held + 2
    assert ni._SCRUB.tokens.count(token) == held + 1  # the open one still scrubs it
    other.close()
    assert ni._SCRUB.tokens.count(token) == held
    logging.getLogger("httpcore.http11").debug("late line Bearer %s", token)
    assert "late line Bearer [redacted]" in http_debug.text


def test_closing_an_ingester_closes_its_connections(token):
    ingester = _ingester()
    client = ingester._client
    ingester.close()
    assert client.is_closed


@pytest.mark.vcr(str(CASSETTES / "test_a_parse_and_its_parsed_paper_are_posted_as_sync_wrote_them"
                     ".yaml"))
def test_an_unreadable_upload_artifact_is_moved_aside_not_overwritten(study, token):
    path = study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME
    path.parent.mkdir(parents=True)
    path.write_text('{"history": [')
    with _ingester() as ingester:
        ingester.upload(study)
    assert path.with_name(path.name + ".unreadable").read_text() == '{"history": ['
    assert [h["status"] for h in _artifact(study)["history"]] == ["uploaded"]


def test_a_dry_run_from_settings_writes_outside_the_pond(study, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    data = tmp_path / "data"
    settings = Settings(neurostore_api_url=LOCAL, dry_run=True, data_root=data)
    with ni.NeurostoreIngester.from_settings(settings) as ingester:
        artifact = ingester.upload(study)

    out = data / ni.DRY_RUN_DIR / study.name
    assert artifact["status"] == "dry_run"
    assert artifact["payload_path"] == str(out / ni.PAYLOAD_NAME)
    assert json.loads((out / ni.DRY_RUN_NAME).read_text()) == artifact
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_dry_run_of_an_invalid_parse_writes_no_upload_artifact(study, tmp_path):
    _edit(study, "coordinate_parse", lambda d: d["analyses"][0].pop("role"))
    root = tmp_path / "dry"
    artifact = ni.NeurostoreIngester(LOCAL, dry_run=True, dry_run_root=root).upload(study)

    assert artifact["status"] == "invalid"
    assert json.loads((root / study.name / ni.DRY_RUN_NAME).read_text()) == artifact
    assert not (root / study.name / ni.ARTIFACT_NAME).exists()
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_dry_run_without_a_root_is_refused(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NEUROSTORE_TOKEN", raising=False)
    with pytest.raises(ValueError, match="dry_run_root"):
        ni.NeurostoreIngester(LOCAL, dry_run=True)


def test_every_neurostore_setting_is_in_the_settings_reference():
    import yaml

    reference = Path(__file__).parents[3] / "configs" / "settings_reference.yaml"
    documented = yaml.safe_load(reference.read_text())
    fields = {n: f for n, f in Settings.model_fields.items() if n.startswith("neurostore_")}
    assert set(fields) <= set(documented)
    for name, field in fields.items():
        default = getattr(field.default, "value", field.default)
        assert documented[name] == default, name


def test_the_cassettes_hold_no_authorization_header():
    import yaml

    cassettes = sorted(CASSETTES.glob("*.yaml"))
    assert cassettes
    for cassette in cassettes:
        assert "Bearer" not in cassette.read_text(), cassette
        for interaction in yaml.safe_load(cassette.read_text())["interactions"]:
            headers = {k.lower() for k in interaction["request"]["headers"]}
            assert "authorization" not in headers, cassette
