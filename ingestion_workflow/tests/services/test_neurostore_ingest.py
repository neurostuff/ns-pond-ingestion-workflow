"""Uploading a parse through neurostore's study_schema ingester.

The network tests send to the `neurostore` fixture (`ingestion_workflow.tests.neurostore`),
which serves the ingester's route by neurostore's OpenAPI spec and study_schema's JSON
Schema: it refuses an upload or a scripted answer the contract does not allow, so these
tests rely only on answers neurostore may give. What neurostore does with an upload
(the base study it creates, the identifiers it adds) is neurostore's to test; these check
what this client sends and what it makes of each answer.
"""

from __future__ import annotations

import hashlib
import json
import logging
import socket
from pathlib import Path

import httpx
import pytest
from ingestion_workflow.catalog import Status
from ingestion_workflow.config import Settings
from ingestion_workflow.services import neurostore_ingest as ni
from ingestion_workflow.tests import neurostore as mock
from ingestion_workflow.tests.services.test_paper_parse import BASE, TEXT, _written

NGINX = "<html><head><title>{0} {1}</title></head><body><center><h1>{0} {1}</h1></center>" \
        "<hr><center>nginx</center></body></html>"


def _nginx(status):
    return NGINX.format(status, httpx.codes.get_reason_phrase(status)).encode()


@pytest.fixture()
def study(tmp_path):
    _, _, summary = _written(tmp_path)
    return summary["dir"].parent


def _ingester(server=None, url=mock.URL, sleeps=None, **kwargs):
    kwargs.setdefault("transport", server.transport if server is not None else None)
    kwargs.setdefault("auth0", (mock.AUTH0_URL, mock.AUDIENCE))
    return ni.NeurostoreIngester(
        url, sleep=(sleeps.append if sleeps is not None else lambda s: None), **kwargs
    )


def _holding_token(server):
    """An ingester that has asked `server`'s tenant for its token, and the token."""
    ingester = _ingester(server)
    ingester._ask_token()
    return ingester, server.token


def _no_client(monkeypatch):
    for name in (ni.CLIENT_ID_ENV, ni.CLIENT_SECRET_ENV):
        monkeypatch.delenv(name, raising=False)


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


def test_a_parse_and_its_parsed_paper_are_posted_as_sync_wrote_them(study, neurostore):
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    (call,) = neurostore.calls
    assert (call.method, call.content_type) == ("POST", "application/json")
    sent = json.loads(call.content)
    assert sent == {**_files(study), "text": TEXT}
    # The text neurostore stores is the one the parsed paper's text_sha256 names.
    assert hashlib.sha256(sent["text"].encode()).hexdigest() == sent["parsed_paper"]["text_sha256"]
    assert artifact["status"] == "uploaded"
    assert ni.Result(artifact["status"]).catalog_status is Status.OK
    summary = artifact["summary"]
    parse_id = _files(study)["coordinate_parse"]["parse_id"]
    assert (summary["kind"], summary["base_study_id"], summary["parse_id"]) == (
        "parse", BASE, parse_id)
    assert summary["study_id"] and "skeleton" in summary
    assert artifact["checks"] == ["model", "check_paper", "json_schema"]
    assert artifact["endpoint"] == mock.URL + ni.UPLOAD_PATH
    assert _artifact(study) == artifact
    assert artifact["last_uploaded"][artifact["endpoint"]]["summary"] == summary
    assert neurostore.token not in (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text()


def test_reposting_the_same_parse_changes_nothing(study, neurostore):
    with _ingester(neurostore) as ingester:
        first = ingester.upload(study)
        again = ingester.upload(study)

    assert (first["status"], again["status"]) == ("uploaded", "uploaded")
    assert again["summary"]["study_id"] == first["summary"]["study_id"]
    assert again["summary"]["skeleton"]["created"] == 0
    assert neurostore.uploads[0] == neurostore.uploads[1]
    assert [h["status"] for h in _artifact(study)["history"]] == ["uploaded", "uploaded"]


def _new_paper(study):
    """The fixture paper as one neurostore has no base study for, with its own ids."""
    ids = {"pmid": "31000001", "pmcid": None, "doi": "10.5555/ingest.new.31000001"}
    for name in ("coordinate_parse", "parsed_paper"):
        _edit(study, name, lambda d: (d["header"]["identifiers"].update(ids),
                                      d["header"].update(neurostore_base_study_id=None)))


def test_a_paper_neurostore_does_not_hold_gets_a_base_study_from_its_parsed_paper(
    study, neurostore
):
    _new_paper(study)
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    created = artifact["summary"]["base_study_id"]
    assert created and created != BASE
    # What the ingester creates the base study from: the identifiers, and the
    # bibliography of the ParsedPaper that goes with them.
    (sent,) = neurostore.uploads
    assert sent["coordinate_parse"]["header"]["neurostore_base_study_id"] is None
    paper = sent["parsed_paper"]
    ids = paper["header"]["identifiers"]
    assert (paper["bibliography"]["title"], ids["pmid"], ids["doi"]) == (
        "Faces and houses", "31000001", "10.5555/ingest.new.31000001")


def _identify(study, ids, status):
    """The fixture paper as its parse names it by `ids`, filed under `status`."""
    for name in ("coordinate_parse", "parsed_paper"):
        _edit(study, name, lambda d: d["header"]["identifiers"].update(
            ids, publication_status=status))


def test_an_upload_adds_the_ids_its_base_study_lacks_in_every_bucket(study, neurostore):
    """The parse's other ids, a preprint's and the alternative ones too, go up in every
    bucket of both documents, with the publication status, for the ingester to add."""
    ids = {
        "alternative_dois": ["10.1371/journal.pone.0041873.alt"],
        "alternative_pmids": ["22848645"],
        "alternative_pmcids": ["PMC3407126"],
        "preprint_dois": [{"doi": "10.1101/2012.01.02.123456", "server": "biorxiv"}],
        "preprint_pmids": ["31000009"],
        "preprint_pmcids": ["PMC7000001"],
    }
    _identify(study, ids, "published")
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    (sent,) = neurostore.uploads
    for name in ("coordinate_parse", "parsed_paper"):
        found = sent[name]["header"]["identifiers"]
        assert (found["pmid"], found["pmcid"], found["doi"]) == (
            "22848644", "PMC3407125", "10.1371/journal.pone.0041873")
        assert found["publication_status"] == "published"
        for bucket, value in ids.items():
            assert found[bucket] == value, (name, bucket)


def test_a_paper_known_only_by_its_preprint_gets_a_base_study_filed_as_a_preprint(
    study, neurostore
):
    preprint = {"doi": "10.1101/2012.05.06.654321", "server": "biorxiv"}
    _identify(study, {"pmid": None, "pmcid": None, "doi": None, "preprint_dois": [preprint]},
              "preprint")
    for name in ("coordinate_parse", "parsed_paper"):
        _edit(study, name, lambda d: d["header"].update(neurostore_base_study_id=None))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    created = artifact["summary"]["base_study_id"]
    assert created and created != BASE
    (sent,) = neurostore.uploads
    found = sent["coordinate_parse"]["header"]["identifiers"]
    assert found["publication_status"] == "preprint"
    assert found["preprint_dois"] == [preprint]
    assert (found["doi"], found["pmid"], found["pmcid"]) == (None, None, None)


def test_a_paper_s_corrections_go_up_in_its_parsed_paper(study, neurostore):
    """Neurostore marks a paper retracted from these; it is never asked to delete one."""
    retraction = {"kind": "retraction", "pmid": "39999999", "doi": None}
    _edit(study, "parsed_paper", lambda d: d["bibliography"].update(corrections=[retraction]))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    (sent,) = neurostore.uploads
    assert sent["parsed_paper"]["bibliography"]["corrections"] == [retraction]
    assert [c.method for c in neurostore.calls] == ["POST"]


def test_an_unknown_base_study_is_the_paper_s_refusal_not_a_stop(study, neurostore):
    _edit(study, "coordinate_parse", lambda d: d["header"].update(
        neurostore_base_study_id="doesnotexist1"))
    neurostore.script(neurostore.error(404, "Record doesnotexist1 not found in BaseStudy"))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["detail"] == "Record doesnotexist1 not found in BaseStudy"
    assert [a["http_status"] for a in artifact["attempts"]] == [404]


KEYS = "Analysis keys must be derived from their name, space and points."
TEXTS = "The text is not the one parsed_paper's text_sha256 names."


@pytest.mark.parametrize("code, detail", [
    ("KEY_MISMATCH", KEYS), ("DUPLICATE_KEY", KEYS), ("TEXT_MISMATCH", TEXTS)],
    ids=["KEY_MISMATCH", "DUPLICATE_KEY", "TEXT_MISMATCH"])
def test_a_key_the_ingester_refuses_is_the_paper_s_failure(study, neurostore, code, detail):
    """Neurostore refuses a parse whose analysis keys its study_schema does not derive
    (KEY_MISMATCH), that keys two analyses alike (DUPLICATE_KEY), or whose text is not the
    one its parsed paper hashes (TEXT_MISMATCH). This client checks the same before
    sending, so the refusal reaches it only when the two pins disagree; the same body
    gets the same answer, so it is never retried."""
    neurostore.script(neurostore.error(422, detail, codes=[code]))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["codes"] == [code]
    assert artifact["error"]["detail"] == detail
    assert len(artifact["attempts"]) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_a_refused_token_stops_the_upload_and_writes_nothing(study, neurostore, status):
    """A 401 is refused again with a new token, a 403 at once."""
    refused = neurostore.error(status, "Only an admin can upload study_schema artifacts.")
    neurostore.script(*[refused] * (2 if status == 401 else 1))
    with _ingester(neurostore) as ingester, pytest.raises(ni.NeurostoreAuthError) as stopped:
        ingester.upload(study)

    assert f"with {status}" in str(stopped.value)
    assert neurostore.token not in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


def test_an_api_url_that_is_not_the_base_url_stops_the_upload(study, neurostore):
    """`/api` belongs to the upload path, so a base URL ending in it reaches no route."""
    with _ingester(neurostore, mock.URL + "/api") as ingester, \
            pytest.raises(ni.NeurostoreConfigError):
        ingester.upload(study)
    assert [(c.method, c.path) for c in neurostore.calls] == [
        ("POST", "/api" + mock.api_path())]
    assert not (study / ni.UPLOAD_DIR).exists()


def _response(status, body=b"", headers=None):
    return httpx.Response(status, content=body, headers=headers or {},
                          request=httpx.Request("POST", mock.URL + ni.UPLOAD_PATH))


@pytest.mark.parametrize("answer, stop", [
    (lambda s: s.proxy(301, headers={"Location": "https://staging.neurostore.xyz/api/"}),
     ni.NeurostoreConfigError),
    (lambda s: s.proxy(307, headers={"Location": "/api/study-schema-uploads/"}),
     ni.NeurostoreConfigError),
    (lambda s: s.error(404, "Not Found"), ni.NeurostoreConfigError),
    (lambda s: s.error(405, "Method Not Allowed"), ni.NeurostoreConfigError),
    (lambda s: s.error(415, "Unsupported Media Type"), ni.NeurostoreConfigError),
    (lambda s: [s.error(401, "Provided token is not valid")] * 2, ni.NeurostoreAuthError),
    (lambda s: s.error(404, "Record x not found in BaseStudy"), None),
    (lambda s: s.error(422, KEYS, codes=["KEY_MISMATCH"]), None),
    (lambda s: s.error(422, TEXTS, codes=["TEXT_MISMATCH"]), None),
    (lambda s: s.error(422, "Not at the pinned schema version.", codes=["SCHEMA_VERSION"]),
     ni.NeurostoreConfigError),
    (lambda s: s.error(400, "Additional properties are not allowed"), None),
], ids=["301", "307", "404-route", "405", "415", "401", "404-base-study", "422-KEY_MISMATCH",
        "422-TEXT_MISMATCH", "422-SCHEMA_VERSION", "400"])
def test_which_statuses_mean_every_paper_would_fail(study, neurostore, answer, stop):
    """A stop raises before anything is written; any other is this paper's refusal. A 401
    stops only when a new token is refused too."""
    answers = answer(neurostore)
    answers = answers if isinstance(answers, list) else [answers]
    neurostore.script(*answers)
    with _ingester(neurostore) as ingester:
        if stop:
            with pytest.raises(stop):
                ingester.upload(study)
            assert not (study / ni.UPLOAD_DIR).exists()
        else:
            assert ingester.upload(study)["status"] == "refused"
    assert len(neurostore.calls) == len(answers)


@pytest.mark.parametrize("answer, problem, result", [
    (lambda s, parse: s.proxy(200, b"<html>maintenance</html>"), "not JSON", "blocked"),
    (lambda s, parse: s.summary(parse_id="someone-else"), "not this one", "blocked"),
    (lambda s, parse: s.reply(200, {"kind": "record", "parse_id": "p"}),
     "not the ingester's summary", "blocked"),
    (lambda s, parse: s.reply(200, {"kind": "parse", "parse_id": parse["parse_id"]}),
     "names no base study", "blocked"),
    (lambda s, parse: s.summary(base_study_id="otherBase001"), "not '22tHjbNRU8t2'",
     "refused"),
], ids=["html", "another-parse", "a-record", "no-base-study", "another-base-study"])
def test_a_2xx_that_is_not_this_parse_s_summary_is_not_an_upload(study, neurostore, answer,
                                                                  problem, result):
    """Blocked after its tries, while the API is up; a summary filed under another base
    study is refused at once."""
    parse = _files(study)["coordinate_parse"]
    neurostore.script(*[answer(neurostore, parse)] * (ni.RETRIES + 1))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == result
    assert problem in artifact["error"]["detail"] and "summary" not in artifact


@pytest.mark.parametrize("merged_from, foreign", [
    ([BASE], False), (["thirdBase001", BASE], False), (["thirdBase001"], True), ([], True)])
def test_a_summary_of_another_base_study_is_this_upload_only_when_it_followed_a_merge(
        study, neurostore, merged_from, foreign):
    neurostore.script(neurostore.summary(base_study_id="otherBase001", merged_from=merged_from))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == ("refused" if foreign else "uploaded")


def test_an_upload_neurostore_filed_under_the_base_study_a_merge_kept_is_an_upload(
        study, neurostore):
    """The header names a base study neurostore merged into another; it followed the merge
    and says so. The write committed, so it is uploaded, under the base study it names."""
    neurostore.script(neurostore.summary(base_study_id="otherBase001", merged_from=[BASE]))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded"
    assert ni.Result(artifact["status"]).catalog_status is Status.OK
    summary = artifact["summary"]
    assert (summary["base_study_id"], summary["merged_from"]) == ("otherBase001", [BASE])
    assert artifact["last_uploaded"][artifact["endpoint"]]["summary"] == summary
    assert [a["http_status"] for a in artifact["attempts"]] == [200]


def test_a_schema_version_neurostore_is_not_pinned_to_stops_the_upload(study, neurostore):
    """Every parse of the run states the same version, so every one would be refused: a
    refusal recorded per paper would never be sent again once neurostore is repinned."""
    neurostore.script(neurostore.error(
        422, "The coordinate_parse is not at the schema version this server takes.",
        codes=["SCHEMA_VERSION"], field_name="coordinate_parse/header/schema_version",
        context={"provided": {"schema_version": "0.11.0", "pinned": "0.10.0"}}))
    with _ingester(neurostore) as ingester, pytest.raises(ni.NeurostoreConfigError) as stopped:
        ingester.upload(study)

    message = str(stopped.value)
    assert "coordinate_parse/header/schema_version states 0.11.0" in message
    assert "neurostore is pinned to 0.10.0" in message
    assert len(neurostore.calls) == 1
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_summary_of_another_base_study_is_refused_not_retried(study, neurostore):
    """Its base study would be learned as the article's: the same body gets the same
    answer, so it is the paper's refusal, recorded at once."""
    neurostore.script(neurostore.summary(base_study_id="otherBase001"))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "refused"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert artifact["error"]["detail"] == (
        "the summary is of base study 'otherBase001', not '22tHjbNRU8t2'")
    assert "summary" not in artifact and sleeps == []
    assert [a["http_status"] for a in artifact["attempts"]] == [200]


def test_a_2xx_that_is_not_a_summary_blocks_the_paper_after_its_tries(study, neurostore):
    record = neurostore.reply(200, {"kind": "record", "base_study_id": BASE,
                                    "record": {"config_id": "cfg-1"}})
    neurostore.script(*[record] * (ni.RETRIES + 1))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
        assert not ingester.down
    assert artifact["status"] == "blocked"
    assert ni.Result(artifact["status"]).catalog_status is Status.FAILED
    assert artifact["error"]["detail"] == (
        "the 2xx body is not the ingester's summary of a parse upload")
    assert [a["http_status"] for a in artifact["attempts"]] == [200] * (ni.RETRIES + 1)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert [c.method for c in neurostore.calls] == ["POST"] * (ni.RETRIES + 1) + ["GET"]


@pytest.mark.parametrize("asked, waited", [(7, 7.0), (600, ni.MAX_DELAY)], ids=["7", "600"])
def test_a_429_is_retried_after_the_delay_it_asks_for(study, neurostore, asked, waited):
    neurostore.script(neurostore.error(429, "rate limited", headers={"Retry-After": str(asked)}))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded"
    assert [a["http_status"] for a in artifact["attempts"]] == [429, 200]
    assert sleeps == [waited]


def test_a_408_is_retried_like_a_timeout(study, neurostore):
    """A proxy that timed the request out says nothing about the paper: refusing it for
    good would never send the paper again."""
    neurostore.script(neurostore.proxy(408, _nginx(408)))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded"
    assert [a["http_status"] for a in artifact["attempts"]] == [408, 200]
    assert sleeps == [ni.BACKOFF]


@pytest.mark.parametrize("error", [httpx.RemoteProtocolError, httpx.ReadTimeout])
def test_a_dropped_connection_or_a_timeout_is_retried(study, neurostore, error):
    neurostore.script(neurostore.drop(error))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded"
    assert artifact["attempts"] == [{"attempt": 1, "error": error.__name__},
                                    {"attempt": 2, "http_status": 200}]
    assert sleeps == [ni.BACKOFF]


@pytest.mark.parametrize("status", [502, 503])
def test_a_proxy_with_nothing_behind_it_is_down(study, neurostore, status):
    """The outage the stop is for: a proxy answers every try and the health check itself."""
    neurostore.script(*[neurostore.proxy(status, _nginx(status))] * (ni.RETRIES + 1))
    neurostore.health = neurostore.proxy(status, _nginx(status))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable, match="nor its health check"):
            ingester.upload(study)
        assert ingester.down
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert not (study / ni.UPLOAD_DIR).exists()


def test_an_api_that_drops_every_try_and_its_health_check_is_down(study, neurostore):
    neurostore.script(*[neurostore.drop(httpx.ReadTimeout)] * (ni.RETRIES + 1))
    neurostore.health = neurostore.drop()
    with _ingester(neurostore) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable, match="ReadTimeout"):
            ingester.upload(study)
    assert [c.method for c in neurostore.calls] == ["POST"] * (ni.RETRIES + 1) + ["GET"]


def test_a_redirect_that_names_the_token_stops_without_it(study, neurostore):
    ingester, token = _holding_token(neurostore)
    neurostore.script(neurostore.proxy(302, headers={
        "Location": "https://login.neurostore.example/?next=/api/study-schema-uploads/"
                    f"&token={token}"}))
    with ingester, pytest.raises(ni.NeurostoreConfigError) as stopped:
        ingester.upload(study)
    assert "redirects (302)" in str(stopped.value)
    assert token not in str(stopped.value) and ni.REDACTED in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_down_api_stops_the_upload_after_its_tries_and_writes_nothing(study, neurostore):
    sleeps = []
    url = f"http://127.0.0.1:{_closed_port()}"
    with _ingester(url=url, sleeps=sleeps, transport=neurostore.with_network()) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable, match="nor its health check"):
            ingester.upload(study)
        assert sleeps == [2.0, 4.0, 8.0, 16.0]
        assert ingester.down
        # Down for the rest of the run: the next paper is not tried at all.
        with pytest.raises(ni.NeurostoreUnavailable):
            ingester.upload(study)
        assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert not (study / ni.UPLOAD_DIR).exists()


def test_an_api_that_is_up_but_fails_on_one_paper_fails_that_paper_only(study, neurostore):
    """Every POST answers 500 (its database is gone, say), and the ingester's route, which
    needs no database to route a GET, still answers 405."""
    failed = neurostore.error(500, "(psycopg2.OperationalError) could not translate host "
                                   "name \"store-pgsql17\" to address")
    neurostore.script(*[failed] * (ni.RETRIES + 1))
    sleeps = []
    with _ingester(neurostore, sleeps=sleeps) as ingester:
        artifact = ingester.upload(study)
        assert not ingester.down

    assert artifact["status"] == "blocked"
    assert ni.Result(artifact["status"]).catalog_status is Status.FAILED
    assert [a["http_status"] for a in artifact["attempts"]] == [500] * (ni.RETRIES + 1)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert _artifact(study)["status"] == "blocked"


def test_an_api_whose_route_answers_otherwise_is_down(neurostore):
    """Only the ingester's own 405 to a GET says it is served: a server without the route
    answers 404 (as here, at a path neurostore does not serve)."""
    elsewhere = mock.URL + "/nothing-here"
    with _ingester(neurostore) as up, _ingester(neurostore, elsewhere) as other:
        assert (up._healthy(), other._healthy()) == (True, False)
    assert [c.path for c in neurostore.calls] == [
        mock.api_path(), "/nothing-here" + mock.api_path()]


def test_a_later_failure_or_dry_run_keeps_the_upload_on_record(study, neurostore, tmp_path):
    neurostore.script(neurostore.summary(),
                      neurostore.error(404, "Record doesnotexist2 not found in BaseStudy"))
    with _ingester(neurostore) as ingester:
        uploaded = ingester.upload(study)
        _edit(study, "coordinate_parse", lambda d: d["header"].update(
            neurostore_base_study_id="doesnotexist2"))
        ingester.upload(study)
    before = (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text()
    root = tmp_path / "dry"
    dry = ni.NeurostoreIngester(mock.URL, dry_run=True, dry_run_root=root).upload(study)

    assert (study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME).read_text() == before
    assert dry["status"] == "dry_run"
    assert ni.Result(dry["status"]).catalog_status is None
    assert json.loads((root / study.name / ni.DRY_RUN_NAME).read_text()) == dry
    artifact = _artifact(study)
    assert artifact["status"] == "refused"
    kept = artifact["last_uploaded"][uploaded["endpoint"]]
    assert (kept["summary"], kept["parse_id"]) == (uploaded["summary"], uploaded["parse_id"])
    assert [h["status"] for h in artifact["history"]] == ["uploaded", "refused"]
    assert len(neurostore.calls) == 2


def test_a_dry_run_writes_the_exact_payload_and_sends_nothing(study, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _no_client(monkeypatch)
    root = tmp_path / "dry"
    ingester = ni.NeurostoreIngester(mock.URL, dry_run=True, dry_run_root=root)
    artifact = ingester.upload(study)

    assert ingester._client is None  # nothing to send with
    assert artifact["status"] == "dry_run"
    payload = (root / study.name / ni.PAYLOAD_NAME).read_bytes()
    assert artifact["payload_sha256"] == hashlib.sha256(payload).hexdigest()
    assert json.loads(payload) == {**_files(study), "text": TEXT}
    assert not (study / ni.UPLOAD_DIR).exists()


def _rewrite_text(study, data):
    """The fixture paper with `data` as its text file, and both documents hashing it."""
    paper = _files(study)["parsed_paper"]
    (study / paper["text_path"]).write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    for name in ("coordinate_parse", "parsed_paper"):
        _edit(study, name, lambda d: d.update(text_sha256=digest))
    return digest


def test_the_text_is_sent_as_the_bytes_its_hash_was_taken_of(study, neurostore):
    """Read as text, a CRLF would come back LF, and neurostore would refuse the text as
    not the one text_sha256 names (TEXT_MISMATCH); the mock checks the hash too."""
    digest = _rewrite_text(study, TEXT.replace("\n", "\r\n").encode())
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "uploaded", artifact.get("error")
    (sent,) = neurostore.uploads
    assert "\r\n" in sent["text"] and hashlib.sha256(sent["text"].encode()).hexdigest() == digest


def test_a_text_that_is_not_utf8_is_not_sent(study, tmp_path):
    _rewrite_text(study, TEXT.replace("Title", "Titl\xe9").encode("latin-1"))
    root = tmp_path / "dry"
    artifact = ni.NeurostoreIngester(mock.URL, dry_run=True, dry_run_root=root).upload(study)

    assert artifact["status"] == "invalid"
    assert any("is not UTF-8" in p for p in artifact["error"]["problems"])
    assert not (root / study.name / ni.PAYLOAD_NAME).exists()


def test_the_dry_run_payload_is_the_body_a_real_upload_sends(study, neurostore, tmp_path):
    root = tmp_path / "dry"
    ni.NeurostoreIngester(mock.URL, dry_run=True, dry_run_root=root).upload(study)
    with _ingester(neurostore) as ingester:
        ingester.upload(study)

    (call,) = neurostore.calls
    assert call.content == (root / study.name / ni.PAYLOAD_NAME).read_bytes()


@pytest.mark.parametrize("change, problem", [
    (lambda d: d["analyses"][0].update(key=d["analyses"][0]["key"][:-4] + "0000"),
     "is not the key its content gives"),
    (lambda d: d["analyses"].append(d["analyses"][0]), "has two analyses keyed"),
    (lambda d: d["analyses"][0].pop("role"), "analyses/0/role"),
])
def test_a_parse_that_fails_its_checks_here_is_not_sent(study, neurostore, change, problem):
    _edit(study, "coordinate_parse", change)
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "invalid"
    assert ni.Result(artifact["status"]).catalog_status is Status.PERMANENT
    assert any(problem in p for p in artifact["error"]["problems"])
    assert neurostore.calls == []


def test_a_parse_only_the_json_schema_refuses_is_not_sent(study, neurostore):
    """The model reads a coordinate written as a string as a number; the ingester's own
    check, study_schema's JSON Schema, does not, so neither does this one."""
    _edit(study, "coordinate_parse", lambda d: d["analyses"][0]["points"][0].update(
        coordinates=[str(c) for c in d["analyses"][0]["points"][0]["coordinates"]]))
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)

    assert artifact["status"] == "invalid"
    problems = artifact["error"]["problems"]
    assert problems and all(p.startswith("coordinate-parse/analyses/0/points/0/coordinates/")
                            for p in problems), problems
    assert neurostore.calls == []


def test_a_paper_sync_wrote_no_parse_for_is_recorded_missing(tmp_path, neurostore):
    study = tmp_path / "pond" / "noparse"
    study.mkdir(parents=True)
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "missing"
    assert _artifact(study)["status"] == "missing"
    assert neurostore.calls == []


def test_the_upload_needs_its_api_url_and_reads_its_client_from_the_environment_only(
        monkeypatch, tmp_path, neurostore):
    with pytest.raises(ni.NeurostoreConfigError, match="neurostore_api_url"):
        ni.api_url(Settings())
    assert not [name for name in Settings.model_fields
                if "token" in name or "secret" in name or "client" in name]
    settings = Settings(neurostore_api_url=mock.URL + "/")
    with ni.NeurostoreIngester.from_settings(settings) as ingester:
        assert ingester.url == mock.URL + ni.UPLOAD_PATH
    # A client in ./.env is not read: the environment variables are the one source.
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(f"{ni.CLIENT_ID_ENV}={mock.CLIENT_ID}\n"
                                   f"{ni.CLIENT_SECRET_ENV}={mock.CLIENT_SECRET}\n")
    _no_client(monkeypatch)
    with pytest.raises(ni.NeurostoreAuthError, match="no neurostore client"):
        ni.NeurostoreIngester.from_settings(settings)


def test_a_missing_client_stops_before_anything_is_sent(monkeypatch, tmp_path, neurostore):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(ni.CLIENT_SECRET_ENV)
    with pytest.raises(ni.NeurostoreAuthError, match="no neurostore client"):
        _ingester(neurostore)
    assert neurostore.calls == [] and neurostore.granted == 0


def test_a_malformed_token_from_the_tenant_stops_before_anything_is_sent(study, neurostore):
    neurostore.auth0_script.append((200, {"access_token": "half token", "expires_in": 60}))
    with _ingester(neurostore) as ingester, pytest.raises(ni.NeurostoreAuthError) as stopped:
        ingester.upload(study)
    assert "half token" not in str(stopped.value)
    assert neurostore.calls == []


def test_a_client_with_trailing_newlines_is_stripped(study, neurostore, monkeypatch):
    """The mock tenant takes only the exact client id and secret."""
    monkeypatch.setenv(ni.CLIENT_ID_ENV, f" {mock.CLIENT_ID}\n")
    monkeypatch.setenv(ni.CLIENT_SECRET_ENV, f"{mock.CLIENT_SECRET}\n")
    with _ingester(neurostore) as ingester:
        assert ingester.upload(study)["status"] == "uploaded"


def test_a_request_that_cannot_be_sent_stops_without_the_httpx_error(study, neurostore):
    sleeps = []
    with _ingester(url=mock.URL.removeprefix("http://"), sleeps=sleeps,
                   transport=neurostore.with_network()) as ingester:
        with pytest.raises(ni.NeurostoreConfigError, match="cannot be sent") as stopped:
            ingester.upload(study)

    assert sleeps == []
    assert stopped.value.__context__ is None and stopped.value.__cause__ is None
    assert neurostore.token not in str(stopped.value)
    assert not (study / ni.UPLOAD_DIR).exists()


def _clocked(server, clock):
    return _ingester(server, clock=lambda: clock[0])


def test_the_client_s_token_is_asked_for_at_the_first_upload_only(study, neurostore):
    with _ingester(neurostore) as ingester:
        assert neurostore.granted == 0
        assert ingester.upload(study)["status"] == "uploaded"
        assert ingester.upload(study)["status"] == "uploaded"
    assert neurostore.granted == 1


def test_a_token_about_to_expire_is_asked_for_again_before_an_upload(study, neurostore):
    clock = [0.0]
    neurostore.expires_in = 3600
    with _clocked(neurostore, clock) as ingester:
        ingester.upload(study)
        clock[0] = 3600 - ni.REFRESH_MARGIN - 1
        ingester.upload(study)
        assert neurostore.granted == 1
        clock[0] = 3600 - ni.REFRESH_MARGIN
        assert ingester.upload(study)["status"] == "uploaded"
    assert neurostore.granted == 2


def test_a_refused_client_token_is_asked_for_once_more_then_stops(study, neurostore):
    refused = neurostore.error(401, "token expired")
    neurostore.script(refused)
    with _ingester(neurostore) as ingester:
        artifact = ingester.upload(study)
    assert artifact["status"] == "uploaded" and neurostore.granted == 2
    assert [a.get("token_refused") for a in artifact["attempts"]] == [True, None]

    neurostore.script(*[neurostore.error(401, "token expired")] * 2)
    with _ingester(neurostore) as ingester, \
            pytest.raises(ni.NeurostoreAuthError, match=f"{mock.CLIENT_ID}@clients") as stopped:
        ingester.upload(study)
    assert mock.CLIENT_SECRET not in str(stopped.value)
    assert neurostore.token not in str(stopped.value)


def test_a_client_without_neurostore_s_admin_role_stops_the_upload(study, neurostore):
    neurostore.script(neurostore.error(403, "Only an admin can upload study_schema artifacts."))
    with _ingester(neurostore) as ingester, \
            pytest.raises(ni.NeurostoreAuthError, match="admin role"):
        ingester.upload(study)
    assert neurostore.granted == 1


def test_a_client_auth0_refuses_stops_before_anything_is_uploaded(study, neurostore, monkeypatch):
    monkeypatch.setenv(ni.CLIENT_SECRET_ENV, "wrong-secret")
    with _ingester(neurostore) as ingester, \
            pytest.raises(ni.NeurostoreAuthError, match="no token") as stopped:
        ingester.upload(study)
    assert "wrong-secret" not in str(stopped.value)
    assert not neurostore.calls and not (study / ni.UPLOAD_DIR).exists()


@pytest.mark.parametrize("status", [429, 503])
def test_an_auth0_that_does_not_answer_is_down(study, neurostore, status):
    neurostore.auth0_script.append((status, {"error": "busy"}))
    with _ingester(neurostore) as ingester, \
            pytest.raises(ni.NeurostoreUnavailable, match=str(status)):
        ingester.upload(study)
    assert not neurostore.calls


def test_each_deployment_s_tenant_and_audience_and_the_settings_that_override_them(
        neurostore, tmp_path):
    staging = Settings(neurostore_api_url="https://staging.neurostore.xyz/")
    assert ni.auth0(staging) == ("https://neurosynth-staging.us.auth0.com",
                                 "https://staging.synth.neurostore.xyz/api/")
    assert ni.auth0(Settings(neurostore_api_url="https://neurostore.org")) == (
        "https://neurosynth.us.auth0.com", "https://neurostore.org/api/")
    mine = Settings(neurostore_api_url=mock.URL, neurostore_auth0_url=mock.AUTH0_URL + "/",
                    neurostore_auth0_audience=mock.AUDIENCE, data_root=tmp_path)
    assert ni.auth0(mine) == (mock.AUTH0_URL, mock.AUDIENCE)
    with ni.NeurostoreIngester.from_settings(mine) as ingester:
        ingester._ask_token()
        assert ingester._token == neurostore.token
    with pytest.raises(ni.NeurostoreConfigError, match="no Auth0 tenant"):
        ni.NeurostoreIngester.from_settings(Settings(neurostore_api_url="http://elsewhere.test"))


def test_the_client_s_secret_and_token_are_scrubbed_from_logs_until_closed(neurostore):
    ingester, token = _holding_token(neurostore)
    record = logging.LogRecord("httpx", logging.INFO, __file__, 1,
                               f"{mock.CLIENT_SECRET} {token}", (), None)
    ni._SCRUB.filter(record)
    assert record.getMessage() == f"{ni.REDACTED} {ni.REDACTED}"
    held = ni._SCRUB.tokens.count(mock.CLIENT_SECRET)  # others open in this process hold it too
    ingester.close()
    assert ni._SCRUB.tokens.count(mock.CLIENT_SECRET) == held - 1
    assert token not in ni._SCRUB.tokens


def test_any_authorization_value_is_scrubbed_not_only_the_token():
    # The exact token is not always what an error prints: a pasted token's repr
    # carries escapes, and a proxy may echo some other credential.
    assert ni.scrub("Illegal header value b'Bearer other-secret\\n'", mock.TOKEN) == (
        f"Illegal header value b'Bearer {ni.REDACTED}'"
    )
    assert "s3cr3t" not in ni.scrub('{"Authorization": "Bearer s3cr3t"}', mock.TOKEN)
    assert "s3cr3t" not in ni.scrub("authorization=s3cr3t", None)
    assert ni.scrub(f"token {mock.TOKEN} rejected", mock.TOKEN) == (
        f"token {ni.REDACTED} rejected")


@pytest.mark.parametrize("before_cut", [5, 10, len(mock.TOKEN) - 1])
def test_a_raw_token_echoed_across_the_excerpt_cut_leaves_no_part_behind(neurostore,
                                                                         before_cut):
    """A proxy or a debug page can echo the raw token; neurostore's own errors do not."""
    ingester, token = _holding_token(neurostore)
    echo = b"x" * (ni.BODY_EXCERPT - before_cut) + token.encode() + b" and more"
    with ingester:
        error = ingester._error(_response(422, echo))
    assert token[:before_cut] not in json.dumps(error)
    assert len(error["body_excerpt"]) == ni.BODY_EXCERPT


def test_a_raw_token_echoed_in_an_error_field_is_redacted(neurostore):
    """Not an answer neurostore gives (its ErrorResponse never echoes the token): the
    scrubber's own check, on fields named as an ErrorResponse names them."""
    ingester, token = _holding_token(neurostore)
    body = json.dumps({"detail": f"token {token} rejected", "title": token,
                       "errors": [{"authorization": "x"}]}).encode()
    with ingester:
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
    study, neurostore, http_debug
):
    """httpcore traces a refused header at DEBUG with the header in it."""
    url = f"http://127.0.0.1:{_closed_port()}"
    with _ingester(url=url, transport=neurostore.with_network()) as ingester:
        with pytest.raises(ni.NeurostoreUnavailable) as stopped:
            ingester.upload(study)
        token = neurostore.token
        logging.getLogger("httpcore.http11").debug("Illegal header value b'Bearer %s'", token)
        logging.getLogger("httpx").debug("sent Authorization: Bearer %s", token)

    assert token not in str(stopped.value)
    assert token not in http_debug.text
    assert "Illegal header value b'Bearer [redacted]'" in http_debug.text


#: Every logger httpx and httpcore print a request or a header on.
HTTP_STACK = ("httpx", "httpcore", "httpcore.connection", "httpcore.http11", "httpcore.http2",
              "httpcore.proxy", "httpcore.socks")


@pytest.mark.parametrize("name", HTTP_STACK)
def test_every_http_logger_is_scrubbed(neurostore, http_debug, name):
    http_debug.set_level(logging.DEBUG, logger=name)
    ingester, token = _holding_token(neurostore)
    with ingester:
        logging.getLogger(name).debug("sent Authorization: Bearer %s (%s)", "other-secret", token)
    assert "other-secret" not in http_debug.text and token not in http_debug.text
    assert f"Bearer {ni.REDACTED}" in http_debug.text


def test_closing_an_ingester_drops_its_token_and_keeps_scrubbing_bearer_values(
    neurostore, http_debug
):
    # Relative counts: an ingester another test in this process left open holds it too.
    held = ni._SCRUB.tokens.count(mock.CLIENT_SECRET)
    other, _ = _holding_token(neurostore)
    ingester, token = _holding_token(neurostore)
    with ingester:
        assert ni._SCRUB.tokens.count(mock.CLIENT_SECRET) == held + 2
        assert token in ni._SCRUB.tokens
    assert ni._SCRUB.tokens.count(mock.CLIENT_SECRET) == held + 1  # the open one still scrubs it
    assert token not in ni._SCRUB.tokens
    other.close()
    assert ni._SCRUB.tokens.count(mock.CLIENT_SECRET) == held
    logging.getLogger("httpcore.http11").debug("late line Bearer %s", token)
    assert "late line Bearer [redacted]" in http_debug.text


def test_closing_an_ingester_closes_its_connections(neurostore):
    ingester = _ingester(neurostore)
    client = ingester._client
    ingester.close()
    assert client.is_closed


def test_an_unreadable_upload_artifact_is_moved_aside_not_overwritten(study, neurostore):
    path = study / ni.UPLOAD_DIR / ni.ARTIFACT_NAME
    path.parent.mkdir(parents=True)
    path.write_text('{"history": [')
    with _ingester(neurostore) as ingester:
        ingester.upload(study)
    assert path.with_name(path.name + ".unreadable").read_text() == '{"history": ['
    assert [h["status"] for h in _artifact(study)["history"]] == ["uploaded"]


def test_a_dry_run_from_settings_writes_outside_the_pond(study, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _no_client(monkeypatch)
    data = tmp_path / "data"
    settings = Settings(neurostore_api_url=mock.URL, dry_run=True, data_root=data)
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
    artifact = ni.NeurostoreIngester(mock.URL, dry_run=True, dry_run_root=root).upload(study)

    assert artifact["status"] == "invalid"
    assert json.loads((root / study.name / ni.DRY_RUN_NAME).read_text()) == artifact
    assert not (root / study.name / ni.ARTIFACT_NAME).exists()
    assert not (study / ni.UPLOAD_DIR).exists()


def test_a_dry_run_without_a_root_is_refused(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _no_client(monkeypatch)
    with pytest.raises(ValueError, match="dry_run_root"):
        ni.NeurostoreIngester(mock.URL, dry_run=True)


def test_every_neurostore_setting_is_in_the_settings_reference():
    import yaml

    reference = Path(__file__).parents[3] / "configs" / "settings_reference.yaml"
    documented = yaml.safe_load(reference.read_text())
    fields = {n: f for n, f in Settings.model_fields.items() if n.startswith("neurostore_")}
    assert set(fields) <= set(documented)
    for name, field in fields.items():
        default = getattr(field.default, "value", field.default)
        assert documented[name] == default, name
