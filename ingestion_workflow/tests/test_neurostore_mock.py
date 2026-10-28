"""The mock neurostore refuses what its contract does not allow, in both directions."""

from __future__ import annotations

import json

import httpx
import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.services import neurostore_ingest as ni
from ingestion_workflow.tests import neurostore as mock
from ingestion_workflow.tests.services.test_paper_parse import BASE, _written


@pytest.fixture()
def body(tmp_path):
    """The fixture paper's upload, as the ingester builds it."""
    _, _, summary = _written(tmp_path)
    payload, _ = ni.build_payload(summary["dir"].parent)
    return payload


def _post(server, body, token=mock.TOKEN):
    with httpx.Client(transport=server.transport) as client:
        return client.post(mock.URL + mock.api_path(), content=json.dumps(body).encode(),
                           headers={"Authorization": f"Bearer {token}",
                                    "Content-Type": "application/json"})


def test_the_contract_is_the_pinned_spec():
    assert mock.api_path() == ni.UPLOAD_PATH
    schemas = mock.spec()["components"]["schemas"]
    assert {"text"} <= set(schemas["study-schema-upload"]["properties"])
    assert {"merged_from", "took_ownership_from"} <= set(
        schemas["study-schema-upload-summary"]["properties"])


def test_an_upload_that_keeps_the_contract_gets_its_summary(body):
    server = mock.Neurostore()
    answered = _post(server, body)
    assert answered.status_code == 200
    summary = answered.json()
    assert (summary["kind"], summary["base_study_id"], summary["parse_id"]) == (
        "parse", BASE, body["coordinate_parse"]["parse_id"])
    assert summary["skeleton"]["created"] == len(body["coordinate_parse"]["analyses"])
    assert server.uploads == [body] and server.violations == []


def test_the_route_answers_a_get_with_405_and_any_other_path_with_404():
    server = mock.Neurostore()
    with httpx.Client(transport=server.transport) as client:
        assert client.get(mock.URL + mock.api_path()).status_code == 405
        assert client.post(mock.URL + "/api/api" + mock.ROUTE).status_code == 404
    server.check()


def test_an_upload_without_its_parsed_paper_fails_loudly(body):
    server = mock.Neurostore()
    del body["parsed_paper"]
    with pytest.raises(mock.ContractViolation, match="no parsed_paper"):
        _post(server, body)
    with pytest.raises(mock.ContractViolation):
        server.check()


def test_a_text_that_is_not_the_one_its_hash_names_fails_loudly(body):
    server = mock.Neurostore()
    body["text"] += " "
    with pytest.raises(mock.ContractViolation, match="not parsed_paper's text_sha256"):
        _post(server, body)


@pytest.mark.parametrize("change, problem", [
    (lambda b: b["coordinate_parse"]["analyses"][0].pop("role"), "coordinate-parse/analyses/0"),
    (lambda b: b["parsed_paper"].pop("bibliography"), "parsed-paper/"),
    (lambda b: b.update(extra=1), "study-schema-upload/"),
])
def test_a_document_study_schema_or_the_spec_refuses_fails_loudly(body, change, problem):
    change(body)
    with pytest.raises(mock.ContractViolation, match=problem):
        _post(mock.Neurostore(), body)


def test_an_upload_without_the_admin_token_fails_loudly(body):
    with pytest.raises(mock.ContractViolation, match="admin token"):
        _post(mock.Neurostore(), body, token="someone-else")


@pytest.mark.parametrize("script", [
    lambda s: s.reply(422, {"detail": "Analysis keys must be derived"}),
    lambda s: s.reply(200, {"kind": "upload", "parse_id": "p"}),
    lambda s: s.error(422, "x", codes=["KEY_MISMATCH"], context="not an object"),
    lambda s: s.proxy(502, b'{"detail": "Bad Gateway"}'),
], ids=["error-without-its-fields", "summary-of-no-kind", "context-not-an-object",
        "proxy-with-json"])
def test_a_scripted_answer_the_contract_does_not_allow_fails_when_scripted(script):
    server = mock.Neurostore()
    with pytest.raises(mock.ContractViolation):
        script(server)
    assert server.violations


def test_a_summary_that_breaks_its_schema_fails_when_sent(body):
    server = mock.Neurostore().script(mock.Neurostore().summary(skeleton={"created": "four"}))
    with pytest.raises(mock.ContractViolation, match="skeleton/created"):
        _post(server, body)
    assert server.violations


def test_a_violation_a_caller_catches_still_fails_the_test(body, monkeypatch):
    """What the `neurostore` fixture does at teardown."""
    body["text"] = ""
    with pytest.raises(mock.ContractViolation, match="text hashes to"):
        with mock.serve(monkeypatch) as server:
            try:
                _post(server, body)
            except mock.ContractViolation:
                pass  # what the upload stage does with any error but a StopRun


def test_an_ingester_built_from_settings_for_its_url_is_served(tmp_path, monkeypatch):
    _, _, summary = _written(tmp_path)
    with mock.serve(monkeypatch) as server:
        settings = Settings(neurostore_api_url=server.url, data_root=tmp_path / "d")
        with ni.NeurostoreIngester.from_settings(settings) as ingester:
            assert ingester.upload(summary["dir"].parent)["status"] == "uploaded"
    assert len(server.uploads) == 1


def test_a_closed_server_takes_no_request(body):
    server = mock.Neurostore(closed=True)
    with pytest.raises(mock.ContractViolation, match="sends nothing"):
        _post(server, body)


def test_the_summary_counts_keys_against_what_the_base_study_holds(body):
    server = mock.Neurostore()
    first, again = _post(server, body).json(), _post(server, body).json()
    n = len(body["coordinate_parse"]["analyses"])
    assert (first["skeleton"]["created"], first["skeleton"]["updated"]) == (n, 0)
    assert (again["skeleton"]["created"], again["skeleton"]["updated"]) == (0, n)
