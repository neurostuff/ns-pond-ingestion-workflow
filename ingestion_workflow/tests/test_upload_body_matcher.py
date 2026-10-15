import json

import pytest
from ingestion_workflow.tests.upload_body import same_body_but_input_fingerprints
from vcr.request import Request


def upload(fingerprint="a", text="T", other="x", kind="analyses", extra=0):
    body = {
        "coordinate_parse": {"header": {"inputs": [{"artifact_kind": kind, "fingerprint": fingerprint}] * (1 + extra)},
                             "text_sha256": text, "analyses": [{"name": other}]},
        "parsed_paper": {"header": {"inputs": [{"artifact_kind": "metadata", "fingerprint": fingerprint}]}},
    }
    return Request("POST", "http://localhost/api/study-schema-uploads/", json.dumps(body).encode(), {})


def test_only_the_input_fingerprints_may_differ():
    same_body_but_input_fingerprints(upload(), upload(fingerprint="b"))


@pytest.mark.parametrize("changed", [{"text": "U"}, {"other": "y"}, {"kind": "metadata"}, {"extra": 1}])
def test_any_other_change_to_the_body_does_not_match(changed):
    with pytest.raises(AssertionError):
        same_body_but_input_fingerprints(upload(), upload(**changed))
