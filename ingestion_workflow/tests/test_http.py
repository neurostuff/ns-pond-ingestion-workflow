"""Which failed requests are worth asking again."""

from __future__ import annotations

import pytest
import requests
from ingestion_workflow.utils.http import transient


def _status(code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(f"{code}", response=response)


@pytest.mark.parametrize("exc, expected", [
    (_status(429), True),
    (_status(500), True),
    (_status(503), True),
    (requests.ReadTimeout("read timed out"), True),
    (requests.ConnectionError("connection refused"), True),
    (requests.exceptions.ChunkedEncodingError("connection broken mid-body"), True),
    (requests.JSONDecodeError("Expecting value", "<html>Bad gateway</html>", 0), True),
    (_status(400), False),
    (_status(403), False),
    (_status(404), False),
])
def test_a_provider_down_or_busy_is_asked_again_and_a_refusal_is_not(exc, expected):
    assert transient(exc) is expected
