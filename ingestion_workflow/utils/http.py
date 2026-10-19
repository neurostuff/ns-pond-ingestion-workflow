"""What a failed request to a provider says about the provider."""

from __future__ import annotations

import requests


def transient(exc: BaseException) -> bool:
    """A provider that was down or busy, not one that refused the request; only these are
    worth asking again: a timeout, a connection dropped before or during the answer, a body
    cut short or not JSON (a gateway's page in place of the answer), 429 or 5xx."""
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status == 429 or (status is not None and status >= 500)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout,
                            requests.exceptions.ChunkedEncodingError, requests.JSONDecodeError))
