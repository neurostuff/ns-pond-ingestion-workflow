"""What a cassette may hold where a credential was.

`filter_headers` and `filter_query_parameters` scrub only what was sent, but
Elsevier echoes the key it was sent in its answer, so answers are scrubbed too.
"""

from __future__ import annotations

REPLAY_ELSEVIER_KEY = "replay"
# Sent to PubMed, Crossref and OpenAlex to identify the caller: a person's address.
REPLAY_EMAIL = "replay@example.com"

# Sent by clients: dropped from the request when recording.
SECRET_REQUEST_HEADERS = (
    "authorization",
    "x-api-key",
    "x-els-apikey",
    "x-els-insttoken",
    "cookie",
)
SECRET_QUERY_PARAMETERS = ("api_key", "apiKey")
EMAIL_QUERY_PARAMETERS = ("email", "mailto")
# Echoed by a host: kept in the answer, with the placeholder in the key's place.
ECHOED_KEY_HEADERS = {"x-els-apikey": REPLAY_ELSEVIER_KEY}


def scrub_response(response):
    headers = response["headers"]
    for name in list(headers):
        if name.lower() == "set-cookie":
            del headers[name]
        elif name.lower() in ECHOED_KEY_HEADERS:
            headers[name] = [ECHOED_KEY_HEADERS[name.lower()]]
    return response
