"""Recording and replaying Wiley TDM cassettes.

A cassette keeps `REPLAY_TOKEN` where the token was, and a replay sends it, so
`same_token` can tell a request that carried the token from one that did not.
"""

from __future__ import annotations

import os

from ingestion_workflow.extractors.wiley_extractor import TOKEN_HEADER

REPLAY_TOKEN = "replay-token-wiley-7c41"
MATCH_ON = ("method", "scheme", "host", "port", "path", "query", "wiley_token")


def token() -> str:
    """The key to record with, or the one the cassettes hold in its place."""
    return os.environ.get("WILEY_API_KEY") or REPLAY_TOKEN


def same_token(recorded, replayed) -> None:
    assert recorded.headers.get(TOKEN_HEADER) == replayed.headers.get(TOKEN_HEADER)
