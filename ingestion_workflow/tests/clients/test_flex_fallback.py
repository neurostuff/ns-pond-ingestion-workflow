"""Flex keeps the cheap tier without losing work when it has no capacity.

Flex is scheduled on spare capacity and answers
``429 Flex does not have sufficient resources`` when there is none. That is a
refusal rather than a throttle, so retrying flex spends the budget again for
the same answer; the call has to drop to the default tier to finish.
"""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.clients.coordinate_parsing import (
    CoordinateParsingClient,
    _is_flex_exhausted,
)
from ingestion_workflow.config import Settings


class _FlexExhausted(Exception):
    def __init__(self) -> None:
        super().__init__(
            "Error code: 429 - {'error': {'message': 'Flex does not have "
            "sufficient resources available to fulfill your request.'}}"
        )


class _RateLimited(Exception):
    def __init__(self) -> None:
        super().__init__("Error code: 429 - {'error': {'message': 'Rate limit reached'}}")


def _reply():
    call = type("FC", (), {"arguments": json.dumps({"analyses": []})})()
    message = type("M", (), {"function_call": call})()
    return type("R", (), {"choices": [type("C", (), {"message": message})()]})()


def _client(tmp_path, **overrides) -> CoordinateParsingClient:
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        llm_api_key="test-key",
        llm_service_tier="flex",
        **overrides,
    )
    return CoordinateParsingClient(settings, api_key="test-key")


def test_detects_only_the_flex_refusal() -> None:
    assert _is_flex_exhausted(_FlexExhausted()) is True
    assert _is_flex_exhausted(_RateLimited()) is False
    assert _is_flex_exhausted(ValueError("boom")) is False


def test_falls_back_to_the_default_tier(tmp_path, monkeypatch) -> None:
    client = _client(tmp_path)
    seen = []

    def create(**kwargs):
        seen.append(kwargs.get("service_tier"))
        if kwargs.get("service_tier") == "flex":
            raise _FlexExhausted()
        return _reply()

    monkeypatch.setattr(client.client.chat.completions, "create", create)

    client.parse_analyses("a table")

    assert seen == ["flex", None], "expected one flex attempt then the default tier"


def test_flex_is_tried_first_and_kept_when_it_works(tmp_path, monkeypatch) -> None:
    client = _client(tmp_path)
    seen = []

    def create(**kwargs):
        seen.append(kwargs.get("service_tier"))
        return _reply()

    monkeypatch.setattr(client.client.chat.completions, "create", create)

    client.parse_analyses("a table")

    assert seen == ["flex"], "a working flex call must not be retried"


def test_an_ordinary_rate_limit_is_not_swallowed(tmp_path, monkeypatch) -> None:
    """Being throttled is the SDK's business; only the capacity refusal falls back."""
    client = _client(tmp_path)

    def create(**kwargs):
        raise _RateLimited()

    monkeypatch.setattr(client.client.chat.completions, "create", create)

    with pytest.raises(_RateLimited):
        client.parse_analyses("a table")


def test_fallback_can_be_switched_off(tmp_path, monkeypatch) -> None:
    client = _client(tmp_path, llm_tier_fallback=False)

    def create(**kwargs):
        raise _FlexExhausted()

    monkeypatch.setattr(client.client.chat.completions, "create", create)

    with pytest.raises(_FlexExhausted):
        client.parse_analyses("a table")
