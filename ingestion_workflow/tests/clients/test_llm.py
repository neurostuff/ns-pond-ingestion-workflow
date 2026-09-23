from __future__ import annotations

from ingestion_workflow.clients import llm as llm_module
from ingestion_workflow.clients.llm import GenericLLMClient


class _DummyOpenAI:
    def __init__(self, *, api_key: str, base_url: str | None = None) -> None:
        self.api_key = api_key
        self.base_url = base_url


def test_generic_llm_client_reads_openai_portkey_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("OPENAI_API_GATEWAY", "https://api.portkey.ai/v1")
    monkeypatch.setenv(
        "OPENAI_EMBEDDING_MODEL",
        "@psyc-aid338-ope-333f18/text-embedding-3-small",
    )
    monkeypatch.setattr(llm_module, "OpenAI", _DummyOpenAI)

    client = GenericLLMClient()

    assert client.api_key == "test-key"
    assert client.base_url == "https://api.portkey.ai/v1"
    assert client.default_model == "@psyc-aid338-ope-333f18/text-embedding-3-small"
