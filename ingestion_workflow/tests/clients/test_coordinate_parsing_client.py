import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import ParseAnalysesOutput


def _require_api_key():
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path, override=False)
    os.environ.setdefault("VCR_RECORD_MODE", "once")
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY not set; skipping live flex/non-flex tests")
    return api_key


DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")


def _client(use_flex: bool, api_key: str) -> CoordinateParsingClient:
    settings = Settings(
        llm_api_key=api_key,
        llm_model=DEFAULT_MODEL,
        openai_flex_processing=use_flex,
    )
    return CoordinateParsingClient(settings=settings, use_flex_processing=use_flex, retry_attempts=1)


def _prompt() -> str:
    return """
You are a neuroimaging table curation assistant. Output JSON using parse_analyses.
Table snippet:
Analysis A
X Y Z
1 2 3
"""


@pytest.mark.xfail(reason="OpenAI Responses flex tool calls currently return no function call")
def test_parse_analyses_flex():
    api_key = _require_api_key()
    client = _client(use_flex=True, api_key=api_key)
    result = client.parse_analyses(_prompt(), model=DEFAULT_MODEL)
    assert isinstance(result, ParseAnalysesOutput)
    assert result.analyses
    assert result.analyses[0].points


def test_parse_analyses_chat():
    api_key = _require_api_key()
    client = _client(use_flex=False, api_key=api_key)
    result = client.parse_analyses(_prompt(), model=DEFAULT_MODEL)
    assert isinstance(result, ParseAnalysesOutput)
    assert result.analyses
    assert result.analyses[0].points
