"""What NCBI's efetch is asked again for."""

from __future__ import annotations

import pytest
import tenacity
from ingestion_workflow.clients.pubmed import PubMedClient

ARTICLES = (b'<?xml version="1.0"?><PubmedArticleSet><PubmedArticle><MedlineCitation>'
            b'<PMID>1</PMID></MedlineCitation></PubmedArticle></PubmedArticleSet>')


@pytest.fixture
def efetch(provider, monkeypatch):
    monkeypatch.setattr(PubMedClient._request_efetch.retry, "wait", tenacity.wait_none())
    monkeypatch.setattr(PubMedClient, "EFETCH_URL", provider.url)
    client = PubMedClient("t@example.com")
    client._min_interval = 0
    return client


def test_an_answer_cut_short_is_asked_again(efetch, provider):
    provider.body = ARTICLES[:60]
    with pytest.raises(Exception, match="unclosed token|no element found"):
        efetch._request_efetch(["1"])
    assert len(provider.asked) == 3


def test_a_refusal_is_asked_once(efetch, provider):
    provider.statuses = [403]
    with pytest.raises(Exception, match="403"):
        efetch._request_efetch(["1"])
    assert len(provider.asked) == 1


def test_a_busy_answer_is_asked_again_and_the_whole_one_is_read(efetch, provider):
    provider.statuses = [503, 200]
    provider.body = ARTICLES
    assert "PubmedArticleSet" in efetch._request_efetch(["1"])
    assert len(provider.asked) == 2
