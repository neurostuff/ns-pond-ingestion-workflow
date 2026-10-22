"""PubMed's summaries of records, asked in batches that esummary can answer.

NCBI's answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import tenacity
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.tests import cassette_secrets

#: 200 PMIDs esummary answers with a 500 every time it is asked them together, and each
#: half of them with a 200. A whole-catalog check stopped on them.
FAILING_BATCH = (Path(__file__).parents[1] / "data" / "esummary_failing_batch.txt").read_text().strip().split(",")


@pytest.fixture()
def pubmed(monkeypatch):
    # the retries are kept, their waits are not: a replay need not sleep through backoff
    monkeypatch.setattr(PubMedClient._request_json.retry, "wait", tenacity.wait_none())
    with PubMedClient(email=cassette_secrets.REPLAY_EMAIL) as client:
        yield client


@pytest.mark.vcr()
def test_a_batch_esummary_cannot_answer_whole_is_asked_in_halves(pubmed):
    found = pubmed.summaries(FAILING_BATCH)
    assert len(FAILING_BATCH) == 200
    assert set(found) <= set(FAILING_BATCH) and len(found) > 190
    assert found[FAILING_BATCH[0]].title
