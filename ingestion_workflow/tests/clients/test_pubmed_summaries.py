"""PubMed's summaries of records, asked in batches that esummary can answer.

NCBI's answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
import tenacity
from ingestion_workflow.clients.pubmed import (
    PUBMED_REQUEST_LIMIT,
    PubMedClient,
    summary_of_record,
)
from ingestion_workflow.models import Identifier
from ingestion_workflow.tests import cassette_secrets

#: 200 PMIDs esummary answers with a 500 every time it is asked them together, and each
#: half of them with a 200. A whole-catalog check stopped on them.
FAILING_BATCH = (Path(__file__).parents[1] / "data" / "esummary_failing_batch.txt"
                 ).read_text().strip().split(",")


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


def test_every_client_in_the_process_keeps_one_pace_with_ncbi():
    # NCBI counts a caller's requests: two clients without a key make 3 a second between them.
    first = PubMedClient(email=cassette_secrets.REPLAY_EMAIL)
    second = PubMedClient(email=cassette_secrets.REPLAY_EMAIL)
    started = time.monotonic()
    for client in (first, second, first, second):
        client._rate_limit_sleep()
    assert time.monotonic() - started >= 3 / PUBMED_REQUEST_LIMIT - 0.05


@pytest.mark.vcr()
@pytest.mark.parametrize("pmid", [
    "38013996",   # a bioRxiv preprint's record, typed Preprint, with its PMC copy
    "11335702",   # Brain, 2001
    "41221578",   # a review whose DOI PubMed spells in capitals
    "41178409",   # a parotid tumour study, which PubMed gives the DOI it gives the review
])
def test_a_stored_record_gives_the_summary_esummary_gives(pubmed, pmid):
    stored = pubmed.get_metadata([Identifier(pmid=pmid)])
    record = next(iter(stored.values())).raw_metadata["pubmed"]

    summary, asked = summary_of_record(record), pubmed.summaries([pmid])[pmid]

    for field in ("doi", "title", "pubtypes", "pmcid", "first_author", "year", "supplement"):
        assert getattr(summary, field) == getattr(asked, field), field
