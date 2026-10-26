"""Crossref's relations read from the other side: the records relating them to a DOI."""

from __future__ import annotations

import pytest
from ingestion_workflow.clients import crossref
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.tests import cassette_secrets

VERSIONS = ("is-preprint-of", "has-preprint")


def _played(vcr):
    return [vcr.requests[i] for i in sorted(vcr.play_counts)]


@pytest.mark.vcr()
def test_every_page_of_the_relating_records_is_read(monkeypatch):
    # eLife's reviewed preprint 10.7554/eLife.91263.1: its concept DOI and its next version
    # both say has-preprint of it
    monkeypatch.setattr(crossref, "CROSSREF_PAGE_ROWS", 1)
    client = CrossrefClient(cassette_secrets.REPLAY_EMAIL)
    found = client.relating(["10.7554/eLife.91263.1"], VERSIONS)
    assert sorted(found["10.7554/eLife.91263.1"]["has-preprint"]) == [
        "10.7554/elife.91263", "10.7554/elife.91263.2"]


@pytest.mark.vcr()
def test_only_the_relations_asked_for_are_read():
    client = CrossrefClient(cassette_secrets.REPLAY_EMAIL)
    assert client.relating(["10.7554/eLife.91263.1"], ("is-preprint-of",)) == {
        "10.7554/eLife.91263.1": {}}


@pytest.mark.vcr()
def test_many_dois_are_asked_in_one_query_and_each_is_answered_alone(vcr):
    # bioRxiv relates a preprint to each of these two papers, by these spellings
    client = CrossrefClient(cassette_secrets.REPLAY_EMAIL)
    found = client.relating(["10.7554/eLife.86439", "10.1523/JNEUROSCI.0665-22.2023"], VERSIONS)

    assert found == {
        "10.7554/eLife.86439": {"is-preprint-of": ["10.1101/2022.10.16.511648"]},
        "10.1523/JNEUROSCI.0665-22.2023": {"is-preprint-of": ["10.1101/2022.04.05.487162"]}}
    assert len(_played(vcr)) == 1
