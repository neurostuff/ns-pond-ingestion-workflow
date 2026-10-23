"""Crossref's relations read from the other side: the records relating them to a DOI."""

from __future__ import annotations

import pytest
from ingestion_workflow.clients import crossref
from ingestion_workflow.clients.crossref import CrossrefClient

VERSIONS = ("is-preprint-of", "has-preprint")


@pytest.mark.vcr()
def test_every_page_of_the_relating_records_is_read(monkeypatch):
    # eLife's reviewed preprint 10.7554/eLife.91263.1: its concept DOI and its next version
    # both say has-preprint of it
    monkeypatch.setattr(crossref, "CROSSREF_PAGE_ROWS", 1)
    found = CrossrefClient().relating("10.7554/eLife.91263.1", VERSIONS)
    assert sorted(found["has-preprint"]) == ["10.7554/elife.91263", "10.7554/elife.91263.2"]


@pytest.mark.vcr()
def test_only_the_relations_asked_for_are_read():
    assert CrossrefClient().relating("10.7554/eLife.91263.1", ("is-preprint-of",)) == {}
