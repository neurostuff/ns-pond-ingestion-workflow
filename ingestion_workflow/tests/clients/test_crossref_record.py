"""What a DOI's own Crossref record says it is, and what it is related to.

Crossref's answers are recorded; requests carry the placeholder email.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.tests import cassette_secrets


@pytest.fixture()
def crossref():
    return CrossrefClient(cassette_secrets.REPLAY_EMAIL)


@pytest.mark.vcr()
def test_a_reviewed_preprint_is_posted_content_of_subtype_preprint(crossref):
    record = crossref.record("10.7554/eLife.106070.1")

    assert (record.type, record.subtype) == ("posted-content", "preprint")
    assert record.relations["is-version-of"] == ("10.1101/2025.02.05.636581",)


@pytest.mark.vcr()
def test_a_published_paper_is_a_journal_article_with_its_relations_in_lower_case(crossref):
    record = crossref.record("10.7554/eLife.106070")

    assert (record.type, record.subtype) == ("journal-article", None)
    assert record.relations["has-preprint"] == ("10.1101/2025.02.05.636581",)
    assert record.relations["is-same-as"] == ("10.7554/elife.106070.3",)
    assert record.spellings == ("10.7554/eLife.106070",)


@pytest.mark.vcr()
def test_a_relation_to_something_that_is_not_a_doi_is_not_a_doi(crossref):
    # Crossref's record says has-preprint the arXiv id 2003.06916v2
    record = crossref.record("10.23638/dmtcs-22-1-20")

    assert record.relations["has-preprint"] == ()
    assert record.relations["is-same-as"] == ("10.48550/arxiv.2003.06916",)


@pytest.mark.vcr()
def test_a_retraction_updates_the_work_it_retracts(crossref):
    record = crossref.record("10.1016/j.nbd.2012.05.020")

    assert record.updates == (("10.1016/j.nbd.2012.05.020", "retraction"),)


@pytest.mark.vcr()
def test_a_doi_crossref_does_not_have_has_no_record(crossref):
    assert crossref.record("10.137/journal.pone.0066241") is None
