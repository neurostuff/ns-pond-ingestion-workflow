"""A paper known only by its preprint has no `doi`; providers are still asked by the
preprint's, and what they answer with is filed under the right bucket. Recorded answers."""

import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import request_id
from ingestion_workflow.models.ids import Identifier, Identifiers

#: bioRxiv's DOI of a paper, and PubMed's record of that preprint.
PREPRINT = "10.1101/2025.05.13.653896"
PMID = "40462995"


def test_semantic_scholar_is_asked_for_the_papers_metadata_by_its_preprint_s_doi():
    assert request_id(Identifier(pmid=PMID, preprint_dois=[PREPRINT])) == f"DOI:{PREPRINT}"
    assert request_id(Identifier(pmid=PMID)) == f"PMID:{PMID}"
    assert request_id(Identifier()) is None


@pytest.mark.vcr()
def test_openalex_files_a_preprint_s_doi_it_answers_with_beside_the_pmid_it_was_asked_by():
    paper = Identifier(pmid=PMID)

    OpenAlexClient("someone@example.org").get_ids("pmid", Identifiers([paper]))

    assert (paper.doi, paper.preprint_dois) == (None, (PREPRINT,))


@pytest.mark.vcr()
def test_a_preprint_cited_by_its_doi_is_answered_by_the_batch_not_asked_again():
    found = OpenAlexClient("someone@example.org").get_cited_works([PREPRINT])

    assert list(found) == [PREPRINT]


def test_openalex_asks_for_ids_of_a_paper_known_only_by_its_preprint():
    OpenAlexClient("someone@example.org").validate_ids(
        "doi", Identifiers([Identifier(pmid=PMID, preprint_dois=[PREPRINT])]))
