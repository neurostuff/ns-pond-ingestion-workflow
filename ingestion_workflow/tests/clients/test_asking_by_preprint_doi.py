"""A paper known only by its preprint has no `doi`; providers are still asked by the
preprint's, and what they answer with is filed under the right bucket. The providers
are replaced at their request method, the network boundary, and nothing else."""

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.models.ids import Identifier, Identifiers

PREPRINT = "10.1101/2025.05.13.653896"


def test_semantic_scholar_is_asked_for_the_papers_metadata_by_its_preprint_s_doi():
    asked = []

    class Client(SemanticScholarClient):
        def _request_metadata_batch(self, request_ids):
            asked.extend(request_ids)
            return [None] * len(request_ids)

    Client("replay").get_metadata([Identifier(pmid="40462995", preprint_dois=[PREPRINT])])

    assert asked == [f"DOI:{PREPRINT}"]


def test_openalex_filing_a_preprint_s_doi_it_answers_with_beside_the_pmid_it_was_asked_by():
    class Client(OpenAlexClient):
        def _request_openalex(self, params, work=""):
            return {"results": [{"ids": {
                "openalex": "https://openalex.org/W4410000001",
                "pmid": "https://pubmed.ncbi.nlm.nih.gov/40462995",
                "doi": f"https://doi.org/{PREPRINT}"}}]}

    paper = Identifier(pmid="40462995")
    Client("someone@example.org").get_ids("pmid", Identifiers([paper]))

    assert (paper.doi, paper.preprint_dois) == (None, (PREPRINT,))
    assert paper.other_ids == {"openalex": "https://openalex.org/W4410000001"}
