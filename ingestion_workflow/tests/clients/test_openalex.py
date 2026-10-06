import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.models.ids import Identifier, Identifiers


@pytest.mark.vcr()
def test_openalex_client_enriches_pmids(manifest_identifiers):
    original = manifest_identifiers[0]
    identifiers = Identifiers(
        [
            Identifier(
                neurostore=original.neurostore,
                pmid=original.pmid,
                doi=original.doi,
                pmcid=original.pmcid,
            )
        ]
    )

    client = OpenAlexClient(email="test@example.com")
    client.get_ids("pmid", identifiers)

    enriched = identifiers[0]
    assert enriched.other_ids is not None
    expected_openalex = "https://openalex.org/W2954579665"
    assert enriched.other_ids.get("openalex") == expected_openalex


SCHRES = {
    "ids": {
        "openalex": "https://openalex.org/W2603550846",
        "doi": "https://doi.org/10.1016/j.schres.2017.03.030",
        "pmid": "https://pubmed.ncbi.nlm.nih.gov/28351544",
    },
    "display_name": "Comparing the effect of clozapine and risperidone on cue reactivity",
    "authorships": [{"author": {"display_name": "Marise W.J. Machielsen"}}],
    "publication_year": 2017,
    "abstract_inverted_index": {"Cannabis": [0], "use": [1, 4], "disorder": [2], "and": [3]},
    "primary_location": {"source": {"display_name": "Schizophrenia Research"}},
    "open_access": {"is_oa": False},
}


def test_the_id_lookup_fills_the_pubmed_ids_openalex_carries(monkeypatch):
    """Semantic Scholar 404s on this DOI and the article is not in PMC, so
    OpenAlex is the only provider that knows its PMID."""
    client = OpenAlexClient(email="test@example.com")
    monkeypatch.setattr(client, "_request_openalex", lambda params: {"results": [SCHRES]})
    identifiers = Identifiers([Identifier(doi="10.1016/j.schres.2017.03.030")])
    identifiers.set_index("doi")

    client.get_ids("doi", identifiers)

    assert identifiers[0].pmid == "28351544"
    assert identifiers[0].other_ids["openalex"] == "https://openalex.org/W2603550846"


def test_metadata_is_read_back_out_of_an_inverted_abstract(monkeypatch):
    client = OpenAlexClient(email="test@example.com")
    monkeypatch.setattr(client, "_request_openalex", lambda params: {"results": [SCHRES]})
    # The catalog can hold the DOI in another case than OpenAlex prints it.
    identifier = Identifier(doi="10.1016/J.SCHRES.2017.03.030")

    found = client.get_metadata([identifier])[identifier.slug]

    assert found.abstract == "Cannabis use disorder and use"
    assert found.journal == "Schizophrenia Research"
    assert found.publication_year == 2017
    assert [a.name for a in found.authors] == ["Marise W.J. Machielsen"]
    assert found.source == "openalex"


def test_a_pmid_only_article_is_looked_up_by_pmid(monkeypatch):
    asked = []

    def request(params):
        asked.append(params["filter"])
        return {"results": [SCHRES] if params["filter"].startswith("pmid:") else []}

    client = OpenAlexClient(email="test@example.com")
    monkeypatch.setattr(client, "_request_openalex", request)
    identifier = Identifier(pmid="28351544")

    assert identifier.slug in client.get_metadata([identifier])
    assert asked == ["pmid:28351544"]
