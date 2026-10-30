"""A batch the provider refuses costs only the items it refuses alone, in every client
that asks for many at once (`isolation.halving`). Each provider here refuses any request
that holds the one item it cannot take."""

import httpx
import pytest
import requests

from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.notices import PartialAnswer

POISON = "10.9999/refused"


def _refusal(cls=requests.HTTPError, code=400):
    if cls is httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://eutils.example")
        return cls(f"{code}", request=request, response=httpx.Response(code, request=request))
    response = requests.Response()
    response.status_code = code
    return cls(f"{code}", response=response)


def test_crossref_list_queries_keep_the_rest_of_a_refused_batch(monkeypatch):
    client = CrossrefClient("someone@example.org")

    def listed(params):
        dois = [f.removeprefix("doi:") for f in params["filter"].split(",")]
        if POISON in dois:
            raise _refusal()
        return [{"DOI": d.upper()} for d in dois]

    monkeypatch.setattr(client, "_list", listed)
    dois = [f"10.1000/{i}" for i in range(20)] + [POISON]

    assert set(client.works(dois, "DOI")) == {f"10.1000/{i}" for i in range(20)}


def test_semantic_scholar_keeps_the_rest_of_a_refused_batch(monkeypatch):
    client = SemanticScholarClient("key")

    def batch(ids):
        if f"DOI:{POISON}" in ids:
            raise _refusal()
        return [{"title": i, "year": 2020, "authors": []} for i in ids]

    monkeypatch.setattr(client, "_request_metadata_batch", batch)
    good = [Identifier(doi=f"10.1000/{i}") for i in range(5)]
    with pytest.raises(PartialAnswer) as raised:  # the refusal is said; the rest are kept
        client.get_metadata([*good, Identifier(doi=POISON)])
    assert set(raised.value.found) == {i.slug for i in good}
    assert "400" in str(raised.value)


def test_pubmed_summaries_keep_the_rest_of_a_refused_batch(monkeypatch):
    client = PubMedClient("someone@example.org")

    def answer(url, params):
        pmids = dict(params)["id"].split(",")
        if "999" in pmids:
            raise _refusal(httpx.HTTPStatusError)
        return {"result": {"uids": pmids, **{p: {"title": p, "articleids": []} for p in pmids}}}

    monkeypatch.setattr(client, "_request_json", answer)

    assert set(client.summaries(["1", "2", "999", "3"])) == {"1", "2", "3"}
