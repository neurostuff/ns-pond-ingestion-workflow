"""One record a provider's batched answer holds that cannot be read fails its own article,
never the rest of the answer. Each answer is built from recorded ones, then edited."""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import httpx
import pytest
import xmltodict
import yaml
from ingestion_workflow.clients import pubmed as pubmed_module
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.notices import PartialAnswer

CASSETTES = Path(__file__).parents[1] / "cassettes"


def _recorded(cassette: str, asked: str) -> str:
    """The body of the cassette's first 200 answer to a request whose URI holds `asked`."""
    for interaction in yaml.safe_load((CASSETTES / f"{cassette}.yaml").read_text())[
            "interactions"]:
        response = interaction["response"]
        if asked in interaction["request"]["uri"] and response["status"]["code"] == 200:
            body = response["body"]["string"]
            if isinstance(body, bytes):
                body = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
                body = body.decode()
            return body
    raise LookupError(f"{cassette} has no answer to {asked}")


def _pubmed_articles():
    return {pmid: xmltodict.parse(_recorded(
        f"test_a_stored_record_gives_the_summary_esummary_gives[{pmid}]", "efetch.fcgi"))[
            "PubmedArticleSet"]["PubmedArticle"]
        for pmid in ("11335702", "38013996")}


def _efetch(articles):
    def request(pmids):
        return {"PubmedArticleSet": {"PubmedArticle": [
            articles[p] for p in pmids if p in articles]}}
    return request


def test_a_pubmed_author_that_cannot_be_read_fails_only_its_article(monkeypatch):
    """11335702's first author was given a ForeName with an attribute, which xmltodict
    reads as a dict."""
    articles = _pubmed_articles()
    author = articles["11335702"]["MedlineCitation"]["Article"]["AuthorList"]["Author"][0]
    author["ForeName"] = {"@Lang": "en", "#text": author["ForeName"]}
    client = PubMedClient(email="t@example.com")
    monkeypatch.setattr(client, "_request_efetch", _efetch(articles))
    bad, good = Identifier(pmid="11335702"), Identifier(pmid="38013996")

    with pytest.raises(PartialAnswer) as raised:
        client.get_metadata([bad, good])

    assert set(raised.value.found) == {good.slug}
    assert raised.value.found[good.slug].authors
    assert list(raised.value.unreadable) == [bad.slug]
    assert raised.value.unreadable[bad.slug].startswith("TypeError")
    assert raised.value.causes == ()


def test_a_failed_pubmed_batch_is_a_cause_and_the_others_are_kept(monkeypatch):
    articles = _pubmed_articles()
    monkeypatch.setattr(pubmed_module, "IDCONV_BATCH_SIZE", 1)
    request = _efetch(articles)

    def efetch(pmids):
        if pmids == ["11335702"]:
            raise httpx.ConnectError("refused")
        return request(pmids)

    client = PubMedClient(email="t@example.com")
    monkeypatch.setattr(client, "_request_efetch", efetch)
    failed, answered = Identifier(pmid="11335702"), Identifier(pmid="38013996")

    with pytest.raises(PartialAnswer) as raised:
        client.get_metadata([failed, answered])

    assert set(raised.value.found) == {answered.slug}
    assert [type(c) for c in raised.value.causes] == [httpx.ConnectError]
    assert raised.value.unreadable == {}


def test_pubmed_notices_that_cannot_be_read_fail_only_their_article(monkeypatch):
    """11335702 was given two CommentsCorrectionsList elements, which xmltodict reads as
    a list."""
    articles = _pubmed_articles()
    citation = articles["11335702"]["MedlineCitation"]
    citation["CommentsCorrectionsList"] = [{"CommentsCorrections": []}] * 2
    client = PubMedClient(email="t@example.com")
    monkeypatch.setattr(client, "_request_efetch", _efetch(articles))

    with pytest.raises(PartialAnswer) as raised:
        client.get_notices(["11335702", "38013996"])

    assert set(raised.value.found) == {"38013996"}
    assert list(raised.value.unreadable) == ["11335702"]


def _s2_record(cassette: str) -> dict:
    return json.loads(_recorded(cassette, "paper/batch?fields=title"))[0]


def test_a_semantic_scholar_paper_that_cannot_be_read_fails_only_its_article(monkeypatch):
    """The first paper's authors were given a null entry."""
    bad_record = _s2_record("test_a_licence_nobody_states_is_recorded_as_assumed")
    good_record = _s2_record("test_each_held_copy_and_the_paper_get_a_licence")
    bad_record["authors"] = [None, *bad_record["authors"]]
    client = SemanticScholarClient("replay")
    monkeypatch.setattr(client, "_request_metadata_batch",
                        lambda ids: [bad_record, good_record])
    bad, good = Identifier(doi="10.1037/npe0000112"), Identifier(doi="10.1002/alz.12697")

    with pytest.raises(PartialAnswer) as raised:
        client.get_metadata([bad, good])

    assert set(raised.value.found) == {good.slug}
    assert raised.value.found[good.slug].title.startswith("Comparative analytical")
    assert list(raised.value.unreadable) == [bad.slug]
    assert raised.value.causes == ()


#: Wiley's old DOI for an article OpenAlex lists under Cambridge's 10.1017/s0012162204001306.
MOVED = "10.1111/j.1469-8749.2004.tb00996.x"
SICI = "10.1002/(SICI)1097-0193(1998)6:3<160::AID-HBM5>3.0.CO;2-1"


def test_an_openalex_work_that_cannot_be_read_fails_only_its_article(monkeypatch):
    """The SICI work's authorships were given a null entry; the moved DOI's work is asked
    for singly, as its batch answer carries it under its current DOI."""
    works = json.loads(_recorded(
        "test_metadata_of_a_moved_doi_is_keyed_by_the_identifier_asked_for", "filter="))
    sici_work, moved_work = works["results"]
    sici_work["authorships"] = [None, *sici_work["authorships"]]

    def request(params, work=""):
        return copy.deepcopy(moved_work) if work else copy.deepcopy(works)

    client = OpenAlexClient()
    monkeypatch.setattr(client, "_request_openalex", request)
    sici, moved = Identifier(doi=SICI), Identifier(doi=MOVED)

    with pytest.raises(PartialAnswer) as raised:
        client.get_metadata([sici, moved])

    assert set(raised.value.found) == {moved.slug}
    assert list(raised.value.unreadable) == [sici.slug]
    assert raised.value.unreadable[sici.slug].startswith("AttributeError")


def test_an_openalex_work_whose_doi_cannot_be_read_is_asked_for_singly(monkeypatch):
    """The plain DOI's work was given its DOI as a list; asked for alone, it answers."""
    answer = json.loads(_recorded("test_a_moved_doi_is_keyed_by_the_doi_that_was_asked",
                                  "filter="))
    plain_work, _ = answer["results"]
    broken = {**plain_work, "doi": [plain_work["doi"]]}

    def request(params, work=""):
        return plain_work if work else {"results": [broken]}

    client = OpenAlexClient()
    monkeypatch.setattr(client, "_request_openalex", request)

    assert client.get_retractions(["10.1038/nature14539"]) == {"10.1038/nature14539": False}


def test_a_crossref_work_whose_deposits_cannot_be_read_fails_only_its_article(monkeypatch):
    """The first work's `license` was given as a number."""
    items = json.loads(_recorded(
        "test_every_doi_asked_is_answered_with_its_deposits_in_one_query", "filter="))[
            "message"]["items"]
    items[0]["license"] = 1
    client = CrossrefClient()
    monkeypatch.setattr(client, "_list", lambda params: items)
    monkeypatch.setattr(client, "work", lambda doi: pytest.fail(f"{doi} asked singly"))
    identifiers = [Identifier(doi=item["DOI"]) for item in items]

    with pytest.raises(PartialAnswer) as raised:
        client.get_metadata(identifiers)

    bad, *good = identifiers
    assert set(raised.value.found) == {i.slug for i in good}
    assert list(raised.value.unreadable) == [bad.slug]
    assert raised.value.causes == ()
