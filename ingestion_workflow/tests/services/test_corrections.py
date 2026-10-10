"""Retraction and erratum notices read from PubMed's CommentsCorrections.

tests/data/pubmed/efetch_retraction.xml is a real efetch response for PMID
9500320 (Wakefield 1998, retracted) and PMID 20137807 (its retraction notice).
"""

from pathlib import Path
from types import SimpleNamespace

import xmltodict
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages.upload import UploadStage
from ingestion_workflow.services.metadata import MetadataService

XML = Path(__file__).parent.parent / "data" / "pubmed" / "efetch_retraction.xml"


def _fetch():
    pmid_map = {"9500320": Identifier(pmid="9500320"), "20137807": Identifier(pmid="20137807")}
    results = {}
    PubMedClient("t@example.com")._process_efetch_response(
        xmltodict.parse(XML.read_text(encoding="utf-8")), pmid_map, results)
    return results


def test_a_retracted_paper_carries_its_notices():
    paper = _fetch()["9500320"]
    kinds = [c["kind"] for c in paper.corrections]
    assert kinds.count("retraction") == 2 and kinds.count("expression_of_concern") == 1
    assert kinds.count("comment") == 26
    assert {"kind": "retraction", "pmid": "15016483", "doi": "10.1016/S0140-6736(04)15715-2"} in paper.corrections
    # The expression of concern's citation has a doi too; one without gives None.
    assert {"kind": "comment", "pmid": "9525390", "doi": None} in paper.corrections
    assert paper.retracted and not paper.retraction_notice


def test_a_retraction_notice_is_marked_and_is_not_retracted():
    notice = _fetch()["20137807"]
    assert notice.retraction_notice and notice.corrections == [] and not notice.retracted


def test_the_metadata_artifact_round_trips_corrections():
    paper = _fetch()["9500320"]
    again = ArticleMetadata.from_dict(paper.to_dict())
    assert again.corrections == paper.corrections and again.retracted


def test_a_record_cached_before_corrections_were_read_derives_them_from_the_raw_xml():
    data = _fetch()["9500320"].to_dict()
    del data["corrections"], data["retraction_notice"]
    assert ArticleMetadata.from_dict(data).retracted
    assert not ArticleMetadata.from_dict({"title": "t"}).retracted


def test_merging_keeps_the_notices_from_either_side():
    a = ArticleMetadata(title="t", corrections=[{"kind": "erratum", "pmid": "1", "doi": None}])
    b = ArticleMetadata(title="t", corrections=[{"kind": "erratum", "pmid": "1", "doi": None},
                                                {"kind": "retraction", "pmid": "2", "doi": None}])
    assert a.merge_from(b).corrections == b.corrections
    assert b.merge_from(a).corrections == b.corrections


def test_pubmed_is_asked_even_when_semantic_scholar_filled_the_record(tmp_path):
    """Only PubMed lists notices, and it was skipped for a filled record."""
    service = MetadataService(Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", ns_pond_root=tmp_path / "p",
        semantic_scholar_api_key="k", pubmed_email="t@example.com"))
    service._openalex_client = None
    service._pubmed_client = object()  # the cached getter is stubbed below
    identifier = Identifier(pmid="9500320")
    filled = ArticleMetadata(title="t", abstract="a", journal="j", publication_year=1998,
                             authors=[SimpleNamespace(name="x")], keywords=["k"], source="semantic_scholar")
    service._get_semantic_scholar_metadata_cached = lambda ids: {identifier.slug: filled}
    service._get_pubmed_metadata_cached = lambda ids: {i.slug: _fetch()["9500320"] for i in ids}
    content = ExtractedContent(identifier=identifier, slug=identifier.slug,
                               source=DownloadSource.PUBGET)
    got = service.enrich_metadata([content])[identifier.slug]
    assert got.retracted and got.title == "t" and got.source == "semantic_scholar"


def test_upload_takes_back_a_retracted_article_and_never_uploads_one_it_lacks(monkeypatch):
    retracted = ArticleMetadata(title="t", corrections=[{"kind": "retraction", "pmid": "2", "doi": None}])
    works = []
    for pmid, base in (("1", "BASE1"), ("2", None)):
        ident = Identifier(pmid=pmid, neurostore=base)
        works.append(Work(ref=SimpleNamespace(id=f"a{pmid}", identifier=ident), source="",
                          fingerprint="f", upstream=None))
    analyses = {w.ref.identifier.slug: {"t": object()} for w in works}
    metadata = {w.ref.identifier.slug: retracted for w in works}
    catalog = SimpleNamespace(exclusions=lambda ids: {}, artifacts=lambda ids, stage: {})
    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    taken = []
    monkeypatch.setattr(stage, "_gather", lambda ctx, w, ex: (analyses, metadata, []))
    monkeypatch.setattr(stage, "_retract", lambda retract: taken.extend(retract) or iter(()))
    out = list(stage.execute(SimpleNamespace(catalog=catalog), works))
    assert [(w.article_id, b) for w, b in taken] == [("a1", "BASE1")]
    assert [(o.article_id, o.summary["reason"]) for o in out] == [("a2", "retracted in PubMed")]
    assert analyses == {}
