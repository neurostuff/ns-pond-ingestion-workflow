from __future__ import annotations

import pytest
import requests
from lxml import etree

from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pubget_extractor import KEEPS_SUPERSCRIPTS, article_text
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import OPT_IN_STAGES, STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint
from ingestion_workflow.pipeline.stages.references import ReferencesStage
from ingestion_workflow.pipeline.stages.reflist import ReflistStage
from ingestion_workflow.tests.services.test_citations import JATS
from ingestion_workflow.tests.services.test_reference_lists import MESSAGE, _OpenAlex


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k")
    article = tmp_path / "pmcid_1" / "article.xml"
    article.parent.mkdir()
    article.write_text(JATS)
    text = tmp_path / "article.txt"
    text.write_text(article_text(etree.parse(str(article)), article.parent), encoding="utf-8")
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, article, text


def _download(ref, path, source):
    from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType

    kind = FileType.PDF if source == "pdf" else FileType.XML
    return DownloadResult(identifier=ref.identifier, source=DownloadSource(source), success=True, files=[
        DownloadedFile(file_path=path, file_type=kind, content_type="application/xml",
                       source=DownloadSource(source))]).to_dict()


def _record(catalog, ref, path, text, source="pubget", tables=0):
    catalog.record([Outcome(article_id=ref.id, stage="download", source=source, fingerprint="dl-1",
                            payload=_download(ref, path, source), summary={})])
    download = catalog.artifact(ref.id, "download", source)
    catalog.record([Outcome(article_id=ref.id, stage="extract", source=source,
                            fingerprint=extraction_fingerprint(source, download),
                            payload={"full_text_path": str(text)}, summary={"tables": tables})])


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def test_references_follow_extract_and_run_only_when_asked(tmp_path):
    assert STAGE_ORDER.index("extract") < STAGE_ORDER.index("references")
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")
    assert "references" in OPT_IN_STAGES
    assert "references" not in {s.name for s in build(None, settings)}
    assert [s.name for s in build(["references"], settings)] == ["references"]


@pytest.mark.parametrize("source", ["pubget", "pmc", "europepmc"])
def test_an_extraction_gets_its_references_and_citations(env, source, evidence):
    """pmc and europepmc are written in pubget's layout and read the same way."""
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="1"), evidence)
    _record(catalog, ref, article, text, source=source)
    ctx = Context(settings, catalog)

    _, (outcome,) = _run(ReferencesStage(settings), ctx, catalog, ref)

    assert outcome.status is Status.OK and outcome.source == source
    payload = outcome.payload
    assert len(payload["references"]) == 5 and payload["references"][0]["doi"] == "10.1523/x.2001"
    assert outcome.summary["citations"] == 3
    assert outcome.summary["markers_not_in_text"] == (0 if KEEPS_SUPERSCRIPTS else 1)
    stored = text.read_text(encoding="utf-8")
    first = payload["citations"][0]["text_span"]
    assert stored[first["start_char"] : first["end_char"]] == "Smith et al., 2001"

    again, _ = _run(ReferencesStage(settings), ctx, catalog, ref)
    assert again.fresh == 1 and not again.pending


def test_only_the_article_s_text_gets_references(env, tmp_path, evidence):
    """Two extractions: the one triage judges is the article's text, and the only one read."""
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="3"), evidence)
    _record(catalog, ref, article, text, source="pubget")
    _record(catalog, ref, article, text, source="pmc", tables=2)

    plan, outcomes = _run(ReferencesStage(settings), Context(settings, catalog), catalog, ref)

    assert [o.source for o in outcomes] == ["pmc"]


def test_a_pdf_marks_no_citations_and_is_not_read(env, evidence):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="2"), evidence)
    _record(catalog, ref, article, text, source="pdf")

    plan, outcomes = _run(ReferencesStage(settings), Context(settings, catalog), catalog, ref)

    assert plan.blocked == 1 and not outcomes


# -- reflist: the Crossref list, and references falling back on it ----------------


class _Crossref:
    """Every DOI's record is `message`; `records` names the DOIs a list query returns,
    when not every one; a DOI in `failing` fails its list query."""

    def __init__(self, message, records=None, failing=()):
        self.message, self.records, self.failing = message, records, set(failing)
        self.lists, self.singly = [], []

    def works(self, dois, select):
        self.lists.append(list(dois))
        if self.failing & set(dois):
            raise requests.HTTPError("503 Server Error", response=_Response(503))
        returned = dois if self.records is None else [d for d in dois if d in self.records]
        return {d.lower(): self.message for d in returned} if self.message is not None else {}

    def work(self, doi):
        self.singly.append(doi)
        return self.message


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code


class _CountingOpenAlex(_OpenAlex):
    def __init__(self, error=None):
        self.asked, self.error = [], error

    def get_cited_works(self, dois):
        self.asked.append(list(dois))
        if self.error is not None:
            raise self.error
        return super().get_cited_works(dois)


def _reflist(settings, message, crossref=None, openalex=None):
    stage = ReflistStage(settings)
    stage._crossref = crossref or _Crossref(message)
    stage._openalex = openalex or _OpenAlex()
    return stage


def _run_all(stage, ctx, catalog, refs):
    ids = [ref.id for ref in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, stage.name), catalog.artifacts(ids, stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def _papers(env, evidence, n):
    settings, catalog, article, text = env
    refs = [catalog.register(Identifier(pmid=str(100 + i), doi=f"10.1016/j.test.2020.{i}"), evidence)
            for i in range(n)]
    for ref in refs:
        _record(catalog, ref, article, text)
    return refs


def test_a_batch_of_papers_is_one_list_query_and_one_pool_of_cited_works(env, evidence):
    settings, catalog, *_ = env
    refs = _papers(env, evidence, 3)
    crossref, openalex = _Crossref(MESSAGE), _CountingOpenAlex()

    _, outcomes = _run_all(_reflist(settings, MESSAGE, crossref, openalex),
                           Context(settings, catalog), catalog, refs)

    assert len(crossref.lists) == 1 and sorted(crossref.lists[0]) == sorted(r.identifier.doi for r in refs)
    assert not crossref.singly
    assert openalex.asked == [["10.1523/x.2001"]]  # three papers cite it; asked once
    for outcome in outcomes:
        assert outcome.status is Status.OK and outcome.summary["named_by_openalex"] == 1
        assert outcome.payload["references"][0]["authors"] == ["Smith", "Jones"]
        assert "errors" not in outcome.summary


def test_a_doi_the_list_query_does_not_return_is_asked_singly(env, evidence):
    settings, catalog, *_ = env
    first, second = _papers(env, evidence, 2)
    crossref = _Crossref(MESSAGE, records={first.identifier.doi})

    _, outcomes = _run_all(_reflist(settings, MESSAGE, crossref), Context(settings, catalog), catalog,
                           [first, second])

    assert crossref.singly == [second.identifier.doi]
    assert [o.payload["found"] for o in outcomes] == [True, True]


def test_a_failed_list_query_fails_only_its_papers(env, evidence):
    settings, catalog, *_ = env
    refs = _papers(env, evidence, 2)
    crossref = _Crossref(MESSAGE, failing={refs[0].identifier.doi})

    _, outcomes = _run_all(_reflist(settings, MESSAGE, crossref), Context(settings, catalog), catalog, refs)

    # both DOIs fit one query, so both fail with it, and neither is asked singly
    assert [o.status for o in outcomes] == [Status.FAILED, Status.FAILED] and not crossref.singly
    assert outcomes[0].error.startswith("crossref: HTTPError")


def test_openalex_busy_keeps_the_lists_and_asks_again_once_due(env, evidence):
    from datetime import timedelta

    settings, catalog, *_ = env
    refs = _papers(env, evidence, 2)
    busy = requests.HTTPError("OpenAlex request failed (429)", response=_Response(429))
    ctx = Context(settings, catalog)

    _, outcomes = _run_all(_reflist(settings, MESSAGE, openalex=_CountingOpenAlex(busy)), ctx, catalog, refs)

    for outcome in outcomes:
        assert outcome.status is Status.OK and outcome.summary["references"] == 3
        assert outcome.summary["named_by_openalex"] == 0
        assert outcome.summary["errors"]["names"].startswith("openalex: HTTPError")
        assert outcome.summary["transient"] == ["names"]
    soon, _ = _run_all(_reflist(settings, MESSAGE), ctx, catalog, refs)
    assert soon.fresh == 2
    ctx.now = ctx.now + ctx.retry_after + timedelta(seconds=1)
    later, _ = _run_all(_reflist(settings, MESSAGE), ctx, catalog, refs)
    assert len(later.pending) == 2


def test_a_paper_with_nothing_to_name_notes_no_openalex_error(env, evidence):
    settings, catalog, *_ = env
    (ref,) = _papers(env, evidence, 1)
    busy = requests.HTTPError("OpenAlex request failed (429)", response=_Response(429))
    bare = {"reference": [{"key": "b1", "author": "Lee", "year": "2002"}]}

    _, (outcome,) = _run_all(_reflist(settings, bare, openalex=_CountingOpenAlex(busy)),
                             Context(settings, catalog), catalog, [ref])

    assert outcome.status is Status.OK and "errors" not in outcome.summary


def test_reflist_keeps_crossref_s_list_named_by_openalex(env, evidence):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="3", doi="10.1016/j.test.2020.1"), evidence)
    _record(catalog, ref, article, text)

    _, (outcome,) = _run(_reflist(settings, MESSAGE), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK and outcome.summary["references"] == 3
    assert outcome.payload["references"][0]["authors"] == ["Smith", "Jones"]


def test_a_paper_crossref_does_not_know_is_not_a_failure(env, evidence):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="4", doi="10.1/unknown"), evidence)
    _record(catalog, ref, article, text)

    _, (outcome,) = _run(_reflist(settings, None), Context(settings, catalog), catalog, ref)

    assert outcome.status is Status.OK and outcome.payload["found"] is False


def test_reflist_needs_a_doi(env, evidence):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="5"), evidence)
    _record(catalog, ref, article, text)

    plan, _ = _run(_reflist(settings, MESSAGE), Context(settings, catalog), catalog, ref)

    assert plan.blocked == 1


def test_a_pdf_is_matched_against_crossref_s_list(env, tmp_path, evidence):
    settings, catalog, article, _ = env
    ref = catalog.register(Identifier(pmid="6", doi="10.1016/j.test.2020.1"), evidence)
    pdf_text = tmp_path / "pdf.txt"
    pdf_text.write_text("Attention shapes perception (Smith et al., 2001). Gusnard (2001) agreed.\n",
                        encoding="utf-8")
    _record(catalog, ref, article, pdf_text, source="pdf")
    ctx = Context(settings, catalog)
    _run(_reflist(settings, MESSAGE), ctx, catalog, ref)

    _, (outcome,) = _run(ReferencesStage(settings), ctx, catalog, ref)

    assert outcome.status is Status.OK and outcome.payload["list_provider"] == "crossref"
    assert [(c["text_span"]["text"], c["references"]) for c in outcome.payload["citations"]] == [
        ("Smith et al., 2001", ["cr1"]), ("Gusnard (2001", ["cr2"])]


def test_a_new_crossref_list_makes_the_references_stale_and_fills_their_dois(env, evidence):
    settings, catalog, article, text = env
    ref = catalog.register(Identifier(pmid="7", doi="10.1016/j.test.2020.1"), evidence)
    _record(catalog, ref, article, text)
    ctx = Context(settings, catalog)
    _run(ReferencesStage(settings), ctx, catalog, ref)
    # B2 (Lee B. Filters. 2002) has no DOI in the XML; Crossref's list has it
    message = {"reference": [{"key": "B2", "DOI": "10.1/filters", "author": "Lee", "year": "2002"}]}
    _run(_reflist(settings, message), ctx, catalog, ref)

    plan, (outcome,) = _run(ReferencesStage(settings), ctx, catalog, ref)

    assert len(plan.pending) == 1 and outcome.payload["list_provider"] == "source"
    b2 = next(r for r in outcome.payload["references"] if r["id"] == "B2")
    assert b2["doi"] == "10.1/filters" and b2["id_providers"] == {"doi": "crossref"}
    assert outcome.summary["ids_from_crossref"] == 1


def test_fingerprints_follow_the_canonical_doi_and_tell_apart_non_ascii_case(env):
    from ingestion_workflow.pipeline.stages.notices import NoticesStage

    settings = env[0]
    for stage, fingerprint_for in ((ReflistStage(settings), lambda s, d: s.fingerprint_for(d)),
                                   (NoticesStage(settings), lambda s, d: s.fingerprint_for("1", d))):
        doi = lambda spelling: Identifier(doi=spelling).doi
        assert fingerprint_for(stage, doi("10.1000/ABC")) == fingerprint_for(stage, doi("doi:10.1000/abc"))
        assert fingerprint_for(stage, doi("10.26321/Á")) != fingerprint_for(stage, doi("10.26321/á"))


def test_a_paper_known_only_by_its_preprint_is_still_planned_by_its_preprint_s_doi(env, evidence):
    """8,058 catalog articles have no version-of-record DOI; their reference lists and
    retraction notices are asked for by the preprint's."""
    from ingestion_workflow.pipeline.stages.notices import NoticesStage

    settings, catalog, article, text = env
    ref = catalog.register(Identifier(doi="10.1101/2023.02.15.528738"), evidence)
    assert ref.identifier.doi is None
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pdf", fingerprint="d",
                            payload={}, summary={}),
                    Outcome(article_id=ref.id, stage="metadata", fingerprint="m", payload={},
                            summary={})])
    ctx = Context(settings, catalog)
    for stage in (ReflistStage(settings), NoticesStage(settings)):
        plan = stage.plan(ctx, [ref], {}, catalog.artifacts([ref.id], stage.requires))
        assert (plan.blocked, len(plan.pending)) == (0, 1), stage.name
