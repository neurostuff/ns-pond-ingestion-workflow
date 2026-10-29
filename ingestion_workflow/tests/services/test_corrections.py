"""Retraction and erratum notices read from PubMed's CommentsCorrections.

tests/data/pubmed/efetch_retraction.xml is a real efetch response for PMID
9500320 (Wakefield 1998, retracted) and PMID 20137807 (its retraction notice).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import json

import pytest
import requests
import xmltodict
from ingestion_workflow.catalog import Artifact, Status
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.notices import corrections_from_pubmed
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.pipeline.stages.notices import NoticesStage
from ingestion_workflow.pipeline.stages.sync import SyncStage

XML = Path(__file__).parent.parent / "data" / "pubmed" / "efetch_retraction.xml"


def _articles():
    articles = xmltodict.parse(XML.read_text(encoding="utf-8"))["PubmedArticleSet"][
        "PubmedArticle"
    ]
    return {a["MedlineCitation"]["PMID"]["#text"]: a for a in articles}


def _article(refs=None, types=("Journal Article",)):
    citation = {
        "PMID": {"#text": "1"},
        "Article": {"PublicationTypeList": {"PublicationType": [{"#text": t} for t in types]}},
    }
    if refs is not None:
        citation["CommentsCorrectionsList"] = {"CommentsCorrections": refs}
    return {"MedlineCitation": citation}


# -- reading CommentsCorrections ---------------------------------------------


def test_a_retracted_paper_carries_its_notices():
    corrections, notice = corrections_from_pubmed(_articles()["9500320"])
    kinds = [c["kind"] for c in corrections]
    assert kinds.count("retraction") == 2 and kinds.count("expression_of_concern") == 1
    assert kinds.count("comment") == 26
    assert {
        "kind": "retraction",
        "pmid": "15016483",
        "doi": "10.1016/S0140-6736(04)15715-2",
        "source": "pubmed",
    } in corrections
    # The expression of concern's citation has a doi too; one without gives None.
    assert {"kind": "comment", "pmid": "9525390", "doi": None, "source": "pubmed"} in corrections
    assert not notice


def test_a_retraction_notice_is_marked_and_lists_nothing():
    assert corrections_from_pubmed(_articles()["20137807"]) == ([], True)


def test_the_publication_type_alone_marks_a_paper_retracted():
    """PubMed types the paper before it links the notice."""
    assert corrections_from_pubmed(_article(types=["Retracted Publication"])) == (
        [{"kind": "retraction", "pmid": None, "doi": None, "source": "pubmed"}],
        False,
    )


def test_the_publication_type_alone_marks_a_retraction_notice():
    assert corrections_from_pubmed(_article(types=["Retraction Notice"])) == ([], True)


def test_a_retraction_of_link_alone_marks_a_retraction_notice():
    assert corrections_from_pubmed(
        _article({"@RefType": "RetractionOf", "PMID": {"#text": "2"}})
    ) == ([], True)


def test_an_erratum_alone_is_not_a_retraction():
    # One CommentsCorrections element is a dict, not a list, in xmltodict's form.
    refs = {
        "@RefType": "ErratumIn",
        "RefSource": "J. 2001;1:2. doi: 10.1000/err1.",
        "PMID": {"#text": "7"},
    }
    assert corrections_from_pubmed(_article(refs)) == (
        [{"kind": "erratum", "pmid": "7", "doi": "10.1000/err1", "source": "pubmed"}],
        False,
    )


def test_an_erratum_and_a_retraction_are_both_kept():
    refs = [
        {"@RefType": "ErratumIn", "PMID": {"#text": "7"}},
        {"@RefType": "RetractionIn", "PMID": {"#text": "8"}},
        {"@RefType": "ErratumFor", "PMID": {"#text": "9"}},
        {"@RefType": "CommentOn", "PMID": {"#text": "10"}},
    ]
    corrections, notice = corrections_from_pubmed(_article(refs, types=["Retracted Publication"]))
    assert [(c["kind"], c["pmid"]) for c in corrections] == [("erratum", "7"), ("retraction", "8")]
    assert not notice


# -- the PubMed request -------------------------------------------------------


def test_notices_are_asked_200_pmids_at_a_time_and_a_failure_raises():
    client = PubMedClient("t@example.com")
    asked = []
    response = {"PubmedArticleSet": {"PubmedArticle": list(_articles().values())}}
    client._request_efetch = lambda pmids: asked.append(len(pmids)) or response
    found = client.get_notices([str(i) for i in range(450)] + ["1"])
    assert asked == [200, 200, 50]
    assert found["9500320"][0] and found["20137807"] == ([], True)

    def fail(pmids):
        raise RuntimeError("503")

    client._request_efetch = fail
    with pytest.raises(RuntimeError):
        client.get_notices(["1"])


def test_an_api_key_lifts_the_throttle_to_ten_a_second():
    assert PubMedClient("t@example.com")._min_interval == pytest.approx(1 / 3)
    assert PubMedClient("t@example.com", api_key="k")._min_interval == pytest.approx(0.1)


# -- the notices stage --------------------------------------------------------


def _ctx(attempts=None):
    from ingestion_workflow.pipeline.stage import Context

    catalog = SimpleNamespace(
        attempt_counts=lambda ids, stage, source: attempts or {},
        blobs=SimpleNamespace(exists=lambda blob: True),
    )
    return Context(Settings(), catalog)


def _ref(pmid="9500320", aid="a1"):
    return SimpleNamespace(id=aid, identifier=Identifier(pmid=pmid))


def test_notices_run_beside_metadata_and_nothing_upstream_reads_them():
    assert (
        STAGE_ORDER.index("metadata") < STAGE_ORDER.index("notices") < STAGE_ORDER.index("upload")
    )
    assert [n for n, t in STAGE_TYPES.items() if t.requires == "notices"] == []


def test_a_notice_lookup_goes_stale_after_the_configured_age():
    stage = NoticesStage(Settings(notices_max_age_days=30))
    fp = stage.fingerprint_for("9500320")
    meta = {"a1": {"": Artifact("a1", "metadata")}}

    def plan(days):
        stamp = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
        done = {"a1": {"": Artifact("a1", "notices", fingerprint=fp, updated_at=stamp)}}
        return stage.plan(_ctx(), [_ref()], done, meta)

    assert plan(29).fresh == 1
    assert [w.article_id for w in plan(31).pending] == ["a1"]
    assert stage.plan(_ctx(), [_ref(pmid=None)], {}, {"a1": meta["a1"]}).blocked == 1


def test_the_stage_records_what_pubmed_said_and_fails_what_it_did_not_return():
    stage = NoticesStage(Settings())
    found = {p: corrections_from_pubmed(a) for p, a in _articles().items()}
    stage._client = SimpleNamespace(get_notices=lambda pmids: found)
    works = [
        Work(ref=_ref(p, f"a{p}"), source="", fingerprint="f", upstream=None)
        for p in ("9500320", "20137807", "5")
    ]
    out = {o.article_id: o for o in stage.execute(_ctx(), works)}
    assert (
        out["a9500320"].summary["retracted"]
        and out["a9500320"].summary["retraction"]["pmid"] == "15016483"
    )
    assert (
        out["a20137807"].summary["retraction_notice"] and not out["a20137807"].summary["retracted"]
    )
    assert out["a5"].status is not Status.OK


def test_every_pubmed_record_of_a_paper_is_asked_for_its_notices():
    """A paper PubMed indexes twice; the retraction is linked to the other record."""
    stage = NoticesStage(Settings())
    found = {p: corrections_from_pubmed(a) for p, a in _articles().items()}
    asked = []

    def get_notices(pmids):
        asked.extend(pmids)
        return {p: found[p] for p in pmids if p in found}

    stage._client = SimpleNamespace(get_notices=get_notices)
    ident = Identifier(pmid="5", alternative_pmids=["9500320"])
    ref = SimpleNamespace(id="a1", identifier=ident)
    [out] = stage.execute(_ctx(), [Work(ref=ref, source="", fingerprint="f", upstream=None)])
    assert asked == ["5", "9500320"]
    assert out.status is Status.OK and out.summary["retraction"]["pmid"] == "15016483"
    meta = {"a1": {"": Artifact("a1", "metadata")}}
    [work] = stage.plan(_ctx(), [ref], {}, meta).pending
    assert work.fingerprint == stage.fingerprint_for("5,9500320") != stage.fingerprint_for("5")


OPENALEX = Path(__file__).parent.parent / "data" / "openalex" / "retractions.json"
RETRACTED_DOI, CLEAN_DOI = "10.1016/S0140-6736(97)11096-0", "10.1038/nature14539"


def _openalex_client(monkeypatch, sent):
    from ingestion_workflow.clients.openalex import OpenAlexClient

    client = OpenAlexClient(email="t@example.com")

    def answer(params, work=""):
        if work:  # a DOI the batch did not return: OpenAlex does not know it either
            raise requests.HTTPError(response=SimpleNamespace(status_code=404))
        sent.append(params)
        return json.loads(OPENALEX.read_text())

    monkeypatch.setattr(client, "_request_openalex", answer)
    return client


def test_openalex_retractions_are_asked_by_doi_in_batches_of_fifty(monkeypatch):
    sent = []
    client = _openalex_client(monkeypatch, sent)
    found = client.get_retractions([RETRACTED_DOI, "https://doi.org/" + CLEAN_DOI.upper()])
    assert found == {RETRACTED_DOI.lower(): True, CLEAN_DOI: False}
    assert sent[0]["filter"] == f"doi:{RETRACTED_DOI.lower()}|{CLEAN_DOI}"
    sent.clear()
    client.get_retractions([f"10.1/x{i}" for i in range(120)])
    assert [len(p["filter"].split("|")) for p in sent] == [50, 50, 20]


def _openalex_stage(retracted, error=None, pubmed=None):
    stage = NoticesStage(Settings())

    def get_retractions(dois):
        if error:
            raise error
        return {d.lower(): r for d, r in retracted.items() if d.lower() in dois}

    stage._openalex = SimpleNamespace(get_retractions=get_retractions)
    stage._client = SimpleNamespace(get_notices=lambda pmids: pubmed or {})
    return stage


def _doi_work(doi, pmid=None, aid="a1"):
    ref = SimpleNamespace(id=aid, identifier=Identifier(pmid=pmid, doi=doi))
    return Work(ref=ref, source="", fingerprint="f", upstream=None)


def test_a_paper_with_only_a_doi_is_planned_and_marked_retracted_by_openalex():
    stage = _openalex_stage({RETRACTED_DOI: True})
    ref = SimpleNamespace(id="a1", identifier=Identifier(doi=RETRACTED_DOI))
    meta = {"a1": {"": Artifact("a1", "metadata")}}
    assert [w.article_id for w in stage.plan(_ctx(), [ref], {}, meta).pending] == ["a1"]
    [out] = stage.execute(_ctx(), [_doi_work(RETRACTED_DOI)])
    assert out.status is Status.OK and out.summary["retracted"]
    assert out.summary["retraction"]["source"] == "openalex"
    assert out.payload["corrections"] == [out.summary["retraction"]]


def test_the_fingerprint_follows_the_doi_as_well_as_the_pmid():
    stage = NoticesStage(Settings())
    assert stage.fingerprint_for(None, "10.1000/a") != stage.fingerprint_for(None, "10.1000/b")
    assert stage.fingerprint_for(None, Identifier(doi="https://doi.org/10.1000/A").doi) == stage.fingerprint_for(None, "10.1000/a")
    assert stage.fingerprint_for("1", None) != stage.fingerprint_for("2", None)


def test_a_retraction_from_either_source_sets_retracted_and_pubmed_is_kept_first():
    pubmed = {p: corrections_from_pubmed(a) for p, a in _articles().items()}
    stage = _openalex_stage({CLEAN_DOI: True, RETRACTED_DOI: False}, pubmed=pubmed)
    # PubMed says nothing is retracted for this PMID, OpenAlex says it is.
    plain = {"5": ([], False)}
    stage._client = SimpleNamespace(get_notices=lambda pmids: plain)
    [out] = stage.execute(_ctx(), [_doi_work(CLEAN_DOI, pmid="5")])
    assert out.summary["retracted"] and out.summary["retraction"]["source"] == "openalex"
    # Both say retracted: one correction, from PubMed.
    stage._client = SimpleNamespace(get_notices=lambda pmids: pubmed)
    [out] = stage.execute(_ctx(), [_doi_work(CLEAN_DOI, pmid="9500320")])
    assert {c["source"] for c in out.payload["corrections"]} == {"pubmed"}


def test_one_source_failing_leaves_every_paper_that_needed_it_unchecked():
    pubmed = {"9500320": corrections_from_pubmed(_articles()["9500320"])}
    stage = _openalex_stage({}, error=RuntimeError("down"), pubmed=pubmed)
    out = {
        o.article_id: o
        for o in stage.execute(
            _ctx(),
            [_doi_work("10.1/x", pmid="9500320", aid="both"), _doi_work("10.1/y", aid="doi-only")],
        )
    }
    assert out["both"].status is Status.FAILED and out["both"].summary["retracted"]
    assert out["doi-only"].status is not Status.OK and "openalex" in out["doi-only"].error


def test_a_doi_in_a_notice_citation_is_found_through_the_shared_helper():
    from ingestion_workflow.utils.doi import find_doi, normalize_doi

    assert find_doi("Lancet. 1997;350:1. doi: 10.1016/S0924-9338(02)00676-4.") == "10.1016/S0924-9338(02)00676-4"
    assert find_doi("Lancet 1997") is None
    assert normalize_doi("https://doi.org/10.1000/x") == normalize_doi("doi:10.1000/x") == "10.1000/x"
    assert normalize_doi("HTTPS://DOI.ORG/10.1000/X") == "10.1000/x"


# -- upload -------------------------------------------------------------------


def _notices(retracted=False, notice=False, status=Status.OK):
    retraction = {"kind": "retraction", "pmid": "8", "doi": None} if retracted else None
    return Artifact(
        "a1",
        "notices",
        status=status,
        summary={"retracted": retracted, "retraction": retraction, "retraction_notice": notice},
    )


class _Upstream:
    fingerprint = "abc"
    summary = {}


def _sync(tmp_path, *notices, evidence):
    """A catalog with one article ready for sync, its notices as given."""
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.pipeline.stage import Context

    cat = Catalog.open(tmp_path / "cat")
    ref = cat.register(Identifier(pmid="1", neurostore="BASE1"), evidence)
    cat.record([Outcome(article_id=ref.id, stage="space", source="", fingerprint="sp",
                        summary={"tables": 1}),
                Outcome(article_id=ref.id, stage="roles", source="", fingerprint="ro")])
    cat.record([Outcome(article_id=ref.id, stage="notices", source="", fingerprint="f", **n)
                for n in notices])
    return cat, ref, Context(Settings(ns_pond_root=tmp_path / "pond"), cat)


def _plan(cat, ref, ctx):
    stage = SyncStage(ctx.settings)
    return stage.plan(ctx, [ref], cat.artifacts([ref.id], "sync"), cat.artifacts([ref.id], "space"))


def _corrections(cat, ref, ctx):
    """The corrections sync gives the parsed paper, through the bibliography it writes."""
    from ingestion_workflow.models.metadata import ArticleMetadata
    from ingestion_workflow.pipeline.stages.sync import _parse_inputs
    from ingestion_workflow.services.paper_parse import _bibliography
    from ingestion_workflow.tests.services.test_paper_parse import COPY

    found = {s: cat.artifacts([ref.id], s).get(ref.id, {}) for s in (
        "extract", "metadata", "space", "triage", "analyses", "prose", "resolve", "passages",
        "notices", "links", "linkprobe", "references")}
    inputs = _parse_inputs(ctx, Work(ref=ref, source="", fingerprint="f"), "BASE1", COPY, {},
                           found, "pubget")
    bibliography = _bibliography(ArticleMetadata(title="T", source="pubmed", license=COPY.license),
                                 "T", (), inputs.corrections)
    return (None if bibliography.corrections is None
            else [c.model_dump(mode="json") for c in bibliography.corrections])


def _answered(retracted=False, notice=False, corrections=None, status=Status.OK):
    retraction = {"kind": "retraction", "pmid": "8", "doi": None} if retracted else None
    found = corrections if corrections is not None else ([retraction] if retracted else [])
    return dict(status=status,
                payload={"corrections": found, "retraction_notice": notice},
                summary={"retracted": retracted, "retraction": retraction,
                         "retraction_notice": notice},
                **({"error": "pubmed: down"} if status is not Status.OK else {}))


def test_only_answered_notices_move_the_sync_fingerprint():
    stage = SyncStage(Settings())
    plain = stage.fingerprint_for(_Upstream(), "BASE1")
    assert stage.fingerprint_for(_Upstream(), "BASE1", None,
                                 _notices(retracted=True, status=Status.FAILED)) == plain
    answered = stage.fingerprint_for(_Upstream(), "BASE1", None, _notices(retracted=True))
    assert answered != plain


def test_a_retracted_paper_s_parsed_paper_carries_its_retraction(tmp_path, evidence):
    """Neurostore's ingester marks the base study retracted from these corrections."""
    cat, ref, ctx = _sync(tmp_path, _answered(retracted=True), evidence=evidence)
    with cat:
        assert len(_plan(cat, ref, ctx).pending) == 1
        assert _corrections(cat, ref, ctx) == [{"kind": "retraction", "pmid": "8", "doi": None}]


def test_a_retraction_notice_is_never_synced_and_is_recorded_with_its_reason(tmp_path, evidence):
    from ingestion_workflow.pipeline.stages.sync import RETRACTION_NOTICE

    cat, ref, ctx = _sync(tmp_path, _answered(notice=True), evidence=evidence)
    with cat:
        plan = _plan(cat, ref, ctx)
        (skipped,) = SyncStage(ctx.settings).execute(ctx, plan.pending)
        assert (skipped.status, skipped.error) == (Status.SKIPPED, RETRACTION_NOTICE)
        assert skipped.summary == {"reason": RETRACTION_NOTICE, "base_study_id": "BASE1"}
        cat.record([skipped])
        assert not (ctx.settings.ns_pond_root / "BASE1").exists()
        plan = _plan(cat, ref, ctx)
        assert (plan.pending, plan.reasons) == ([], {RETRACTION_NOTICE: 1})


def test_a_withdrawn_retraction_is_cleared(tmp_path, evidence):
    """Notices that answered with no retraction send an empty list, which the ingester reads
    as "not retracted" and clears an earlier mark by."""
    cat, ref, ctx = _sync(tmp_path, _answered(), evidence=evidence)
    with cat:
        assert _corrections(cat, ref, ctx) == []


def test_a_failed_refresh_keeps_the_last_ok_notices_and_sync_still_sees_it(tmp_path, evidence):
    from ingestion_workflow.catalog import Outcome

    cat, ref, ctx = _sync(tmp_path, _answered(retracted=True), evidence=evidence)
    with cat:
        before = _plan(cat, ref, ctx).pending[0].fingerprint
        cat.record([Outcome.failure(ref.id, "notices", "", "pubmed: down", fingerprint="f")])
        kept = cat.artifacts([ref.id], "notices")[ref.id][""]
        assert kept.status is Status.OK and kept.summary["retracted"]
        assert _plan(cat, ref, ctx).pending[0].fingerprint == before
        assert _corrections(cat, ref, ctx) == [{"kind": "retraction", "pmid": "8", "doi": None}]
        # The failure is still an attempt, so the scheduler's backoff sees it.
        assert cat.attempt_counts([ref.id], "notices", "")[ref.id][0] == 1


def test_never_answered_notices_send_no_corrections(tmp_path, evidence):
    """Unknown, not "no notices": the ingester leaves the retraction mark as it is."""
    cat, ref, ctx = _sync(tmp_path, _answered(status=Status.FAILED), evidence=evidence)
    with cat:
        assert len(_plan(cat, ref, ctx).pending) == 1
        assert _corrections(cat, ref, ctx) is None


def test_a_paper_one_source_failed_for_is_not_fresh_and_is_planned_again():
    pubmed = {"9500320": ([], False)}
    stage = _openalex_stage({}, error=RuntimeError("503"), pubmed=pubmed)
    [out] = stage.execute(_ctx(), [_doi_work(RETRACTED_DOI, pmid="9500320")])
    assert out.status is Status.FAILED and "openalex" in out.error
    assert out.payload["pmid"] == "9500320"
    ref = SimpleNamespace(id="a1", identifier=Identifier(pmid="9500320", doi=RETRACTED_DOI))
    meta = {"a1": {"": Artifact("a1", "metadata")}}
    stored = {"a1": {"": Artifact("a1", "notices", status=out.status, fingerprint="f",
                                   updated_at=datetime.now(timezone.utc).isoformat())}}
    assert [w.article_id for w in stage.plan(_ctx(), [ref], stored, meta).pending] == ["a1"]


def test_a_failed_batch_keeps_the_batches_before_it(monkeypatch):
    from ingestion_workflow.models.notices import PartialAnswer

    sent = []
    client = _openalex_client(monkeypatch, sent)
    real = client._request_openalex

    def third_fails(params, work=""):
        if len(sent) == 2:
            raise RuntimeError("503")
        return real(params, work)

    monkeypatch.setattr(client, "_request_openalex", third_fails)
    with pytest.raises(PartialAnswer) as caught:
        client.get_retractions([f"10.1/x{i}" for i in range(120)] + [RETRACTED_DOI])
    assert len(sent) == 2 and caught.value.found.get(RETRACTED_DOI.lower()) in (True, None)

    stage = NoticesStage(Settings())
    stage._openalex = SimpleNamespace(
        get_retractions=lambda dois: (_ for _ in ()).throw(PartialAnswer("503", {RETRACTED_DOI.lower(): True}))
    )
    stage._client = SimpleNamespace(get_notices=lambda pmids: {})
    first, second = stage.execute(_ctx(), [_doi_work(RETRACTED_DOI), _doi_work("10.1/lost", aid="a2")])
    assert first.status is Status.OK and first.summary["retracted"]
    assert second.status is Status.FAILED


def test_a_record_openalex_could_not_read_fails_only_its_own_paper():
    """The DOIs OpenAlex does not know are not its failures."""
    from ingestion_workflow.models.notices import PartialAnswer

    unknown, odd = "10.1234/unknown", "10.1234/odd"
    stage = _openalex_stage({}, error=PartialAnswer(
        "OpenAlex: 1 record(s) could not be read", {}, unreadable={odd: "TypeError: list"}))
    outs = {o.article_id: o for o in stage.execute(
        _ctx(), [_doi_work(unknown, aid="known"), _doi_work(odd, aid="odd")])}

    assert outs["odd"].status is Status.FAILED
    assert outs["odd"].error == "openalex: TypeError: list"
    assert outs["known"].error == "no source returned a record"


def _quota_spent():
    """OpenAlex's answer once its daily quota is spent, as the client raises it."""
    from ingestion_workflow.models.notices import PartialAnswer

    response = requests.Response()
    response.status_code = 429
    cause = requests.HTTPError("OpenAlex request failed (429)", response=response)
    return PartialAnswer(str(cause), {}, (cause,))


def test_a_source_busy_or_down_leaves_the_papers_it_missed_pending():
    """No outcome, so no try is spent and what the paper held is kept; the report says how
    many. A paper the other source answered for in full is still recorded."""
    pubmed = {"9500320": corrections_from_pubmed(_articles()["9500320"])}
    stage = _openalex_stage({}, error=_quota_spent(), pubmed=pubmed)
    out = {o.article_id: o for o in stage.execute(_ctx(), [
        _doi_work("10.1/x", pmid="9500320", aid="both"), _doi_work("10.1/y", aid="doi-only"),
        _doi_work(None, pmid="9500320", aid="pmid-only")])}

    assert set(out) == {"pmid-only"} and out["pmid-only"].status is Status.OK
    assert stage.take_notes() == ["2 article(s) left pending: a source was busy or down"]
    assert stage.take_notes() == []
