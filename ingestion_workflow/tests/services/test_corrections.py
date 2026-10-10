"""Retraction and erratum notices read from PubMed's CommentsCorrections.

tests/data/pubmed/efetch_retraction.xml is a real efetch response for PMID
9500320 (Wakefield 1998, retracted) and PMID 20137807 (its retraction notice).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import xmltodict
from ingestion_workflow.catalog import Artifact, Status
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.notices import corrections_from_pubmed
from ingestion_workflow.models.upload import UploadOutcome
from ingestion_workflow.pipeline.plan import Work
from ingestion_workflow.pipeline.stages import STAGE_ORDER, STAGE_TYPES
from ingestion_workflow.pipeline.stages.notices import NoticesStage
from ingestion_workflow.pipeline.stages.upload import UploadStage
from sqlalchemy import text
from test_exclusions import _uploaded

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
    } in corrections
    # The expression of concern's citation has a doi too; one without gives None.
    assert {"kind": "comment", "pmid": "9525390", "doi": None} in corrections
    assert not notice


def test_a_retraction_notice_is_marked_and_lists_nothing():
    assert corrections_from_pubmed(_articles()["20137807"]) == ([], True)


def test_the_publication_type_alone_marks_a_paper_retracted():
    """PubMed types the paper before it links the notice."""
    assert corrections_from_pubmed(_article(types=["Retracted Publication"])) == (
        [{"kind": "retraction", "pmid": None, "doi": None}],
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
        "RefSource": "J. 2001;1:2. doi: 10.1/err.",
        "PMID": {"#text": "7"},
    }
    assert corrections_from_pubmed(_article(refs)) == (
        [{"kind": "erratum", "pmid": "7", "doi": "10.1/err"}],
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


def test_only_a_retraction_moves_the_upload_fingerprint():
    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    plain = stage.fingerprint_for(_Upstream())
    assert stage.fingerprint_for(_Upstream(), None, _notices()) == plain
    assert (
        stage.fingerprint_for(_Upstream(), None, _notices(retracted=True, status=Status.FAILED))
        == plain
    )
    retracted = stage.fingerprint_for(_Upstream(), None, _notices(retracted=True))
    assert retracted != plain
    assert stage.fingerprint_for(_Upstream(), None, _notices(notice=True)) not in (
        plain,
        retracted,
    )


def _execute(monkeypatch, notices, base_study_id="BASE1", empty=False, prior=None):
    from ingestion_workflow.services import db
    from ingestion_workflow.services import upload as svc

    work = Work(
        ref=SimpleNamespace(id="a1", identifier=Identifier(pmid="1")),
        source="",
        fingerprint="f",
        upstream=None,
    )
    catalog = SimpleNamespace(
        exclusions=lambda ids: {},
        add_aliases=lambda learned: None,
        artifacts=lambda ids, stage: {"a1": {"": notices}}
        if stage == "notices"
        else ({"a1": {"": prior}} if prior and stage == "upload" else {}),
    )
    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    calls = {"marked": [], "retract": [], "uploaded": [], "cleared": []}
    monkeypatch.setattr(
        stage,
        "_gather",
        lambda ctx, ws, ex: (
            ({}, {}, list(ws))
            if empty
            else ({w.ref.identifier.slug: {"t": object()} for w in ws}, {}, [])
        ),
    )

    class Tunnel:
        def __init__(self, settings):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(db, "SSHTunnel", Tunnel)
    monkeypatch.setattr(db, "SessionFactory", lambda settings, tunnel=None: None)
    monkeypatch.setattr(svc.UploadService, "__init__", lambda self, s, f: None)
    monkeypatch.setattr(
        svc.UploadService,
        "prepare_work_items",
        lambda self, a, m, **kw: calls["uploaded"].extend(a) or list(a),
    )
    monkeypatch.setattr(
        svc.UploadService,
        "run",
        lambda self, items, **kw: [
            UploadOutcome(slug=s, base_study_id=base_study_id, study_id="S1", success=True)
            for s in items
        ],
    )
    monkeypatch.setattr(
        svc.UploadService, "mark_retracted", lambda self, t, clear=(): calls["marked"].append(t) or calls["cleared"].extend(clear) or 1,
    )
    monkeypatch.setattr(
        svc.UploadService, "retract", lambda self, t: calls["retract"].append(t) or []
    )
    return list(stage.execute(SimpleNamespace(catalog=catalog), [work])), calls


def test_a_retracted_paper_is_uploaded_kept_and_marked(monkeypatch):
    [out], calls = _execute(monkeypatch, _notices(retracted=True))
    assert out.status is Status.OK and out.summary["retraction_marked"] is True
    assert "retracted" not in out.summary  # sync would take it out of the corpus
    assert calls["marked"] == [{"BASE1": {"kind": "retraction", "pmid": "8", "doi": None}}]
    assert calls["uploaded"] and calls["retract"] == []


def test_a_retraction_notice_is_never_uploaded(monkeypatch):
    [out], calls = _execute(monkeypatch, _notices(notice=True))
    assert out.status is Status.SKIPPED and out.summary == {"reason": "retraction notice"}
    assert calls["uploaded"] == [] and calls["marked"] == []


def test_a_paper_with_no_retraction_is_uploaded_unmarked(monkeypatch):
    [out], calls = _execute(monkeypatch, _notices())
    assert (
        out.status is Status.OK
        and "retraction_marked" not in out.summary
        and calls["marked"] == []
    )


def test_an_upload_neurostore_could_not_mark_is_planned_again():
    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    spaced = Artifact("a1", "space", fingerprint="abc")
    fp = stage.fingerprint_for(spaced, None, _notices(retracted=True))
    catalog = SimpleNamespace(
        attempt_counts=lambda *a: {},
        exclusions=lambda ids: {},
        artifacts=lambda ids, s: {"a1": {"": _notices(retracted=True)}},
        blobs=SimpleNamespace(exists=lambda blob: True),
    )
    from ingestion_workflow.pipeline.stage import Context

    ctx = Context(Settings(), catalog)
    for marked, fresh in ((True, 1), (False, 0)):
        done = {
            "a1": {
                "": Artifact("a1", "upload", fingerprint=fp, summary={"retraction_marked": marked})
            }
        }
        assert stage.plan(ctx, [_ref(aid="a1")], done, {"a1": {"": spaced}}).fresh == fresh


# -- neurostore ---------------------------------------------------------------


def test_marking_skips_a_database_without_the_m7_columns(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    assert service.mark_retracted({uploaded.base_study_id: {"kind": "retraction"}}) is None


def test_marking_sets_the_flag_and_keeps_the_study(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE base_studies ADD COLUMN is_retracted BOOLEAN"))
        conn.execute(text("ALTER TABLE base_studies ADD COLUMN retraction_notice JSON"))
    notice = {"kind": "retraction", "pmid": "8", "doi": None}
    assert service.mark_retracted({uploaded.base_study_id: notice}) == 1
    with engine.connect() as conn:
        flag, stored = conn.execute(
            text("SELECT is_retracted, retraction_notice FROM base_studies")
        ).one()
        assert flag and '"pmid": "8"' in stored
        assert conn.execute(text("SELECT count(*) FROM studies")).scalar() == 1
        assert conn.execute(text("SELECT count(*) FROM points")).scalar() == 2


def test_a_failed_refresh_keeps_the_last_ok_notices_and_upload_still_sees_it(tmp_path, monkeypatch):
    from ingestion_workflow.catalog import Catalog, Outcome
    from ingestion_workflow.pipeline.stage import Context

    stage = UploadStage(Settings(upload_source="nuextract-v21"))
    with Catalog.open(tmp_path / "cat") as cat:
        [retracted, notice_paper] = cat.register_many(
            [Identifier(pmid="1"), Identifier(pmid="2")]
        )

        def ok(ref, **summary):
            return Outcome(article_id=ref.id, stage="notices", source="", status=Status.OK,
                           fingerprint="f", summary=summary, payload={})

        cat.record([
            ok(retracted, retracted=True, retraction={"kind": "retraction", "pmid": "8", "doi": None},
               retraction_notice=False),
            ok(notice_paper, retracted=False, retraction=None, retraction_notice=True),
        ])
        before = cat.artifacts([retracted.id], "notices")[retracted.id][""]
        fp_before = stage.fingerprint_for(_Upstream(), None, before)
        cat.record([Outcome.failure(r.id, "notices", "", "pubmed: down", fingerprint="f")
                    for r in (retracted, notice_paper)])

        kept = cat.artifacts([retracted.id, notice_paper.id], "notices")
        assert kept[retracted.id][""].status is Status.OK
        assert kept[retracted.id][""].summary["retracted"]
        assert stage.fingerprint_for(_Upstream(), None, kept[retracted.id][""]) == fp_before
        # The failure is still an attempt, so the scheduler's backoff sees it.
        assert cat.attempt_counts([retracted.id], "notices", "")[retracted.id][0] == 1

        # Upload still skips the notice paper.
        [out], calls = _execute(monkeypatch, kept[notice_paper.id][""])
        assert out.status is Status.SKIPPED and calls["uploaded"] == []

        # Never answered: unknown, recorded as a failure, uploaded as if no notices.
        [fresh] = cat.register_many([Identifier(pmid="3")])
        cat.record([Outcome.failure(fresh.id, "notices", "", "pubmed: down", fingerprint="f")])
        unknown = cat.artifacts([fresh.id], "notices")[fresh.id][""]
        assert unknown.status is Status.FAILED
        assert stage.fingerprint_for(_Upstream(), None, unknown) == stage.fingerprint_for(_Upstream())


def test_a_retracted_paper_with_no_analyses_is_still_marked(monkeypatch):
    prior = Artifact("a1", "upload", status=Status.OK, summary={"base_study_id": "OLD1"})
    [out], calls = _execute(monkeypatch, _notices(retracted=True), empty=True, prior=prior)
    assert out.status is Status.SKIPPED and out.summary["reason"] == "no analyses to upload"
    assert out.summary["base_study_id"] == "OLD1" and out.summary["retraction_marked"] is True
    assert calls["marked"] == [{"OLD1": {"kind": "retraction", "pmid": "8", "doi": None}}]
    assert calls["uploaded"] == []


def test_a_withdrawn_retraction_is_cleared(monkeypatch):
    prior = Artifact(
        "a1", "upload", status=Status.OK,
        summary={"base_study_id": "OLD1", "retraction_marked": True},
    )
    [out], calls = _execute(monkeypatch, _notices(), prior=prior)
    assert out.status is Status.OK and "retraction_marked" not in out.summary
    assert calls["cleared"] == ["OLD1"] and calls["uploaded"]
    # Never-marked papers have nothing to clear.
    _, calls = _execute(monkeypatch, _notices())
    assert calls["cleared"] == []


def test_clearing_unsets_the_flag_and_keeps_the_study(tmp_path):
    service, engine, uploaded = _uploaded(tmp_path)
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE base_studies ADD COLUMN is_retracted BOOLEAN"))
        conn.execute(text("ALTER TABLE base_studies ADD COLUMN retraction_notice JSON"))
    service.mark_retracted({uploaded.base_study_id: {"kind": "retraction"}})
    assert service.mark_retracted({}, [uploaded.base_study_id]) == 1
    with engine.connect() as conn:
        flag, stored = conn.execute(
            text("SELECT is_retracted, retraction_notice FROM base_studies")
        ).one()
        assert not flag and stored is None
        assert conn.execute(text("SELECT count(*) FROM studies")).scalar() == 1
