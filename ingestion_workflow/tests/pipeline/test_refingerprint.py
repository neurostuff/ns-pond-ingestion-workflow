"""`ingest repair` stores the fingerprints artifacts lack, and carries what was made
from them, so the work they hold is not done again.

The catalog here has the shape a migration leaves: downloads and metadata with no
fingerprint, and every stage after them made from those, so chained on the empty one.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.cli.main import _render_refingerprint
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.refingerprint import refingerprint
from ingestion_workflow.pipeline.stages import (
    AnalysesStage,
    DownloadStage,
    ExtractStage,
    MetadataStage,
    PassagesStage,
    ProseStage,
    ResolveStage,
    RolesStage,
    SpaceStage,
    TriageStage,
)
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint, file_sha256
from ingestion_workflow.pipeline.stage import metadata_digests

TEXT = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = −22, y = −4, z = −18; t = 4.1). The reverse contrast revealed no clusters.\n"
)


class _Reader:
    """Stands in for the prose model: every coordinate a passage holds, in one analysis."""

    def read(self, passage, *, title="", abstract=""):
        points = [{"x": h.x, "y": h.y, "z": h.z, "statistic": "T", "value": 4.1,
                   "cluster_size": None} for h in passage.hits]
        return {"space": None,
                "analyses": [{"name": "patients > controls", "measure": None, "points": points}]}


class _Results:
    """Stands in for the role classifier: every set is a result."""

    source = "results@1"

    def predict(self, texts):
        from ingestion_workflow.services.set_roles import COORDINATE_ROLES, Prediction

        return [Prediction("result", None, {r: float(r == "result") for r in COORDINATE_ROLES})
                for _ in texts]


@pytest.fixture()
def env(tmp_path, monkeypatch, evidence):
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a fitted gate")
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", prose_model="nu-prose-v2", llm_api_key="x",
                        coordinate_gate_path=gate, download_sources=["pubget"])
    prose = ProseStage(settings)
    prose.client = lambda: _Reader()
    roles = RolesStage(settings)
    monkeypatch.setattr(roles, "classifier", lambda origin: _Results())
    monkeypatch.setattr(roles, "model_state", lambda origin: (f"fake-{origin}@1", None))
    stages = [DownloadStage(settings), ExtractStage(settings), PassagesStage(settings),
              MetadataStage(settings), TriageStage(settings), AnalysesStage(settings), prose,
              ResolveStage(settings), roles, SpaceStage(settings)]
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        yield settings, catalog, stages, tmp_path


def _download(catalog, ref, tmp_path):
    """A download as the migration imported it: no fingerprint."""
    path = tmp_path / f"{ref.id}.xml"
    path.write_text("<article/>")
    file = DownloadedFile(file_path=path, file_type=FileType.XML,
                          content_type="application/xml", source=DownloadSource.PUBGET)
    payload = DownloadResult(identifier=ref.identifier, source=DownloadSource.PUBGET,
                             success=True, files=[file]).to_dict()
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            payload=payload)])
    return catalog.artifact(ref.id, "download", "pubget")


def _extraction(catalog, ref, tmp_path, fingerprint):
    text = tmp_path / f"{ref.id}.txt"
    text.write_text(TEXT, encoding="utf-8")
    catalog.record([Outcome(
        article_id=ref.id, stage="extract", source="pubget", fingerprint=fingerprint,
        payload={"full_text_path": str(text), "tables": [], "figure_captions": [],
                 "slug": ref.identifier.slug, "source": "pubget",
                 "identifier": ref.identifier.to_dict(), "extracted_at": "2026-10-10T00:00:00"},
        summary={"tables": 0, "has_text": True, "captions_sha256": captions_digest(()),
                 "text_sha256": file_sha256(text)})])


def _ran(stage, ctx, catalog, refs, payload=None, summary=None):
    """What a run of `stage` left: its outcome for every work, or `payload` and `summary`
    in place of a model's answer."""
    ids = [r.id for r in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, stage.requires) if stage.requires else {})
    if payload is None:
        catalog.record(list(stage.execute(ctx, plan.pending)))
    else:
        catalog.record([Outcome(article_id=w.article_id, stage=stage.name, source=w.source,
                                fingerprint=w.fingerprint, payload=payload(w.article_id),
                                summary=summary)
                        for w in plan.pending])


def _table():
    return {"t1": {"slug": "a::t1", "coordinate_space": "MNI", "identifier": None, "analyses": [{
        "name": "patients > controls", "table_caption": "", "table_footer": "",
        "coordinates": [{"x": -22, "y": -4, "z": -18, "space": "MNI"}]}]}}


@pytest.fixture()
def migrated(env):
    """`tables` and `prose_only` made all the way to space from a migrated download and
    metadata; `lost` whose download's blob is gone; `stale`, extracted by an older extractor."""
    settings, catalog, stages, tmp_path = env
    ctx = Context(settings, catalog)
    tables, prose_only, lost, stale = (catalog.register(Identifier(pmid=str(n), pmcid=f"PMC{n}"))
                                       for n in range(1, 5))
    for ref in (tables, prose_only):
        download = _download(catalog, ref, tmp_path)
        _extraction(catalog, ref, tmp_path, extraction_fingerprint("pubget", download))
        catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                                payload={"title": "T", "abstract": "A"},
                                summary={"digests": metadata_digests({"title": "T",
                                                                      "abstract": "A"})})])
    made = [tables, prose_only]
    by = {s.name: s for s in stages}
    _ran(by["triage"], ctx, catalog, made, lambda _: {"source": "pubget", "tables": []},
         {"source": "pubget", "tables": 0, "passed": 0, "passed_tables": []})
    _ran(by["analyses"], ctx, catalog, made, lambda a: _table() if a == tables.id else {})
    for name in ("passages", "prose", "resolve", "roles", "space"):
        _ran(by[name], ctx, catalog, made)

    gone = _download(catalog, lost, tmp_path)
    catalog.blobs._path(gone.blob).unlink()
    _download(catalog, stale, tmp_path)
    _extraction(catalog, stale, tmp_path, "an older extractor's")
    return settings, catalog, stages, (tables, prose_only, lost, stale)


def _pending(settings, catalog, stages, refs):
    """Every (stage, article) a run would do, by plan."""
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    return {(stage.name, w.article_id)
            for stage in stages
            for w in stage.plan(ctx, refs, catalog.artifacts(ids, stage.name),
                                catalog.artifacts(ids, stage.requires) if stage.requires else {}
                                ).pending}


def test_the_made_chain_is_fresh_but_for_what_has_no_fingerprint(migrated):
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    pending = _pending(settings, catalog, stages, [tables, prose_only])
    assert pending == {(stage, ref.id) for stage in ("download", "metadata")
                       for ref in (tables, prose_only)}


def test_storing_the_fingerprints_leaves_only_what_cannot_be_reused_pending(migrated):
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    refs = [tables, prose_only, lost, stale]
    extracted = catalog.artifact(stale.id, "extract", "pubget").fingerprint

    found = refingerprint(Context(settings, catalog), stages, refs, catalog.empty_fingerprints())
    assert catalog.store_fingerprints(list(found.rewrites.values())) == len(found.rewrites)

    pending = _pending(settings, catalog, stages, refs)
    assert not {p for p in pending if p[1] in (tables.id, prose_only.id)}
    assert ("download", lost.id) in pending
    assert ("extract", stale.id) in pending
    assert catalog.artifact(stale.id, "extract", "pubget").fingerprint == extracted
    assert catalog.artifact(lost.id, "download", "pubget").fingerprint == ""


def test_the_counts_say_which_fingerprints_were_filled_carried_or_left(migrated):
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    found = refingerprint(Context(settings, catalog), stages, [tables, prose_only, lost, stale],
                          catalog.empty_fingerprints())
    counts = {stage: (n.filled, n.carried, n.empty_rerun, n.empty_unplanned, n.lost)
              for stage, n in found.stages.items()}
    assert counts["download"] == (3, 0, 1, 0, 0)
    assert counts["metadata"] == (2, 0, 0, 0, 0)
    for stage in ("extract", "passages", "triage", "analyses", "prose", "resolve", "roles",
                  "space"):
        assert counts[stage] == (0, 2, 0, 0, 0), stage


def test_the_fingerprints_resolve_copied_follow_the_artifacts_they_name(migrated):
    """Roles reads a resolve and a prose only when the fingerprints they copied are the
    current ones; the tables-only resolve names the analyses its upload follows."""
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    found = refingerprint(Context(settings, catalog), stages, [tables, prose_only],
                          catalog.empty_fingerprints())
    catalog.store_fingerprints(list(found.rewrites.values()))

    for ref in (tables, prose_only):
        prose, passages, resolved = (catalog.artifact(ref.id, s)
                                     for s in ("prose", "passages", "resolve"))
        assert resolved.summary["prose_read"] == prose.fingerprint
        assert prose.summary["passages_read"] == passages.fingerprint
    analysed = catalog.artifact(tables.id, "analyses").fingerprint
    assert catalog.artifact(tables.id, "resolve").summary["basis"] == analysed
    assert catalog.artifact(prose_only.id, "resolve").summary["basis"] == ""


def test_what_a_stage_without_its_model_cannot_be_asked_about_is_counted(migrated):
    """Roles blocks while its models are not served, so it asks about nothing, and the
    roles made from the carried resolves would go stale."""
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    unserved = [RolesStage(settings) if s.name == "roles" else s for s in stages]
    found = refingerprint(Context(settings, catalog), unserved, [tables, prose_only],
                          catalog.empty_fingerprints())
    assert found.stages["resolve"].carried == 2
    assert found.stages["roles"].unasked == 2
    assert any("role model" in reason for reason in found.plan.stages["roles"].reasons)
    assert found.rerun() == {"roles": 2}
    assert ("roles", prose_only.id, "") not in found.rewrites


def test_a_fingerprint_written_meanwhile_is_left_alone(migrated):
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    found = refingerprint(Context(settings, catalog), stages, [tables],
                          catalog.empty_fingerprints())
    old = catalog.artifact(tables.id, "download", "pubget")
    catalog.record([Outcome(article_id=tables.id, stage="download", source="pubget",
                            fingerprint="a run's", payload={"files": []})])

    stored = catalog.store_fingerprints(list(found.rewrites.values()))

    assert stored == len(found.rewrites) - 1
    assert catalog.artifact(tables.id, "download", "pubget").fingerprint == "a run's"
    assert old.fingerprint == ""


def test_an_artifact_no_plan_asks_about_keeps_no_fingerprint(env):
    """A second download is never read while the first source's is fresh."""
    settings, catalog, stages, tmp_path = env
    settings.download_sources = ["pubget", "ace"]
    ref = catalog.register(Identifier(pmid="9", pmcid="PMC9"))
    fresh = DownloadStage(settings).fingerprint_for(DownloadSource.PUBGET)
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint=fresh, payload={"files": []}),
                    Outcome(article_id=ref.id, stage="download", source="ace",
                            payload={"files": ["ace"]})])

    found = refingerprint(Context(settings, catalog), [DownloadStage(settings)], [ref],
                          catalog.empty_fingerprints())

    assert found.rewrites == {}
    n = found.stages["download"]
    assert (n.filled, n.empty_rerun, n.empty_unplanned) == (0, 0, 1)
    assert catalog.artifact(ref.id, "download", "ace").fingerprint == ""


def test_the_extraction_standing_in_for_one_set_aside_is_carried(env):
    """A set-aside extraction lets the next source's stand. With no fingerprint it was
    fresh, so the one standing in was read, and it is carried with its download.

    The pubget download is stale, so the plan goes on to the ACE one and fills it."""
    settings, catalog, stages, tmp_path = env
    settings.download_sources = ["pubget", "ace"]
    ref = catalog.register(Identifier(pmid="7", pmcid="PMC7"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint="an older download's", payload={"files": ["pubget"]}),
                    Outcome(article_id=ref.id, stage="download", source="ace",
                            payload={"files": ["ace"]})])
    catalog.record([
        Outcome(article_id=ref.id, stage="extract", source="pubget", payload={"tables": []},
                summary={"symbols_as_digits": True}),
        Outcome(article_id=ref.id, stage="extract", source="ace", payload={"tables": []},
                fingerprint=extraction_fingerprint(
                    "ace", catalog.artifact(ref.id, "download", "ace"))),
    ])
    planned = [DownloadStage(settings), ExtractStage(settings)]

    found = refingerprint(Context(settings, catalog), planned, [ref], catalog.empty_fingerprints())
    catalog.store_fingerprints(list(found.rewrites.values()))

    assert found.stages["download"].filled == 1
    assert found.stages["extract"].carried == 1
    assert found.stages["extract"].lost == 0
    assert _pending(settings, catalog, planned, [ref]) == set()


class _Counting(_Reader):
    def __init__(self):
        self.calls = 0

    def read(self, passage, *, title="", abstract=""):
        self.calls += 1
        return super().read(passage, title=title, abstract=abstract)


def test_prose_read_before_the_metadata_had_a_fingerprint_reuses_its_answers(migrated):
    """Prose takes an earlier answer only while what it read of the metadata is unchanged,
    which filling the metadata's fingerprint does not change."""
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    refs = [tables, prose_only]
    found = refingerprint(Context(settings, catalog), stages, refs, catalog.empty_fingerprints())
    assert found.stages["metadata"].filled == 2
    catalog.store_fingerprints(list(found.rewrites.values()))

    prose = ProseStage(settings)
    reader = _Counting()
    prose.client = lambda: reader
    _ran(prose, Context(settings, catalog, refresh=["prose"]), catalog, refs)

    assert reader.calls == 0
    for ref in refs:
        assert catalog.artifact(ref.id, "prose").summary["read_again"] == {}


def test_a_summary_written_meanwhile_under_the_same_fingerprint_is_left_alone(env):
    settings, catalog, stages, tmp_path = env
    ref = catalog.register(Identifier(pmid="8", pmcid="PMC8"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            payload={"files": []})])
    catalog._conn.execute("UPDATE artifacts SET updated_at='2026-01-01T00:00:00+00:00'")
    planned = [DownloadStage(settings)]
    found = refingerprint(Context(settings, catalog), planned, [ref], catalog.empty_fingerprints())
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            payload={"files": []}, summary={"a run's": True})])

    assert catalog.store_fingerprints(list(found.rewrites.values())) == 0
    assert catalog.artifact(ref.id, "download", "pubget").summary == {"a run's": True}


def test_only_ok_artifacts_on_current_articles_count_as_empty(env):
    settings, catalog, stages, tmp_path = env
    first = catalog.register(Identifier(doi="10.1/x"))
    second = catalog.register(Identifier(pmcid="PMC5"))
    survivor = catalog.register(Identifier(doi="10.1/x", pmcid="PMC5")).id
    retired = second.id if survivor == first.id else first.id
    catalog.record([Outcome(article_id=survivor, stage="download", source="pubget",
                            payload={"files": []}),
                    Outcome.failure(survivor, "download", "ace", "gone")])
    catalog._conn.execute(
        "INSERT INTO artifacts(article_id, stage, source, status, fingerprint, updated_at) "
        "VALUES (?, 'download', 'pubget', 'ok', '', '2026-01-01')", (retired,))

    assert catalog.empty_fingerprints() == {"download": 1}


@pytest.mark.parametrize("written", [{"fingerprint": "a run's", "payload": {"files": []}},
                                     {"fingerprint": "", "payload": {"files": ["a run's"]}}])
def test_a_row_written_meanwhile_in_the_same_second_is_left_alone(env, written):
    """`updated_at` holds whole seconds, so a run writing the row in the second it was
    read is told apart by its fingerprint or its blob."""
    settings, catalog, stages, tmp_path = env
    ref = catalog.register(Identifier(pmid="6", pmcid="PMC6"))
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            payload={"files": []})])
    second = catalog.artifact(ref.id, "download", "pubget").updated_at
    found = refingerprint(Context(settings, catalog), [DownloadStage(settings)], [ref],
                          catalog.empty_fingerprints())
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget", **written)])
    catalog._conn.execute("UPDATE artifacts SET updated_at=?", (second,))
    run = catalog.artifact(ref.id, "download", "pubget")

    assert catalog.store_fingerprints(list(found.rewrites.values())) == 0
    assert catalog.artifact(ref.id, "download", "pubget") == run


def test_a_model_stopped_between_the_plans_counts_its_artifacts_as_running_again(
        migrated, monkeypatch):
    """Roles was fresh in the first plan and blocks in the second: its artifacts are
    reused now and would not be, so an apply must not go ahead."""
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    roles = next(s for s in stages if s.name == "roles")
    served = iter([True, False])
    planned = roles.plan

    def plan(*args, **kwargs):
        up = next(served)
        monkeypatch.setattr(roles, "model_state", lambda origin: (
            (f"fake-{origin}@1", None) if up else (None, "the role model is not served")))
        return planned(*args, **kwargs)

    monkeypatch.setattr(roles, "plan", plan)
    found = refingerprint(Context(settings, catalog), stages, [tables, prose_only],
                          catalog.empty_fingerprints())

    assert found.stages["roles"].lost == 2
    assert found.rerun() == {"roles": 2}


def _download_of(catalog, ref, tmp_path, source):
    """A download of `source` as the migration imported it: no fingerprint."""
    path = tmp_path / f"{ref.id}.{source}.xml"
    path.write_text("<article/>")
    file = DownloadedFile(file_path=path, file_type=FileType.XML,
                          content_type="application/xml", source=DownloadSource(source))
    payload = DownloadResult(identifier=ref.identifier, source=DownloadSource(source),
                             success=True, files=[file]).to_dict()
    catalog.record([Outcome(article_id=ref.id, stage="download", source=source,
                            payload=payload)])
    return catalog.artifact(ref.id, "download", source)


def _no_text(**summary):
    return {"payload": {"tables": [], "figure_captions": []},
            "summary": {"tables": 0, "has_text": False, "captions_sha256": captions_digest(()),
                        **summary}}


@pytest.fixture()
def elsevier(env):
    """The env with elsevier first in the download order, as the production catalog has it."""
    settings, catalog, stages, tmp_path = env
    settings.download_sources = ["elsevier", "pubget"]
    stages = [DownloadStage(settings) if s.name == "download" else s for s in stages]
    return settings, catalog, stages, tmp_path


def _held(catalog, tmp_path, number):
    """An article read from elsevier, whose extraction is fresh once the download's
    fingerprint is stored."""
    ref = catalog.register(Identifier(pmid=str(number), pmcid=f"PMC{number}"))
    download = _download_of(catalog, ref, tmp_path, "elsevier")
    catalog.record([Outcome(article_id=ref.id, stage="extract", source="elsevier",
                            fingerprint=extraction_fingerprint("elsevier", download),
                            **_no_text())])
    return ref


def _stale_extraction(catalog, ref, source, **summary):
    catalog.record([Outcome(article_id=ref.id, stage="extract", source=source,
                            fingerprint="5042cb9aebec", **_no_text(**summary))])


def _refingerprint(elsevier, *refs):
    settings, catalog, stages, _ = elsevier
    return refingerprint(Context(settings, catalog), stages, list(refs),
                         catalog.empty_fingerprints())


def test_an_extraction_with_no_download_of_its_source_is_reported_not_a_reason_to_stop(elsevier):
    """Production, 2026-10-20: an ace extraction on an article whose only download is
    elsevier's. No plan asks about it, and no run reads it."""
    _, catalog, _, tmp_path = elsevier
    ref = _held(catalog, tmp_path, 7)
    _stale_extraction(catalog, ref, "ace", download_source="elsevier")
    found = _refingerprint(elsevier, ref)

    assert found.stages["extract"].unreachable == 1
    assert found.stages["extract"].unasked == 0
    assert found.rerun() == {}
    assert [line.split(":")[0].strip() for line in _render_refingerprint(found).splitlines()
            if "unreachable" in line] == ["extract"]


def test_a_pdf_extraction_read_from_a_wiley_download_is_not_unreachable(elsevier):
    """11,444 production pdf extractions are read from a wiley download, which `pdf`
    stands for. Planned with extract alone, as a later stage asks about it otherwise, the
    stale pdf extraction is left unasked, and is still a reason to stop."""
    settings, catalog, stages, tmp_path = elsevier
    ref = _held(catalog, tmp_path, 7)
    _download_of(catalog, ref, tmp_path, "wiley")
    _stale_extraction(catalog, ref, "pdf", download_source="wiley")
    found = refingerprint(Context(settings, catalog),
                          [s for s in stages if s.name in ("download", "extract")], [ref],
                          catalog.empty_fingerprints())

    assert found.stages["extract"].unreachable == 0
    assert found.stages["extract"].unasked == 1
    assert found.rerun() == {"extract": 1}


def test_a_verdict_read_from_an_extraction_that_failed_is_left_as_it_is(elsevier):
    """Production, 2026-10-05: triage verdicts read from an ace extraction that has since
    failed as a page that is not the article. The ace download is OK, metadata is filled
    in, and no plan asks about the verdict, but nothing it was made from is rewritten."""
    _, catalog, _, tmp_path = elsevier
    ref = catalog.register(Identifier(pmid="8", pmcid="PMC8"))
    _download_of(catalog, ref, tmp_path, "ace")
    catalog.record([
        Outcome.failure(ref.id, "extract", "ace", "download failed validation: error_page",
                        fingerprint="62972a9c3dfa9bd64c3363d3250b7562",
                        summary={"bad_page": "error_page"}),
        Outcome(article_id=ref.id, stage="metadata", source="",
                payload={"title": "T", "abstract": "A"},
                summary={"digests": metadata_digests({"title": "T", "abstract": "A"})}),
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="2996adc13528",
                payload={"source": "ace", "tables": []},
                summary={"source": "ace", "tables": 0, "passed": 0, "passed_tables": []}),
    ])
    found = _refingerprint(elsevier, ref)

    assert found.stages["metadata"].filled == 1
    assert found.stages["triage"].failed_read == 1
    assert found.stages["triage"].unasked == 0
    assert found.rerun() == {}
    assert [line.split(":")[0].strip() for line in _render_refingerprint(found).splitlines()
            if "failed" in line] == ["triage"]


def test_passages_read_from_an_extraction_that_is_not_failed_still_stop_the_repair(elsevier):
    """Left stale where the extraction they chain on changes: the passages are read from
    the elsevier extraction, which is OK and whose fingerprint is carried."""
    _, catalog, _, tmp_path = elsevier
    ref = _held(catalog, tmp_path, 7)
    catalog.record([Outcome(article_id=ref.id, stage="passages", source="",
                            fingerprint="2ba7e68e00ce", payload={"source": "elsevier",
                                                                  "passages": []},
                            summary={"source": "elsevier", "passages": 0})])
    found = _refingerprint(elsevier, ref)

    assert found.stages["extract"].carried == 1
    assert found.stages["passages"].unasked == 1
    assert found.rerun() == {"passages": 1}


def _imported_without_text(catalog, tmp_path, **summary):
    """Production, 2026-10-23: an article whose pubget extraction was imported with no text
    file, beside an ace extraction that has it and the passages read from that."""
    ref = catalog.register(Identifier(pmid="7", pmcid="PMC7"))
    _download(catalog, ref, tmp_path)
    text = tmp_path / f"{ref.id}.txt"
    text.write_text(TEXT, encoding="utf-8")
    catalog.record([
        Outcome(article_id=ref.id, stage="extract", source="pubget",
                payload={"full_text_path": None, "tables": [], "figure_captions": []},
                summary={"migrated": True, **summary}),
        Outcome(article_id=ref.id, stage="extract", source="ace", fingerprint="5042cb9aebec",
                payload={"full_text_path": str(text), "tables": [], "figure_captions": []},
                summary={"tables": 7, "has_text": True, "captions_sha256": captions_digest(()),
                         "text_sha256": file_sha256(text)}),
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint="70f0ccca2ae7",
                payload={"source": "ace", "passages": []},
                summary={"source": "ace", "passages": 0}),
    ])
    return ref


def test_an_extraction_imported_without_its_text_is_not_stored_as_current(env):
    """Stored, it would be chosen over the ace extraction that has the text, and passages
    would wait on a text it cannot read. It is left to run again."""
    settings, catalog, stages, tmp_path = env
    ref = _imported_without_text(catalog, tmp_path)
    found = refingerprint(Context(settings, catalog), stages, [ref], catalog.empty_fingerprints())

    assert found.stages["extract"].filled == 0
    assert found.stages["extract"].empty_rerun == 1
    assert "passages" not in found.stages
    assert found.rerun() == {"extract": 1}
    assert ("extract", "pubget", ref.id) not in {(k[1], k[2], k[0]) for k in found.rewrites}


def test_an_extraction_that_says_it_holds_no_text_is_stored(env):
    settings, catalog, stages, tmp_path = env
    ref = _imported_without_text(catalog, tmp_path, has_text=False)
    found = refingerprint(Context(settings, catalog), stages, [ref], catalog.empty_fingerprints())

    assert found.stages["extract"].filled == 1
    assert found.stages["extract"].empty_rerun == 0


def test_an_extraction_imported_with_its_text_is_stored(env):
    """Production, 2026-10-23: an import with no fingerprint whose text file is there is
    the one a run would have made, and is stored."""
    settings, catalog, stages, tmp_path = env
    ref = catalog.register(Identifier(pmid="7", pmcid="PMC7"))
    _download(catalog, ref, tmp_path)
    text = tmp_path / f"{ref.id}.txt"
    text.write_text(TEXT, encoding="utf-8")
    catalog.record([Outcome(
        article_id=ref.id, stage="extract", source="pubget",
        payload={"full_text_path": str(text), "tables": [], "figure_captions": []},
        summary={"migrated": True, "text_sha256": file_sha256(text),
                 "captions_sha256": captions_digest(())})])
    found = refingerprint(Context(settings, catalog), stages, [ref], catalog.empty_fingerprints())

    assert found.stages["extract"].filled == 1
    assert found.stages["extract"].empty_rerun == 0


def test_an_extraction_whose_download_failed_is_not_unreachable(elsevier):
    """A failed download still names the source the extraction was read from, as the stages
    that choose among extractions count it. Planned with extract alone, as a later stage
    asks about it otherwise, the stale extraction still stops the repair."""
    settings, catalog, stages, tmp_path = elsevier
    ref = _held(catalog, tmp_path, 7)
    catalog.record([Outcome.failure(ref.id, "download", "ace", "did not return a payload")])
    _stale_extraction(catalog, ref, "ace")
    found = refingerprint(Context(settings, catalog),
                          [s for s in stages if s.name in ("download", "extract")], [ref],
                          catalog.empty_fingerprints())

    assert found.stages["extract"].unreachable == 0
    assert found.stages["extract"].unasked == 1
    assert found.rerun() == {"extract": 1}


def test_a_stage_that_blocks_on_its_model_still_stops_the_repair(migrated):
    """Roles asks about nothing while its models are not served, and what it made would
    go stale: its artifacts are not read from an extraction, so none is left alone."""
    settings, catalog, stages, (tables, prose_only, lost, stale) = migrated
    unserved = [RolesStage(settings) if s.name == "roles" else s for s in stages]
    found = refingerprint(Context(settings, catalog), unserved, [tables, prose_only],
                          catalog.empty_fingerprints())
    assert found.stages["roles"].unreachable == 0
    assert found.rerun() == {"roles": 2}
