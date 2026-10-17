"""`ingest repair` stores the fingerprints artifacts lack, and carries what was made
from them, so the work they hold is not done again.

The catalog here has the shape a migration leaves: downloads and metadata with no
fingerprint, and every stage after them made from those, so chained on the empty one.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
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
def env(tmp_path, monkeypatch):
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
    with Catalog.open(settings.catalog_root) as catalog:
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


def _ran(stage, ctx, catalog, refs, payload=None):
    """What a run of `stage` left: its outcome for every work, or `payload` in place of
    a model's answer."""
    ids = [r.id for r in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, stage.requires) if stage.requires else {})
    if payload is None:
        catalog.record(list(stage.execute(ctx, plan.pending)))
    else:
        catalog.record([Outcome(article_id=w.article_id, stage=stage.name, source=w.source,
                                fingerprint=w.fingerprint, payload=payload(w.article_id))
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
                                payload={"title": "T", "abstract": "A"})])
    made = [tables, prose_only]
    by = {s.name: s for s in stages}
    _ran(by["triage"], ctx, catalog, made, lambda _: {"source": "pubget", "tables": {}})
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
    assert found.conflicts == 0


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
