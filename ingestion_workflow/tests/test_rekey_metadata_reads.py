"""The one-off rekey of triage and prose to what they read of the metadata, on a real catalog.

The catalog is made as main made it before the change: triage and prose fingerprinted
the metadata artifact's whole fingerprint, and prose recorded it with each answer.
"""

from __future__ import annotations

import importlib.util
import io
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.refingerprint import refingerprint
from ingestion_workflow.pipeline.stage import NO_DIGESTS, metadata_digests
from ingestion_workflow.pipeline.stages import (
    MetadataStage,
    AnalysesStage,
    PassagesStage,
    ProseStage,
    ResolveStage,
    RolesStage,
    SpaceStage,
    TriageStage,
)
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint, file_sha256

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rekey_metadata_reads.py"
spec = importlib.util.spec_from_file_location("rekey_metadata_reads", SCRIPT)
rekey = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = rekey
spec.loader.exec_module(rekey)

#: What main at 22cee5b gave the article below: triage and prose, from the metadata
#: artifact's fingerprint `me-1`.
MAIN_TRIAGE = "3bd12ce053872e4b350aa89f78e6b3e0"
MAIN_PROSE = "a6fa2dde6c845000e378d74f4270f09a"

TEXT = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = −22, y = −4, z = −18; t = 4.1). The reverse contrast revealed no clusters.\n"
)
TABLE = ("<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>t</th></tr>"
         "<tr><td>Left amygdala</td><td>-22</td><td>-4</td><td>-18</td><td>4.1</td></tr>"
         "</table>")
METADATA = {"title": "Amygdala responses to faces", "abstract": "We scanned 20 patients.",
            "journal": "NeuroImage", "license": None, "authors": [], "keywords": []}
ANALYSES = {"t1": {"slug": "a::t1", "coordinate_space": "MNI", "identifier": None, "analyses": [{
    "name": "patients > controls", "table_caption": "", "table_footer": "",
    "coordinates": [{"x": -22, "y": -4, "z": -18, "space": "MNI"}]}]}}


class _Reader:
    """Stands in for the prose model, counting its calls."""

    def __init__(self):
        self.calls = 0

    def read(self, passage, *, title="", abstract=""):
        self.calls += 1
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
    from nspond_tables.classify.model import Gate, RoutedGate

    gate = tmp_path / "gate.json"
    RoutedGate(candidates=Gate(names=[]), residual=Gate(names=[])).save(gate)
    config = tmp_path / "settings.yaml"
    config.write_text(
        f"data_root: {tmp_path / 'd'}\ncache_root: {tmp_path / 'c'}\n"
        f"catalog_root: {tmp_path / 'k'}\nprose_model: nu-prose-v2\nllm_api_key: x\n"
        f"coordinate_gate_path: {gate}\ndownload_sources: [pubget]\n"
        "log_to_file: false\nshow_progress: false\n", encoding="utf-8")
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", prose_model="nu-prose-v2", llm_api_key="x",
                        coordinate_gate_path=gate, download_sources=["pubget"],
                        log_to_file=False, show_progress=False)
    reader = _Reader()
    prose = ProseStage(settings)
    prose.client = lambda: reader
    roles = RolesStage(settings)
    monkeypatch.setattr(roles, "classifier", lambda origin: _Results())
    monkeypatch.setattr(roles, "model_state", lambda origin: (f"fake-{origin}@1", None))
    stages = [TriageStage(settings), AnalysesStage(settings), PassagesStage(settings), prose,
              ResolveStage(settings), roles, SpaceStage(settings)]
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, stages, reader, config, tmp_path


def _article(catalog, tmp_path, pmid, metadata_fingerprint="me-1"):
    """Downloaded and extracted, with one coordinate table, and its metadata as main wrote
    it: with no digests."""
    ref = catalog.register(Identifier(pmid=pmid))
    text, table = (tmp_path / f"{pmid}{ext}" for ext in (".txt", ".html"))
    xml = tmp_path / pmid / "article.xml"
    xml.parent.mkdir()
    xml.write_text("<article/>")
    text.write_text(TEXT, encoding="utf-8")
    table.write_text(TABLE, encoding="utf-8")
    download = DownloadResult(
        identifier=ref.identifier, source=DownloadSource.PUBGET, success=True,
        files=[DownloadedFile(file_path=xml, file_type=FileType.XML,
                              content_type="application/xml", source=DownloadSource.PUBGET)])
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint="dl-1", payload=download.to_dict(),
                            summary={"copy": copy_of(download).to_dict()})])
    catalog.record([Outcome(
        article_id=ref.id, stage="extract", source="pubget",
        fingerprint=extraction_fingerprint("pubget", catalog.artifact(ref.id, "download",
                                                                      "pubget")),
        payload={"full_text_path": str(text), "figure_captions": [],
                 "tables": [{"table_id": "t1", "raw_content_path": str(table),
                             "caption": "Table 1. Activations", "footer": ""}],
                 "slug": ref.identifier.slug, "source": "pubget",
                 "identifier": ref.identifier.to_dict(), "extracted_at": "2026-10-10T00:00:00"},
        summary={"tables": 1, "has_text": True, "captions_sha256": captions_digest(()),
                 "text_sha256": file_sha256(text)})])
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint=metadata_fingerprint, payload=METADATA)])
    return ref


def _plan(stage, ctx, catalog, ref):
    return stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires) if stage.requires else {})


def _as_main_made_it(settings, catalog, stages, ref):
    """The chain as main made it: triage and prose fingerprinted, and prose recorded with
    each answer, the metadata artifact's fingerprint."""
    ctx = Context(settings, catalog)
    for stage in stages:
        made = rekey.former(stage, rekey._as_stored) if stage.name in rekey.READERS else stage
        works = _plan(made, ctx, catalog, ref).pending
        if stage.name == "analyses":
            outcomes = [Outcome(article_id=w.article_id, stage="analyses", source="",
                                fingerprint=w.fingerprint, payload=ANALYSES,
                                summary={"tables": 1}) for w in works]
        else:
            outcomes = list(made.execute(ctx, works))
        assert [o.status for o in outcomes] == [Status.OK], stage.name
        catalog.record(outcomes)


def _pending(settings, catalog, stages, ref):
    ctx = Context(settings, catalog)
    return {stage.name for stage in stages if _plan(stage, ctx, catalog, ref).pending}


def _later(catalog, ref, stage):
    """Mark `ref`'s `stage` artifact written after everything else."""
    catalog._conn.execute("UPDATE artifacts SET updated_at='2999-01-01T00:00:00+00:00' "
                          "WHERE article_id=? AND stage=?", (ref.id, stage))


@pytest.fixture()
def made(env):
    """`kept`, made as main made it; `stale`, whose metadata changed after it was."""
    settings, catalog, stages, reader, config, tmp_path = env
    kept, stale = _article(catalog, tmp_path, "1"), _article(catalog, tmp_path, "2")
    for ref in (kept, stale):
        _as_main_made_it(settings, catalog, stages, ref)
    catalog.record([Outcome(article_id=stale.id, stage="metadata", source="",
                            fingerprint="me-2", payload=METADATA)])
    return env, kept, stale


def test_the_former_formula_is_the_one_main_used(made):
    (settings, catalog, *_), kept, _ = made
    assert catalog.artifact(kept.id, "triage").fingerprint == MAIN_TRIAGE
    assert catalog.artifact(kept.id, "prose").fingerprint == MAIN_PROSE


def test_until_it_is_rekeyed_what_main_made_waits_rather_than_runs(made):
    """Main's metadata records no digests, so triage and prose block, and nothing is made."""
    (settings, catalog, stages, *_), kept, stale = made
    ctx = Context(settings, catalog)
    assert _pending(settings, catalog, stages, kept) == set()
    for stage in stages:
        if stage.name in rekey.READERS:
            assert _plan(stage, ctx, catalog, kept).reasons == {NO_DIGESTS: 1}, stage.name


def test_the_rekeyed_chain_is_fresh_and_the_stale_one_is_left(made):
    (settings, catalog, stages, *_), kept, stale = made
    found = rekey.rekey(Context(settings, catalog), stages, [kept, stale])

    counts = {stage: (n["rekeyed"], n["carried"], n["stale"]) for stage, n in found.stages.items()}
    assert counts["triage"] == (1, 0, 1)
    assert counts["prose"] == (1, 0, 1)
    assert counts["passages"] == (0, 0, 0)
    for stage in ("analyses", "resolve", "roles", "space"):
        assert counts[stage] == (0, 1, 0), stage
    assert found.stages["passages"]["fresh"] == 2
    assert found.stages["metadata"]["digests"] == 2
    assert found.rerun() == {}
    before = {s.name: catalog.artifact(stale.id, s.name) for s in stages}

    catalog.store_fingerprints(list(found.rewrites.values()))

    for ref in (kept, stale):
        stored = catalog.artifact(ref.id, "metadata")
        assert stored.summary["digests"] == metadata_digests(catalog.payload(stored))
    assert _pending(settings, catalog, stages, kept) == set()
    assert {s.name: catalog.artifact(stale.id, s.name) for s in stages} == before
    assert {"triage", "prose"} <= _pending(settings, catalog, stages, stale)
    assert rekey.rekey(Context(settings, catalog), stages, [kept]).rewrites == {}


def test_a_rekeyed_prose_keeps_its_answers_and_names_what_it_read(made):
    (settings, catalog, stages, reader, *_), kept, _ = made
    found = rekey.rekey(Context(settings, catalog), stages, [kept])
    assert found.stages["prose"]["relabelled"] == 1
    catalog.store_fingerprints(list(found.rewrites.values()))

    prose = next(s for s in stages if s.name == "prose")
    calls = reader.calls
    ctx = Context(settings, catalog, refresh=["prose"])
    works = _plan(prose, ctx, catalog, kept).pending
    (again,) = prose.execute(ctx, works)
    stored = catalog.artifact(kept.id, "prose")

    assert reader.calls == calls
    assert again.summary["read_again"] == {}
    assert again.fingerprint == stored.fingerprint
    assert again.summary["read_fingerprint"] == stored.summary["read_fingerprint"]


def test_what_was_made_before_its_metadata_was_written_again_is_left(made):
    """A metadata written again under the same fingerprint may hold another title: what
    was made before it never read that."""
    (settings, catalog, stages, *_), kept, _ = made
    _later(catalog, kept, "metadata")

    found = rekey.rekey(Context(settings, catalog), stages, [kept])

    assert found.stages["triage"]["metadata_newer"] == 1
    assert found.stages["prose"]["metadata_newer"] == 1
    assert (found.stages["triage"]["rekeyed"], found.stages["prose"]["relabelled"]) == (0, 0)
    assert found.rerun() == {}


def test_answers_read_with_a_metadata_that_since_failed_are_not_relabelled(made):
    (settings, catalog, stages, *_), kept, _ = made
    catalog.record([Outcome.failure(kept.id, "metadata", "", "no provider returned metadata",
                                    fingerprint="me-1")])
    catalog._conn.execute("UPDATE artifacts SET updated_at='2000-01-01T00:00:00+00:00' "
                          "WHERE article_id=? AND stage='metadata'", (kept.id,))

    found = rekey.rekey(Context(settings, catalog), stages, [kept])

    assert found.stages["prose"]["relabelled"] == 0


def test_an_empty_fingerprint_made_from_is_left_for_repair(made):
    (settings, catalog, stages, *_), kept, _ = made
    catalog._conn.execute("UPDATE artifacts SET fingerprint='' WHERE article_id=? "
                          "AND stage='analyses'", (kept.id,))

    found = rekey.rekey(Context(settings, catalog), stages, [kept])

    assert (kept.id, "analyses", "") not in found.rewrites
    assert found.stages["triage"]["rekeyed"] == 1


@pytest.fixture()
def migrated(env):
    """`kept` made as main made it from metadata imported with no fingerprint."""
    settings, catalog, stages, reader, config, tmp_path = env
    kept = _article(catalog, tmp_path, "1", metadata_fingerprint="")
    _as_main_made_it(settings, catalog, stages, kept)
    return env, kept


def _repaired(settings, catalog, stages, refs):
    """`ingest repair` over the metadata stage alone: it fills the metadata's fingerprint."""
    found = refingerprint(Context(settings, catalog), [MetadataStage(settings)], refs,
                          catalog.empty_fingerprints())
    assert {key[1] for key in found.rewrites} == {"metadata"}
    catalog.store_fingerprints(list(found.rewrites.values()))


@pytest.mark.parametrize("repair_first", [False, True], ids=["rekey first", "repair first"])
def test_repair_filling_the_metadata_s_fingerprint_before_or_after_changes_nothing(
        migrated, repair_first):
    (settings, catalog, stages, reader, *_), kept = migrated
    if repair_first:
        _repaired(settings, catalog, stages, [kept])
        assert catalog.artifact(kept.id, "metadata").fingerprint

    found = rekey.rekey(Context(settings, catalog), stages, [kept])
    assert (found.stages["triage"]["rekeyed"], found.stages["prose"]["rekeyed"],
            found.stages["prose"]["relabelled"]) == (1, 1, 1)
    catalog.store_fingerprints(list(found.rewrites.values()))
    if not repair_first:
        _repaired(settings, catalog, stages, [kept])

    assert catalog.artifact(kept.id, "metadata").fingerprint
    assert _pending(settings, catalog, stages, kept) == set()
    prose = next(s for s in stages if s.name == "prose")
    ctx = Context(settings, catalog, refresh=["prose"])
    calls = reader.calls
    (again,) = prose.execute(ctx, _plan(prose, ctx, catalog, kept).pending)
    assert (reader.calls, again.summary["read_again"]) == (calls, {})


def test_a_full_repair_before_the_rekey_refuses(migrated):
    """Triage waits for the digests, so a repair filling its metadata's fingerprint cannot
    ask about it, and would leave it to go stale."""
    (settings, catalog, stages, *_), kept = migrated
    found = refingerprint(Context(settings, catalog), [MetadataStage(settings), *stages], [kept],
                          catalog.empty_fingerprints())
    assert "triage" in found.rerun()


def test_what_a_stage_without_its_model_cannot_be_asked_about_stops_the_apply(made):
    """Roles blocks while its models are not served, so it asks about nothing, and the
    roles made from the rekeyed resolve would go stale."""
    (settings, catalog, stages, *_), kept, _ = made
    unserved = [RolesStage(settings) if s.name == "roles" else s for s in stages]

    found = rekey.rekey(Context(settings, catalog), unserved, [kept])

    assert found.stages["resolve"]["carried"] == 1
    assert found.stages["roles"]["unasked"] == 1
    assert found.rerun() == {"roles": 1}


def _state(settings):
    with closing(sqlite3.connect(settings.catalog_root / "catalog.sqlite")) as conn:
        rows = sorted(conn.execute("SELECT * FROM artifacts"))
    return rows, sorted(p.name for p in (settings.catalog_root / "blobs").rglob("*"))


@pytest.fixture()
def served(monkeypatch):
    """The role models served to every RolesStage `run` builds, while `up` holds."""
    up = {"served": True}
    monkeypatch.setattr(RolesStage, "classifier", lambda self, origin: _Results())
    monkeypatch.setattr(RolesStage, "model_state", lambda self, origin: (
        (f"fake-{origin}@1", None) if up["served"] else (None, "the role model is not served")))
    return up


def test_the_dry_run_writes_nothing(made, served):
    (settings, catalog, stages, reader, config, tmp_path), kept, stale = made
    before = _state(settings)
    out = io.StringIO()
    rekey.run(settings, out=out)

    assert "dry run, nothing written" in out.getvalue()
    assert out.getvalue().splitlines()[0].split("\t") == ["stage", *rekey.COUNTS]
    assert _state(settings) == before


def test_apply_backs_up_then_stores_and_a_second_run_finds_nothing(made, served):
    (settings, catalog, stages, reader, config, tmp_path), kept, stale = made
    backup = tmp_path / "before.sqlite"
    old = catalog.artifact(kept.id, "triage").fingerprint

    found = rekey.run(settings, apply=True, backup=backup, out=io.StringIO())

    with closing(sqlite3.connect(backup)) as copied:
        assert copied.execute("SELECT fingerprint FROM artifacts WHERE article_id=? AND "
                              "stage='triage'", (kept.id,)).fetchone()[0] == old
    assert catalog.artifact(kept.id, "triage").fingerprint == \
        found.rewrites[(kept.id, "triage", "")].new.fingerprint != old
    assert _pending(settings, catalog, stages, kept) == set()
    assert rekey.run(settings, out=io.StringIO()).rewrites == {}


def test_apply_refuses_without_the_role_models_and_writes_nothing(made, served):
    (settings, catalog, stages, reader, config, tmp_path), kept, stale = made
    served["served"] = False
    before, backup = _state(settings), tmp_path / "before.sqlite"

    with pytest.raises(SystemExit, match="refusing to write"):
        rekey.run(settings, apply=True, backup=backup, out=io.StringIO())

    assert _state(settings) == before and not backup.exists()


def test_apply_refuses_when_what_is_fresh_would_not_be(made, served, monkeypatch):
    """The role models stop between the first plan and the last: the roles fresh in the
    first are not in the last."""
    (settings, catalog, stages, reader, config, tmp_path), kept, stale = made
    planned = RolesStage.plan

    def plan(self, *args, **kwargs):
        try:
            return planned(self, *args, **kwargs)
        finally:
            served["served"] = False

    monkeypatch.setattr(RolesStage, "plan", plan)
    before, backup = _state(settings), tmp_path / "before.sqlite"

    with pytest.raises(SystemExit, match="refusing to write: 2 roles"):
        rekey.run(settings, apply=True, backup=backup, out=io.StringIO())

    assert _state(settings) == before and not backup.exists()


def test_apply_needs_a_new_backup_path(made):
    (settings, catalog, stages, reader, config, tmp_path), kept, stale = made
    taken = tmp_path / "taken.sqlite"
    taken.write_text("x")
    for backup in (None, taken):
        with pytest.raises(SystemExit):
            rekey.run(settings, apply=True, backup=backup, out=io.StringIO())
