"""The one-off rekey of analyses to what it reads, on a real catalog.

The catalog is made as main made it before the change: analyses fingerprinted triage's
whole fingerprint, and triage recorded no `passed_tables`. A copy of it is kept as the
earlier catalog; then the metadata changes and triage runs again, as in production.
"""

from __future__ import annotations

import importlib.util
import io
import json
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
from ingestion_workflow.pipeline.stage import metadata_digests
from ingestion_workflow.pipeline.stages import (
    AnalysesStage,
    PassagesStage,
    ProseStage,
    ResolveStage,
    RolesStage,
    SpaceStage,
    TriageStage,
)
from ingestion_workflow.pipeline.stages.analyses import NO_VERDICT
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint, file_sha256

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rekey_analyses_reads.py"
spec = importlib.util.spec_from_file_location("rekey_analyses_reads", SCRIPT)
rekey = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = rekey
spec.loader.exec_module(rekey)

#: What main at 658a13c gave an analyses artifact made from the triage fingerprint
#: "tr-1", with the default settings.
MAIN_ANALYSES = "97cfabb7958be6cabd2cf28da9d7e890"

TEXT = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = −22, y = −4, z = −18; t = 4.1). The reverse contrast revealed no clusters.\n"
)
TABLE = ("<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>t</th></tr>"
         "<tr><td>Left amygdala</td><td>-22</td><td>-4</td><td>-18</td><td>4.1</td></tr>"
         "</table>")
METADATA = {"title": "Amygdala responses to faces", "abstract": "We scanned 20 patients.",
            "journal": "NeuroImage", "license": None, "authors": [], "keywords": [],
            "raw_metadata": {"pubmed": {"MedlineCitation": {"Article": {}}}}}
TYPES = ("raw_metadata", "pubmed", "MedlineCitation", "Article", "PublicationTypeList")
ANALYSES = {"t1": {"slug": "a::t1", "coordinate_space": "MNI", "identifier": None, "analyses": [{
    "name": "patients > controls", "table_caption": "", "table_footer": "",
    "coordinates": [{"x": -22, "y": -4, "z": -18, "space": "MNI"}]}]}}


class _Reader:
    """Stands in for the prose model."""

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
def served(monkeypatch):
    """The role models served to every RolesStage, while `up` holds."""
    up = {"served": True}
    monkeypatch.setattr(RolesStage, "classifier", lambda self, origin: _Results())
    monkeypatch.setattr(RolesStage, "model_state", lambda self, origin: (
        (f"fake-{origin}@1", None) if up["served"] else (None, "the role model is not served")))
    return up


@pytest.fixture()
def env(tmp_path, served):
    from nspond_tables.classify.model import Gate, RoutedGate

    gate = tmp_path / "gate.json"
    # a real gate, whose halves weigh no feature, so pass every table
    RoutedGate(candidates=Gate(names=[]), residual=Gate(names=[])).save(gate)
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", prose_model="nu-prose-v2", llm_api_key="x",
                        coordinate_gate_path=gate, download_sources=["pubget"],
                        log_to_file=False, show_progress=False)
    prose = ProseStage(settings)
    prose.client = lambda: _Reader()
    stages = [TriageStage(settings), AnalysesStage(settings), PassagesStage(settings), prose,
              ResolveStage(settings), RolesStage(settings), SpaceStage(settings)]
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, stages, tmp_path


def _at(record, path, value):
    import copy

    record = copy.deepcopy(record)
    *parents, last = path
    inner = record
    for name in parents:
        inner = inner[name]
    inner[last] = value
    return record


def _metadata(catalog, ref, record, fingerprint):
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint=fingerprint, payload=record,
                            summary={"digests": metadata_digests(record)})])


def _article(catalog, tmp_path, pmid):
    """Downloaded and extracted, with one coordinate table, and its metadata."""
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
    _metadata(catalog, ref, METADATA, "me-1")
    return ref


def _plan(stage, ctx, catalog, ref):
    return stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires) if stage.requires else {})


def _triaged_as_main(settings, catalog, ref, passes=None):
    """Triage run as main ran it, which recorded no `passed_tables`; `passes` overrides
    the gate's verdict on the table."""
    triage, ctx = TriageStage(settings), Context(settings, catalog, refresh=["triage"])
    (outcome,) = triage.execute(ctx, _plan(triage, ctx, catalog, ref).pending)
    assert outcome.status is Status.OK
    summary = {k: v for k, v in outcome.summary.items() if k != "passed_tables"}
    payload = outcome.payload
    if passes is not None:
        payload = {**payload, "tables": [{**v, "passes": passes} for v in payload["tables"]]}
        summary["passed"] = int(passes)
    catalog.record([Outcome(article_id=ref.id, stage="triage", source="",
                            fingerprint=outcome.fingerprint, payload=payload, summary=summary)])


def _as_main_made_it(settings, catalog, stages, ref):
    """The chain as main made it: analyses fingerprinted triage's fingerprint."""
    _triaged_as_main(settings, catalog, ref)
    ctx = Context(settings, catalog)
    analyses = next(s for s in stages if s.name == "analyses")
    triaged = catalog.artifact(ref.id, "triage")
    catalog.record([Outcome(article_id=ref.id, stage="analyses", source="",
                            fingerprint=rekey.former_fingerprint(analyses, triaged.fingerprint),
                            payload=ANALYSES, summary={"tables": 1, "coordinates": 1})])
    for stage in stages[2:]:
        outcomes = list(stage.execute(ctx, _plan(stage, ctx, catalog, ref).pending))
        assert [o.status for o in outcomes] == [Status.OK], stage.name
        catalog.record(outcomes)


def _pending(settings, catalog, stages, ref):
    ctx = Context(settings, catalog)
    return {stage.name for stage in stages if _plan(stage, ctx, catalog, ref).pending}


#: What changed after the earlier copy was taken, by article.
CHANGES = ("untouched", "types", "abstract", "verdict", "extraction")


@pytest.fixture()
def made(env):
    """One article for each of `CHANGES`, made as main made it and copied to the earlier
    catalog; then their metadata changes and triage runs again, as main runs it."""
    settings, catalog, stages, tmp_path = env
    refs = {name: _article(catalog, tmp_path, str(n)) for n, name in enumerate(CHANGES, 1)}
    for ref in refs.values():
        _as_main_made_it(settings, catalog, stages, ref)
    earlier = catalog.backup(tmp_path / "earlier.sqlite")

    _metadata(catalog, refs["types"], _at(METADATA, TYPES, {"PublicationType": {
        "#text": "Meta-Analysis", "@UI": "D017418"}}), "me-2")
    _metadata(catalog, refs["abstract"], {**METADATA, "abstract": "We scanned 21 patients."},
              "me-2")
    for name in ("types", "abstract"):
        _triaged_as_main(settings, catalog, refs[name])
    # Judged again, the table fails, as it might under a refitted gate.
    _triaged_as_main(settings, catalog, refs["verdict"], passes=False)
    # Extracted again by a newer extractor, and judged again: the same table passes.
    extracted = catalog.artifact(refs["extraction"].id, "extract", "pubget")
    catalog.record([Outcome(article_id=refs["extraction"].id, stage="extract", source="pubget",
                            fingerprint="a newer extractor's", summary=extracted.summary,
                            payload=catalog.payload(extracted))])
    _triaged_as_main(settings, catalog, refs["extraction"])
    return settings, catalog, stages, tmp_path, earlier, refs


def _earlier(path, root):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return Catalog(conn, root)


def test_the_former_formula_is_the_one_main_used(env):
    settings, *_ = env
    assert rekey.former_fingerprint(AnalysesStage(settings), "tr-1") == MAIN_ANALYSES


def test_until_it_is_rekeyed_analyses_waits_for_triage_s_verdict(made):
    settings, catalog, stages, _, _, refs = made
    analyses = next(s for s in stages if s.name == "analyses")
    plan = _plan(analyses, Context(settings, catalog), catalog, refs["untouched"])
    assert (plan.pending, plan.fresh, plan.reasons) == ([], 0, {NO_VERDICT: 1})


def test_a_change_to_the_types_alone_leaves_analyses_fresh_and_one_to_what_it_reads_does_not(
        made):
    settings, catalog, stages, tmp_path, earlier, refs = made
    then = _earlier(earlier, settings.catalog_root)
    found = rekey.rekey(Context(settings, catalog), then, stages, list(refs.values()))

    n = found.stages
    assert (n["analyses"]["rekeyed"], n["analyses"]["rekeyed_stale"]) == (2, 3)
    assert found.changed == {"verdict": 1, "extraction": 1, "title_or_abstract": 1}
    assert n["triage"]["verdicts"] == 5
    for stage in ("resolve", "roles", "space"):
        assert n[stage]["carried"] == 5, stage
    assert found.rerun() == {}
    before = {name: catalog.artifact(refs[name].id, "analyses") for name in CHANGES}

    catalog.store_fingerprints(list(found.rewrites.values()))

    assert _pending(settings, catalog, stages, refs["untouched"]) == set()
    assert _pending(settings, catalog, stages, refs["types"]) == set()
    assert _pending(settings, catalog, stages, refs["abstract"]) == {"analyses", "prose"}
    assert _pending(settings, catalog, stages, refs["verdict"]) == {"analyses"}
    assert _pending(settings, catalog, stages, refs["extraction"]) == {"analyses", "passages"}
    for name in CHANGES:
        stored = catalog.artifact(refs[name].id, "analyses")
        assert (stored.blob, stored.updated_at) == (before[name].blob, before[name].updated_at)
    assert catalog.artifact(refs["verdict"].id, "triage").summary["passed_tables"] == []
    assert rekey.rekey(Context(settings, catalog), then, stages,
                       list(refs.values())).rewrites == {}
    then._conn.close()


def test_what_was_made_from_a_triage_stale_in_the_earlier_catalog_is_left(made):
    """The earlier copy's metadata was written again after its triage, with another
    title: the analysis read the one before, which nothing holds."""
    settings, catalog, stages, tmp_path, earlier, refs = made
    record = {**METADATA, "title": "Another title"}
    with closing(sqlite3.connect(earlier)) as conn:
        conn.execute("UPDATE artifacts SET summary=?, blob=? WHERE article_id=? AND "
                     "stage='metadata'", (json.dumps({"digests": metadata_digests(record)}),
                                          catalog.blobs.put(record), refs["types"].id))
        conn.commit()
    then = _earlier(earlier, settings.catalog_root)
    found = rekey.rekey(Context(settings, catalog), then, stages, list(refs.values()))

    assert found.stages["analyses"]["triage_stale_then"] == 1
    assert (refs["types"].id, "analyses", "") not in found.rewrites
    assert found.stages["analyses"]["rekeyed"] == 1
    assert found.rerun() == {}
    then._conn.close()


def test_an_empty_fingerprint_is_left_for_repair(made):
    settings, catalog, stages, tmp_path, earlier, refs = made
    catalog._conn.execute("UPDATE artifacts SET fingerprint='' WHERE stage='extract'")
    then = _earlier(earlier, settings.catalog_root)
    found = rekey.rekey(Context(settings, catalog), then, stages, list(refs.values()))

    assert not [key for key in found.rewrites if key[1] == "extract"]
    assert found.stages["analyses"]["rekeyed"] == 0
    then._conn.close()


def test_an_analyses_the_former_formula_does_not_match_is_left(made):
    """Made from a triage the earlier catalog does not hold: it was stale already."""
    settings, catalog, stages, tmp_path, earlier, refs = made
    catalog._conn.execute("UPDATE artifacts SET fingerprint='made-from-another' "
                          "WHERE article_id=? AND stage='analyses'", (refs["untouched"].id,))
    then = _earlier(earlier, settings.catalog_root)
    found = rekey.rekey(Context(settings, catalog), then, stages, list(refs.values()))

    assert found.stages["analyses"]["unmatched"] == 1
    assert (refs["untouched"].id, "analyses", "") not in found.rewrites
    then._conn.close()


def _state(settings):
    with closing(sqlite3.connect(settings.catalog_root / "catalog.sqlite")) as conn:
        rows = sorted(conn.execute("SELECT * FROM artifacts"))
    return rows, sorted(p.name for p in (settings.catalog_root / "blobs").rglob("*"))


def test_the_dry_run_writes_nothing(made):
    settings, catalog, stages, tmp_path, earlier, refs = made
    before = _state(settings)
    out = io.StringIO()
    rekey.run(settings, earlier, out=out)

    assert "dry run, nothing written" in out.getvalue()
    assert out.getvalue().splitlines()[0].split("\t") == ["stage", *rekey.COUNTS]
    assert _state(settings) == before


def test_apply_backs_up_then_stores_and_a_second_run_finds_nothing(made):
    settings, catalog, stages, tmp_path, earlier, refs = made
    backup = tmp_path / "before.sqlite"
    old = catalog.artifact(refs["types"].id, "analyses").fingerprint

    found = rekey.run(settings, earlier, apply=True, backup=backup, out=io.StringIO())

    with closing(sqlite3.connect(backup)) as copied:
        assert copied.execute("SELECT fingerprint FROM artifacts WHERE article_id=? AND "
                              "stage='analyses'", (refs["types"].id,)).fetchone()[0] == old
    assert catalog.artifact(refs["types"].id, "analyses").fingerprint == \
        found.rewrites[(refs["types"].id, "analyses", "")].new.fingerprint != old
    assert _pending(settings, catalog, stages, refs["types"]) == set()
    assert rekey.run(settings, earlier, out=io.StringIO()).rewrites == {}


def test_apply_refuses_without_the_role_models_and_writes_nothing(made, served):
    settings, catalog, stages, tmp_path, earlier, refs = made
    served["served"] = False
    before, backup = _state(settings), tmp_path / "before.sqlite"

    with pytest.raises(SystemExit, match="refusing to write"):
        rekey.run(settings, earlier, apply=True, backup=backup, out=io.StringIO())

    assert _state(settings) == before and not backup.exists()


def test_apply_needs_a_new_backup_path_and_the_earlier_catalog(made):
    settings, catalog, stages, tmp_path, earlier, refs = made
    taken = tmp_path / "taken.sqlite"
    taken.write_text("x")
    for backup in (None, taken):
        with pytest.raises(SystemExit):
            rekey.run(settings, earlier, apply=True, backup=backup, out=io.StringIO())
    with pytest.raises(SystemExit, match="no catalog"):
        rekey.run(settings, tmp_path / "missing.sqlite", out=io.StringIO())
