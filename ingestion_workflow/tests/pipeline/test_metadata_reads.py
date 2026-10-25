"""Triage, analyses and prose go stale on what they read of the metadata, and on nothing else.

The metadata is a real record from the catalog: the metadata stage's payload for
"On rotational invariance in adaptive spatial filtering of fMRI data" (NeuroImage
2006), with its PubMed and Semantic Scholar records.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.models import DownloadedFile, DownloadResult, DownloadSource, FileType
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stage import metadata_digests
from ingestion_workflow.pipeline.stages import (
    AnalysesStage,
    PassagesStage,
    ProseStage,
    TriageStage,
)
from ingestion_workflow.pipeline.stages.extract import extraction_fingerprint, file_sha256

RECORD = json.loads((Path(__file__).resolve().parents[1] / "data" / "metadata"
                     / "rotational_invariance.json").read_text(encoding="utf-8"))
#: Where the record's publication types sit, the one part of its raw records triage reads.
TYPES = ("raw_metadata", "pubmed", "MedlineCitation", "Article", "PublicationTypeList")

TEXT = (
    "# T\n\n## Methods\n\nCoordinates are reported in MNI space.\n\n"
    "## Results\n\nPatients showed greater activation than controls in the left amygdala\n"
    "(x = −22, y = −4, z = −18; t = 4.1). The reverse contrast revealed no clusters.\n"
)
TABLE = ("<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>t</th></tr>"
         "<tr><td>Left amygdala</td><td>-22</td><td>-4</td><td>-18</td><td>4.1</td></tr>"
         "</table>")


class _Reader:
    """Stands in for the prose model, and keeps the title and abstract it was given."""

    def __init__(self):
        self.given = []

    def read(self, passage, *, title="", abstract=""):
        self.given.append((title, abstract))
        return {"space": None, "analyses": [{"name": "patients > controls", "measure": None,
                                             "points": []}]}


class _Parser:
    """Stands in for the table model's service, and keeps the metadata it was given."""

    def __init__(self):
        self.given = []

    def run(self, bundle, *, readings, unread):
        self.given.append(bundle.article_metadata)
        return {}


@pytest.fixture()
def env(tmp_path):
    from nspond_tables.classify.model import Gate, RoutedGate

    gate = tmp_path / "gate.json"
    # a real gate, whose halves weigh no feature, so pass every table
    RoutedGate(candidates=Gate(names=[]), residual=Gate(names=[])).save(gate)
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", prose_model="nu-prose-v2", llm_api_key="x",
                        coordinate_gate_path=gate, download_sources=["pubget"])
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, tmp_path


def _article(catalog, tmp_path, pmid, record, evidence):
    """A downloaded and extracted article, with one coordinate table, and `record`."""
    ref = catalog.register(Identifier(pmid=pmid), evidence)
    xml, text, table = (tmp_path / f"{ref.id}{ext}" for ext in (".xml", ".txt", ".html"))
    xml.write_text("<article/>")
    text.write_text(TEXT, encoding="utf-8")
    table.write_text(TABLE, encoding="utf-8")
    download = DownloadResult(
        identifier=ref.identifier, source=DownloadSource.PUBGET, success=True,
        files=[DownloadedFile(file_path=xml, file_type=FileType.XML,
                              content_type="application/xml", source=DownloadSource.PUBGET)])
    catalog.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            fingerprint="dl-1", payload=download.to_dict())])
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
    _metadata(catalog, ref, record, "me-1")
    return ref


def _metadata(catalog, ref, record, fingerprint):
    """`record`, as the metadata stage writes it."""
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint=fingerprint, payload=record,
                            summary={"digests": metadata_digests(record)})])


def _plan(stage, ctx, catalog, ref):
    return stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))


def _run(stage, ctx, catalog, ref):
    outcomes = list(stage.execute(ctx, _plan(stage, ctx, catalog, ref).pending))
    catalog.record(outcomes)
    return outcomes


def _stages(settings):
    """Triage, analyses, passages and prose, with what reads for each model kept."""
    analyses, prose = AnalysesStage(settings), ProseStage(settings)
    analyses._shared_service, reader = _Parser(), _Reader()
    prose.client = lambda: reader
    return TriageStage(settings), analyses, PassagesStage(settings), prose


def _made(settings, catalog, ref):
    """Run the stages, and return them with every outcome they wrote."""
    ctx = Context(settings, catalog)
    stages = _stages(settings)
    outcomes = [o for stage in stages for o in _run(stage, ctx, catalog, ref)]
    assert [(o.stage, o.status) for o in outcomes] == [
        ("triage", Status.OK), ("analyses", Status.OK), ("passages", Status.OK),
        ("prose", Status.OK)]
    return stages, outcomes


def _stale(settings, catalog, ref):
    ctx = Context(settings, catalog)
    triage, analyses, _, prose = _stages(settings)
    return {stage.name for stage in (triage, analyses, prose)
            if _plan(stage, ctx, catalog, ref).pending}


def _licensed(record):
    """The record as the metadata stage now writes it: licences decided, the rest as it was."""
    lic = {"license_id": "cc-by", "determination": "stated"}
    return {**record, "license": lic, "copy_licenses": {"pubget": lic}, "oa_status": "gold",
            "license_embargo_until": None, "open_access": True,
            "pdf_url": "https://example.org/a.pdf"}


def _at(record, path, value):
    record = copy.deepcopy(record)
    *parents, last = path
    inner = record
    for name in parents:
        inner = inner[name]
    inner[last] = value
    return record


def _everything_else_changed(value, path=()):
    """`value` with every field changed but the ones triage and prose read."""
    if path in (("title",), ("abstract",), TYPES):
        return value
    if isinstance(value, dict):
        return {k: _everything_else_changed(v, path + (k,)) for k, v in value.items()}
    if isinstance(value, list):
        return [_everything_else_changed(v, path) for v in value] + ["added"]
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float)):
        return value + 1
    return f"{value} (changed)"


def test_a_licence_change_leaves_triage_analyses_and_prose_fresh(env, evidence):
    settings, catalog, tmp_path = env
    ref = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    _made(settings, catalog, ref)
    assert _stale(settings, catalog, ref) == set()

    _metadata(catalog, ref, _licensed(RECORD), "me-3")

    assert _stale(settings, catalog, ref) == set()


@pytest.mark.parametrize("path, value, stale", [
    (("title",), "A meta-analysis of adaptive spatial filtering",
     {"triage", "analyses", "prose"}),
    (("abstract",), "Coordinates are in Talairach space.", {"triage", "analyses", "prose"}),
    (TYPES, {"PublicationType": {"#text": "Meta-Analysis", "@UI": "D017418"}}, {"triage"}),
])
def test_a_change_to_what_is_read_makes_its_readers_stale(env, path, value, stale, evidence):
    settings, catalog, tmp_path = env
    ref = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    _made(settings, catalog, ref)

    _metadata(catalog, ref, _at(RECORD, path, value), "me-3")
    assert _stale(settings, catalog, ref) == stale

    triage = TriageStage(settings)
    (judged,) = _run(triage, Context(settings, catalog), catalog, ref)
    assert judged.summary["passed_tables"] == ["t1"]
    # Triage judged again and passed the same table: analyses follows its verdict, not
    # its fingerprint, so only a title or abstract it reads leaves it stale.
    assert _stale(settings, catalog, ref) == stale - {"triage"}


def test_analyses_reads_nothing_but_what_it_fingerprints(env, monkeypatch, evidence):
    """Two articles alike but for every metadata field but the title and abstract, the
    publication types included, so triage marks one a meta-analysis and its payload
    differs: analyses makes the same of both, gives its model the same, and reads no
    part of triage's payload."""
    from ingestion_workflow.catalog.blobs import BlobStore

    settings, catalog, tmp_path = env
    changed = _everything_else_changed(_at(RECORD, TYPES, {
        "PublicationType": {"#text": "Meta-Analysis", "@UI": "D017418"}}))
    one = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    other = _article(catalog, tmp_path, "2", changed, evidence=evidence)
    read, get = [], BlobStore.get
    monkeypatch.setattr(BlobStore, "get", lambda self, digest: read.append(digest)
                        or get(self, digest))

    (_, analysed, _, _), made = _made(settings, catalog, one)
    (_, analysed_other, _, _), made_other = _made(settings, catalog, other)

    by_stage = {o.stage: o for o in made}
    by_stage_other = {o.stage: o for o in made_other}
    assert by_stage["triage"].payload["is_meta_analysis"] is False
    assert by_stage_other["triage"].payload["is_meta_analysis"] is True
    this, that = by_stage["analyses"], by_stage_other["analyses"]
    assert (this.fingerprint, this.payload, this.summary) == (
        that.fingerprint, that.payload, that.summary)
    assert analysed._shared_service.given == analysed_other._shared_service.given
    triaged = {catalog.artifact(ref.id, "triage").blob for ref in (one, other)}
    read.clear()
    ctx = Context(settings, catalog, refresh=["analyses"])
    analyses = _stages(settings)[1]
    for ref in (one, other):
        (again,) = _run(analyses, ctx, catalog, ref)
        assert again.fingerprint == this.fingerprint
    assert read and not triaged & set(read)


def test_the_stages_read_nothing_of_the_metadata_but_what_they_fingerprint(
        env, monkeypatch, evidence):
    """Two articles alike but for every metadata field triage and prose do not read: each
    stage makes the same of both, and gives each model, the table reader and the
    meta-analysis test the same."""
    from nspond_tables import read as reader

    from ingestion_workflow.pipeline.stages import triage

    settings, catalog, tmp_path = env
    given = []
    extract, marks = reader.extract, triage.is_a_meta_analysis
    monkeypatch.setattr(reader, "extract", lambda *a, **k: given.append(("read", a, k))
                        or extract(*a, **k))
    monkeypatch.setattr(triage, "is_a_meta_analysis", lambda *a: given.append(("marks", a))
                        or marks(*a))
    changed = _everything_else_changed(RECORD)
    assert changed["raw_metadata"]["pubmed"] != RECORD["raw_metadata"]["pubmed"]
    one = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    other = _article(catalog, tmp_path, "2", changed, evidence=evidence)

    (_, analysed, _, read), made = _made(settings, catalog, one)
    given_one = given[:]
    given.clear()
    (_, analysed_other, _, read_other), made_other = _made(settings, catalog, other)

    assert {g[0] for g in given_one} == {"marks", "read"}
    assert given == given_one

    for this, that in zip(made, made_other):
        if this.stage == "passages":
            continue  # reads no metadata, and names its article's own text file
        assert (this.fingerprint, this.payload, this.summary) == (
            that.fingerprint, that.payload, that.summary), this.stage
    assert analysed._shared_service.given == analysed_other._shared_service.given
    assert analysed._shared_service.given[0].title == RECORD["title"]
    assert read.client().given == read_other.client().given == [
        (RECORD["title"], RECORD["abstract"])]


def test_the_plans_read_the_digests_not_the_payload(env, monkeypatch, evidence):
    """What each reads is judged from the metadata's row, so planning reads no payload."""
    from ingestion_workflow.catalog.blobs import BlobStore

    settings, catalog, tmp_path = env
    ref = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    _made(settings, catalog, ref)
    monkeypatch.setattr(BlobStore, "get", lambda self, digest: pytest.fail("a payload was read"))

    assert _stale(settings, catalog, ref) == set()


def test_an_ok_metadata_that_records_no_digests_blocks_its_readers(env, evidence):
    from ingestion_workflow.pipeline.stage import NO_DIGESTS

    settings, catalog, tmp_path = env
    ref = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    _made(settings, catalog, ref)
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint="me-1", payload=RECORD)])
    ctx = Context(settings, catalog)
    triage, analyses, _, prose = _stages(settings)

    for stage in (triage, analyses, prose):
        plan = _plan(stage, ctx, catalog, ref)
        assert (plan.pending, plan.fresh, plan.reasons) == ([], 0, {NO_DIGESTS: 1}), stage.name


def test_a_row_missing_the_digest_of_a_field_read_blocks_its_readers(env, evidence):
    """A field added to what stages read is not in the rows written before it."""
    from ingestion_workflow.pipeline.stage import NO_DIGESTS

    settings, catalog, tmp_path = env
    ref = _article(catalog, tmp_path, "1", RECORD, evidence=evidence)
    _made(settings, catalog, ref)
    digests = metadata_digests(RECORD)
    del digests["abstract"]
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="",
                            fingerprint="me-1", payload=RECORD, summary={"digests": digests})])
    ctx = Context(settings, catalog)
    triage, analyses, _, prose = _stages(settings)

    for stage in (triage, analyses, prose):
        assert _plan(stage, ctx, catalog, ref).reasons == {NO_DIGESTS: 1}, stage.name
