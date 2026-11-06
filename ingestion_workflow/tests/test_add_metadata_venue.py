"""The one-off venue for metadata stored under version 6, on a real catalog.

The paper is Movement Disorders' 10.1002/mds.26295 (PMID 26095114). Its OpenAlex work, as
`test_venues` records it, has Radboud Repository as its primary location and the journal,
S163027424, second.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stage import metadata_digests
from ingestion_workflow.pipeline.stages.metadata import MetadataStage, record_digests

spec = importlib.util.spec_from_file_location(
    "add_metadata_venue",
    Path(__file__).resolve().parents[2] / "scripts" / "add_metadata_venue.py")
script = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = script
spec.loader.exec_module(script)

ARTICLE, PMID, DOI = "mds26295", "26095114", "10.1002/mds.26295"
WORK = {"id": "https://openalex.org/W1", "primary_location": {"source": {
            "id": "https://openalex.org/S4306400009", "type": "repository",
            "display_name": "Radboud Repository (Radboud University)"}},
        "locations": [{"source": {"id": "https://openalex.org/S163027424", "type": "journal",
                                  "display_name": "Movement Disorders"}}]}


@pytest.fixture()
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", log_to_file=False, show_progress=False)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        for kind, value in (("pmid", PMID), ("doi", DOI)):
            catalog._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                                  (kind, value, ARTICLE))
        yield settings, catalog


def _stored_under_v6(settings, catalog):
    """The metadata as version 6 stored it: no venue, made from the cached OpenAlex work."""
    stage = MetadataStage(settings)
    ref = catalog.ref(ARTICLE)
    cached = stage.service._cache_file("openalex", ref.identifier.slug)
    cached.parent.mkdir(parents=True)
    cached.write_text(json.dumps(ArticleMetadata(title="T", raw_metadata={"openalex": WORK})
                                 .to_dict()))
    payload = ArticleMetadata(title="Gait in Parkinson's disease", abstract="We walked.",
                              journal="Movement disorders").to_dict()
    del payload["venue"]
    records = record_digests(settings, [cached])
    catalog.record([Outcome(
        ARTICLE, "metadata", "", Status.OK,
        stage.fingerprint_for((PMID, [DOI]), {}, {}, records, version=6), payload=payload,
        summary={"title": payload["title"], "digests": metadata_digests(payload),
                 "records": records})])
    return payload, cached


def test_the_venue_is_added_and_nothing_else_moves(env):
    settings, catalog = env
    payload, _ = _stored_under_v6(settings, catalog)
    before = catalog.artifact(ARTICLE, "metadata")
    stage = MetadataStage(settings)
    ctx = Context(settings, catalog)
    assert stage.inputs_of(ctx, catalog.ref(ARTICLE), {}, {}, before) != before.fingerprint

    out = io.StringIO()
    script.run(settings, out=out)
    assert "venue doi\t1" in out.getvalue()
    assert catalog.artifact(ARTICLE, "metadata") == before  # a dry run writes nothing

    script.run(settings, apply=True, backup=settings.catalog_root / "backup.sqlite", out=out)
    after = catalog.artifact(ARTICLE, "metadata")
    stored = catalog.payload(after)
    assert stored.pop("venue") == {"openalex_id": "S163027424", "match": "doi"}
    assert stored == payload
    assert after.summary["digests"] == before.summary["digests"]  # what triage, prose read
    assert after.updated_at == before.updated_at
    assert stage.inputs_of(ctx, catalog.ref(ARTICLE), {}, {}, after) == after.fingerprint

    again = io.StringIO()
    assert script.run(settings, out=again).rewrites == []
    assert "has venue\t1" in again.getvalue()


def test_metadata_stale_already_is_left_for_the_stage(env):
    settings, catalog = env
    _, cached = _stored_under_v6(settings, catalog)
    cached.write_text(json.dumps(ArticleMetadata(title="T").to_dict()))  # OpenAlex answered anew
    before = catalog.artifact(ARTICLE, "metadata")

    found = script.run(settings, apply=True, backup=settings.catalog_root / "backup.sqlite",
                       out=io.StringIO())
    assert found.rewrites == [] and found.counts == {"stale": 1}
    assert catalog.artifact(ARTICLE, "metadata") == before
