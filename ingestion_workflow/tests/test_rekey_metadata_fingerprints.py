"""The one-off rekey of metadata fingerprints to the new formula, on a catalog made as main
made it: metadata fingerprinted with the settings' provider list, and no provider asked."""

from __future__ import annotations

import importlib.util
import io
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Outcome
from ingestion_workflow.pipeline.stage import Context, metadata_fingerprint
from ingestion_workflow.pipeline.stages.metadata import MetadataStage
from ingestion_workflow.pipeline.stages.triage import METADATA_READ
from ingestion_workflow.services.metadata import PROVIDER_CACHE_DIRS
from ingestion_workflow.tests.pipeline.test_metadata_licences import (  # noqa: F401
    TDM_MANUSCRIPT,
    _held,
    env,
)

spec = importlib.util.spec_from_file_location(
    "rekey_metadata_fingerprints",
    Path(__file__).resolve().parents[2] / "scripts" / "rekey_metadata_fingerprints.py")
rekey = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = rekey
spec.loader.exec_module(rekey)

DIGESTS = {"title": "t-1", "abstract": "a-1", "publication_types": "p-1"}


def _as_main_made_it(settings, catalog, ref, fingerprint=None):
    """The article's metadata, fingerprinted with the provider list as the stage did."""
    former = fingerprint or rekey.former_fingerprint(
        settings.metadata_providers, catalog.artifacts([ref.id], "download")[ref.id],
        catalog.artifacts([ref.id], "extract")[ref.id])
    catalog.record([Outcome(article_id=ref.id, stage="metadata", source="", fingerprint=former,
                            payload={"title": "Amygdala"}, summary={"digests": DIGESTS})])
    return former


def _row(settings, ref):
    with closing(sqlite3.connect(Path(settings.catalog_root) / "catalog.sqlite")) as conn:
        return conn.execute("SELECT fingerprint, status, summary, blob, updated_at FROM "
                            "artifacts WHERE article_id=? AND stage='metadata'",
                            (ref.id,)).fetchone()


def test_a_metadata_made_as_main_made_it_is_fresh_under_the_new_formula_with_no_provider_asked(
        env, tmp_path):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    record = (settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS["pubmed"]
              / f"{ref.identifier.slug}.json")
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text('{"title": "Amygdala"}', encoding="utf-8")
    former = _as_main_made_it(settings, catalog, ref)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)
    before = _row(settings, ref)
    read_before = metadata_fingerprint(catalog.artifact(ref.id, "metadata"), METADATA_READ)

    def plan():
        return stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                          catalog.artifacts([ref.id], "extract"))
    assert plan().fresh == 0, "the formula changed, so it is stale until rekeyed"

    dry = rekey.run(settings, out=io.StringIO())
    assert dry.counts["rekeyed"] == 1 and _row(settings, ref) == before

    rekey.run(settings, apply=True, backup=tmp_path / "backup.sqlite", out=io.StringIO())
    after = _row(settings, ref)
    assert after[0] != former
    # only the fingerprint moved: not the status, the summary, the blob or when it was written
    assert after[1:] == before[1:]
    assert plan().fresh == 1
    # what triage and prose read of the metadata, the digests of its summary, is unchanged
    assert metadata_fingerprint(catalog.artifact(ref.id, "metadata"), METADATA_READ) == read_before
    assert stage._service is None, "no provider was constructed, let alone asked"

    again = rekey.run(settings, apply=True, backup=tmp_path / "backup-2.sqlite",
                      out=io.StringIO())
    assert (again.counts["rekeyed"], again.counts["already_new"]) == (0, 1)


def test_a_metadata_the_former_formula_did_not_make_is_left_stale(env, tmp_path):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    _as_main_made_it(settings, catalog, ref, fingerprint="made-from-an-older-download")
    before = _row(settings, ref)

    found = rekey.run(settings, apply=True, backup=tmp_path / "backup.sqlite", out=io.StringIO())

    assert (found.counts["stale"], found.counts["rekeyed"]) == (1, 0)
    assert _row(settings, ref) == before


def test_a_rekeyed_metadata_goes_stale_when_the_article_gains_an_id(env, tmp_path):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    _as_main_made_it(settings, catalog, ref)
    rekey.run(settings, apply=True, backup=tmp_path / "backup.sqlite", out=io.StringIO())
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    catalog.add_aliases([(ref.id, "preprint_doi", "10.1101/2021.01.01.425000")])

    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                      catalog.artifacts([ref.id], "extract"))
    assert (plan.fresh, len(plan.pending)) == (0, 1)


def test_apply_needs_a_backup_path_not_yet_taken(env, tmp_path):  # noqa: F811
    settings, _ = env
    with pytest.raises(SystemExit, match="--apply needs --backup"):
        rekey.run(settings, apply=True, out=io.StringIO())
