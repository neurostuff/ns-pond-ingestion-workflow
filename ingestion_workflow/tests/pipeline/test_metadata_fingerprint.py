"""What the metadata stage's fingerprint covers: the ids it asks by, the downloads and
extractions, and the cache files its metadata was made from, and no setting."""

from __future__ import annotations

import pytest
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stage import Context
from ingestion_workflow.pipeline.stages.metadata import MetadataStage
from ingestion_workflow.tests.pipeline.test_metadata_licences import (  # noqa: F401
    TDM_MANUSCRIPT,
    _held,
    _run,
    env,
)


def _plan(stage, ctx, catalog, ref):
    return stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                      catalog.artifacts([ref.id], "extract"))


def test_each_id_the_stage_asks_by_is_in_the_fingerprint(env):  # noqa: F811
    settings, _ = env
    stage = MetadataStage(settings)

    def fingerprint(pmid, dois):
        return stage.fingerprint_for((pmid, dois), {})

    base = fingerprint("35820077", ["10.1002/alz.12697"])
    assert fingerprint(None, ["10.1002/alz.12697"]) != base
    assert fingerprint("35820078", ["10.1002/alz.12697"]) != base
    assert fingerprint("35820077", ["10.1002/alz.12697", "10.1101/2021.01.01.425000"]) != base
    assert fingerprint("35820077", ["10.1002/alz.12697"]) == base


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_cache_files_the_stage_read_are_recorded_and_hashed(env, evidence):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    planned, (outcome,) = _run(stage, ctx, catalog, ref)

    records = outcome.summary["records"]
    # Crossref's licences are under the DOI's slug, and an ORCID record under the author's iD:
    # not under the article's own slug
    assert sorted(records) == [
        "crossref-licenses/10-1002-alz-12697.json",
        "openalex-authorships/35820077-10-1002-alz-12697-pmc10518222.json",
        "orcid-emails/0000-0001-9700-8444.json",
        "semantic_scholar/35820077-10-1002-alz-12697-pmc10518222.json"]
    assert any(path.startswith("orcid-emails/") for path in records)
    assert all(records.values()), "each file read was there to be hashed"
    assert outcome.fingerprint != planned.pending[0].fingerprint
    catalog.record([outcome])
    assert _plan(stage, ctx, catalog, ref).fresh == 1

    # a licence deposit changing in the cache makes the metadata stale
    deposit = settings.get_cache_dir("metadata") / "crossref-licenses/10-1002-alz-12697.json"
    deposit.write_text('{"license": []}', encoding="utf-8")
    again = _plan(stage, ctx, catalog, ref)
    assert (again.fresh, len(again.pending)) == (0, 1)


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_providers_listed_in_the_settings_are_not_in_it(env, evidence):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)
    catalog.record(_run(stage, ctx, catalog, ref)[1])

    settings.metadata_providers = ["pubmed", "crossref"]

    assert _plan(stage, ctx, catalog, ref).fresh == 1


def test_a_preprint_doi_the_article_gains_makes_its_metadata_stale(env, evidence):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)
    before = stage.inputs_of(ctx, ref, catalog.artifacts([ref.id], "download")[ref.id],
                             catalog.artifacts([ref.id], "extract")[ref.id])

    catalog.add_aliases([(ref.id, "preprint_doi", "10.1101/2021.01.01.425000")])

    assert stage.inputs_of(ctx, ref, catalog.artifacts([ref.id], "download")[ref.id],
                           catalog.artifacts([ref.id], "extract")[ref.id]) != before


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_fingerprint_stored_is_the_one_the_records_it_fetched_give(env, evidence):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT, evidence=evidence)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    planned, (outcome,) = _run(stage, ctx, catalog, ref)

    # nothing was recorded when planned, and the files are in the cache now
    assert outcome.fingerprint != planned.pending[0].fingerprint
    catalog.record([outcome])
    again = _plan(stage, ctx, catalog, ref)
    assert (again.fresh, again.pending) == (1, [])


@pytest.mark.vcr()
def test_a_paper_with_a_pubmed_record_and_a_preprint_reads_a_file_of_each(env, evidence):  # noqa: F811
    settings, catalog = env
    settings.pubmed_email = "replay@example.com"
    ref = _held(catalog, Identifier(pmid="38345852", doi="10.7554/elife.92563"),
                source="pdf", fixture=None, evidence=evidence)
    catalog.add_aliases([(ref.id, "preprint_doi", "10.1101/2023.09.27.559792")])
    ref = catalog.ref(ref.id)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    _, (outcome,) = _run(stage, ctx, catalog, ref)

    slug = ref.identifier.slug
    assert sorted(outcome.summary["records"]) == sorted([
        f"semantic_scholar/{slug}.json", f"pubmed-authors/{slug}.json",
        "crossref-licenses/10-7554-elife-92563.json",
        "crossref-licenses/10-1101-2023-09-27-559792.json",
        f"openalex-authorships/{slug}.json",
        "openalex-authorships/10-1101-2023-09-27-559792.json"])
