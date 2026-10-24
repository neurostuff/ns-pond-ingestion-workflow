"""What the metadata stage's fingerprint covers: the ids it asks by, the downloads and
extractions, and each provider's cached record, and no setting."""

from __future__ import annotations

import pytest
from ingestion_workflow.pipeline.stage import Context
from ingestion_workflow.pipeline.stages.metadata import MetadataStage, cached_records, held_dois
from ingestion_workflow.services.metadata import PROVIDER_CACHE_DIRS
from ingestion_workflow.tests.pipeline.test_metadata_licences import (  # noqa: F401
    TDM_MANUSCRIPT,
    _held,
    _run,
    env,
)


def _fingerprint(settings, catalog, ref):
    stage = MetadataStage(settings)
    ids = (ref.identifier.pmid, held_dois(catalog, ref.id))
    return stage.fingerprint_for(ids, catalog.artifacts([ref.id], "download")[ref.id],
                                 catalog.artifacts([ref.id], "extract")[ref.id],
                                 cached_records(settings, ref.identifier))


def test_the_providers_listed_in_the_settings_are_not_in_it(env):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    before = _fingerprint(settings, catalog, ref)

    settings.metadata_providers = ["pubmed", "crossref"]

    assert _fingerprint(settings, catalog, ref) == before


def test_a_preprint_doi_the_article_gains_makes_its_metadata_stale(env):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    before = _fingerprint(settings, catalog, ref)

    catalog.add_aliases([(ref.id, "preprint_doi", "10.1101/2021.01.01.425000")])

    assert _fingerprint(settings, catalog, ref) != before


def test_a_providers_cached_record_changing_makes_its_metadata_stale(env):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    record = (settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS["crossref"]
              / f"{ref.identifier.slug}.json")
    record.parent.mkdir(parents=True, exist_ok=True)
    before = _fingerprint(settings, catalog, ref)

    record.write_text('{"license": []}', encoding="utf-8")
    deposited = _fingerprint(settings, catalog, ref)
    record.write_text('{"license": [{"URL": "https://creativecommons.org/licenses/by/4.0/"}]}',
                      encoding="utf-8")

    assert len({before, deposited, _fingerprint(settings, catalog, ref)}) == 3


@pytest.mark.vcr("test_each_held_copy_and_the_paper_get_a_licence.yaml")
def test_the_fingerprint_stored_is_the_one_the_records_it_fetched_give(env):  # noqa: F811
    settings, catalog = env
    ref = _held(catalog, TDM_MANUSCRIPT)
    stage, ctx = MetadataStage(settings), Context(settings, catalog)

    planned, (outcome,) = _run(stage, ctx, catalog, ref)

    # the records were not cached when planned, and are: stored so that it is fresh next run
    assert outcome.fingerprint != planned.pending[0].fingerprint
    again = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "metadata"),
                       catalog.artifacts([ref.id], "extract"))
    assert (again.fresh, again.pending) == (1, [])
