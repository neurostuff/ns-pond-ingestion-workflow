"""What the metadata stage's fingerprint covers."""

from __future__ import annotations

from ingestion_workflow.catalog import fingerprint
from ingestion_workflow.config import Settings
from ingestion_workflow.pipeline.stages.metadata import METADATA_VERSION, MetadataStage


def test_the_lookup_chain_s_providers_do_not_make_metadata_stale(tmp_path):
    def stage(providers):
        return MetadataStage(Settings(cache_root=tmp_path, metadata_providers=providers))

    # the fingerprint main computed with the default providers, before the lookup chain
    # gained Crossref: no article's metadata restages for it
    before = fingerprint("metadata", METADATA_VERSION,
                         ("semantic_scholar", "pubmed", "openalex"), ())

    assert stage(["semantic_scholar", "pubmed", "openalex"]).fingerprint_for({}) == before
    assert stage(["pubmed", "crossref"]).fingerprint_for({}) == before
