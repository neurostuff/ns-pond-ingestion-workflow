import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from ingestion_workflow.config import Settings, get_settings
from ingestion_workflow.metadata.enrichment import MetadataEnricher
from ingestion_workflow.models import Identifier

TEST_DOI = "10.1016/j.dcn.2015.10.001"
TEST_PMID = "26507433"

load_dotenv()


@pytest.mark.vcr(record="once")
def test_metadata_enricher_prefers_semantic_scholar():
    api_key = get_settings().semantic_scholar_api_key
    settings = Settings(
        semantic_scholar_api_key=api_key,
        metadata_provider_order=["semantic_scholar"],
    )
    enricher = MetadataEnricher(settings)
    identifier = Identifier(doi=TEST_DOI)

    enriched = enricher.enrich(identifier, {"doi": TEST_DOI})

    assert enriched.get("title", "").lower().startswith("preliminary findings")
    assert "semantic_scholar" in enriched.get("external_metadata", {})


@pytest.mark.vcr(record="once")
def test_metadata_enricher_openalex_fallback():
    settings = Settings(
        metadata_provider_order=["openalex"],
        openalex_email="jamesdkent21@gmail.com",
    )
    enricher = MetadataEnricher(settings)
    identifier = Identifier(doi=TEST_DOI)

    enriched = enricher.enrich(identifier, {"doi": TEST_DOI})

    assert enriched.get("title", "").lower().startswith("preliminary findings")
    assert enriched.get("pmid") == TEST_PMID
    assert "openalex" in enriched.get("external_metadata", {})


@pytest.mark.vcr(record="once")
def test_metadata_enricher_pubmed_by_pmid():
    settings = Settings(
        metadata_provider_order=["pubmed"],
        pubmed_email="jamesdkent21@gmail.com",
        pubmed_api_key=None,
    )
    enricher = MetadataEnricher(settings)
    identifier = Identifier(pmid=TEST_PMID)

    enriched = enricher.enrich(identifier, {"pmid": TEST_PMID})

    assert enriched.get("doi") == TEST_DOI
    assert enriched.get("title", "").lower().startswith("preliminary findings")
