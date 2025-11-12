"""Tests for metadata service."""

import os
import hashlib
import json
from unittest.mock import patch

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.services.metadata import MetadataService


@pytest.fixture
def test_settings(tmp_path):
    """Create test settings with API keys."""
    return Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond",
        semantic_scholar_api_key="test_s2_key",
        pubmed_email="test@example.com",
        pubmed_api_key="test_pubmed_key",
    )


@pytest.fixture
def metadata_service(test_settings):
    """Create a metadata service instance."""
    return MetadataService(test_settings)


class TestMetadataService:
    """Test MetadataService class."""

    def test_init(self, test_settings):
        """Test MetadataService initialization."""
        service = MetadataService(test_settings)
        
        assert service.settings == test_settings
        assert service._s2_client is not None
        assert service._pubmed_client is not None

    def test_init_with_only_s2_key(self, tmp_path):
        """Test MetadataService initialization with only S2 API key."""
        settings = Settings(
            data_root=tmp_path / "data",
            cache_root=tmp_path / "cache",
            ns_pond_root=tmp_path / "ns-pond",
            semantic_scholar_api_key="test_s2_key",
            pubmed_email=None,  # No PubMed email
        )
        service = MetadataService(settings)
        
        # S2 client should be initialized
        assert service._s2_client is not None
        # PubMed client might still be None if no email
        # (depends on env vars, so we just check it exists as attribute)
        assert hasattr(service, '_pubmed_client')

    def test_enrich_metadata_empty_list(self, metadata_service):
        """Test metadata enrichment with empty list."""
        result = metadata_service.enrich_metadata([])
        assert result == {}

    def test_enrich_metadata_with_mocked_s2(self, metadata_service):
        """Test metadata enrichment with mocked Semantic Scholar."""
        identifier = Identifier(doi="10.1234/test")
        extracted = ExtractedContent(
            hash_id=identifier.hash_id,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        
        mock_metadata = ArticleMetadata(
            title="Test Article",
            authors=[Author(name="Test Author")],
            publication_year=2023,
        )
        
        with patch.object(
            metadata_service._s2_client,
            "get_metadata",
            return_value={identifier.hash_id: mock_metadata},
        ):
            result = metadata_service.enrich_metadata([extracted])
        
        assert extracted.hash_id in result
        assert result[extracted.hash_id].title == "Test Article"

    def test_enrich_metadata_fallback_to_pubmed(self, metadata_service):
        """Test fallback to PubMed when S2 fails."""
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            hash_id=identifier.hash_id,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        
        mock_metadata = ArticleMetadata(
            title="PubMed Article",
            authors=[Author(name="PM Author")],
        )
        
        with patch.object(
            metadata_service,
            "_get_semantic_scholar_metadata_cached",
            return_value={},
        ), patch.object(
            metadata_service,
            "_get_pubmed_metadata_cached",
            return_value={identifier.hash_id: mock_metadata},
        ):
            result = metadata_service.enrich_metadata([extracted])
        
        assert extracted.hash_id in result
        assert result[extracted.hash_id].title == "PubMed Article"

    def test_get_elsevier_fallback(self, metadata_service):
        """Test fallback metadata extraction from Elsevier files."""
        # Create mock metadata.json in the expected location
        identifier = Identifier(doi="10.1000/test")
        digest = hashlib.sha256(
            identifier.hash_id.encode("utf-8")
        ).hexdigest()[:32]
        cache_dir = (
            metadata_service.settings.cache_root / "elsevier" / digest
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_json = cache_dir / "metadata.json"
        metadata_data = {
            "title": "Elsevier Test Article",
            "authors": [
                {"given-name": "John", "surname": "Doe"},
            ],
            "publicationName": "Elsevier Journal",
            "coverDate": "2023-01-15",
        }
        
        with open(metadata_json, "w") as f:
            json.dump(metadata_data, f)
        
        extracted = ExtractedContent(
            hash_id="test_elsevier",
            source=DownloadSource.ELSEVIER,
            identifier=identifier,
            has_coordinates=False,
            full_text_path=cache_dir / "article.pdf",
        )
        
        metadata = metadata_service._get_elsevier_fallback(extracted)
        
        assert metadata is not None
        assert metadata.title == "Elsevier Test Article"
        assert len(metadata.authors) == 1
        assert metadata.journal == "Elsevier Journal"
        assert metadata.publication_year == 2023

    def test_get_pubget_fallback(self, metadata_service):
        """Test fallback metadata extraction from Pubget files."""
        # Create mock article.xml
        identifier = Identifier(pmcid="PMC1234567")
        pmcid_suffix = "1234567"
        cache_dir = (
            metadata_service.settings.cache_root
            / "pubget"
            / "records"
            / f"pmcid_{pmcid_suffix}"
        )
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        article_xml = cache_dir / "article.xml"
        xml_content = """<?xml version="1.0"?>
<article>
    <front>
        <article-meta>
            <title-group>
                <article-title>Pubget Test</article-title>
            </title-group>
            <contrib-group>
                <contrib>
                    <name>
                        <given-names>Jane</given-names>
                        <surname>Smith</surname>
                    </name>
                </contrib>
            </contrib-group>
            <pub-date>
                <year>2023</year>
            </pub-date>
        </article-meta>
        <journal-meta>
            <journal-title-group>
                <journal-title>Test Journal</journal-title>
            </journal-title-group>
        </journal-meta>
    </front>
</article>
"""
        
        with open(article_xml, "w") as f:
            f.write(xml_content)
        
        extracted = ExtractedContent(
            hash_id="test_pubget",
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=False,
            full_text_path=cache_dir / "article.pdf",
        )
        
        metadata = metadata_service._get_pubget_fallback(extracted)
        
        assert metadata is not None
        assert metadata.title == "Pubget Test"
        assert len(metadata.authors) == 1


class TestMetadataIntegration:
    """Integration tests for metadata workflow."""

    @pytest.mark.vcr()
    def test_semantic_scholar_real_api(
        self,
        test_settings,
        manifest_identifiers,
    ):
        """Test with real Semantic Scholar API (VCR cassette)."""
        api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
        if not api_key:
            pytest.skip("SEMANTIC_SCHOLAR_API_KEY not configured")

        identifier = next(
            (
                ident
                for ident in manifest_identifiers.identifiers
                if ident.doi
            ),
            None,
        )
        if identifier is None:
            pytest.skip("manifest_identifiers did not provide a DOI")

        settings = test_settings.model_copy(
            update={
                "semantic_scholar_api_key": api_key,
                "pubmed_email": None,
                "pubmed_api_key": None,
            }
        )
        service = MetadataService(settings)
        
        extracted = ExtractedContent(
            hash_id=identifier.hash_id,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        
        result = service.enrich_metadata([extracted])
        
        assert extracted.hash_id in result
        assert result[extracted.hash_id].title is not None

    @pytest.mark.vcr()
    def test_pubmed_real_api(
        self,
        test_settings,
        manifest_identifiers,
    ):
        """Test with real PubMed API (VCR cassette)."""
        pubmed_email = (
            os.getenv("PUBMED_EMAIL")
            or os.getenv("EMAIL")
        )
        if not pubmed_email:
            pytest.skip("PUBMED_EMAIL/EMAIL environment variable not set")
        pubmed_api_key = (
            os.getenv("PUBMED_API_KEY")
        )

        identifier = next(
            (
                ident
                for ident in manifest_identifiers.identifiers
                if ident.pmid
            ),
            None,
        )
        if identifier is None:
            pytest.skip("manifest_identifiers did not provide a PMID")

        settings = test_settings.model_copy(
            update={
                "semantic_scholar_api_key": None,
                "pubmed_email": pubmed_email,
                "pubmed_api_key": pubmed_api_key,
            }
        )
        service = MetadataService(settings)
        
        extracted = ExtractedContent(
            hash_id=identifier.hash_id,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        
        # Mock S2 to force PubMed fallback
        with patch.object(
            service,
            "_get_semantic_scholar_metadata_cached",
            return_value={},
        ):
            result = service.enrich_metadata([extracted])
        
        assert extracted.hash_id in result
        assert result[extracted.hash_id].title is not None


