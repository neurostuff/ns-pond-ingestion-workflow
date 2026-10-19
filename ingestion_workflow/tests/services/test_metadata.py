"""Tests for metadata service."""

import hashlib
import json
import os
import shutil
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.services.metadata import MetadataService
from ingestion_workflow.tests import cassette_secrets


@pytest.fixture
def test_settings(tmp_path):
    """Create test settings with API keys."""
    return Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond",
        semantic_scholar_api_key="test_s2_key",
        openalex_api_key="test_openalex_key",
        pubmed_email="test@example.com",
        pubmed_api_key="test_pubmed_key",
    )


@pytest.fixture
def metadata_service(test_settings):
    """Create a metadata service instance."""
    return _offline(MetadataService(test_settings))


def _offline(service: MetadataService) -> MetadataService:
    """These tests patch the providers they read; Crossref and OpenAlex, which they do
    not, answer nothing rather than reach the network."""
    service._get_crossref_metadata_cached = lambda identifiers, unanswered=None: {}
    service._get_openalex_metadata_cached = lambda identifiers, unanswered=None: {}
    return service


def _enrich(service, contents, held=None):
    """`enrich_metadata` with each content's identifier's DOI as the catalog's."""
    dois = {c.slug: [c.identifier.doi] if c.identifier and c.identifier.doi else []
            for c in contents}
    return service.enrich_metadata(contents, held or {}, dois)


def _with_openalex(tmp_path) -> MetadataService:
    """`openalex_email` is read through its alias, so it is set on the client."""
    service = MetadataService(Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond", semantic_scholar_api_key="test_s2_key",
    ))
    service._openalex_client = OpenAlexClient("test@example.com")
    service._get_crossref_metadata_cached = lambda identifiers, unanswered=None: {}
    return service


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
        assert hasattr(service, "_pubmed_client")

    def test_enrich_metadata_empty_list(self, metadata_service):
        """Test metadata enrichment with empty list."""
        result = metadata_service.enrich_metadata([], {}, {})
        assert result == {}

    def test_enrich_metadata_with_mocked_s2(self, metadata_service):
        """Test metadata enrichment with mocked Semantic Scholar."""
        identifier = Identifier(doi="10.1234/test")
        extracted = ExtractedContent(
            slug=identifier.slug,
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
            return_value={identifier.slug: mock_metadata},
        ):
            result = _enrich(metadata_service, [extracted])

        assert extracted.slug in result
        assert result[extracted.slug].title == "Test Article"

    def test_enrich_metadata_fallback_to_pubmed(self, metadata_service):
        """Test fallback to PubMed when S2 fails."""
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )

        mock_metadata = ArticleMetadata(
            title="PubMed Article",
            authors=[Author(name="PM Author")],
        )

        with (
            patch.object(
                metadata_service,
                "_get_semantic_scholar_metadata_cached",
                return_value={},
            ),
            patch.object(
                metadata_service,
                "_get_pubmed_metadata_cached",
                return_value={identifier.slug: mock_metadata},
            ),
        ):
            result = _enrich(metadata_service, [extracted])

        assert extracted.slug in result
        assert result[extracted.slug].title == "PubMed Article"

    def test_a_sufficient_record_is_still_filled_by_the_next_provider(self, metadata_service):
        """`sufficient` marks an article ready for the next stage; it does not
        stop the providers. Semantic Scholar's record had title, journal and
        year but no abstract, and PubMed, never asked, had the abstract."""
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        s2 = ArticleMetadata(
            title="An Article", authors=[Author(name="A")], journal="J",
            publication_year=2020, source="semantic_scholar",
        )
        pubmed = ArticleMetadata(title="An Article", abstract="What it found.", source="pubmed")

        with (
            patch.object(metadata_service, "_get_semantic_scholar_metadata_cached",
                         return_value={identifier.slug: s2}),
            patch.object(metadata_service, "_get_pubmed_metadata_cached",
                         return_value={identifier.slug: pubmed}) as asked,
        ):
            result = _enrich(metadata_service, [extracted])

        asked.assert_called_once()
        merged = result[extracted.slug]
        assert merged.abstract == "What it found."
        assert (merged.source, merged.abstract_provider) == ("semantic_scholar", "pubmed")
        assert merged.journal == "J"

    def test_a_filled_record_skips_the_later_providers(self, metadata_service):
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )
        s2 = ArticleMetadata(
            title="An Article", authors=[Author(name="A")], journal="J",
            publication_year=2020, abstract="What it found.",
        )
        with (
            patch.object(metadata_service, "_get_semantic_scholar_metadata_cached",
                         return_value={identifier.slug: s2}),
            patch.object(metadata_service, "_get_pubmed_metadata_cached",
                         return_value={}) as asked,
            patch.object(metadata_service, "_get_fallback_metadata") as fallback,
        ):
            result = _enrich(metadata_service, [extracted])

        asked.assert_not_called()
        fallback.assert_not_called()
        assert result[extracted.slug].abstract == "What it found."

    def test_openalex_fills_what_semantic_scholar_and_pubmed_did_not(self, tmp_path):
        """A preprint: no record at Semantic Scholar or PubMed, all of it at OpenAlex."""
        service = _with_openalex(tmp_path)
        identifier = Identifier(doi="10.1101/2022.09.20.508750")
        extracted = ExtractedContent(
            slug=identifier.slug, source=DownloadSource.PDF, identifier=identifier,
            has_coordinates=False,
        )
        openalex = ArticleMetadata(
            title="Pre- and post-task resting-state differs in clinical populations",
            authors=[Author(name="A")], abstract="What it found.", journal="bioRxiv",
            publication_year=2022, source="openalex",
        )
        with (
            patch.object(service, "_get_semantic_scholar_metadata_cached", return_value={}),
            patch.object(service, "_get_pubmed_metadata_cached", return_value={}),
            patch.object(service, "_get_openalex_metadata_cached",
                         return_value={identifier.slug: openalex}) as asked,
        ):
            result = _enrich(service, [extracted])

        asked.assert_called_once()
        assert asked.call_args.args[0] == [identifier]
        assert result[extracted.slug].journal == "bioRxiv"

    def test_openalex_is_asked_about_a_filled_record_and_fills_nothing(self, tmp_path):
        """OpenAlex is asked about every article, for the copies it lists and their
        licences; a record the others filled keeps their fields."""
        service = _with_openalex(tmp_path)
        service._s2_client = SemanticScholarClient("test_s2_key")
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug, source=DownloadSource.PUBGET, identifier=identifier,
            has_coordinates=True,
        )
        s2 = ArticleMetadata(
            title="An Article", authors=[Author(name="A")], journal="J",
            publication_year=2020, abstract="What it found.", source="semantic_scholar",
        )
        openalex = ArticleMetadata(
            title="An Article", authors=[Author(name="A"), Author(name="B")], journal="K",
            publication_year=2020, abstract="What it found, at greater length.",
            source="openalex",
        )
        with (
            patch.object(service, "_get_semantic_scholar_metadata_cached",
                         return_value={identifier.slug: s2}),
            patch.object(service, "_get_openalex_metadata_cached",
                         return_value={identifier.slug: openalex}) as asked,
        ):
            merged = _enrich(service, [extracted])[extracted.slug]
        asked.assert_called_once()
        assert (merged.journal, merged.abstract, merged.abstract_provider, len(merged.authors)) \
            == ("J", "What it found.", "semantic_scholar", 1)

    def test_get_elsevier_fallback(self, metadata_service):
        """Test fallback metadata extraction from Elsevier files."""
        # Create mock metadata.json in the expected location
        identifier = Identifier(doi="10.1000/test")
        digest = hashlib.sha256(identifier.slug.encode("utf-8")).hexdigest()[:32]
        cache_dir = metadata_service.settings.cache_root / "elsevier" / digest
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
            slug="test_elsevier",
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
        assert metadata.source == "elsevier"

    def test_get_pubget_fallback(self, metadata_service):
        """Test fallback metadata extraction from Pubget files."""
        # Create mock article.xml
        identifier = Identifier(pmcid="PMC1234567")
        pmcid_suffix = "1234567"
        cache_dir = (
            metadata_service.settings.cache_root / "pubget" / "records" / f"pmcid_{pmcid_suffix}"
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
            slug="test_pubget",
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=False,
            full_text_path=cache_dir / "article.pdf",
        )

        metadata = metadata_service._get_pubget_fallback(extracted)

        assert metadata is not None
        assert metadata.title == "Pubget Test"
        assert metadata.source == "jats"
        assert len(metadata.authors) == 1

    def test_pubget_fallback_finds_the_article_by_its_pmcid(self, metadata_service, tmp_path):
        """pubget files PMC9056519 as `articles/1a4/pmcid_9056519`, as in its test data."""
        fixture = Path(__file__).resolve().parents[1] / "data" / "test_pubget" / "articles"
        root = metadata_service.settings.pubget_cache_root or (
            metadata_service.settings.cache_root / "pubget"
        )
        target = root / "articles" / "1a4" / "pmcid_9056519"
        target.mkdir(parents=True)
        shutil.copy(fixture / "1a4" / "pmcid_9056519" / "article.xml", target / "article.xml")
        extracted = ExtractedContent(
            slug="pubget-by-pmcid",
            source=DownloadSource.PUBGET,
            identifier=Identifier(pmcid="PMC9056519"),
            has_coordinates=False,
            full_text_path=tmp_path / "elsewhere" / "text.txt",
        )

        metadata = metadata_service._get_pubget_fallback(extracted)

        assert metadata is not None
        assert metadata.title.startswith("Preliminary findings demonstrating latent effects")


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
            (ident for ident in manifest_identifiers.identifiers if ident.doi),
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
            slug=identifier.slug,
            source=DownloadSource.PUBGET,
            identifier=identifier,
            has_coordinates=True,
        )

        result = _enrich(service, [extracted])

        assert extracted.slug in result
        assert result[extracted.slug].title is not None

    @pytest.mark.vcr()
    def test_pubmed_real_api(
        self,
        test_settings,
        manifest_identifiers,
    ):
        """Test with real PubMed API (VCR cassette)."""
        pubmed_email = cassette_secrets.REPLAY_EMAIL
        pubmed_api_key = os.getenv("PUBMED_API_KEY")

        identifier = next(
            (ident for ident in manifest_identifiers.identifiers if ident.pmid),
            None,
        )
        if identifier is None:
            pytest.skip("manifest_identifiers did not provide a PMID")

        settings = test_settings.model_copy(
            update={
                "semantic_scholar_api_key": "test_s2_key",
                "pubmed_email": pubmed_email,
                "pubmed_api_key": pubmed_api_key,
            }
        )
        service = MetadataService(settings)

        extracted = ExtractedContent(
            slug=identifier.slug,
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
            result = _enrich(service, [extracted])

        assert extracted.slug in result
        assert result[extracted.slug].title is not None


COPIES = Path(__file__).parents[1] / "data" / "copies"


def _licence_settings(tmp_path, **given):
    """Settings for recorded answers from Semantic Scholar, Crossref and OpenAlex; the
    keys are dropped from the cassettes, so any value replays. No email is sent."""
    return Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond", pubmed_email=None,
        **{"semantic_scholar_api_key": os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"),
           "openalex_api_key": os.environ.get("OPENALEX_API_KEY", "replay"), **given},
    )


def _held(source, fixture, identifier):
    """A download of the copy the fixture holds, as the download stage records it."""
    from ingestion_workflow.extractors.copies import copy_of
    from ingestion_workflow.models import DownloadedFile, DownloadResult, FileType
    from ingestion_workflow.services.metadata import HeldDownload

    files = sorted((COPIES / fixture).iterdir())
    result = DownloadResult(
        identifier=identifier, source=DownloadSource(source), success=True,
        files=[DownloadedFile(file_path=f, file_type=FileType.XML if f.suffix == ".xml"
                              else FileType.JSON, content_type="application/xml",
                              source=DownloadSource(source)) for f in files])
    return HeldDownload(source, copy_of(result), files)


def _license(found):
    return found.model_dump(mode="json", exclude_none=True)


class TestLicences:
    @pytest.mark.vcr()
    def test_a_tdm_manuscript_of_an_openly_licensed_article(self, tmp_path):
        """PMC10518222 is the NIH manuscript of 10.1002/alz.12697, which PMC offers for
        text mining only; the version of record is CC BY-NC at Wiley."""
        identifier = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.PMC,
                                     identifier=identifier)
        service = MetadataService(_licence_settings(tmp_path))
        held = {extracted.slug: [_held("pmc", "pmc_tdm_manuscript", identifier)]}
        found = _enrich(service, [extracted], held)

        assert service.undecided == {}
        meta = found[extracted.slug]
        assert _license(meta.copy_licenses["pmc"]) == {
            "license_id": "all-rights-reserved", "determination": "stated",
            "stated_by": "pmc_cloud", "stated_as": "TDM"}
        assert _license(meta.license) == {
            "license_id": "cc-by-nc", "determination": "stated", "stated_by": "crossref",
            "stated_as": "http://creativecommons.org/licenses/by-nc/4.0/"}
        assert meta.oa_status == "hybrid"
        wiley = [c for c in meta.reported_copies if c.route == "publisher"]
        assert [(c.version, c.reported_by, c.license.license_id) for c in wiley] == [
            ("vor", "openalex", "cc-by-nc")]
        # PMC's own copy is the one held, and PubMed's record is no copy
        assert not [c for c in meta.reported_copies if c.route == "pmc"]
        assert not [c for c in meta.reported_copies if "pubmed.ncbi" in c.url]
        assert meta.raw_metadata["openalex"][0]["locations"][1]["landing_page_url"] == (
            "https://pubmed.ncbi.nlm.nih.gov/35820077")
        # what was decided survives the artifact
        assert ArticleMetadata.from_dict(meta.to_dict()).to_dict() == meta.to_dict()
        assert meta.raw_metadata["crossref"]["10.1002/alz.12697"]["license"]

    @pytest.mark.vcr("TestLicences.test_a_tdm_manuscript_of_an_openly_licensed_article.yaml")
    def test_a_held_pdf_takes_the_licence_its_location_is_listed_with(self, tmp_path):
        """The same answers, with Wiley's open PDF held instead: its licence is the one
        OpenAlex listed for that URL when the PDF was fetched, which its copy records, and
        it is no longer a reported copy."""
        from ingestion_workflow.models import DownloadCopy
        from ingestion_workflow.services.metadata import HeldDownload
        from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

        url = "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/alz.12697"
        identifier = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.PDF,
                                     identifier=identifier)
        pdf = HeldDownload("pdf", DownloadCopy(route=CopyRoute.publisher, version=CopyVersion.vor,
                                               access=CopyAccess.public, url=url,
                                               license="cc-by-nc", license_by="openalex"))
        service = MetadataService(_licence_settings(tmp_path))
        meta = _enrich(service, [extracted], {extracted.slug: [pdf]})[extracted.slug]

        assert _license(meta.copy_licenses["pdf"]) == {
            "license_id": "cc-by-nc", "determination": "stated", "stated_by": "openalex",
            "stated_as": "cc-by-nc"}
        assert url not in {c.url for c in meta.reported_copies}
        # PMC's copy, not held now, is reported with the licence OpenAlex lists for it
        pmc = [c for c in meta.reported_copies if c.route == "pmc"]
        assert [(c.version, c.license.license_id, c.license.stated_as) for c in pmc] == [
            ("submitted", "other", "other-oa")]

    @pytest.mark.vcr()
    def test_a_preprint_s_licence_is_the_one_its_server_deposits(self, tmp_path):
        """10.1101/2025.05.13.653896, known only by its bioRxiv DOI: the paper's licence is
        the preprint's."""
        identifier = Identifier(doi="10.1101/2025.05.13.653896")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.PDF,
                                     identifier=identifier)
        service = MetadataService(_licence_settings(tmp_path))
        meta = _enrich(service, [extracted])[extracted.slug]

        assert service.undecided == {}
        assert (meta.license.determination, meta.license.stated_by) == ("stated", "crossref")
        assert meta.license.stated_as.startswith("http://creativecommons.org/licenses/")

    @pytest.mark.vcr()
    def test_nothing_stated_anywhere_is_an_assumed_all_rights_reserved(self, tmp_path):
        """10.1037/npe0000112: APA deposits no licence, and no provider lists one."""
        identifier = Identifier(doi="10.1037/npe0000112")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.ACE,
                                     identifier=identifier)
        from ingestion_workflow.models import DownloadCopy
        from ingestion_workflow.services.metadata import HeldDownload
        from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

        ace = HeldDownload("ace", DownloadCopy(route=CopyRoute.publisher, version=CopyVersion.vor,
                                               access=CopyAccess.unrecorded))
        service = MetadataService(_licence_settings(tmp_path))
        meta = _enrich(service, [extracted], {extracted.slug: [ace]})[extracted.slug]

        assumed = {"license_id": "all-rights-reserved", "determination": "assumed"}
        assert service.undecided == {}
        assert (_license(meta.license), _license(meta.copy_licenses["ace"])) == (assumed, assumed)
        # OpenAlex calls it closed, and lists its submitted versions at HAL and Toulouse
        # with no licence; Semantic Scholar lists HAL's PDF; the publisher's is ACE's.
        assert meta.oa_status == "closed"
        assert [(c.reported_by, c.route, c.version, c.url, _license(c.license))
                for c in meta.reported_copies] == [
            ("openalex", "unrecorded", "submitted", "https://hal.archives-ouvertes.fr/hal-02586104",
             assumed),
            ("openalex", "repository", "submitted", "https://hal.science/hal-03025378", assumed),
            ("openalex", "repository", "submitted",
             "https://publications.ut-capitole.fr/id/eprint/34986/", assumed),
            ("semantic_scholar", "unrecorded", None, "https://hal.archives-ouvertes.fr/hal-03025378"
             "/file/trust%20%20fmri%20paper%20JNPE%20postprint.pdf", assumed)]

    def test_without_semantic_scholar_no_licence_is_decided(self, tmp_path):
        """Semantic Scholar might have listed a copy and its licence: without its key the
        service refuses to run rather than assume one."""
        service = MetadataService(_licence_settings(tmp_path, semantic_scholar_api_key=None))
        with pytest.raises(RuntimeError, match="needs semantic_scholar configured"):
            _enrich(service, [_content(APA, DownloadSource.ACE)])


def test_the_abstract_kept_is_credited_to_the_provider_it_came_from():
    s2 = ArticleMetadata(title="T", abstract="Short.", source="semantic_scholar")
    pubmed = ArticleMetadata(title="T", abstract="A longer abstract.", source="pubmed")
    openalex = ArticleMetadata(title="T", abstract="Tiny", source="openalex")
    merged = s2.merge_from(pubmed).merge_from(openalex)
    assert (merged.source, merged.abstract, merged.abstract_provider) == (
        "semantic_scholar", "A longer abstract.", "pubmed")
    assert ArticleMetadata(title="T", source="pubmed").abstract_provider is None


def test_a_provider_that_does_not_answer_is_noted_for_each_article_it_left(tmp_path,
                                                                          unavailable):
    from ingestion_workflow.clients.crossref import CrossrefClient

    client = CrossrefClient()
    client.BASE_URL = unavailable + "/works/"
    service = MetadataService(_licence_settings(tmp_path))
    identifiers = [Identifier(doi="10.1037/npe0000112"), Identifier(doi="10.1002/alz.12697")]
    unanswered = {}
    _Unavailable.paths.clear()
    found = service._cached("crossref", client, identifiers, unanswered)
    assert found == {}
    assert unanswered == {i.slug: {"crossref": "did not answer"} for i in identifiers}
    # the failed query's DOIs are not asked for again one at a time
    assert _Unavailable.paths and all(p.startswith("/works?filter=") for p in _Unavailable.paths)


TDM_CASSETTE = "TestLicences.test_a_tdm_manuscript_of_an_openly_licensed_article.yaml"
NOTHING_CASSETTE = "TestLicences.test_nothing_stated_anywhere_is_an_assumed_all_rights_reserved.yaml"
ALZ = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")
APA = Identifier(doi="10.1037/npe0000112")


def _copy(source, route, version, access="public", url=None, files=()):
    from ingestion_workflow.models import DownloadCopy
    from ingestion_workflow.services.metadata import HeldDownload

    return HeldDownload(source, DownloadCopy(route=route, version=version, access=access, url=url),
                        list(files))


def _content(identifier, source=DownloadSource.PDF):
    return ExtractedContent(slug=identifier.slug, source=source, identifier=identifier)


@pytest.mark.vcr(TDM_CASSETTE)
def test_a_manuscript_and_a_copy_of_no_known_version_never_take_the_vor_s_licence(tmp_path):
    """10.1002/alz.12697's version of record is CC BY-NC by Crossref's `vor` deposit, and
    Crossref holds no `am` deposit: a held manuscript, and a copy whose version no source
    gives, are assumed."""
    held = [_copy("pdf", "repository", "am", url="https://discovery.ucl.ac.uk/id/eprint/10152210/"),
            _copy("europepmc", "pmc", None)]
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(ALZ)], {ALZ.slug: held})[ALZ.slug]

    assert service.undecided == {}
    assumed = {"license_id": "all-rights-reserved", "determination": "assumed"}
    assert {k: _license(v) for k, v in meta.copy_licenses.items()} == {
        "pdf": assumed, "europepmc": assumed}
    assert meta.license.license_id == "cc-by-nc"


@pytest.mark.vcr(TDM_CASSETTE)
def test_an_unreadable_own_record_leaves_its_copy_undecided(tmp_path):
    record = tmp_path / "pmc_cloud.json"
    record.write_text('{"license_code": ', encoding="utf-8")
    held = [_copy("pmc", "pmc", "am", access="tdm_grant",
                  files=[COPIES / "pmc_tdm_manuscript" / "article.xml", record])]
    service = MetadataService(_licence_settings(tmp_path))
    _enrich(service, [_content(ALZ, DownloadSource.PMC)], {ALZ.slug: held})

    undecided = service.undecided[ALZ.slug]
    assert str(undecided) == ("the licence of the pmc copy is undecided: "
                              "its own record could not be read")
    assert undecided.sources == ("own",)


class _Unavailable(BaseHTTPRequestHandler):
    """A provider while it is down: 503 to everything, with no wait asked for."""

    #: The path of every request it was sent.
    paths = []

    def do_GET(self):
        type(self).paths.append(self.path)
        self.send_response(503)
        self.send_header("Retry-After", "0")
        self.end_headers()

    do_POST = do_GET

    def log_message(self, *args):
        pass


@pytest.fixture()
def unavailable():
    """The base URL of a local server that answers 503: a recorded outage cannot be had
    on demand."""
    server = HTTPServer(("127.0.0.1", 0), _Unavailable)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


@pytest.mark.vcr(NOTHING_CASSETTE, ignore_localhost=True)
def test_crossref_down_leaves_a_licence_it_comes_before_undecided(tmp_path, unavailable):
    """Semantic Scholar and OpenAlex answer as recorded; Crossref does not."""
    service = MetadataService(_licence_settings(tmp_path))
    service._crossref_client.BASE_URL = unavailable + "/works/"
    held = [_copy("ace", "publisher", "vor", access="unrecorded")]
    found = _enrich(service, [_content(APA, DownloadSource.ACE)], {APA.slug: held})

    assert found[APA.slug].license is None
    undecided = service.undecided[APA.slug]
    assert str(undecided) == "the licence of the ace copy is undecided: crossref did not answer"
    assert (undecided.sources, undecided.outage) == (("crossref",), True)


@pytest.mark.vcr(NOTHING_CASSETTE, ignore_localhost=True)
def test_openalex_down_leaves_the_listed_copies_unknown(tmp_path, unavailable):
    service = MetadataService(_licence_settings(tmp_path))
    service._openalex_client.BASE_URL = unavailable
    _enrich(service, [_content(APA, DownloadSource.ACE)])

    undecided = service.undecided[APA.slug]
    assert str(undecided) == "the copies the providers list are unknown: openalex did not answer"
    assert undecided.sources == ("openalex",)


class _HalfAnswered(BaseHTTPRequestHandler):
    """Semantic Scholar answering its first batch, then failing."""

    calls = 0

    def do_POST(self):
        type(self).calls += 1
        if type(self).calls > 1:
            self.send_response(500)
            self.end_headers()
            return
        ids = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["ids"]
        body = json.dumps([{"paperId": "1", "title": "Answered", "authors": [], "year": 2020}
                           for _ in ids]).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def test_a_partial_semantic_scholar_answer_keeps_what_was_answered(tmp_path, monkeypatch):
    from ingestion_workflow.clients import semantic_scholar as s2
    from ingestion_workflow.models.notices import PartialAnswer

    monkeypatch.setattr(s2, "SEMANTIC_SCHOLAR_BATCH_SIZE", 1)
    monkeypatch.setattr(s2.SemanticScholarClient._request_metadata_batch.retry, "wait",
                        lambda retry_state: 0)
    server = HTTPServer(("127.0.0.1", 0), _HalfAnswered)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        client = s2.SemanticScholarClient("replay")
        client.BASE_URL = f"http://127.0.0.1:{server.server_port}/graph/v1/paper/batch"
        one, two = Identifier(doi="10.1037/npe0000112"), Identifier(doi="10.1002/alz.12697")
        with pytest.raises(PartialAnswer) as raised:
            client.get_metadata([one, two])
        assert set(raised.value.found) == {one.slug}
        _HalfAnswered.calls = 0
        unanswered = {}
        service = MetadataService(_licence_settings(tmp_path))
        found = service._cached("semantic_scholar", client, [one, two], unanswered)
    finally:
        server.shutdown()
        server.server_close()
    assert found[one.slug].title == "Answered"
    assert unanswered == {two.slug: {"semantic_scholar": "did not answer"}}


@pytest.mark.vcr()
@pytest.mark.parametrize("doi, stated_as", [
    ("10.1111/ejn.14701", "http://onlinelibrary.wiley.com/termsAndConditions#vor"),
    ("10.1093/cercor/bhab483", "https://academic.oup.com/journals/pages/open_access/"
                               "funder_policies/chorus/standard_publication_model"),
    ("10.1002/hbm.24523", "http://onlinelibrary.wiley.com/termsAndConditions#vor"),
    ("10.1002/hbm.21020", "http://onlinelibrary.wiley.com/termsAndConditions#vor"),
])
def test_another_copy_s_open_licence_is_not_the_version_of_record_s(tmp_path, doi, stated_as):
    """Each paper's Crossref `vor` deposit is its publisher's terms; Semantic Scholar's
    open PDF (a preprint, a manuscript, another work) or a repository location is CC."""
    identifier = Identifier(doi=doi)
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(identifier)])[identifier.slug]

    assert service.undecided == {}
    assert (meta.license.license_id, meta.license.stated_by, meta.license.stated_as) == (
        "other", "crossref", stated_as)
    # the open copies keep their own licences
    assert any(c.license.license_id.startswith("cc") for c in meta.reported_copies)


@pytest.mark.vcr()
def test_an_ace_copy_of_an_elsevier_vor_takes_the_elsevier_copy_s_record(tmp_path):
    """wig5qljtu6qy, 10.1016/j.cortex.2017.05.018: Elsevier's coredata says all rights are
    reserved; an open copy elsewhere does not make the page ACE read open."""
    identifier = Identifier(pmid="28668221", doi="10.1016/j.cortex.2017.05.018")
    files = [Path(__file__).parents[1] / "data" / "licenses" / "elsevier_cortex_2017_05_018"
             / "content.xml"]
    held = [_copy("elsevier", "publisher", "vor", access="tdm_grant", files=files),
            _copy("ace", "publisher", "vor", access="unrecorded")]
    service = MetadataService(_licence_settings(tmp_path))
    content = _content(identifier, DownloadSource.ELSEVIER)
    meta = _enrich(service, [content], {identifier.slug: held})[identifier.slug]

    arr = {"license_id": "all-rights-reserved", "determination": "stated",
           "stated_by": "elsevier", "stated_as": "© 2017 Elsevier Ltd. All rights reserved."}
    assert {k: _license(v) for k, v in meta.copy_licenses.items()} == {"elsevier": arr, "ace": arr}
    assert _license(meta.license) == arr


@pytest.mark.vcr("test_an_ace_copy_of_an_elsevier_vor_takes_the_elsevier_copy_s_record.yaml")
def test_a_page_s_bare_copyright_line_ranks_after_another_copy_s_record(tmp_path):
    """wig5qljtu6qy, 10.1016/j.cortex.2017.05.018: the page ACE read prints only a copyright
    line, and Elsevier's coredata states all rights reserved."""
    identifier = Identifier(pmid="28668221", doi="10.1016/j.cortex.2017.05.018")
    data = Path(__file__).parents[1] / "data" / "licenses"
    held = [_copy("ace", "publisher", "vor", access="unrecorded",
                  files=[data / "pages" / "sciencedirect_cortex_2017_05_018.html"]),
            _copy("elsevier", "publisher", "vor", access="tdm_grant",
                  files=[data / "elsevier_cortex_2017_05_018" / "content.xml"])]
    service = MetadataService(_licence_settings(tmp_path))
    content = _content(identifier, DownloadSource.ACE)
    meta = _enrich(service, [content], {identifier.slug: held})[identifier.slug]

    elsevier = {"license_id": "all-rights-reserved", "determination": "stated",
                "stated_by": "elsevier", "stated_as": "© 2017 Elsevier Ltd. All rights reserved."}
    assert service.undecided == {}
    # tried first, the ACE copy still takes Elsevier's record before its page's line
    assert {k: _license(v) for k, v in meta.copy_licenses.items()} == {
        "ace": elsevier, "elsevier": elsevier}
    assert _license(meta.license) == elsevier
    # the manuscript at Durham keeps the licence OpenAlex lists for it
    assert [(c.version, c.license.stated_by) for c in meta.reported_copies
            if c.version == "am"] == [("am", "openalex")]


@pytest.mark.vcr()
def test_a_page_s_footer_does_not_override_crossref_s_licence(tmp_path):
    """gyrrynmpuilc, 10.1523/JNEUROSCI.0258-06.2006: the page ACE read prints only the
    Society for Neuroscience's site copyright, in its footer; Crossref deposits CC BY-NC-SA
    for the version of record."""
    identifier = Identifier(pmid="16723542", doi="10.1523/JNEUROSCI.0258-06.2006")
    page = Path(__file__).parents[1] / "data" / "licenses" / "pages" / \
        "jneurosci_0258_06_2006_site_footer.html"
    held = [_copy("ace", "publisher", "vor", access="unrecorded", files=[page])]
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(identifier, DownloadSource.ACE)],
                   {identifier.slug: held})[identifier.slug]

    assert service.undecided == {}
    found = _license(meta.copy_licenses["ace"])
    assert (found["license_id"], found["stated_by"]) == ("cc-by-nc-sa", "crossref")
    assert _license(meta.license) == found


@pytest.mark.vcr()
def test_a_page_s_licence_mark_is_its_copy_s_own_record(tmp_path):
    """kwe7n3facvce, 10.1002/hbm.23411: the page ACE read links CC BY-NC-ND, as Crossref's
    `vor` deposit states."""
    identifier = Identifier(doi="10.1002/hbm.23411")
    page = Path(__file__).parents[1] / "data" / "licenses" / "pages" / "wiley_hbm_23411.html"
    held = [_copy("ace", "publisher", "vor", access="unrecorded", files=[page])]
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(identifier, DownloadSource.ACE)],
                   {identifier.slug: held})[identifier.slug]

    assert service.undecided == {}
    assert _license(meta.copy_licenses["ace"]) == {
        "license_id": "cc-by-nc-nd", "determination": "stated", "stated_by": "publisher_page",
        "stated_as": "http://creativecommons.org/licenses/by-nc-nd/4.0/"}
    # the page is the paper's last source
    assert (meta.license.license_id, meta.license.stated_by) == ("cc-by-nc-nd", "crossref")


@pytest.mark.vcr()
def test_a_reported_manuscript_takes_the_licence_the_page_names_for_it(tmp_path):
    """10.1016/j.neuropsychologia.2015.12.005: the page ACE read reserves all rights to the
    article, and lets its accepted manuscript be shared under CC BY-NC-ND from 2017-06-17.
    OpenAlex lists that manuscript at UEA with no licence; Crossref deposits only a mining
    grant, and OpenAlex lists no licence at the publisher."""
    identifier = Identifier(pmid="26683384", doi="10.1016/j.neuropsychologia.2015.12.005")
    page = Path(__file__).parents[1] / "data" / "licenses" / "pages" / \
        "sciencedirect_neuropsychologia_2015_12_005.html"
    held = [_copy("ace", "publisher", "vor", access="unrecorded", files=[page])]
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(identifier, DownloadSource.ACE)],
                   {identifier.slug: held})[identifier.slug]

    stated_by_page = {"license_id": "all-rights-reserved", "determination": "stated",
                      "stated_by": "publisher_page",
                      "stated_as": "Copyright © 2015 Elsevier Ltd. All rights reserved."}
    assert service.undecided == {}
    assert _license(meta.copy_licenses["ace"]) == _license(meta.license) == stated_by_page
    manuscripts = [(c.url, _license(c.license)) for c in meta.reported_copies
                   if c.version == "am"]
    assert manuscripts == [(
        "https://ueaeprints.uea.ac.uk/id/eprint/56193/1/MIrish_Manuscript_R2_final.pdf",
        {"license_id": "cc-by-nc-nd", "determination": "stated", "stated_by": "publisher_page",
         "stated_as": "http://creativecommons.org/licenses/by-nc-nd/4.0/"})]


@pytest.mark.vcr("test_an_ace_copy_of_an_elsevier_vor_takes_the_elsevier_copy_s_record.yaml")
def test_the_page_speaks_for_the_paper_when_no_other_source_does(tmp_path):
    """10.1016/j.cortex.2017.05.018 with only its ACE copy: Crossref deposits only a mining
    grant, and OpenAlex lists no licence at the publisher."""
    identifier = Identifier(pmid="28668221", doi="10.1016/j.cortex.2017.05.018")
    page = Path(__file__).parents[1] / "data" / "licenses" / "pages" / \
        "sciencedirect_cortex_2017_05_018.html"
    held = [_copy("ace", "publisher", "vor", access="unrecorded", files=[page])]
    service = MetadataService(_licence_settings(tmp_path))
    meta = _enrich(service, [_content(identifier, DownloadSource.ACE)],
                   {identifier.slug: held})[identifier.slug]

    stated_by_page = {"license_id": "all-rights-reserved", "determination": "stated",
                      "stated_by": "publisher_page",
                      "stated_as": "© 2017 Elsevier Ltd. All rights reserved."}
    assert service.undecided == {}
    assert _license(meta.copy_licenses["ace"]) == _license(meta.license) == stated_by_page


@pytest.mark.vcr()
def test_a_paper_presented_by_its_preprint_doi_takes_its_journal_s_licence(tmp_path, vcr):
    """dpl6phhniqbj holds 10.1002/hbm.25137 and its bioRxiv preprint 10.1101/2020.03.25.008318,
    and is presented by the preprint's DOI. Crossref holds a CC BY deposit for the journal's
    DOI and none for the preprint's."""
    identifier = Identifier(doi="10.1101/2020.03.25.008318")
    dois = ["10.1002/hbm.25137", "10.1101/2020.03.25.008318"]
    service = MetadataService(_licence_settings(tmp_path))
    meta = service.enrich_metadata([_content(identifier)], {}, {identifier.slug: dois})[
        identifier.slug]

    assert service.undecided == {}
    assert sorted(meta.raw_metadata["crossref"]) == dois
    assert (meta.license.stated_by, meta.license.stated_as) == (
        "crossref", "http://creativecommons.org/licenses/by/4.0/")


def test_a_merged_abstract_s_provider_survives_the_artifact():
    merged = ArticleMetadata(title="T", abstract="Short.", source="semantic_scholar").merge_from(
        ArticleMetadata(title="T", abstract="A longer abstract.", source="pubmed"))
    reloaded = ArticleMetadata.from_dict(json.loads(json.dumps(merged.to_dict())))
    assert (reloaded.source, reloaded.abstract_provider) == ("semantic_scholar", "pubmed")


def _decided(works, paper_work=None, crossref=None, now=None):
    """`_decide_licenses` for a paper no download holds, from OpenAlex's `works`."""
    from ingestion_workflow.services.metadata import _decide_licenses
    from study_schema.models.paper_parse import ArticleIdentifiers

    meta = ArticleMetadata(title="T")
    _decide_licenses(meta, ArticleIdentifiers(publication_status="published"), None,
                     crossref or {}, works, paper_work, {}, [], {}, now or datetime.now(timezone.utc))
    return meta


#: 10.1002/alz.12697's OpenAlex work, its Wiley location relabelled as an accepted manuscript,
#: as OpenAlex lists an early-access manuscript on a publisher's site.
EARLY_ACCESS = {"open_access": {"oa_status": "hybrid"}, "locations": [{
    "pdf_url": "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/alz.12697",
    "landing_page_url": "https://doi.org/10.1002/alz.12697", "version": "acceptedVersion",
    "license": "cc-by-nc", "is_oa": True,
    "source": {"type": "journal", "display_name": "Alzheimer's & Dementia"}}]}


def test_a_publisher_s_manuscript_licence_is_not_the_version_of_record_s():
    meta = _decided([EARLY_ACCESS], EARLY_ACCESS)
    assert (meta.license.license_id, meta.license.determination) == (
        "all-rights-reserved", "assumed")
    assert [(c.route, c.version, c.license.license_id) for c in meta.reported_copies] == [
        ("publisher", "am", "cc-by-nc")]


@pytest.mark.parametrize("oa_status, expected", [("green", "green"), ("unknown", None)])
def test_oa_status_is_openalex_s_when_it_is_one_of_study_schema_s(oa_status, expected):
    work = {"open_access": {"oa_status": oa_status}}
    assert _decided([work], work).oa_status == expected


def test_oa_status_is_the_journal_doi_s_work_s_never_the_preprint_s():
    """dpl6phhniqbj: OpenAlex knows the journal DOI's work (hybrid) and the bioRxiv one
    (green); without the journal's, the paper's status is unknown."""
    journal, preprint = {"open_access": {"oa_status": "hybrid"}}, {"open_access": {"oa_status": "green"}}
    assert _decided([preprint, journal], journal).oa_status == "hybrid"
    assert _decided([preprint], None).oa_status is None


def test_two_journal_dois_are_a_catalog_conflict_and_nothing_is_asked(test_settings):
    """d62fs2rhxmro: two different papers merged in the catalog."""
    service = MetadataService(test_settings)
    identifier = Identifier(doi="10.1016/j.biopsych.2010.04.013")
    content = _content(identifier)
    found = service.enrich_metadata(
        [content], {}, {content.slug: ["10.1016/j.biopsych.2010.04.013", "10.1038/mp.2009.121"]})
    assert found == {} and service.undecided == {}
    assert service.conflicts == {content.slug: (
        "catalog conflict: the article holds two DOIs of a version of record, "
        "10.1016/j.biopsych.2010.04.013 and 10.1038/mp.2009.121; its aliases need repair")}


class _Refusing(BaseHTTPRequestHandler):
    """A provider refusing the request: 400 to everything."""

    def do_GET(self):
        self.send_response(400)
        self.end_headers()

    do_POST = do_GET

    def log_message(self, *args):
        pass


@pytest.mark.vcr()
def test_a_batch_semantic_scholar_knows_none_of_is_an_answer(tmp_path):
    """Asked only for 10.1016/j.neuroimage.2020.116616, which it does not know, Semantic
    Scholar's batch endpoint answers 400 "No valid paper ids given". That is its answer for
    the article, as `null` is inside a batch it knows some of: nothing is left unanswered,
    so a re-run on a warm cache does not leave the article undecided."""
    unknown = Identifier(doi="10.1016/j.neuroimage.2020.116616")
    client = SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"))
    unanswered = {}
    found = MetadataService(_licence_settings(tmp_path))._cached(
        "semantic_scholar", client, [unknown], unanswered)

    assert (found, unanswered) == ({}, {})


@pytest.mark.vcr()
def test_another_400_from_semantic_scholar_is_a_refusal(tmp_path):
    """The batch endpoint's 400 for another reason than knowing none of the ids is no answer
    about the article (the cassette is the one above, its error edited)."""
    unknown = Identifier(doi="10.1016/j.neuroimage.2020.116616")
    client = SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"))
    unanswered = {}
    found = MetadataService(_licence_settings(tmp_path))._cached(
        "semantic_scholar", client, [unknown], unanswered)

    assert found == {}
    assert unanswered[unknown.slug]["semantic_scholar"].startswith(
        "refused the request (400 Client Error: Bad Request")


@pytest.mark.vcr()
def test_semantic_scholar_is_asked_once_when_it_refuses(tmp_path):
    """A key it does not accept is refused with 403, which asking again does not change: the
    cassette holds one request, and replaying a second would fail."""
    client = SemanticScholarClient("not-a-key")
    unanswered = {}
    MetadataService(_licence_settings(tmp_path))._cached(
        "semantic_scholar", client, [APA], unanswered)

    assert unanswered[APA.slug]["semantic_scholar"].startswith(
        "refused the request (403 Client Error: Forbidden")


@pytest.mark.vcr(NOTHING_CASSETTE, ignore_localhost=True)
def test_a_refused_request_is_not_an_outage(tmp_path):
    server = HTTPServer(("127.0.0.1", 0), _Refusing)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        service = MetadataService(_licence_settings(tmp_path))
        service._crossref_client.BASE_URL = f"http://127.0.0.1:{server.server_port}/works/"
        held = [_copy("ace", "publisher", "vor", access="unrecorded")]
        _enrich(service, [_content(APA, DownloadSource.ACE)], {APA.slug: held})
    finally:
        server.shutdown()
        server.server_close()
    undecided = service.undecided[APA.slug]
    assert str(undecided).startswith(
        "the licence of the ace copy is undecided: crossref refused the request (400")
    assert (undecided.sources, undecided.outage) == (("crossref",), False)


def test_an_article_with_two_dois_is_embargoed_until_the_first_deposit_starts():
    """Two DOIs' manuscript deposits, as Crossref had them on 2026-10-19, start on
    2027-04-07 and on 2026-11-25."""
    meta = _decided([], crossref={
        "10.1016/j.tics.2025.09.002": {"license": [
            {"URL": "http://www.elsevier.com/open-access/userlicense/1.0/",
             "content-version": "am", "start": {"date-time": "2027-04-07T00:00:00Z"}}]},
        "10.1016/j.cogpsych.2025.101768": {"license": [
            {"URL": "http://www.elsevier.com/open-access/userlicense/1.0/",
             "content-version": "am", "start": {"date-time": "2026-11-25T00:00:00Z"}}]},
    }, now=datetime(2026, 10, 19, tzinfo=timezone.utc))

    assert meta.license_embargo_until == datetime(2026, 11, 25, tzinfo=timezone.utc)
    assert ArticleMetadata.from_dict(meta.to_dict()).license_embargo_until == \
        meta.license_embargo_until
