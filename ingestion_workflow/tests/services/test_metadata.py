"""Tests for metadata service."""

import hashlib
import json
import os
import shutil
import threading
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
        pubmed_email="test@example.com",
        pubmed_api_key="test_pubmed_key",
    )


@pytest.fixture
def metadata_service(test_settings):
    """Create a metadata service instance."""
    service = MetadataService(test_settings)
    # An EMAIL in the environment switches OpenAlex on, and these tests do not
    # patch it, so it would reach the network.
    service._openalex_client = None
    return service


def _with_openalex(tmp_path) -> MetadataService:
    """`openalex_email` is read through its alias, so it is set on the client."""
    service = MetadataService(Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond",
    ))
    service._openalex_client = OpenAlexClient("test@example.com")
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
        result = metadata_service.enrich_metadata([])
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
            result = metadata_service.enrich_metadata([extracted])

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
            result = metadata_service.enrich_metadata([extracted])

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
            result = metadata_service.enrich_metadata([extracted])

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
            result = metadata_service.enrich_metadata([extracted])

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
            result = service.enrich_metadata([extracted])

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
            merged = service.enrich_metadata([extracted])[extracted.slug]
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

        result = service.enrich_metadata([extracted])

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
                "semantic_scholar_api_key": None,
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
            result = service.enrich_metadata([extracted])

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
        found = service.enrich_metadata(
            [extracted], {extracted.slug: [_held("pmc", "pmc_tdm_manuscript", identifier)]})

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
        # PMC's own copy is the one held
        assert not [c for c in meta.reported_copies if c.route == "pmc"]
        # what was decided survives the artifact
        assert ArticleMetadata.from_dict(meta.to_dict()).to_dict() == meta.to_dict()
        assert meta.raw_metadata["crossref"]["license"]

    @pytest.mark.vcr("TestLicences.test_a_tdm_manuscript_of_an_openly_licensed_article.yaml")
    def test_a_held_pdf_takes_the_licence_its_location_is_listed_with(self, tmp_path):
        """The same answers, with Wiley's open PDF held instead: its licence is the one
        OpenAlex lists for that URL, and it is no longer a reported copy."""
        from ingestion_workflow.models import DownloadCopy
        from ingestion_workflow.services.metadata import HeldDownload
        from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

        url = "https://onlinelibrary.wiley.com/doi/pdfdirect/10.1002/alz.12697"
        identifier = Identifier(pmid="35820077", pmcid="PMC10518222", doi="10.1002/alz.12697")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.PDF,
                                     identifier=identifier)
        pdf = HeldDownload("pdf", DownloadCopy(route=CopyRoute.publisher, version=CopyVersion.vor,
                                               access=CopyAccess.public, url=url))
        service = MetadataService(_licence_settings(tmp_path))
        meta = service.enrich_metadata([extracted], {extracted.slug: [pdf]})[extracted.slug]

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
        meta = service.enrich_metadata([extracted])[extracted.slug]

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
        meta = service.enrich_metadata([extracted], {extracted.slug: [ace]})[extracted.slug]

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

    @pytest.mark.vcr()
    def test_with_semantic_scholar_not_asked_nothing_is_assumed(self, tmp_path):
        """The same article, without Semantic Scholar's key: Semantic Scholar might have
        stated a licence, so none is assumed and the article is asked about again."""
        identifier = Identifier(doi="10.1037/npe0000112")
        extracted = ExtractedContent(slug=identifier.slug, source=DownloadSource.ACE,
                                     identifier=identifier)
        service = MetadataService(_licence_settings(tmp_path, semantic_scholar_api_key=None))
        found = service.enrich_metadata([extracted])

        assert found[extracted.slug].license is None
        assert service.undecided == {extracted.slug: (
            "the licence of the paper is undecided: semantic_scholar is not configured")}


def test_the_abstract_kept_is_credited_to_the_provider_it_came_from():
    s2 = ArticleMetadata(title="T", abstract="Short.", source="semantic_scholar")
    pubmed = ArticleMetadata(title="T", abstract="A longer abstract.", source="pubmed")
    openalex = ArticleMetadata(title="T", abstract="Tiny", source="openalex")
    merged = s2.merge_from(pubmed).merge_from(openalex)
    assert (merged.source, merged.abstract, merged.abstract_provider) == (
        "semantic_scholar", "A longer abstract.", "pubmed")
    assert ArticleMetadata(title="T", source="pubmed").abstract_provider is None


class _Unavailable(BaseHTTPRequestHandler):
    """Crossref while it is down: 503 to everything, with no wait asked for."""

    def do_GET(self):
        self.send_response(503)
        self.send_header("Retry-After", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def test_a_provider_that_does_not_answer_is_noted_for_each_article_it_left(tmp_path):
    """Served by a local server, because a recorded outage cannot be had on demand."""
    from ingestion_workflow.clients.crossref import CrossrefClient

    server = HTTPServer(("127.0.0.1", 0), _Unavailable)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        client = CrossrefClient()
        client.BASE_URL = f"http://127.0.0.1:{server.server_port}/works/"
        service = MetadataService(_licence_settings(tmp_path))
        identifiers = [Identifier(doi="10.1037/npe0000112"), Identifier(doi="10.1002/alz.12697")]
        unanswered = {}
        found = service._cached("crossref", client, identifiers, unanswered)
    finally:
        server.shutdown()
        server.server_close()
    assert found == {}
    assert unanswered == {i.slug: {"crossref": "did not answer"} for i in identifiers}
