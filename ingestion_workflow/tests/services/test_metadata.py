"""Tests for metadata service."""

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from dotenv import load_dotenv
from ingestion_workflow.clients.crossref import CrossrefClient, license_from
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import (
    ArticleMetadata,
    Author,
    License,
    LicenseBasis,
    LicenseCode,
    LicenseSource,
    preferred_license,
)
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
    service = MetadataService(test_settings)
    # An EMAIL in the environment switches OpenAlex on, and these tests do not
    # patch it, so it would reach the network; Crossref needs no key at all.
    service._openalex_client = None
    service._crossref_client = None
    return service


def _with_openalex(tmp_path) -> MetadataService:
    """`openalex_email` is read through its alias, so it is set on the client."""
    service = MetadataService(Settings(
        data_root=tmp_path / "data", cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "ns-pond",
    ))
    service._openalex_client = OpenAlexClient("test@example.com")
    service._crossref_client = None
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
            publication_year=2020,
            license=License.stated(LicenseSource.SEMANTIC_SCHOLAR, "CCBY"),
        )
        pubmed = ArticleMetadata(title="An Article", abstract="What it found.")

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
        assert merged.license.code is LicenseCode.CC_BY
        assert merged.license.stated_by is LicenseSource.SEMANTIC_SCHOLAR
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
        ):
            result = metadata_service.enrich_metadata([extracted])

        asked.assert_not_called()
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

        asked.assert_called_once_with([identifier])
        assert result[extracted.slug].journal == "bioRxiv"

    def test_openalex_is_not_asked_for_a_filled_record(self, tmp_path):
        service = _with_openalex(tmp_path)
        service._s2_client = SemanticScholarClient("test_s2_key")
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug, source=DownloadSource.PUBGET, identifier=identifier,
            has_coordinates=True,
        )
        s2 = ArticleMetadata(
            title="An Article", authors=[Author(name="A")], journal="J",
            publication_year=2020, abstract="What it found.",
            license=License.stated(LicenseSource.SEMANTIC_SCHOLAR, "CCBY"),
        )
        with (
            patch.object(service, "_get_semantic_scholar_metadata_cached",
                         return_value={identifier.slug: s2}),
            patch.object(service, "_get_openalex_metadata_cached", return_value={}) as asked,
        ):
            service.enrich_metadata([extracted])
        asked.assert_not_called()

    def test_openalex_is_asked_for_the_licence_a_filled_record_lacks(self, tmp_path):
        service = _with_openalex(tmp_path)
        service._s2_client = SemanticScholarClient("test_s2_key")
        identifier = Identifier(pmid="12345678")
        extracted = ExtractedContent(
            slug=identifier.slug, source=DownloadSource.PUBGET, identifier=identifier,
            has_coordinates=True,
        )
        s2 = ArticleMetadata(
            title="An Article", authors=[Author(name="A")], journal="J",
            publication_year=2020, abstract="What it found.",
        )
        with (
            patch.object(service, "_get_semantic_scholar_metadata_cached",
                         return_value={identifier.slug: s2}),
            patch.object(service, "_get_openalex_metadata_cached", return_value={}) as asked,
        ):
            result = service.enrich_metadata([extracted])
        asked.assert_called_once_with([identifier])
        assert result[extracted.slug].license == License.assumed()

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
        service._crossref_client = None  # its cassette holds no Crossref answer

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
        load_dotenv()
        pubmed_email = os.getenv("PUBMED_EMAIL") or os.getenv("EMAIL")
        if not pubmed_email:
            pytest.skip("PUBMED_EMAIL/EMAIL environment variable not set")
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
        service._crossref_client = None  # its cassette holds no Crossref answer

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


LICENSES = Path(__file__).parents[1] / "data" / "licenses"


def _jats_article(service, fixture: str, doi: str) -> ExtractedContent:
    """A JATS fixture where the PMC source keeps an article's XML, by its slug."""
    identifier = Identifier(doi=doi)
    article_dir = service.settings.cache_root / "pmc" / safe_hash_stem(identifier.slug)
    article_dir.mkdir(parents=True)
    shutil.copy(LICENSES / fixture, article_dir / "article.xml")
    return ExtractedContent(slug=identifier.slug, source=DownloadSource.PMC,
                            identifier=identifier, has_coordinates=True)


def _elsevier_article(service, fixture: str) -> ExtractedContent:
    """An Elsevier fixture where the Elsevier source keeps it, by the identifier's digest."""
    record = json.loads((LICENSES / fixture / "metadata.json").read_text())
    identifier = Identifier(doi=record["doi"], pmid=record["identifier_lookup"]["pmid"])
    digest = hashlib.sha256(identifier.slug.encode("utf-8")).hexdigest()[:32]
    shutil.copytree(LICENSES / fixture, service.settings.cache_root / "elsevier" / digest)
    return ExtractedContent(slug=identifier.slug, source=DownloadSource.ELSEVIER,
                            identifier=identifier, has_coordinates=True)


class TestLicenseNormalisation:
    @pytest.mark.parametrize("stated, code", [
        # Crossref and JATS URLs
        ("http://creativecommons.org/licenses/by/4.0/", LicenseCode.CC_BY),
        ("https://creativecommons.org/licenses/by-nc-nd/4.0/", LicenseCode.CC_BY_NC_ND),
        ("http://creativecommons.org/licenses/by-nc-sa/3.0/", LicenseCode.CC_BY_NC_SA),
        ("https://creativecommons.org/publicdomain/zero/1.0/", LicenseCode.CC0),
        ("https://www.elsevier.com/tdm/userlicense/1.0/", LicenseCode.PUBLISHER_TDM),
        ("http://www.elsevier.com/open-access/userlicense/1.0/", LicenseCode.PUBLISHER_TDM),
        ("http://www.springer.com/tdm", LicenseCode.PUBLISHER_TDM),
        ("http://onlinelibrary.wiley.com/termsAndConditions#vor", LicenseCode.OTHER),
        # Semantic Scholar's openAccessPdf.license and OpenAlex's location license
        ("CCBY", LicenseCode.CC_BY),
        ("CCBYNCND", LicenseCode.CC_BY_NC_ND),
        ("cc-by-nc", LicenseCode.CC_BY_NC),
        ("public-domain", LicenseCode.PUBLIC_DOMAIN),
        ("publisher-specific-oa", LicenseCode.OTHER),
        ("other-oa", LicenseCode.OTHER),
        # JATS license-type and license-p text
        ("creativeCommonsBy-nc", LicenseCode.CC_BY_NC),
        ("CC BY 4.0", LicenseCode.CC_BY),
        ("This is an open access article distributed under the terms of the Creative Commons "
         "Attribution-NonCommercial-NoDerivs License", LicenseCode.CC_BY_NC_ND),
        ("This is an open access article under the CC BY-NC license", LicenseCode.CC_BY_NC),
        ("This file is available for text mining. It may also be used consistent with the "
         "principles of fair use under the copyright law.", LicenseCode.PUBLISHER_TDM),
        ("Copyright © 2008 Elsevier Inc. All rights reserved.", LicenseCode.ALL_RIGHTS_RESERVED),
    ])
    def test_each_way_a_source_states_a_licence_has_its_code(self, stated, code):
        assert License.stated(LicenseSource.CROSSREF, stated).code is code

    def test_the_article_file_beats_crossref_vor_beats_openalex_and_s2_beats_crossref_other(self):
        jats = License.stated(LicenseSource.JATS, "© 2009 The Authors",
                              code=LicenseCode.ALL_RIGHTS_RESERVED)
        vor = License.stated(LicenseSource.CROSSREF, "http://creativecommons.org/licenses/by/4.0/",
                             version="vor")
        openalex = License.stated(LicenseSource.OPENALEX, "cc-by-nc")
        s2 = License.stated(LicenseSource.SEMANTIC_SCHOLAR, "CCBYNC")
        tdm = License.stated(LicenseSource.CROSSREF, "http://www.springer.com/tdm")
        assumed = License.assumed()
        assert preferred_license(tdm, s2, vor, jats, assumed) is jats
        assert preferred_license(tdm, openalex, vor, assumed) is vor
        assert preferred_license(tdm, s2, openalex, assumed) is s2  # a tie keeps the first
        assert preferred_license(assumed, tdm) is tdm

    def test_a_licence_survives_the_cache(self):
        stated = License.stated(LicenseSource.CROSSREF, "https://www.springer.com/tdm",
                                version="am")
        metadata = ArticleMetadata(title="T", license=stated)
        cached = json.loads(json.dumps(metadata.to_dict()))
        assert ArticleMetadata.from_dict(cached).license == stated


class TestOwnFileLicense:
    """The downloaded article's own statement, from real PMC and Elsevier records."""

    @pytest.mark.parametrize("fixture, code, stated_as", [
        ("jats_cc_by_href_pmc2430701.xml", LicenseCode.CC_BY,
         "https://creativecommons.org/licenses/by/2.0/"),
        ("jats_ali_license_ref_pmc8358179.xml", LicenseCode.CC_BY,
         "https://creativecommons.org/licenses/by/4.0/"),
        ("jats_copyright_only_pmc3822871.xml", LicenseCode.ALL_RIGHTS_RESERVED,
         "© 2009 The Authors Journal compilation © 2009 Foundation for Cellular and Molecular "
         "Medicine/Blackwell Publishing Ltd"),
        ("jats_manuscript_text_mining_pmc2739903.xml", LicenseCode.PUBLISHER_TDM,
         "This file is available for text mining. It may also be used consistent with the "
         "principles of fair use under the copyright law."),
    ])
    def test_jats_permissions(self, metadata_service, fixture, code, stated_as):
        article = _jats_article(metadata_service, fixture, "10.1000/fixture")
        found = metadata_service._get_fallback_metadata(article).license
        assert (found.code, found.basis, found.stated_by) == (code, LicenseBasis.STATED,
                                                              LicenseSource.JATS)
        assert found.stated_as == stated_as

    @pytest.mark.parametrize("fixture, code, stated_as", [
        ("elsevier_user_license", LicenseCode.PUBLISHER_TDM,
         "http://www.elsevier.com/open-access/userlicense/1.0/"),
        ("elsevier_copyright_only", LicenseCode.ALL_RIGHTS_RESERVED,
         "Copyright © 2013 Elsevier Inc. All rights reserved."),
    ])
    def test_elsevier_full_text_record(self, metadata_service, fixture, code, stated_as):
        article = _elsevier_article(metadata_service, fixture)
        found = metadata_service._get_fallback_metadata(article).license
        assert found == License(code, LicenseBasis.STATED, LicenseSource.ELSEVIER, stated_as,
                                "vor")


class TestLicenseFromProviders:
    @pytest.mark.vcr()
    def test_crossref_states_the_version_of_records_licence_else_any_in_force(self):
        found = CrossrefClient("test@example.com").get_metadata([
            Identifier(doi="10.1016/j.seizure.2015.08.008"),   # vor: Elsevier user licence
            Identifier(doi="10.1093/scan/nsw123"),             # vor: CC BY-NC
            Identifier(doi="10.3758/s13415-016-0459-7"),       # unspecified: Springer TDM
            Identifier(doi="10.1080/20445911.2021.1954013"),   # no licence deposited
            Identifier(doi="10.9999/no-such-work"),
        ])
        by_doi = {slug: meta.license for slug, meta in found.items()}
        assert by_doi[Identifier(doi="10.1016/j.seizure.2015.08.008").slug].code \
            is LicenseCode.PUBLISHER_TDM
        scan = by_doi[Identifier(doi="10.1093/scan/nsw123").slug]
        assert (scan.code, scan.version) == (LicenseCode.CC_BY_NC, "vor")
        springer = by_doi[Identifier(doi="10.3758/s13415-016-0459-7").slug]
        assert (springer.code, springer.version) == (LicenseCode.PUBLISHER_TDM, None)
        assert by_doi[Identifier(doi="10.1080/20445911.2021.1954013").slug] is None
        assert Identifier(doi="10.9999/no-such-work").slug not in by_doi

    def test_an_embargoed_crossref_licence_is_not_yet_in_force(self):
        entries = [
            {"URL": "https://www.elsevier.com/tdm/userlicense/1.0/", "content-version": "tdm",
             "start": {"date-time": "2014-11-01T00:00:00Z"}},
            {"URL": "https://www.elsevier.com/open-access/userlicense/1.0/",
             "content-version": "vor", "start": {"date-time": "2015-11-11T00:00:00Z"}},
        ]
        before = license_from(entries, now=datetime(2015, 1, 1, tzinfo=timezone.utc))
        after = license_from(entries, now=datetime(2016, 1, 1, tzinfo=timezone.utc))
        assert (before.version, before.stated_as) == (None, entries[0]["URL"])
        assert (after.version, after.stated_as) == ("vor", entries[1]["URL"])

    @pytest.mark.vcr()
    def test_openalex_states_the_primary_locations_licence(self):
        identifier = Identifier(doi="10.3389/fmed.2021.721551")
        found = OpenAlexClient("test@example.com").get_metadata([identifier])[identifier.slug]
        assert found.license.stated_by is LicenseSource.OPENALEX
        assert (found.license.code, found.license.version) == (LicenseCode.CC_BY, "vor")
        assert found.oa_status == "gold"  # how it is open, apart from its licence

    @pytest.mark.vcr()
    def test_semantic_scholar_states_its_open_access_pdfs_licence(self):
        identifier = Identifier(doi="10.3389/fmed.2021.721551")
        client = SemanticScholarClient(os.getenv("SEMANTIC_SCHOLAR_API_KEY") or "replayed")
        found = client.get_metadata([identifier])[identifier.slug]
        assert found.license.stated_by is LicenseSource.SEMANTIC_SCHOLAR
        assert found.license.code is LicenseCode.CC_BY
        assert found.oa_status == "gold"


class TestLicenseFlow:
    """Which sources are asked, and which statement the article ends up with."""

    def _service(self, tmp_path) -> MetadataService:
        service = MetadataService(Settings(
            data_root=tmp_path / "data", cache_root=tmp_path / "cache",
            ns_pond_root=tmp_path / "ns-pond",
        ))
        service._s2_client = service._pubmed_client = None
        service._openalex_client = OpenAlexClient("test@example.com")
        service._crossref_client = CrossrefClient("test@example.com")
        return service

    @pytest.mark.vcr()
    def test_the_articles_own_copyright_line_beats_crossrefs_vor_deposit(self, tmp_path, vcr):
        """Wiley deposits its terms as the vor licence; the PMC copy says only (c)."""
        service = self._service(tmp_path)
        service._openalex_client = None
        article = _jats_article(service, "jats_copyright_only_pmc3822871.xml",
                                "10.1111/j.1582-4934.2009.00659.x")
        crossref = service._crossref_client.get_metadata([article.identifier])
        assert crossref[article.slug].license.version == "vor"  # it does say something else
        result = service.enrich_metadata([article])[article.slug]
        assert result.license.stated_by is LicenseSource.JATS
        assert result.license.code is LicenseCode.ALL_RIGHTS_RESERVED
        assert result.license.basis is LicenseBasis.STATED

    @pytest.mark.vcr()
    def test_a_licence_in_the_articles_own_file_is_not_asked_of_crossref(self, tmp_path, vcr):
        service = self._service(tmp_path)
        service._s2_client = SemanticScholarClient(
            os.getenv("SEMANTIC_SCHOLAR_API_KEY") or "replayed")
        service._openalex_client = None
        article = _jats_article(service, "jats_ali_license_ref_pmc8358179.xml",
                                "10.3389/fmed.2021.721551")
        result = service.enrich_metadata([article])[article.slug]
        assert {request.host for request in vcr.requests} == {"api.semanticscholar.org"}
        assert result.license.code is LicenseCode.CC_BY
        assert result.license.stated_by is LicenseSource.JATS

    @pytest.mark.vcr()
    def test_crossrefs_vor_deposit_beats_semantic_scholar(self, tmp_path, vcr):
        """An article with no file of its own: both state CC BY-NC, and Crossref's is kept."""
        service = self._service(tmp_path)
        service._s2_client = SemanticScholarClient(
            os.getenv("SEMANTIC_SCHOLAR_API_KEY") or "replayed")
        identifier = Identifier(doi="10.1093/scan/nsw123")
        article = ExtractedContent(slug=identifier.slug, source=DownloadSource.ACE,
                                   identifier=identifier, has_coordinates=True)
        s2 = service._s2_client.get_metadata([identifier])[identifier.slug]
        assert s2.license.stated_by is LicenseSource.SEMANTIC_SCHOLAR  # it states one too
        result = service.enrich_metadata([article])[article.slug]
        assert (result.license.stated_by, result.license.code, result.license.version) == (
            LicenseSource.CROSSREF, LicenseCode.CC_BY_NC, "vor")

    @pytest.mark.vcr()
    def test_nothing_stated_anywhere_is_an_assumed_all_rights_reserved(self, tmp_path):
        service = self._service(tmp_path)
        identifier = Identifier(doi="10.1037/npe0000112")
        article = ExtractedContent(slug=identifier.slug, source=DownloadSource.PDF,
                                   identifier=identifier, has_coordinates=True)
        result = service.enrich_metadata([article])[article.slug]
        assert result.title  # OpenAlex answered, without a licence
        assert result.license == License.assumed()
        assert result.license.basis is LicenseBasis.ASSUMED


class TestLicenseInTheParse:
    """The parse's Bibliography.license is a bare string: a stated code, never an assumed one."""

    def test_a_stated_licence_is_its_code(self):
        from ingestion_workflow.services.paper_parse import _bibliography

        meta = ArticleMetadata(title="T", license=License.stated(
            LicenseSource.JATS, "© 2009 The Authors", code=LicenseCode.ALL_RIGHTS_RESERVED))
        assert _bibliography(meta, "T", ()).license == "all-rights-reserved"

    def test_an_assumed_licence_is_left_out(self):
        from ingestion_workflow.services.paper_parse import _bibliography

        meta = ArticleMetadata(title="T", license=License.assumed())
        assert _bibliography(meta, "T", ()).license is None


class TestMetadataStageReadsTheDownloadedFile:
    def test_the_file_the_download_recorded_is_read_wherever_it_lies(self, tmp_path):
        """Pubget's searches keep an article.xml under original/<search>/query_*/, deeper
        than the metadata lookup's guesses; the download's own record names it."""
        from ingestion_workflow.catalog import Catalog, Outcome, Status
        from ingestion_workflow.models import DownloadedFile, DownloadResult, FileType
        from ingestion_workflow.pipeline import Context
        from ingestion_workflow.pipeline.stages.metadata import MetadataStage

        settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                            catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "n",
                            semantic_scholar_api_key=None, pubmed_email=None)
        kept = (tmp_path / "pubget" / "original" / "fmri_journal" / "query_1" / "articles"
                / "a05" / "pmcid_3822871" / "article.xml")
        kept.parent.mkdir(parents=True)
        shutil.copy(LICENSES / "jats_copyright_only_pmc3822871.xml", kept)
        identifier = Identifier(doi="10.1111/j.1582-4934.2009.00659.x", pmcid="PMC3822871")
        with Catalog.open(settings.catalog_root) as catalog:
            ref = catalog.register(identifier)
            download = DownloadResult(identifier, DownloadSource.PUBGET, True, files=[
                DownloadedFile(kept, FileType.XML, "text/xml", DownloadSource.PUBGET)])
            extraction = ExtractedContent(slug=ref.identifier.slug, source=DownloadSource.PUBGET,
                                          identifier=ref.identifier, has_coordinates=True)
            catalog.record([
                Outcome(ref.id, "download", "pubget", Status.OK, "d-1", download.to_dict()),
                Outcome(ref.id, "extract", "pubget", Status.OK, "e-1", extraction.to_dict()),
            ])
            stage = MetadataStage(settings)
            stage.service._s2_client = stage.service._pubmed_client = None
            stage.service._openalex_client = stage.service._crossref_client = None
            ctx = Context(settings, catalog)
            plan = stage.plan(ctx, [ref], {}, catalog.artifacts([ref.id], "extract"))
            (outcome,) = list(stage.execute(ctx, plan.pending))
        assert outcome.status is Status.OK
        assert License.from_dict(outcome.payload["license"]).stated_by is LicenseSource.JATS
        assert (outcome.summary["license"], outcome.summary["license_basis"]) == (
            "all-rights-reserved", "stated")
