"""
This module will grab metadata for articles
given a list of article ids.
The metadata will be retrieved from the following sources
in this order:
1. Semantic Scholar
2. PubMed
3. OpenAlex
4. fallback to processed metadata from extractors
   (information from the downloaded article)
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from lxml import etree
from pubget._utils import article_bucket_from_pmcid

from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author

logger = logging.getLogger(__name__)


class MetadataService:
    """
    Service for enriching ExtractedContent with article metadata.

    Coordinates metadata fetching from multiple sources:
    1. Semantic Scholar (if API key available)
    2. PubMed (if email configured)
    3. OpenAlex (if email configured)
    4. Fallback to extractor-specific files
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._s2_client: Optional[SemanticScholarClient] = None
        self._pubmed_client: Optional[PubMedClient] = None
        self._openalex_client: Optional[OpenAlexClient] = None

        # Initialize clients if credentials available
        if settings.semantic_scholar_api_key:
            self._s2_client = SemanticScholarClient(settings.semantic_scholar_api_key)

        if settings.pubmed_email:
            self._pubmed_client = PubMedClient(
                email=settings.pubmed_email,
                api_key=settings.pubmed_api_key,
            )

        if settings.openalex_email:
            self._openalex_client = OpenAlexClient(settings.openalex_email)

    def enrich_metadata(
        self, extracted_contents: List[ExtractedContent]
    ) -> Dict[str, ArticleMetadata]:
        """
        Enrich extracted contents with article metadata.

        Parameters
        ----------
        extracted_contents : list of ExtractedContent
            Content objects to enrich with metadata

        Returns
        -------
        dict
            Mapping from slug to ArticleMetadata
        """
        if not extracted_contents:
            return {}

        identified_items = [item for item in extracted_contents if item.identifier]

        id_to_content = {item.identifier.slug: item for item in identified_items}

        results: Dict[str, ArticleMetadata] = {}
        sources_checked: List[str] = []

        # Try Semantic Scholar first
        if self._s2_client and identified_items:
            logger.info(
                "Fetching metadata from Semantic Scholar for %d articles",
                len(identified_items),
            )
            sources_checked.append("semantic_scholar")
            s2_results = self._get_semantic_scholar_metadata_cached(
                [item.identifier for item in identified_items]
            )
            for identifier_slug, metadata in s2_results.items():
                content = id_to_content.get(identifier_slug)
                if content is None:
                    continue
                if self._has_useful_metadata(metadata):
                    results[content.slug] = metadata
            logger.info(
                "Semantic Scholar returned metadata for %d articles",
                len(s2_results),
            )

        # Each provider fills what the earlier ones left empty, and is skipped
        # only for an article with nothing left to fill. Stopping instead at the
        # first `sufficient` record left 69% of one batch without an abstract
        # that PubMed had for 98.5% of them.
        if self._pubmed_client and identified_items:
            identifiers = [
                item.identifier
                for item in identified_items
                if not _filled(results.get(item.slug))
            ]
            if identifiers:
                logger.info(
                    "Fetching metadata from PubMed for %d articles",
                    len(identifiers),
                )
                sources_checked.append("pubmed")
                pubmed_results = self._get_pubmed_metadata_cached(identifiers)
                # Merge with existing results
                for identifier_slug, pubmed_meta in pubmed_results.items():
                    content = id_to_content.get(identifier_slug)
                    if content is None:
                        continue
                    if not self._has_useful_metadata(pubmed_meta):
                        continue
                    article_slug = content.slug
                    if article_slug in results:
                        results[article_slug] = results[article_slug].merge_from(pubmed_meta)
                    else:
                        results[article_slug] = pubmed_meta
                logger.info(
                    "PubMed returned metadata for %d articles",
                    len(pubmed_results),
                )

        # OpenAlex indexes what the other two miss: preprints, and articles
        # outside PMC that Semantic Scholar has no record of. Of three PDFs in
        # one shared folder that no provider answered for, it had all three.
        if self._openalex_client and identified_items:
            identifiers = [
                item.identifier
                for item in identified_items
                if not _filled(results.get(item.slug))
            ]
            if identifiers:
                logger.info("Fetching metadata from OpenAlex for %d articles", len(identifiers))
                sources_checked.append("openalex")
                for identifier_slug, meta in self._get_openalex_metadata_cached(
                    identifiers
                ).items():
                    content = id_to_content.get(identifier_slug)
                    if content is None or not self._has_useful_metadata(meta):
                        continue
                    if content.slug in results:
                        results[content.slug] = results[content.slug].merge_from(meta)
                    else:
                        results[content.slug] = meta

        # The extractor's own metadata last, to fill what the APIs did not.
        unfilled = [item for item in extracted_contents if not _filled(results.get(item.slug))]
        if unfilled:
            logger.info(
                "Reading extractor metadata for %d articles",
                len(unfilled),
            )
            sources_checked.append("fallback")
            for item in unfilled:
                try:
                    fallback_meta = self._get_fallback_metadata(item)
                    if fallback_meta and self._has_useful_metadata(fallback_meta):
                        if item.slug in results:
                            results[item.slug] = results[item.slug].merge_from(fallback_meta)
                        else:
                            results[item.slug] = fallback_meta
                except Exception as exc:
                    logger.warning(
                        "Failed to extract fallback metadata for %s: %s",
                        item.slug,
                        exc,
                    )

        return results

    def _get_semantic_scholar_metadata_cached(
        self, identifiers: List[Identifier]
    ) -> Dict[str, ArticleMetadata]:
        return self._cached("semantic_scholar", self._s2_client, identifiers)

    def _get_pubmed_metadata_cached(
        self, identifiers: List[Identifier]
    ) -> Dict[str, ArticleMetadata]:
        return self._cached("pubmed", self._pubmed_client, identifiers)

    def _get_openalex_metadata_cached(
        self, identifiers: List[Identifier]
    ) -> Dict[str, ArticleMetadata]:
        return self._cached("openalex", self._openalex_client, identifiers)

    def _cached(
        self, name: str, client, identifiers: List[Identifier]
    ) -> Dict[str, ArticleMetadata]:
        """A provider's metadata, from `<cache>/metadata/<name>/<slug>.json` when held."""
        cache_dir = self.settings.get_cache_dir("metadata") / name
        cache_dir.mkdir(parents=True, exist_ok=True)

        results: Dict[str, ArticleMetadata] = {}
        uncached: List[Identifier] = []
        for identifier in identifiers:
            cache_file = cache_dir / f"{identifier.slug}.json"
            if not cache_file.exists():
                uncached.append(identifier)
                continue
            try:
                data = json.loads(cache_file.read_text(encoding="utf-8"))
                results[identifier.slug] = ArticleMetadata.from_dict(data)
            except Exception as exc:
                logger.warning("Failed to load cached %s metadata for %s: %s",
                               name, identifier.slug, exc)
                uncached.append(identifier)

        if uncached and client:
            try:
                fresh_results = client.get_metadata(uncached)
            except Exception as exc:
                logger.error("%s metadata request failed: %s", name, exc)
                return results
            for slug, metadata in fresh_results.items():
                try:
                    (cache_dir / f"{slug}.json").write_text(
                        json.dumps(metadata.to_dict(), indent=2), encoding="utf-8"
                    )
                except Exception as exc:
                    logger.warning("Failed to cache %s metadata for %s: %s", name, slug, exc)
                results[slug] = metadata

        return results

    def _get_fallback_metadata(
        self, extracted_content: ExtractedContent
    ) -> Optional[ArticleMetadata]:
        """Extract metadata from extractor-specific files."""
        if extracted_content.source == DownloadSource.ELSEVIER:
            return self._get_elsevier_fallback(extracted_content)
        elif extracted_content.source in (DownloadSource.PUBGET, DownloadSource.PMC, DownloadSource.EUROPEPMC):
            return self._get_pubget_fallback(extracted_content)
        elif extracted_content.source == DownloadSource.ACE:
            # ACE doesn't provide reliable metadata
            return None
        return None

    @staticmethod
    def _has_useful_metadata(metadata: ArticleMetadata) -> bool:
        """Return True when metadata contains at least one meaningful field."""
        title_present = bool(metadata.title and metadata.title.strip())
        return any(
            [
                title_present,
                bool(metadata.authors),
                bool(metadata.abstract),
                bool(metadata.journal),
                metadata.publication_year is not None,
                bool(metadata.keywords),
                bool(metadata.license),
                metadata.source is not None,
                metadata.open_access is not None,
            ]
        )

    def _get_elsevier_fallback(
        self, extracted_content: ExtractedContent
    ) -> Optional[ArticleMetadata]:
        """Extract metadata from Elsevier metadata.json file."""
        # Find the metadata.json file from the download
        candidate_files: List[Path] = []

        if extracted_content.full_text_path:
            article_dir = extracted_content.full_text_path.parent
            candidate_files.append(article_dir / "metadata.json")
            candidate_files.append(article_dir.parent / "metadata.json")

        if extracted_content.identifier:
            identifier_slug = extracted_content.identifier.slug.strip()
            if identifier_slug:
                digest = hashlib.sha256(identifier_slug.encode("utf-8")).hexdigest()[:32]
                base_dir = (
                    self.settings.elsevier_cache_root
                    if self.settings.elsevier_cache_root is not None
                    else self.settings.cache_root / "elsevier"
                )
                candidate_files.append(base_dir / digest / "metadata.json")

        metadata_file = next(
            (path for path in candidate_files if path.exists()),
            None,
        )

        if metadata_file is None:
            return None

        try:
            data = json.loads(metadata_file.read_text(encoding="utf-8"))

            # Elsevier metadata structure varies, extract what we can
            title = None
            authors: List[Author] = []
            abstract = None
            journal = None
            year = None

            # Try to extract fields if they exist
            if "title" in data:
                title = str(data["title"])

            if not title:
                title = data.get("articleTitle")

            # Year from publication date
            if "publication_date" in data:
                try:
                    pub_date = str(data["publication_date"])
                    year = int(pub_date.split("-")[0])
                except (ValueError, IndexError):
                    pass
            elif data.get("coverDate"):
                try:
                    year = int(str(data["coverDate"]).split("-")[0])
                except (ValueError, IndexError):
                    pass

            journal = data.get("publicationName") or data.get("journal")

            author_entries = data.get("authors") or []
            if isinstance(author_entries, list):
                for author in author_entries:
                    given = author.get("given-name") or author.get("given")
                    family = author.get("surname") or author.get("family")
                    name_parts = [part for part in (given, family) if part]
                    if name_parts:
                        authors.append(Author(name=" ".join(name_parts)))
                    elif author.get("name"):
                        authors.append(Author(name=str(author["name"])))

            if not title:
                # No title means this isn't useful metadata
                return None

            return ArticleMetadata(
                title=title,
                authors=authors,
                abstract=abstract,
                journal=journal,
                publication_year=year,
                source="elsevier_fallback",
                raw_metadata={"elsevier": data},
            )
        except Exception as exc:
            logger.warning(
                "Failed to parse Elsevier metadata for %s: %s",
                extracted_content.slug,
                exc,
            )
            return None

    def _get_pubget_fallback(
        self, extracted_content: ExtractedContent
    ) -> Optional[ArticleMetadata]:
        """Extract metadata from Pubget article.xml file."""
        candidate_files: List[Path] = []

        if extracted_content.full_text_path:
            article_dir = extracted_content.full_text_path.parent
            candidate_files.append(article_dir.parent / "article.xml")
            candidate_files.append(article_dir / "article.xml")

        if extracted_content.identifier and extracted_content.identifier.pmcid:
            pmcid_value = extracted_content.identifier.pmcid.strip().upper()
            if pmcid_value.startswith("PMC"):
                pmcid_value = pmcid_value[3:]

            # Normalize to the canonical integer format used on disk (removes leading zeros).
            if pmcid_value.isdigit():
                pmcid_value = str(int(pmcid_value))

            # PMC and Europe PMC lay their XML out as pubget does, under their own root
            source = extracted_content.source.value
            configured = getattr(self.settings, f"{source}_cache_root", None)
            base_dir = configured if configured is not None else self.settings.cache_root / source

            bucket = article_bucket_from_pmcid(int(pmcid_value))
            pmcid_dir = f"pmcid_{pmcid_value}"

            # Check the common layouts without a recursive glob to avoid walking the entire tree.
            candidate_paths: list[Path] = [
                base_dir / "articles" / bucket / pmcid_dir / "article.xml",
                base_dir / bucket / pmcid_dir / "article.xml",
            ]

            if base_dir.exists():
                for subdir in base_dir.iterdir():
                    if not subdir.is_dir():
                        continue
                    if not subdir.name.startswith(("pmcidList_", "query_")):
                        continue
                    candidate_paths.append(
                        subdir / "articles" / bucket / pmcid_dir / "article.xml"
                    )

            # Preserve earlier ordering while avoiding duplicates.
            seen: set[Path] = set()
            for path in candidate_paths:
                if path in seen:
                    continue
                seen.add(path)
                candidate_files.append(path)

        article_xml = next(
            (path for path in candidate_files if path.exists()),
            None,
        )

        if article_xml is None:
            return None

        try:
            tree = etree.parse(str(article_xml))
            root = tree.getroot()

            # Extract title
            title_elem = root.find(".//article-title")
            title = " ".join(title_elem.itertext()).strip() if title_elem is not None else None

            # Extract authors
            authors: List[Author] = []
            for contrib in root.findall(".//contrib"):
                contrib_type = (contrib.get("contrib-type") or "").strip()
                if contrib_type and contrib_type != "author":
                    continue

                given = contrib.findtext(".//given-names")
                surname = contrib.findtext(".//surname")
                if surname:
                    name = f"{given} {surname}" if given else surname
                    authors.append(Author(name=name))
                    continue

                collab = contrib.findtext(".//collab")
                if collab:
                    authors.append(Author(name=str(collab)))

            # Extract abstract
            abstract_elem = root.find(".//abstract")
            abstract = (
                " ".join(abstract_elem.itertext()).strip() if abstract_elem is not None else None
            )

            # Extract journal
            journal_elem = root.find(".//journal-title")
            journal = (
                " ".join(journal_elem.itertext()).strip() if journal_elem is not None else None
            )

            # Extract year
            year = None
            pub_date = root.find(".//pub-date[@pub-type='epub']")
            if pub_date is None:
                pub_date = root.find(".//pub-date")
            if pub_date is not None:
                year_elem = pub_date.find("year")
                if year_elem is not None and year_elem.text:
                    try:
                        year = int(year_elem.text)
                    except ValueError:
                        pass

            if not title:
                return None

            return ArticleMetadata(
                title=title,
                authors=authors,
                abstract=abstract,
                journal=journal,
                publication_year=year,
                source="pubget_fallback",
                raw_metadata={},
            )
        except Exception as exc:
            logger.warning(
                "Failed to parse Pubget article XML for %s: %s",
                extracted_content.slug,
                exc,
            )
            return None


#: What a later provider is still asked for. Keywords, license and open
#: access are left out: no provider fills them reliably, so demanding them
#: sent every article to every provider.
FILLED = ("title", "abstract", "journal", "publication_year", "authors")


def _filled(metadata: Optional[ArticleMetadata]) -> bool:
    """Nothing left for a later provider to add."""
    if metadata is None:
        return False
    for name in FILLED:
        value = getattr(metadata, name, None)
        if value is None or (isinstance(value, (str, list)) and not value):
            return False
        if isinstance(value, str) and not value.strip():
            return False
    return True
