"""
This module will grab metadata for articles
given a list of article ids.
The metadata will be retrieved from the following sources
in this order:
1. Semantic Scholar
2. PubMed
3. fallback to processed metadata from extractors
   (information from the downloaded article)
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from lxml import etree

from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from pubget._utils import article_bucket_from_pmcid


logger = logging.getLogger(__name__)


class MetadataService:
    """
    Service for enriching ExtractedContent with article metadata.

    Coordinates metadata fetching from multiple sources:
    1. Semantic Scholar (if API key available)
    2. PubMed (if email configured)
    3. Fallback to extractor-specific files
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._s2_client: Optional[SemanticScholarClient] = None
        self._pubmed_client: Optional[PubMedClient] = None

        # Initialize clients if credentials available
        if settings.semantic_scholar_api_key:
            self._s2_client = SemanticScholarClient(settings.semantic_scholar_api_key)

        if settings.pubmed_email:
            self._pubmed_client = PubMedClient(
                email=settings.pubmed_email,
                api_key=settings.pubmed_api_key,
            )

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

        # Try Semantic Scholar first
        if self._s2_client and identified_items:
            logger.info(
                "Fetching metadata from Semantic Scholar for %d articles",
                len(identified_items),
            )
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

        # Try PubMed for remaining items
        if self._pubmed_client and identified_items:
            missing = [
                item.identifier
                for item in identified_items
                if self._needs_more_metadata(results.get(item.slug))
            ]
            if missing:
                logger.info(
                    "Fetching metadata from PubMed for %d articles",
                    len(missing),
                )
                pubmed_results = self._get_pubmed_metadata_cached(missing)
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

        # Fallback to extractor metadata for remaining items
        still_missing = [
            item for item in extracted_contents if self._needs_more_metadata(results.get(item.slug))
        ]
        if still_missing:
            logger.info(
                "Falling back to extractor metadata for %d articles",
                len(still_missing),
            )
            for item in still_missing:
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
        """Fetch S2 metadata with caching."""
        metadata_dir = self.settings.get_cache_dir("metadata")
        cache_dir = metadata_dir / "semantic_scholar"
        cache_dir.mkdir(parents=True, exist_ok=True)

        results: Dict[str, ArticleMetadata] = {}
        uncached: List[Identifier] = []

        # Check cache first
        for identifier in identifiers:
            cache_file = cache_dir / f"{identifier.slug}.json"
            if cache_file.exists():
                try:
                    data = json.loads(cache_file.read_text(encoding="utf-8"))
                    results[identifier.slug] = ArticleMetadata.from_dict(data)
                except Exception as exc:
                    logger.warning(
                        "Failed to load cached S2 metadata for %s: %s",
                        identifier.slug,
                        exc,
                    )
                    uncached.append(identifier)
            else:
                uncached.append(identifier)

        # Fetch uncached items
        if uncached and self._s2_client:
            try:
                fresh_results = self._s2_client.get_metadata(uncached)
                # Cache and add to results
                for slug, metadata in fresh_results.items():
                    cache_file = cache_dir / f"{slug}.json"
                    try:
                        cache_file.write_text(
                            json.dumps(metadata.to_dict(), indent=2),
                            encoding="utf-8",
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to cache S2 metadata for %s: %s",
                            slug,
                            exc,
                        )
                    results[slug] = metadata
            except Exception as exc:
                logger.error("Semantic Scholar metadata request failed: %s", exc)

        return results

    def _get_pubmed_metadata_cached(
        self, identifiers: List[Identifier]
    ) -> Dict[str, ArticleMetadata]:
        """Fetch PubMed metadata with caching."""
        cache_dir = self.settings.get_cache_dir("metadata") / "pubmed"
        cache_dir.mkdir(parents=True, exist_ok=True)

        results: Dict[str, ArticleMetadata] = {}
        uncached: List[Identifier] = []

        # Check cache first
        for identifier in identifiers:
            cache_file = cache_dir / f"{identifier.slug}.json"
            if cache_file.exists():
                try:
                    data = json.loads(cache_file.read_text(encoding="utf-8"))
                    results[identifier.slug] = ArticleMetadata.from_dict(data)
                except Exception as exc:
                    logger.warning(
                        "Failed to load cached PubMed metadata for %s: %s",
                        identifier.slug,
                        exc,
                    )
                    uncached.append(identifier)
            else:
                uncached.append(identifier)

        # Fetch uncached items
        if uncached and self._pubmed_client:
            try:
                fresh_results = self._pubmed_client.get_metadata(uncached)
                # Cache and add to results
                for slug, metadata in fresh_results.items():
                    cache_file = cache_dir / f"{slug}.json"
                    try:
                        cache_file.write_text(
                            json.dumps(metadata.to_dict(), indent=2),
                            encoding="utf-8",
                        )
                    except Exception as exc:
                        logger.warning(
                            "Failed to cache PubMed metadata for %s: %s",
                            slug,
                            exc,
                        )
                    results[slug] = metadata
            except Exception as exc:
                logger.error("PubMed metadata request failed: %s", exc)

        return results

    def _get_fallback_metadata(
        self, extracted_content: ExtractedContent
    ) -> Optional[ArticleMetadata]:
        """Extract metadata from extractor-specific files."""
        if extracted_content.source == DownloadSource.ELSEVIER:
            return self._get_elsevier_fallback(extracted_content)
        elif extracted_content.source == DownloadSource.PUBGET:
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

    @classmethod
    def _is_complete(cls, metadata: ArticleMetadata) -> bool:
        """Return True when all primary metadata attributes are populated."""
        return all(
            [
                bool(metadata.title and metadata.title.strip()),
                bool(metadata.authors),
                bool(metadata.abstract and metadata.abstract.strip()),
                bool(metadata.journal and metadata.journal.strip()),
                metadata.publication_year is not None,
                bool(metadata.keywords),
                bool(metadata.license and metadata.license.strip()),
                bool(metadata.source and metadata.source.strip()),
                metadata.open_access is not None,
            ]
        )

    @classmethod
    def _needs_more_metadata(cls, metadata: Optional[ArticleMetadata]) -> bool:
        if metadata is None:
            return True
        return not cls._is_complete(metadata)

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

            base_dir = (
                self.settings.pubget_cache_root
                if self.settings.pubget_cache_root is not None
                else self.settings.cache_root / "pubget"
            )

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
