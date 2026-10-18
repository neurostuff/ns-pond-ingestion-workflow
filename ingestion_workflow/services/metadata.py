"""
This module will grab metadata for articles
given a list of article ids.
The metadata will be retrieved from the following sources
in this order:
1. Semantic Scholar
2. PubMed
3. Crossref (licence deposits only)
4. OpenAlex
5. fallback to processed metadata from extractors
   (information from the downloaded article)

Every article returned also gets its licences (`services.licenses`): one for each copy a
download holds, one for the paper, and one for each copy a provider lists that no
download holds. Semantic Scholar, Crossref and OpenAlex are asked for every article,
since each can state a licence, and so is each held copy's own record.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Set
from urllib.parse import urlsplit

from lxml import etree
from pubget._utils import article_bucket_from_pmcid
from study_schema.models.paper_parse import (
    CopyRoute,
    CopyVersion,
    License,
    MetadataSource,
    OaStatus,
    ReportedCopy,
)

from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.openalex import OpenAlexClient, _locations_from
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import OPENALEX_VERSIONS, located
from ingestion_workflow.extractors.pdf_extractor import _type_of
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models.download import DownloadCopy, DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.services.citations import JATS_SOURCES
from ingestion_workflow.services.licenses import (
    LICENSE_PROVIDERS,
    OWN,
    Statement,
    Undecided,
    crossref_statements,
    license_of,
    own_license,
    stated,
)
from ingestion_workflow.utils.preprints import is_preprint_doi

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeldDownload:
    """One ok download of an article: its source, the copy it holds, and its files."""

    source: str
    copy: DownloadCopy
    files: Sequence[Path] = ()


class MetadataService:
    """
    Service for enriching ExtractedContent with article metadata.

    Coordinates metadata fetching from multiple sources:
    1. Semantic Scholar (if API key available)
    2. PubMed (if email configured)
    3. Crossref, for its licence deposits
    4. OpenAlex (if email or key configured)
    5. Fallback to extractor-specific files
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._s2_client: Optional[SemanticScholarClient] = None
        self._pubmed_client: Optional[PubMedClient] = None
        self._openalex_client: Optional[OpenAlexClient] = None
        self._crossref_client = CrossrefClient(settings.crossref_email)
        #: Article slug -> why its licences could not be decided, for the last batch.
        self.undecided: Dict[str, str] = {}

        # Initialize clients if credentials available
        if settings.semantic_scholar_api_key:
            self._s2_client = SemanticScholarClient(settings.semantic_scholar_api_key)

        if settings.pubmed_email:
            self._pubmed_client = PubMedClient(
                email=settings.pubmed_email,
                api_key=settings.pubmed_api_key,
            )

        self._openalex_client = OpenAlexClient.from_settings(settings)

    def enrich_metadata(
        self,
        extracted_contents: List[ExtractedContent],
        held: Optional[Mapping[str, Sequence[HeldDownload]]] = None,
    ) -> Dict[str, ArticleMetadata]:
        """
        Enrich extracted contents with article metadata.

        Parameters
        ----------
        extracted_contents : list of ExtractedContent
            Content objects to enrich with metadata
        held : mapping of slug to HeldDownload, optional
            Each article's ok downloads, whose copies get a licence each

        Returns
        -------
        dict
            Mapping from slug to ArticleMetadata, each with its licences decided,
            except an article `undecided` names: a source that could state one of its
            licences did not answer, and `undecided` says which.
        """
        self.undecided = {}
        if not extracted_contents:
            return {}

        identified_items = [item for item in extracted_contents if item.identifier]

        id_to_content = {item.identifier.slug: item for item in identified_items}

        results: Dict[str, ArticleMetadata] = {}
        # Article slug -> each provider's record, for the licences.
        answers: Dict[str, Dict[str, ArticleMetadata]] = {}
        # Article slug -> each licence provider that was not asked or did not answer, and why.
        unanswered: Dict[str, Dict[str, str]] = {}

        def ask(name: str, fetch, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
            found = fetch(identifiers, unanswered)
            for identifier_slug, metadata in found.items():
                content = id_to_content.get(identifier_slug)
                if content is not None:
                    answers.setdefault(content.slug, {})[name] = metadata
            return found

        def merge(found: Dict[str, ArticleMetadata]) -> None:
            for identifier_slug, metadata in found.items():
                content = id_to_content.get(identifier_slug)
                if content is None or not self._has_useful_metadata(metadata):
                    continue
                if content.slug in results:
                    results[content.slug] = results[content.slug].merge_from(metadata)
                else:
                    results[content.slug] = metadata

        # Try Semantic Scholar first
        if identified_items:
            logger.info(
                "Fetching metadata from Semantic Scholar for %d articles",
                len(identified_items),
            )
            s2_results = ask(MetadataSource.semantic_scholar.value,
                             self._get_semantic_scholar_metadata_cached,
                             [item.identifier for item in identified_items])
            merge(s2_results)
            logger.info(
                "Semantic Scholar returned metadata for %d articles",
                len(s2_results),
            )

        # Each provider fills what the earlier ones left empty, and is skipped
        # only for an article with nothing left to fill. Stopping instead at the
        # first `sufficient` record left 69% of one batch without an abstract
        # that PubMed had for 98.5% of them. PubMed states no licence.
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
                pubmed_results = ask(MetadataSource.pubmed.value,
                                     self._get_pubmed_metadata_cached, identifiers)
                merge(pubmed_results)
                logger.info(
                    "PubMed returned metadata for %d articles",
                    len(pubmed_results),
                )

        # Crossref holds the licences the publisher deposits; a work without a DOI has
        # no Crossref record to state one.
        with_doi = [item.identifier for item in identified_items if item.identifier.doi]
        if with_doi:
            logger.info("Fetching licences from Crossref for %d articles", len(with_doi))
            ask(MetadataSource.crossref.value, self._get_crossref_metadata_cached, with_doi)

        # OpenAlex indexes what the other two miss: preprints, and articles
        # outside PMC that Semantic Scholar has no record of. Of three PDFs in
        # one shared folder that no provider answered for, it had all three.
        # It is asked about every article for the copies it lists and their
        # licences, and fills only what the others left empty.
        if identified_items:
            logger.info("Fetching metadata from OpenAlex for %d articles", len(identified_items))
            openalex_results = ask(MetadataSource.openalex.value,
                                   self._get_openalex_metadata_cached,
                                   [item.identifier for item in identified_items])
            merge({slug: meta for slug, meta in openalex_results.items()
                   if not _filled(results.get(id_to_content[slug].slug)
                                  if slug in id_to_content else None)})

        # The extractor's own metadata last, to fill what the APIs did not.
        unfilled = [item for item in extracted_contents if not _filled(results.get(item.slug))]
        if unfilled:
            logger.info(
                "Reading extractor metadata for %d articles",
                len(unfilled),
            )
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

        by_slug = {item.slug: item for item in extracted_contents}
        for slug, metadata in results.items():
            item = by_slug[slug]
            replies = answers.get(slug, {})
            for name in (MetadataSource.crossref.value, MetadataSource.openalex.value):
                if name in replies:
                    metadata.raw_metadata[name] = replies[name].raw_metadata.get(name)
            try:
                _decide_licenses(metadata, item.identifier, replies,
                                 (held or {}).get(slug, ()),
                                 unanswered.get(item.identifier.slug if item.identifier
                                                else slug, {}))
            except Undecided as exc:
                self.undecided[slug] = str(exc)
        return results

    def _get_semantic_scholar_metadata_cached(
        self, identifiers: List[Identifier], unanswered: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, ArticleMetadata]:
        return self._cached(MetadataSource.semantic_scholar.value, self._s2_client,
                            identifiers, unanswered)

    def _get_pubmed_metadata_cached(
        self, identifiers: List[Identifier], unanswered: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, ArticleMetadata]:
        return self._cached(MetadataSource.pubmed.value, self._pubmed_client, identifiers,
                            unanswered)

    def _get_crossref_metadata_cached(
        self, identifiers: List[Identifier], unanswered: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, ArticleMetadata]:
        return self._cached(MetadataSource.crossref.value, self._crossref_client, identifiers,
                            unanswered)

    def _get_openalex_metadata_cached(
        self, identifiers: List[Identifier], unanswered: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, ArticleMetadata]:
        return self._cached(MetadataSource.openalex.value, self._openalex_client, identifiers,
                            unanswered)

    def _cached(
        self, name: str, client, identifiers: List[Identifier],
        unanswered: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> Dict[str, ArticleMetadata]:
        """A provider's metadata, from `<cache>/metadata/<dir>/<slug>.json` when held.

        A licence provider that is not configured, or whose request fails, is noted in
        `unanswered` for each article it did not answer for.
        """
        cache_dir = self.settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS[name]
        cache_dir.mkdir(parents=True, exist_ok=True)

        def missed(slugs, why: str) -> None:
            if unanswered is not None and name in _LICENSE_PROVIDERS:
                for slug in slugs:
                    unanswered.setdefault(slug, {})[name] = why

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

        if uncached and client is None:
            missed([identifier.slug for identifier in uncached], "is not configured")
        elif uncached:
            try:
                fresh_results = client.get_metadata(uncached)
            except Exception as exc:
                logger.error("%s metadata request failed: %s", name, exc)
                fresh_results = exc.found if isinstance(exc, PartialAnswer) else {}
                missed([i.slug for i in uncached if i.slug not in fresh_results],
                       "did not answer")
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
        elif extracted_content.source.value in JATS_SOURCES:
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
                metadata.source is not None,
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
                source=MetadataSource.elsevier.value,
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

        # PMC and Europe PMC lay their XML out as pubget does, under their own root;
        # Springer Nature, whose articles may have no PMCID, by the article's slug.
        source = extracted_content.source.value
        configured = getattr(self.settings, f"{source}_cache_root", None)
        base_dir = configured if configured is not None else self.settings.cache_root / source
        candidate_files.append(base_dir / safe_hash_stem(extracted_content.slug) / "article.xml")

        if extracted_content.identifier and extracted_content.identifier.pmcid:
            pmcid_value = extracted_content.identifier.pmcid.removeprefix("PMC")
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
                source=MetadataSource.jats.value,
                raw_metadata={},
            )
        except Exception as exc:
            logger.warning(
                "Failed to parse Pubget article XML for %s: %s",
                extracted_content.slug,
                exc,
            )
            return None


#: What a later provider is still asked for. Keywords are left out: no
#: provider fills them reliably, so demanding them sent every article to
#: every provider.
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


#: Each provider's cache under `<cache>/metadata/`. The cache holds parsed records, so a
#: record that keeps more of the response (OpenAlex's locations) needs a directory of its own.
PROVIDER_CACHE_DIRS = {
    MetadataSource.semantic_scholar.value: "semantic_scholar",
    MetadataSource.pubmed.value: "pubmed",
    MetadataSource.crossref.value: "crossref-licenses",
    MetadataSource.openalex.value: "openalex-locations",
}

_LICENSE_PROVIDERS = {source.value for source in LICENSE_PROVIDERS}
_OA_STATUSES = {status.value for status in OaStatus}
#: OpenAlex lists the paper's PubMed record as a location; it is an index entry, not a copy.
_PUBMED = "pubmed.ncbi.nlm.nih.gov"
#: The providers that list copies at URLs, so may state the licence of a copy at one.
_LOCATING = {MetadataSource.openalex.value, MetadataSource.semantic_scholar.value}


def _openalex_statements(raw: Optional[Dict]) -> List[Statement]:
    """A statement for each OpenAlex location with a licence, the primary one first."""
    raw = raw or {}
    listed = [raw.get("primary_location"), raw.get("best_oa_location"),
              *(raw.get("locations") or [])]
    out: List[Statement] = []
    for location in listed:
        if not isinstance(location, dict):
            continue
        found = stated(MetadataSource.openalex, location.get("license"))
        url = location.get("pdf_url") or location.get("landing_page_url")
        if found is not None:
            out.append(Statement(found, OPENALEX_VERSIONS.get(str(location.get("version"))),
                                 str(url) if url else None))
    return out


def _s2_pdf(raw: Optional[Dict]) -> Dict:
    pdf = (raw or {}).get("openAccessPdf")
    return pdf if isinstance(pdf, dict) else {}


def _decide_licenses(
    metadata: ArticleMetadata,
    identifier: Optional[Identifier],
    replies: Mapping[str, ArticleMetadata],
    held: Sequence[HeldDownload],
    unanswered: Mapping[str, str],
) -> None:
    """Set the licence of each held copy, of the paper and of each reported copy, and
    `oa_status`, from every provider's record. Raises Undecided."""
    doi = identifier.doi if identifier else None
    raw = {name: (record.raw_metadata or {}).get(name) for name, record in replies.items()}
    waiting = set(unanswered)
    preprint = is_preprint_doi(doi) and not any(h.copy.version is CopyVersion.vor for h in held)

    crossref = crossref_statements((raw.get(MetadataSource.crossref.value) or {}).get("license"))
    if preprint:
        # A preprint DOI's record is the preprint's, whatever version Crossref calls it.
        crossref = [Statement(s.license, CopyVersion.submitted if s.version is CopyVersion.vor
                              else s.version) for s in crossref]
    s2_pdf = _s2_pdf(raw.get(MetadataSource.semantic_scholar.value))
    s2_license = stated(MetadataSource.semantic_scholar, s2_pdf.get("license"))
    statements = {
        MetadataSource.crossref: crossref,
        MetadataSource.openalex: _openalex_statements(raw.get(MetadataSource.openalex.value)),
        MetadataSource.semantic_scholar: (
            [Statement(s2_license, None, s2_pdf.get("url"))] if s2_license else []),
    }

    def at(url: Optional[str]) -> Optional[License]:
        """The licence a provider states for the copy at `url`."""
        return next((s.license for listed in statements.values() for s in listed
                     if url and s.url == url), None)

    def decide(what: str, version, own: Optional[License], unread: bool,
               url: Optional[str] = None) -> License:
        waits = set(waiting)
        if unread or (own is None and url and waiting & _LOCATING):
            waits.add(OWN)
        try:
            return license_of(version, own, statements, waits)
        except Undecided as exc:
            why = [f"{name} {reason}" for name, reason in sorted(unanswered.items())]
            why += ["its own record could not be read"] if unread else []
            raise Undecided(f"the licence of {what} is undecided: "
                            f"{'; '.join(why) or exc}") from exc

    paper_version = CopyVersion.submitted if preprint else CopyVersion.vor
    paper_own: Optional[License] = None
    paper_unread = False
    held_urls: Set[str] = set()
    for download in held:
        try:
            own = own_license(download.source, download.files)
            unread = False
        except Exception as exc:  # noqa: BLE001 - recorded as undecided, retried
            logger.warning("Failed to read the %s copy's licence: %s", download.source, exc)
            own, unread = None, True
        own = own or at(download.copy.url)
        if download.copy.url:
            held_urls.add(download.copy.url)
        metadata.copy_licenses[download.source] = decide(
            f"the {download.source} copy", download.copy.version, own, unread, download.copy.url)
        if download.copy.version is paper_version:
            paper_own = paper_own or own
            paper_unread = paper_unread or unread
    metadata.license = decide("the paper", paper_version, paper_own,
                              paper_unread and not paper_own)

    # A copy at PMC or at the publisher is the one a download from there holds.
    held_hosts = {h.copy.route for h in held} & {CopyRoute.pmc, CopyRoute.publisher}
    oa = raw.get(MetadataSource.openalex.value) or {}
    listed = [(loc.pdf_url or loc.landing_page_url, OPENALEX_VERSIONS.get(str(loc.version)),
               _type_of(loc.source_type), MetadataSource.openalex)
              for loc in _locations_from(oa)]
    if s2_pdf.get("url") and s2_pdf["url"] not in {url for url, *_ in listed}:
        listed.append((s2_pdf["url"], None, None, MetadataSource.semantic_scholar))
    for url, version, location_type, by in listed:
        route, version, server = located(url, doi, version, location_type)
        if url in held_urls or route in held_hosts or urlsplit(url).hostname == _PUBMED:
            continue
        metadata.reported_copies.append(ReportedCopy(
            route=route, version=version, url=url, preprint_server=server, reported_by=by,
            license=decide(f"the copy at {url}", version, at(url), False, url)))

    statuses = ((oa.get("open_access") or {}).get("oa_status"), str(s2_pdf.get("status")).lower())
    metadata.oa_status = next((s for s in statuses if s in _OA_STATUSES), None)
