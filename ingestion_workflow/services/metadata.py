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
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
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

from ingestion_workflow.catalog import Notes
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.openalex import OpenAlexClient, _locations_from
from ingestion_workflow.clients.orcid import OrcidClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.copies import OPENALEX_VERSIONS, located
from ingestion_workflow.extractors.pdf_extractor import _type_of
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models.download import DownloadCopy, DownloadSource
from ingestion_workflow.models.extract import ExtractedContent
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.models.metadata import (
    ArticleMetadata,
    Author,
    CorrespondingAuthor,
    missing_fields,
)
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.services import corresponding_authors
from ingestion_workflow.services.citations import JATS_SOURCES
from ingestion_workflow.services.corresponding_authors.common import surname
from ingestion_workflow.services.licenses import (
    ASSUMED,
    HELD,
    OWN,
    PAGE,
    Statement,
    Undecided,
    crossref_embargo_end,
    crossref_statements,
    license_of,
    own_license,
    page_embargo,
    page_statements,
    stated,
)
from ingestion_workflow.services.paper_parse import article_identifiers
from ingestion_workflow.utils.http import transient
from ingestion_workflow.utils.isolation import error_text
from ingestion_workflow.utils.preprints import is_preprint_doi

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HeldDownload:
    """One ok download of an article: its source, the copy it holds, its files, and the
    text its extraction gave."""

    source: str
    copy: DownloadCopy
    files: Sequence[Path] = ()
    text: Optional[Path] = None


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
        self._orcid_client = OrcidClient()
        #: Article slug -> the catalog conflict it was not asked about for, for the last batch.
        self.conflicts: Dict[str, str] = {}
        #: Article slug -> what its record lacks and why, for the last batch.
        self.notes: Dict[str, Notes] = {}
        #: Article slug -> the cache files its metadata was made from (a provider's record of
        #: each identifier asked, an author's ORCID record), for the last batch.
        self.records_read: Dict[str, List[Path]] = {}

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
        held: Mapping[str, Sequence[HeldDownload]],
        dois: Mapping[str, Sequence[str]],
        now: datetime,
    ) -> Dict[str, ArticleMetadata]:
        """
        Enrich extracted contents with article metadata.

        Parameters
        ----------
        extracted_contents : list of ExtractedContent
            Content objects to enrich with metadata
        held : mapping of slug to HeldDownload
            Each article's ok downloads, in the order the download sources are tried;
            each copy gets a licence
        dois : mapping of slug to DOIs
            Every DOI the catalog holds for each article. Crossref and OpenAlex are asked
            by each: a journal's DOI for the version of record, a preprint server's for
            the preprint. An article with two journal DOIs is a catalog conflict: it is
            not asked about, and `conflicts` says why.
        now : datetime
            The time the licence statements are read at: one starting later is embargoed,
            and the earliest such start is recorded as `license_embargo_until`.

        Returns
        -------
        dict
            Mapping from slug to ArticleMetadata, each with its licences decided. What a
            record lacks for an error is noted in `notes`: a licence a source that could
            state it did not answer for, or that could not be read, is assumed all rights
            reserved; authors that could not be decided are left empty.
        """
        self.require_licence_providers()
        self.conflicts = {}
        self.notes = {}
        self.records_read = {}
        if not extracted_contents:
            return {}

        identified_items = []
        for item in extracted_contents:
            if not item.identifier:
                continue
            published = sorted(d for d in dois[item.slug] if not is_preprint_doi(d))
            if len(published) > 1:
                self.conflicts[item.slug] = (
                    f"catalog conflict: the article holds two DOIs of a version of record, "
                    f"{' and '.join(published)}; its aliases need repair")
                continue
            identified_items.append(item)
        extracted_contents = [item for item in extracted_contents
                              if item.slug not in self.conflicts]

        id_to_content = {item.identifier.slug: item for item in identified_items}

        results: Dict[str, ArticleMetadata] = {}
        # Article slug -> each provider's record, for the licences.
        answers: Dict[str, Dict[str, ArticleMetadata]] = {}
        # Asked identifier slug -> each licence provider that was not asked or did not
        # answer for it, and why.
        unanswered: Dict[str, Dict[str, str]] = {}

        def ask(name: str, fetch, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
            found = fetch(identifiers, unanswered)
            for identifier_slug, metadata in found.items():
                content = id_to_content.get(identifier_slug)
                if content is not None:
                    answers.setdefault(content.slug, {})[name] = metadata
            return found

        # Each article's DOIs, a journal's apart from a preprint server's, and the
        # identifiers Crossref and OpenAlex are asked by for each.
        journal: Dict[str, Optional[str]] = {}
        preprints: Dict[str, List[str]] = {}
        by_doi: Dict[str, List[Tuple[str, Identifier]]] = {}
        for item in identified_items:
            known = sorted({d for d in dois[item.slug] if d})
            published = [d for d in known if not is_preprint_doi(d)]
            journal[item.slug] = published[0] if published else None
            preprints[item.slug] = [d for d in known if is_preprint_doi(d)]
            by_doi[item.slug] = [(d, Identifier(doi=d)) for d in known]

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

        # PubMed is asked about every article, however much Semantic Scholar filled:
        # only its record holds the MeSH publication types, which triage reads to drop
        # meta-analyses and sync to tell a preprint, and the language. It fills the
        # fields as any later provider does. PubMed states no licence.
        if self._pubmed_client and identified_items:
            logger.info("Fetching metadata from PubMed for %d articles", len(identified_items))
            pubmed_results = ask(MetadataSource.pubmed.value, self._get_pubmed_metadata_cached,
                                 [item.identifier for item in identified_items])
            merge(pubmed_results)
            logger.info("PubMed returned metadata for %d articles", len(pubmed_results))

        # Crossref holds the licences the publisher deposits, by DOI; a work without a
        # DOI has no Crossref record to state one.
        asked_dois = [ident for item in identified_items for _, ident in by_doi[item.slug]]
        crossref: Dict[str, ArticleMetadata] = {}
        if asked_dois:
            logger.info("Fetching licences from Crossref for %d DOIs", len(asked_dois))
            crossref = self._get_crossref_metadata_cached(asked_dois, unanswered)

        # OpenAlex indexes what the other two miss: preprints, and articles
        # outside PMC that Semantic Scholar has no record of. Of three PDFs in
        # one shared folder that no provider answered for, it had all three.
        # It is asked about every article for the copies it lists and their
        # licences, and fills only what the others left empty.
        # An article whose presented DOI is a preprint's is asked by its journal's DOI,
        # and each preprint DOI beside a journal's is asked by itself, for its locations.
        openalex: Dict[str, ArticleMetadata] = {}
        if identified_items:
            logger.info("Fetching metadata from OpenAlex for %d articles", len(identified_items))
            main = {item.slug: _by_journal_doi(item.identifier, journal[item.slug])
                    for item in identified_items}
            extra = [ident for item in identified_items for d, ident in by_doi[item.slug]
                     if d != main[item.slug].asking_doi]
            openalex = self._get_openalex_metadata_cached(
                list({i.slug: i for i in [*main.values(), *extra]}.values()), unanswered)
            merge({item.identifier.slug: openalex[main[item.slug].slug]
                   for item in identified_items
                   if main[item.slug].slug in openalex and not _filled(results.get(item.slug))})

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
            notes = self.notes.setdefault(slug, Notes())
            waiting: Dict[str, str] = {}
            try:
                if item.identifier is None:
                    continue
                main_ident = _by_journal_doi(item.identifier, journal[slug])
                self.records_read[slug] = [
                    self._cache_file(MetadataSource.semantic_scholar.value, item.identifier.slug),
                    *([self._cache_file(MetadataSource.pubmed.value, item.identifier.slug)]
                      if self._pubmed_client else []),
                    *(self._cache_file(MetadataSource.crossref.value, i.slug)
                      for _, i in by_doi[slug]),
                    *(self._cache_file(MetadataSource.openalex.value, i)
                      for i in dict.fromkeys([main_ident.slug, *(i.slug for d, i in by_doi[slug]
                                                                 if d != main_ident.asking_doi)]))]
                asked = [item.identifier.slug, main_ident.slug,
                         *(ident.slug for _, ident in by_doi[slug])]
                waiting = {name: why for a in asked for name, why in unanswered.get(a, {}).items()}
                crossref_by_doi = {d: (crossref[i.slug].raw_metadata.get("crossref") or {})
                                   for d, i in by_doi[slug] if i.slug in crossref}
                works = [(openalex[i].raw_metadata.get("openalex") or {})
                         for i in dict.fromkeys([main_ident.slug, *(ident.slug for d, ident
                                                                    in by_doi[slug])])
                         if i in openalex]
                # Read before the record's own raw data is replaced: the record may be OpenAlex's.
                paper_work = (openalex[main_ident.slug].raw_metadata.get("openalex")
                              if main_ident.slug in openalex else None)
                if crossref_by_doi:
                    metadata.raw_metadata["crossref"] = crossref_by_doi
                if works:
                    metadata.raw_metadata["openalex"] = works
                ids = article_identifiers(item.identifier, metadata)
                try:
                    _decide_licenses(metadata, ids, item.identifier.asking_doi, crossref_by_doi,
                                     works, paper_work, answers.get(slug, {}), held.get(slug, ()),
                                     waiting, now, notes)
                except Exception as exc:
                    logger.warning("licences of %s failed: %s", slug, exc)
                    _assume_licenses(metadata, held.get(slug, ()))
                    notes.error("license", f"licences: {error_text(exc)}")
                try:
                    _decide_authors(metadata, answers.get(slug, {}), openalex.get(main_ident.slug),
                                    _decide_corresponding(metadata, held.get(slug, ()),
                                                          openalex.get(main_ident.slug)))
                except Exception as exc:
                    logger.warning("authors of %s failed: %s", slug, exc)
                    metadata.authors, metadata.correspondence_emails = [], []
                    metadata.authors_provider = None
                    notes.error(AUTHOR_FIELDS, f"authors: {error_text(exc)}")
            except Exception as exc:
                # The record stands as the providers gave it, its licences assumed.
                logger.warning("metadata of %s failed: %s", slug, exc)
                _assume_licenses(metadata, held.get(slug, ()))
                notes.error(("license", *AUTHOR_FIELDS), f"metadata: {error_text(exc)}")
                self.records_read.setdefault(slug, [])
            finally:
                if item.identifier is not None:
                    _note_unanswered(notes, metadata, waiting)
        for slug, read in zip(results, self._add_orcid_emails(results.values(),
                                                              [self.notes[s] for s in results])):
            self.records_read.setdefault(slug, []).extend(read)
        return results

    def _cache_file(self, name: str, slug: str) -> Path:
        """Where provider `name`'s record of the identifier with this slug is cached."""
        return (self.settings.get_cache_dir("metadata") / PROVIDER_CACHE_DIRS[name]
                / f"{slug}.json")

    def _add_orcid_emails(self, records: Iterable[ArticleMetadata],
                          notes: Optional[Sequence[Notes]] = None) -> List[List[Path]]:
        """Give each corresponding author no source gave an address the public ones on
        their ORCID record, each iD asked once and cached. A record that cannot be read
        leaves its author without, noted in the article's `notes`. Returns, for each
        record, the cache files it read."""
        cache_dir = self.settings.get_cache_dir("metadata") / ORCID_CACHE_DIR
        cache_dir.mkdir(parents=True, exist_ok=True)
        read: List[List[Path]] = []
        records = list(records)
        for record, noted in zip(records, notes or [Notes() for _ in records]):
            read.append([])
            for author in record.authors:
                if not (author.corresponding and author.orcid and not author.emails):
                    continue
                cache_file = cache_dir / f"{author.orcid}.json"
                read[-1].append(cache_file)
                try:
                    if cache_file.exists():
                        author.emails = json.loads(cache_file.read_text())
                        continue
                    author.emails = self._orcid_client.emails(author.orcid)
                    cache_file.write_text(json.dumps(author.emails))
                except Exception as exc:  # noqa: BLE001 - this author's addresses alone
                    logger.warning("ORCID did not give %s's addresses: %s", author.orcid, exc)
                    noted.error("orcid_emails", f"orcid {author.orcid}: {error_text(exc)}",
                                transient=transient(exc))
        return read

    def require_licence_providers(self) -> None:
        """Raise unless Semantic Scholar and OpenAlex are configured: without either, no
        licence could be assumed, and no listed copy known."""
        missing = [name for name, client in ((MetadataSource.semantic_scholar.value,
                                              self._s2_client),
                                             (MetadataSource.openalex.value,
                                              self._openalex_client)) if client is None]
        if missing:
            raise RuntimeError(f"metadata needs {' and '.join(missing)} configured to decide "
                               "licences")

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

        A provider whose request fails, or whose record of an article cannot be read, is
        noted in `unanswered` for each article it did not answer for.
        """
        cache_dir = self._cache_file(name, "").parent
        cache_dir.mkdir(parents=True, exist_ok=True)

        def missed(slugs, why: str) -> None:
            if unanswered is not None:
                for slug in slugs:
                    unanswered.setdefault(slug, {})[name] = why

        results: Dict[str, ArticleMetadata] = {}
        uncached: List[Identifier] = []
        for identifier in identifiers:
            cache_file = self._cache_file(name, identifier.slug)
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

        if uncached and client is not None:
            try:
                fresh_results = client.get_metadata(uncached)
            except Exception as exc:
                logger.error("%s metadata request failed: %s", name, exc)
                fresh_results = exc.found if isinstance(exc, PartialAnswer) else {}
                causes = exc.causes if isinstance(exc, PartialAnswer) else (exc,)
                unreadable = exc.unreadable if isinstance(exc, PartialAnswer) else {}
                for slug, why in unreadable.items():
                    missed([slug], f"{UNREADABLE} ({why})")
                missed([i.slug for i in uncached
                        if causes and i.slug not in fresh_results and i.slug not in unreadable],
                       OUTAGE if causes and all(map(_outage, causes))
                       else f"refused the request ({causes[0] if causes else exc})")
            for slug, metadata in fresh_results.items():
                try:
                    self._cache_file(name, slug).write_text(
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
#: Each provider's cache, named for what its records hold; a record that comes to hold more
#: is cached anew under a new name. PubMed's and OpenAlex's hold structured authors: ids,
#: every affiliation, OpenAlex's ROR institutions.
PROVIDER_CACHE_DIRS = {
    MetadataSource.semantic_scholar.value: "semantic_scholar",
    MetadataSource.pubmed.value: "pubmed-authors",
    MetadataSource.crossref.value: "crossref-licenses",
    MetadataSource.openalex.value: "openalex-authorships",
}
#: The public addresses on each ORCID record asked about, by iD.
ORCID_CACHE_DIR = "orcid-emails"

#: Why a provider that was down, or busy, left an article: asked again on a later run.
OUTAGE = "did not answer"
#: Why a provider's answer left an article out: its record there could not be read.
UNREADABLE = "could not read its record"
#: The fields an error deciding the authors leaves without.
AUTHOR_FIELDS = ("authors", "affiliations", "corresponding_authors")
#: The fields each provider's record fills, which it not answering may leave without. The
#: licence is noted by the licence decision itself, which knows whose statement it needed.
PROVIDER_FIELDS = {
    MetadataSource.semantic_scholar.value: ("abstract", "journal", "publication_year"),
    MetadataSource.pubmed.value: ("abstract", "authors", "affiliations", "journal",
                                  "publication_year"),
    MetadataSource.openalex.value: ("authors", "affiliations", "ror", "corresponding_authors",
                                    "abstract", "journal", "publication_year"),
    MetadataSource.crossref.value: (),
}


def _note_unanswered(notes: Notes, metadata: ArticleMetadata, unanswered: Mapping[str, str]):
    """Note each provider that did not answer under those of its fields the record lacks."""
    lacking = set(missing_fields(metadata))
    for name, why in sorted(unanswered.items()):
        fields = [f for f in PROVIDER_FIELDS.get(name, ()) if f in lacking]
        notes.error(fields, f"{name} {why}", transient=why == OUTAGE)


def _assume_licenses(metadata: ArticleMetadata, held: Sequence["HeldDownload"]) -> None:
    """Every licence the record states assumed: all rights reserved."""
    metadata.license = ASSUMED
    metadata.copy_licenses = {download.source: ASSUMED for download in held}
    metadata.reported_copies = [replace(copy, license=ASSUMED)
                                for copy in metadata.reported_copies]


def _outage(exc: BaseException) -> bool:
    """A provider that was down or busy, not one that refused the request."""
    return transient(exc)
_OA_STATUSES = {status.value for status in OaStatus}
#: OpenAlex lists the paper's PubMed record as a location; it is an index entry, not a copy.
_PUBMED = "pubmed.ncbi.nlm.nih.gov"
#: The providers that list copies at URLs, so may state the licence of a copy at one.
_LOCATING = {MetadataSource.openalex.value, MetadataSource.semantic_scholar.value}


def _s2_pdf(raw: Optional[Dict]) -> Dict:
    pdf = (raw or {}).get("openAccessPdf")
    return pdf if isinstance(pdf, dict) else {}


def _by_journal_doi(identifier: Identifier, journal: Optional[str]) -> Identifier:
    """The identifier to ask about the version of record by: the article's, with its
    journal's DOI in place of a preprint's."""
    if journal is None or journal == identifier.doi:
        return identifier
    return Identifier(pmid=identifier.pmid, pmcid=identifier.pmcid, doi=journal)


@dataclass(frozen=True)
class _Listed:
    """A copy a provider lists: where it is, which version, and the licence it gives."""

    by: MetadataSource
    url: str
    route: CopyRoute
    version: Optional[CopyVersion]
    server: Optional[str]
    license: Optional[License]


def _listed(by: MetadataSource, url: str, doi: Optional[str], version: Optional[CopyVersion],
            location_type: Optional[str], license: Optional[str]) -> _Listed:
    route, version, server = located(url, doi, version, location_type)
    return _Listed(by, url, route, version, server, stated(by, license))


def _decide_authors(
    metadata: ArticleMetadata,
    replies: Mapping[str, ArticleMetadata],
    openalex: Optional[ArticleMetadata],
    corresponding: Sequence[CorrespondingAuthor],
) -> None:
    """Set the paper's author list, its source, who of them corresponds and the
    correspondence addresses no author takes.

    The list is OpenAlex's, which ties each author to their ids and each affiliation to
    its ROR institutions, else PubMed's, else the merged record's. PubMed's ORCID iDs and
    affiliations fill an author the list leaves without, matched by place and surname.
    """
    pubmed = replies.get(MetadataSource.pubmed.value)
    chosen = next(((source, record.authors)
                   for source, record in ((MetadataSource.openalex.value, openalex),
                                          (MetadataSource.pubmed.value, pubmed))
                   if record is not None and record.authors),
                  (metadata.source, metadata.authors))
    provider, listed = chosen
    if pubmed is not None and provider != MetadataSource.pubmed.value:
        listed = [_filled_from(author, pubmed.authors, place)
                  for place, author in enumerate(listed)]
    metadata.authors, metadata.correspondence_emails = (
        corresponding_authors.mark_corresponding(listed, corresponding))
    metadata.authors_provider = provider if metadata.authors else None


def _filled_from(author: Author, others: Sequence[Author], place: int) -> Author:
    """`author` with the ORCID iD and affiliations it lacks taken from the author at the
    same place in `others`, when that one has the same surname."""
    other = others[place] if place < len(others) else None
    if other is None or surname(other.name) != surname(author.name):
        return author
    return replace(author, orcid=author.orcid or other.orcid,
                   affiliations=author.affiliations or other.affiliations)


def _decide_corresponding(
    metadata: ArticleMetadata,
    held: Sequence[HeldDownload],
    openalex: Optional[ArticleMetadata],
) -> List[CorrespondingAuthor]:
    """The paper's corresponding authors: what the held copies' full text marks, in the
    order their sources are tried, then PubMed's, then OpenAlex's.

    PubMed's are read from its raw record, which its cache has always kept; OpenAlex's
    record keeps only its list, so one cached before the list was kept has none."""
    stated = []
    for download in held:
        try:
            stated.append(corresponding_authors.from_held(download.source, download.files,
                                                          download.text))
        except Exception as exc:  # noqa: BLE001 - a copy that cannot be read names no one
            logger.warning("Failed to read the %s copy's corresponding authors: %s",
                           download.source, exc)
    stated.append(corresponding_authors.from_pubmed(
        metadata.raw_metadata.get(MetadataSource.pubmed.value) or {}))
    if openalex is not None:
        stated.append(openalex.corresponding_authors)
    return corresponding_authors.decide(stated, [a.name for a in metadata.authors])


def _decide_licenses(
    metadata: ArticleMetadata,
    ids,
    doi: Optional[str],
    crossref: Mapping[str, Mapping],
    works: Sequence[Mapping],
    paper_work: Optional[Mapping],
    replies: Mapping[str, ArticleMetadata],
    held: Sequence[HeldDownload],
    unanswered: Mapping[str, str],
    now: datetime,
    notes: Optional[Notes] = None,
) -> None:
    """Set the licence of each held copy, of the paper and of each reported copy, and
    `oa_status`, from every provider's record.

    `ids` are the paper's identifiers (`paper_parse.article_identifiers`), `crossref` each
    DOI's deposits, `works` OpenAlex's works for the article and `paper_work` the one it
    is asked about by (by its journal's DOI when it has one), and `held` the downloads in
    the order their sources are tried. A licence a provider that comes before its deciding
    statement did not answer for, or a record that could not be read, is assumed, and
    noted under `license`; so is a provider that lists copies (OpenAlex, Semantic Scholar)
    not answering, which leaves its copies unknown.
    """
    notes = notes if notes is not None else Notes()

    def outage(sources) -> bool:
        return all(unanswered.get(name) == OUTAGE for name in sources)

    listing = sorted(set(unanswered) & _LOCATING)
    if listing:
        notes.error("license", "the copies the providers list are unknown: " + "; ".join(
            f"{name} {unanswered[name]}" for name in listing), transient=outage(listing))

    listed = [_listed(MetadataSource.openalex, loc.pdf_url or loc.landing_page_url, doi,
                      OPENALEX_VERSIONS.get(str(loc.version)), _type_of(loc.source_type),
                      loc.license)
              for work in works for loc in _locations_from(work)]
    s2 = replies.get(MetadataSource.semantic_scholar.value)
    s2_pdf = _s2_pdf(s2.raw_metadata.get(MetadataSource.semantic_scholar.value) if s2 else None)
    if s2_pdf.get("url"):
        listed.append(_listed(MetadataSource.semantic_scholar, s2_pdf["url"], doi, None, None,
                              s2_pdf.get("license")))
    preprint_dois = {p.doi for p in ids.preprint_dois or []}
    statements: Dict[str, List[Statement]] = {
        MetadataSource.crossref.value: [
            s for d in sorted(crossref) for s in crossref_statements(
                (crossref[d] or {}).get("license"), now, preprint=d in preprint_dois)],
        MetadataSource.openalex.value: [
            Statement(c.license, c.version, c.url, c.route) for c in listed
            if c.by == MetadataSource.openalex and c.license is not None],
        HELD: [],
        PAGE: [],
    }
    waiting = set(unanswered)
    embargoes = [end for d in crossref
                 if (end := crossref_embargo_end((crossref[d] or {}).get("license"), now))]

    def decide(what: str, version, own: Optional[License], waits) -> License:
        try:
            return license_of(version, own, statements, waits)
        except Undecided as exc:
            why = "; ".join(f"{name} {unanswered[name]}" if name in unanswered else
                            "its own record could not be read" for name in exc.sources)
            notes.error("license", f"the licence of {what} is assumed: {why}",
                        transient=outage(exc.sources))
            return ASSUMED

    # Each held copy's own record, read once; a copy fetched from a listed URL carries the
    # licence that listing stated then.
    records: List[Tuple[HeldDownload, Optional[License], bool]] = []
    for download in held:
        try:
            own, unread = own_license(download.source, download.files), False
            ranked = page_statements(download.source, download.files, now)
            embargoes.append(page_embargo(download.source, download.files, now))
        except Exception as exc:  # noqa: BLE001 - the copy's licence is assumed, and noted
            logger.warning("Failed to read the %s copy's licence: %s", download.source, exc)
            own, ranked, unread = None, [], True
        records.append((download, own, unread))
        if own is not None and own.stated_by != MetadataSource.publisher_page:
            statements[HELD].append(Statement(own, download.copy.version))
        statements[PAGE] += ranked
    metadata.license_embargo_until = min((end for end in embargoes if end), default=None)
    # Every record is read before any copy is decided, so that each copy can take another's.
    for download, own, unread in records:
        listing = download.copy
        if own is None and listing.license:
            own = stated(MetadataSource(listing.license_by), listing.license)
        metadata.copy_licenses[download.source] = decide(
            f"the {download.source} copy", download.copy.version, own,
            waiting | ({OWN} if unread else set()))

    paper_version = (CopyVersion.submitted if ids.publication_status == "preprint"
                     else CopyVersion.vor)
    metadata.license = decide("the paper", paper_version, None, waiting)

    held_urls = {h.copy.url for h in held if h.copy.url}
    # A copy at PMC or at the publisher is the one a download from there holds.
    held_hosts = {h.copy.route for h in held} & {CopyRoute.pmc, CopyRoute.publisher}
    seen: Set[str] = set()
    for copy in listed:
        if (copy.url in seen or copy.url in held_urls or copy.route in held_hosts
                or urlsplit(copy.url).hostname == _PUBMED):
            continue
        seen.add(copy.url)
        metadata.reported_copies.append(ReportedCopy(
            route=copy.route, version=copy.version, url=copy.url, preprint_server=copy.server,
            reported_by=copy.by,
            license=decide(f"the copy at {copy.url}", copy.version, copy.license, waiting)))

    status = ((paper_work or {}).get("open_access") or {}).get("oa_status")
    metadata.oa_status = status if status in _OA_STATUSES else None
