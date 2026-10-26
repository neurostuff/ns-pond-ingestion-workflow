"""OpenAlex client helpers."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote, urljoin

import requests
from study_schema.models import paper_parse as pp
from study_schema.models.paper_parse import MetadataSource

from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.models.notices import PartialAnswer
from ingestion_workflow.services import corresponding_authors
from ingestion_workflow.services.corresponding_authors.common import words
from ingestion_workflow.utils import authors as author_ids
from ingestion_workflow.utils.doi import normalize_doi
from ingestion_workflow.utils.http import Pacer, retry_transient

logger = logging.getLogger(__name__)

OPENALEX_BATCH_LOOKUP_SIZE = 100  # the most a page holds, and the most values one OR filter takes
OPENALEX_RETRACTION_BATCH_SIZE = 50  # the most DOIs one filter accepts
OPENALEX_SEARCH_PAGE_SIZE = 100  # the most a page holds; 200 is a deprecated legacy maximum
OPENALEX_REQUEST_LIMIT = 10  # polite pool: 10 req / second
_MIN_REQUEST_INTERVAL = 1 / OPENALEX_REQUEST_LIMIT


_METADATA_FIELDS = (
    "ids,display_name,authorships,publication_year,abstract_inverted_index,"
    "primary_location,best_oa_location,locations,open_access"
)
#: What a record keeps of the work verbatim: where its copies are, under which licences,
#: and how it is open.
_RAW_FIELDS = ("primary_location", "best_oa_location", "locations", "open_access")


# OpenAlex's Work schema documents `ids.pmcid`, but no recorded or live answer checked on
# 2026-10-17 carried it.
def _identifier_from(
    ids: Optional[Dict], kinds: Sequence[str] = ("doi", "pmid", "pmcid")
) -> Identifier:
    ids = ids or {}
    return Identifier(**{kind: ids.get(kind) for kind in kinds})


def _identified(works: Optional[Iterable[Dict]]) -> Iterator[Tuple[Dict, Identifier]]:
    """Each work with its identifier.

    A work with a malformed id is logged and skipped whole, its other ids too; nothing
    is kept for it, so its article is asked about again on the next run.
    """
    for work in works or []:
        ids = work.get("ids") or {}
        try:
            yield work, _identifier_from({**ids, "doi": ids.get("doi") or work.get("doi")})
        except ValueError as exc:
            logger.warning("OpenAlex work %s (doi %s, pmid %s): %s; its ids not used",
                           ids.get("openalex"), ids.get("doi"), ids.get("pmid"), exc)


def _metadata_from(work: Dict) -> ArticleMetadata:
    source = ((work.get("primary_location") or {}).get("source") or {})
    return ArticleMetadata(
        title=str(work.get("display_name") or ""),
        authors=_authors_from(work),
        abstract=_abstract(work.get("abstract_inverted_index")),
        journal=source.get("display_name"),
        publication_year=work.get("publication_year"),
        source=MetadataSource.openalex.value,
        raw_metadata={"openalex": {name: work.get(name) for name in _RAW_FIELDS}},
        corresponding_authors=corresponding_authors.from_openalex(work),
    )


def _authors_from(work: Dict) -> List[Author]:
    """Each authorship as an Author: the name the paper printed, its affiliations with the
    ROR institutions OpenAlex ties each to, and OpenAlex's ids for the person.

    OpenAlex's ids come from its own disambiguation, and it sometimes gives a printed name
    another author's identity ("Nan Lang" as Ning Zhang): an id is kept only when the
    printed name and OpenAlex's name for the person share a surname. An authorship
    repeating one already listed is merged into it: the same printed name with the same
    id, or straight after it with either id missing. A text without OpenAlex's tie to
    institutions takes the authorship's institutions only when it is the authorship's one
    text.
    """
    found: List[Author] = []
    seen: Dict[Tuple[str, str], Author] = {}
    for authorship in work.get("authorships") or []:
        person = authorship.get("author") or {}
        shown = person.get("display_name") or ""
        name = _printed(authorship.get("raw_author_name")) or shown
        if not name:
            continue
        same_person = _same_surname(name, shown)
        openalex_id = author_ids.openalex_author(person.get("id")) if same_person else None
        affiliations = _affiliations(authorship)
        repeat = seen.get((openalex_id, name)) if openalex_id else None
        if repeat is None and found and found[-1].name == name and not (
                openalex_id and found[-1].openalex_id):
            repeat = found[-1]
        if repeat is not None:
            repeat.affiliations += [a for a in affiliations if a not in repeat.affiliations]
            if openalex_id and not repeat.openalex_id:
                repeat.openalex_id = openalex_id
                repeat.orcid = author_ids.orcid(person.get("orcid"))
            continue
        author = Author(name=name, affiliations=affiliations, openalex_id=openalex_id,
                        orcid=author_ids.orcid(person.get("orcid")) if same_person else None)
        if openalex_id:
            seen[(openalex_id, name)] = author
        found.append(author)
    return found


def _printed(raw: Optional[str]) -> Optional[str]:
    """OpenAlex's `raw_author_name`, given name first: it can be "Tang, Jerry", or UTF-8
    read as Latin-1 ("BrÃ¼gger")."""
    if not raw or not raw.strip():
        return None
    try:
        raw = raw.encode("latin-1").decode("utf-8")
    except UnicodeError:
        pass
    family, comma, given = raw.strip().partition(", ")
    return f"{given} {family}".strip() if comma and given else raw.strip()


def _same_surname(printed: str, shown: str) -> bool:
    """Whether two names of one author slot share a surname, either way round ("Li Yao",
    "Yao Li") or written apart ("Debroucker", "de Broucker"), and so name one person."""
    a, b = words(printed), words(shown)
    if not (a and b):
        return False
    return (a[-1] in b or b[-1] in a
            or len(a[-1]) > 3 and a[-1] in "".join(b) or len(b[-1]) > 3 and b[-1] in "".join(a))


def _affiliations(authorship: Dict) -> List[pp.Affiliation]:
    by_id = {i.get("id"): i for i in authorship.get("institutions") or []}
    texts = authorship.get("raw_affiliation_strings") or []
    listed = authorship.get("affiliations") or [
        {"raw_affiliation_string": text,
         "institution_ids": list(by_id) if len(texts) == 1 else []}
        for text in texts]
    affiliations = []
    for affiliation in listed:
        text = author_ids.affiliation_text(affiliation.get("raw_affiliation_string"))
        if not text:
            continue
        institutions = [
            pp.Institution(ror=author_ids.ror(institution.get("ror")),
                           name=institution.get("display_name"),
                           country=institution.get("country_code"))
            for institution_id in affiliation.get("institution_ids") or []
            for institution in [by_id.get(institution_id) or {}]
            if author_ids.ror(institution.get("ror"))]
        affiliations.append(pp.Affiliation(text=text, institutions=institutions or None))
    return affiliations


def _cited_work(work: Dict) -> Dict:
    return {
        "authors": [
            str((a.get("author") or {}).get("display_name") or "")
            for a in work.get("authorships", []) or []
        ],
        "year": work.get("publication_year"),
        "pmid": _identifier_from(work.get("ids"), ["pmid"]).pmid,
    }


@dataclass(frozen=True)
class OaLocation:
    """One place OpenAlex lists a copy of the work."""

    pdf_url: Optional[str]
    landing_page_url: Optional[str]
    #: OpenAlex's `source.type`: "repository", "journal", "conference", ...
    source_type: Optional[str]
    source_name: Optional[str]
    #: "publishedVersion", "acceptedVersion" or "submittedVersion".
    version: Optional[str]
    is_oa: bool = True
    #: The location's licence, as OpenAlex gives it ("cc-by", "publisher-specific-oa").
    license: Optional[str] = None


class LocationLookupFailed(PartialAnswer):
    """Some location batches failed. `found` holds what the others returned, and
    `failed` each article that was only in a failed batch, with the error."""

    def __init__(self, message: str, found: Dict, failed: Dict[str, str]) -> None:
        super().__init__(message, found)
        self.failed = failed


def _locations_from(work: Dict) -> List[OaLocation]:
    """The work's locations with a URL, `best_oa_location` first, each once."""
    best = work.get("best_oa_location") or {}
    raw = [best] if isinstance(best, dict) and best else []
    raw += [loc for loc in work.get("locations") or [] if isinstance(loc, dict)]
    seen = set()
    locations: List[OaLocation] = []
    for loc in raw:
        pdf_url, landing = loc.get("pdf_url") or None, loc.get("landing_page_url") or None
        if (pdf_url, landing) in seen or not (pdf_url or landing):
            continue
        seen.add((pdf_url, landing))
        source = loc.get("source") or {}
        locations.append(
            OaLocation(
                pdf_url=str(pdf_url) if pdf_url else None,
                landing_page_url=str(landing) if landing else None,
                source_type=source.get("type"),
                source_name=source.get("display_name"),
                version=loc.get("version"),
                is_oa=bool(loc.get("is_oa")),
                license=loc.get("license") or None,
            )
        )
    return locations


def _abstract(inverted: Optional[Dict[str, List[int]]]) -> Optional[str]:
    """OpenAlex ships an abstract as word -> positions, for licensing reasons."""
    if not inverted:
        return None
    placed = sorted((at, word) for word, positions in inverted.items() for at in positions)
    return " ".join(word for _, word in placed) or None


class OpenAlexClient:
    BASE_URL = "https://api.openalex.org"
    LOOKUP_ENDPOINT = "/works"
    ID_QUERY_ARGS = "select=ids,doi"

    def __init__(self, email: Optional[str] = None, api_key: Optional[str] = None) -> None:
        self.email = email
        self.api_key = api_key
        self._session = requests.Session()
        self._pace = Pacer()

    @classmethod
    def from_settings(cls, settings) -> Optional["OpenAlexClient"]:
        """A client with the configured email and key, or None when neither is set."""
        if not (settings.openalex_email or settings.openalex_api_key):
            return None
        return cls(settings.openalex_email, settings.openalex_api_key)

    def get_ids(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """
        Fetch OpenAlex IDs for a list of identifiers.

        Parameters
        ----------
        identifiers : Identifiers
            Identifiers containing DOIs or PMIDs

        Returns
        -------
        Identifiers
            Identifiers with OpenAlex IDs populated
        """
        self.validate_ids(id_type, identifiers)
        return self.get_ids_by_type(id_type, identifiers)

    def validate_ids(self, id_type: str, identifiers: Identifiers) -> None:
        """Ensure we have values available for the requested id_type."""
        if id_type == "doi":
            if any(identifier.asking_doi is None for identifier in identifiers):
                raise ValueError("All identifiers must have a DOI for doi lookup.")
        elif id_type == "pmid":
            if any(identifier.pmid is None for identifier in identifiers):
                raise ValueError("All identifiers must have a PMID for pmid lookup.")
        else:
            raise ValueError(f"Unsupported id_type: {id_type}")

    def get_ids_by_type(self, id_type: str, identifiers: Identifiers) -> Identifiers:
        """Query OpenAlex using the provided id type and enrich identifiers."""
        values = [
            str(identifier.asking(id_type)).strip()
            for identifier in identifiers
            if identifier.asking(id_type)
        ]

        batches = [
            values[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
            for index in range(0, len(values), OPENALEX_BATCH_LOOKUP_SIZE)
        ]
        for batch in batches:
            params = {
                "filter": f"{id_type}:{'|'.join(batch)}",
                "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                "mailto": self.email,
                "select": "ids",
            }
            payload = self._request_openalex(params)

            for work, found in _identified(payload.get("results")):
                ids_data = work.get("ids", {})
                key = ids_data.get(id_type)
                if not key:
                    continue

                identifier = identifiers.lookup(key, key=id_type)

                if identifier is None:
                    continue

                openalex_id = ids_data.get("openalex")
                if openalex_id:
                    if identifier.other_ids is None:
                        identifier.other_ids = {}
                    identifier.other_ids["openalex"] = openalex_id

                # OpenAlex carries the PubMed ids as well. For an article
                # outside PMC that Semantic Scholar does not index, it is the
                # only provider that does: without this, a DOI-only article
                # never gained the PMID its PubMed metadata is fetched by.
                for kind in ("pmid", "pmcid", "doi"):
                    if not getattr(identifier, kind) and found.asking(kind):
                        setattr(identifier, kind, found.asking(kind))
                identifier.normalize()

        return identifiers

    def get_locations(self, identifiers: Identifiers) -> Dict[str, List[OaLocation]]:
        """Every location OpenAlex lists per identifier slug, best open copy first.

        A work OpenAlex returns gets an entry even when it has no location, so an
        absent slug means OpenAlex does not know the article. A failed batch
        raises `LocationLookupFailed`, naming the articles in it, rather than
        reading as "no open-access copy".
        """
        found: Dict[str, List[OaLocation]] = {}
        failed: Dict[str, str] = {}
        for id_type in ("doi", "pmid"):
            values = [
                str(identifier.asking(id_type)).strip()
                for identifier in identifiers
                if identifier.asking(id_type) and identifier.slug not in found
            ]
            for index in range(0, len(values), OPENALEX_BATCH_LOOKUP_SIZE):
                batch = values[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
                params = {
                    "filter": f"{id_type}:{'|'.join(batch)}",
                    "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                    "mailto": self.email,
                    "select": "ids,best_oa_location,locations",
                }
                try:
                    payload = self._request_openalex(params)
                except Exception as exc:  # noqa: BLE001 - reported per article
                    for value in batch:
                        identifier = identifiers.lookup(value, key=id_type)
                        if identifier is not None:
                            failed[identifier.slug] = f"{id_type} batch of {len(batch)}: {exc}"
                    continue

                for work in payload.get("results", []) or []:
                    key = (work.get("ids", {}) or {}).get(id_type)
                    identifier = identifiers.lookup(key, key=id_type) if key else None
                    if identifier is not None:
                        found[identifier.slug] = _locations_from(work)
        # an article a later batch (by its other id) answered for did not fail
        failed = {slug: why for slug, why in failed.items() if slug not in found}
        if failed:
            raise LocationLookupFailed(
                f"OpenAlex location lookup failed for {len(failed)} article(s)", found, failed
            )
        return found

    def search_title(self, title: str, limit: int = 5) -> List[Tuple[Identifier, ArticleMetadata]]:
        """Works whose title matches `title`, best first, with their authors.

        A comma separates filters in OpenAlex's syntax, so punctuation is
        dropped from the title before it is sent. `title.search` wants every
        word; when it finds nothing -- a truncated title -- the relevance
        search over all fields is asked instead.
        """
        words = " ".join(re.findall(r"\w+", title))
        if not words:
            return []
        params = {"per_page": str(limit), "mailto": self.email, "select": _METADATA_FIELDS}
        works = self._request_openalex({**params, "filter": f"title.search:{words}"}).get(
            "results"
        ) or self._request_openalex({**params, "search": words}).get("results")
        return [(identifier, _metadata_from(work)) for work, identifier in _identified(works)]

    def search(self, filter: str) -> Identifiers:
        """Every work matching the /works `filter`, by cursor to the last page.

        OpenAlex follows a cursor past the first page only for a request with an
        API key. A work with no DOI, PMID or PMCID is kept, carrying its OpenAlex
        id, so the caller can count what the catalog cannot hold.
        """
        params = {
            "filter": filter,
            "select": "ids,doi",
            "per_page": str(OPENALEX_SEARCH_PAGE_SIZE),
            "mailto": self.email,
            "cursor": "*",
        }
        found: List[Identifier] = []
        while params["cursor"]:
            payload = self._request_openalex(params)
            for work, identifier in _identified(payload.get("results")):
                ids = work.get("ids") or {}
                if ids.get("openalex"):
                    identifier["openalex"] = ids["openalex"]
                found.append(identifier)
            params = {**params, "cursor": payload["meta"].get("next_cursor")}
        return Identifiers(found)

    def get_metadata(self, identifiers: List[Identifier]) -> Dict[str, ArticleMetadata]:
        """Article metadata by DOI or PMID, keyed by identifier slug.

        The provider of last resort: it indexes preprints and articles that
        neither Semantic Scholar nor PubMed carries.
        """
        results: Dict[str, ArticleMetadata] = {}
        for id_type in ("doi", "pmid"):
            wanted = {
                str(identifier.asking(id_type)): identifier
                for identifier in identifiers
                if identifier.asking(id_type) and identifier.slug not in results
            }
            values = list(wanted)
            for index in range(0, len(values), OPENALEX_BATCH_LOOKUP_SIZE):
                batch = values[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
                payload = self._request_openalex(
                    {
                        "filter": f"{id_type}:{'|'.join(batch)}",
                        "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                        "mailto": self.email,
                        "select": _METADATA_FIELDS,
                    }
                )
                answered = set()
                for work in payload.get("results", []) or []:
                    value = _identifier_from(work.get("ids"), [id_type]).asking(id_type)
                    identifier = wanted.get(str(value)) if value else None
                    if identifier is not None:
                        answered.add(str(value))
                        results[identifier.slug] = _metadata_from(work)
                if id_type == "doi":
                    for doi, work in self._works_asked_singly(batch, answered, _METADATA_FIELDS):
                        results[wanted[doi].slug] = _metadata_from(work)
        return results

    def get_cited_works(self, dois: List[str]) -> Dict[str, Dict]:
        """Authors, year and PMID of works by DOI, keyed by canonical DOI.

        For a reference list that names its entries by DOI alone (most of
        Crossref's): author-year citations are matched on these.
        """
        found: Dict[str, Dict] = {}
        wanted = sorted({d for d in (normalize_doi(d) for d in dois) if d})
        for index in range(0, len(wanted), OPENALEX_BATCH_LOOKUP_SIZE):
            batch = wanted[index : index + OPENALEX_BATCH_LOOKUP_SIZE]
            payload = self._request_openalex(
                {
                    "filter": f"doi:{'|'.join(batch)}",
                    "per_page": str(OPENALEX_BATCH_LOOKUP_SIZE),
                    "mailto": self.email,
                    "select": "ids,publication_year,authorships",
                }
            )
            answered = set()
            for work in payload.get("results", []) or []:
                ids = _identifier_from(work.get("ids"), ["doi"])
                if ids.asking_doi in batch:
                    answered.add(ids.asking_doi)
                    found[ids.asking_doi] = _cited_work(work)
            for doi, work in self._works_asked_singly(batch, answered, "ids,publication_year,authorships"):
                found[doi] = _cited_work(work)
        return found

    def get_retractions(self, dois: List[str]) -> Dict[str, bool]:
        """OpenAlex's `is_retracted` by canonical DOI, for the DOIs it knows.

        A DOI OpenAlex does not return is absent, not False. A failed batch raises
        `PartialAnswer`, which carries the earlier batches' results, rather than
        reading as "not retracted".
        """
        found: Dict[str, bool] = {}
        wanted = sorted({d for d in (normalize_doi(d) for d in dois) if d})
        for index in range(0, len(wanted), OPENALEX_RETRACTION_BATCH_SIZE):
            batch = wanted[index : index + OPENALEX_RETRACTION_BATCH_SIZE]
            try:
                payload = self._request_openalex(
                    {
                        "filter": f"doi:{'|'.join(batch)}",
                        "per_page": str(OPENALEX_RETRACTION_BATCH_SIZE),
                        "mailto": self.email,
                        "select": "doi,is_retracted",
                    }
                )
            except Exception as exc:  # noqa: BLE001 - carries the earlier batches
                raise PartialAnswer(str(exc), found) from exc
            answered = set()
            for work in payload.get("results", []) or []:
                doi = normalize_doi(work.get("doi"))
                if doi in batch and work.get("is_retracted") is not None:
                    answered.add(doi)
                    found[doi] = bool(work["is_retracted"])
            try:
                singly = self._works_asked_singly(batch, answered, "is_retracted")
            except Exception as exc:  # noqa: BLE001 - carries the earlier batches
                raise PartialAnswer(str(exc), found) from exc
            for doi, work in singly:
                if work.get("is_retracted") is not None:
                    found[doi] = bool(work["is_retracted"])
        return found

    def _works_asked_singly(
        self, asked: List[str], answered: set, select: str
    ) -> List[Tuple[str, Dict]]:
        """The works of the asked DOIs the batch answer did not return, by asked DOI.

        OpenAlex answers a batch with each work's current DOI, so a DOI the
        journal has since moved comes back under another name; `/works/doi:`
        resolves the old one. A DOI OpenAlex does not know is left out.
        """
        works = []
        for doi in asked:
            if doi in answered:
                continue
            try:
                works.append((doi, self._request_openalex({"mailto": self.email, "select": select}, f"doi:{quote(doi, safe='/')}")))
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 404:
                    raise
        return works

    @retry_transient(3, 8)
    def _request_openalex(self, params: Dict[str, str], work: str = "") -> Dict:
        """Issue a GET request to the OpenAlex Works endpoint, or to one `work` of it."""
        self._pace.wait(_MIN_REQUEST_INTERVAL)
        url = urljoin(self.BASE_URL, f"{self.LOOKUP_ENDPOINT}/{work}" if work else self.LOOKUP_ENDPOINT)
        if self.api_key:
            params = {**params, "api_key": self.api_key}
        try:
            response = self._session.get(url, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            if not self.api_key:
                raise
            # requests quotes the full URL, key included, in its messages; logs must not
            status = getattr(exc.response, "status_code", None) or type(exc).__name__
            raise type(exc)(f"OpenAlex request failed ({status}) for {url}", response=exc.response) from None
        return response.json()
