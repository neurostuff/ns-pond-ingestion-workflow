"""The journal, preprint server or conference series a paper appeared in: its OpenAlex source.

The first of these that answers decides it:

1. ``doi`` / ``pmid``: OpenAlex's work for the version of record, asked by its DOI, else by
   its PMID. Its venue is the first of its locations at a journal, conference, book series
   or ebook platform: OpenAlex sometimes makes a repository (Radboud Repository, PubMed
   Central) the primary location of a journal article. Without one, the primary location
   is the venue only when it is a preprint server's; any other repository holds a copy.
2. ``preprint_server``: a paper known only by a preprint takes its server's source.
3. ``issn``: the ISSNs PubMed's record gives, when they belong to one source.
4. ``title``: `journal`, normalized (`normalized`), is a title or abbreviation NLM's journal
   list (J_Medline) gives, and its ISSNs belong to one source.
5. ``fuzzy``: `journal` scores at least `FUZZY_ACCEPT` against one; one scoring from
   `FUZZY_REVIEW` is noted for review instead.

Two or more sources under one name, none with ten times the works of the next, are
ambiguous: no venue, and the name is noted for review.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from rapidfuzz import fuzz, process
from study_schema.models import paper_parse as pp

from ingestion_workflow.catalog import Notes
from ingestion_workflow.extractors.pdf_extractor import _PUBLISHER_SOURCE_TYPES
from ingestion_workflow.models.ids import Identifier

#: Each preprint server's OpenAlex repository source, by study_schema's PreprintServer name.
PREPRINT_SERVER_SOURCES = {
    "biorxiv": "S4306402567",
    "medrxiv": "S3005729997",
    "researchsquare": "S4306525896",
    "arxiv": "S4306400194",
    "psyarxiv": "S4306401687",
    "ssrn": "S4210172589",
    "osf_preprints": "S4306401127",
}
#: A fuzzy score from which a name is taken as the source's, and one from which it is noted.
FUZZY_ACCEPT = 95
FUZZY_REVIEW = 80
#: How many times the works of the next source the first under one name needs to be taken.
DOMINANT = 10
#: The field the venue's notes are kept under.
FIELD = "venue"

#: An OpenAlex source by ISSN: its record, or None when no source holds the ISSN.
SourceLookup = Callable[[str], Optional[Mapping]]


def normalized(name: str) -> str:
    """`name` folded to ASCII and lower case, `&` as "and", without a parenthetical qualifier,
    a subtitle after " : ", "; " or " = " (a parallel title), punctuation or a leading
    "the": "The Journal of neuroscience : the official journal..." is "journal of
    neuroscience", "Cerebral cortex (New York, N.Y. : 1991)" is "cerebral cortex"."""
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    text = text.replace("&amp;", "&").replace("&", " and ")
    text = re.sub(r"\s*\([^)]*\)", " ", text)
    text = re.split(r"\s+:\s+|;\s|\s=\s", text)[0]
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return re.sub(r"^the ", "", text)


def bare(openalex_id: str) -> str:
    """An OpenAlex id without its URL: `S121666818`."""
    return openalex_id.rsplit("/", 1)[-1]


def source_of_work(work: Mapping) -> Optional[str]:
    """The source a work appeared in: the first location at a journal, conference, book
    series or ebook platform, the primary location first, else the primary location's when
    it is a preprint server's; None when the work lists only copies."""
    primary = (work.get("primary_location") or {}).get("source") or {}
    for location in [work.get("primary_location") or {}, *(work.get("locations") or [])]:
        source = (location or {}).get("source") or {}
        if source.get("type") in _PUBLISHER_SOURCE_TYPES and source.get("id"):
            return bare(source["id"])
    server = bare(primary["id"]) if primary.get("id") else None
    return server if server in PREPRINT_SERVER_SOURCES.values() else None


def pubmed_issns(record: Mapping) -> List[str]:
    """The ISSNs of PubMed's record of the paper: the journal's, then NLM's linking one."""
    citation = (record or {}).get("MedlineCitation") or {}
    listed = ((citation.get("Article") or {}).get("Journal") or {}).get("ISSN")
    listed = listed if isinstance(listed, list) else [listed] if listed else []
    texts = [i.get("#text") if isinstance(i, dict) else i for i in listed]
    texts.append((citation.get("MedlineJournalInfo") or {}).get("ISSNLinking"))
    return list(dict.fromkeys(t.strip().upper() for t in texts
                              if isinstance(t, str) and t.strip()))


@dataclass(frozen=True)
class Journal:
    """One title in NLM's list: its name, NLM id and ISSNs."""

    title: str
    nlm_id: str
    issns: Sequence[str]


class JournalIndex:
    """NLM's journal list (J_Medline), by the normalized form of each title, MEDLINE
    abbreviation and ISO abbreviation."""

    def __init__(self, journals: Iterable[Journal], names: Mapping[str, Sequence[str]]):
        by_nlm = {j.nlm_id: j for j in journals}
        self._by_key: Dict[str, List[Journal]] = {}
        for nlm_id, texts in names.items():
            for key in dict.fromkeys(normalized(t) for t in texts if t):
                if key:
                    self._by_key.setdefault(key, []).append(by_nlm[nlm_id])
        self._keys = list(self._by_key)

    @classmethod
    def parse(cls, text: str) -> "JournalIndex":
        """The index of J_Medline.txt's records, each a block of `Field: value` lines."""
        journals, names = [], {}
        for block in text.split("\n-----"):
            fields = dict(line.split(":", 1) for line in block.splitlines() if ":" in line)
            fields = {k.strip(): v.strip() for k, v in fields.items()}
            if not fields.get("NlmId"):
                continue
            issns = [fields.get("ISSN (Print)"), fields.get("ISSN (Online)")]
            journals.append(Journal(fields.get("JournalTitle", ""), fields["NlmId"],
                                    tuple(i.upper() for i in issns if i)))
            names[fields["NlmId"]] = [fields.get(k) for k in ("JournalTitle", "MedAbbr",
                                                              "IsoAbbr")]
        return cls(journals, names)

    def named(self, key: str) -> List[Journal]:
        return self._by_key.get(key, [])

    def closest(self, key: str) -> tuple[float, List[Journal]]:
        """The best fuzzy score of `key` against the names, from `FUZZY_REVIEW`, and the
        journals of every name scoring it; (0, []) when none does."""
        found = process.extract(key, self._keys, scorer=fuzz.token_sort_ratio,
                                score_cutoff=FUZZY_REVIEW, limit=None)
        if not found:
            return 0.0, []
        best = max(score for _, score, _ in found)
        return best, [j for name, score, _ in found if score == best for j in self._by_key[name]]


def decide(ids: pp.ArticleIdentifiers, asked: Identifier, vor_doi: Optional[str],
           work: Optional[Mapping], pubmed: Optional[Mapping], journal: Optional[str],
           sources: SourceLookup, index: Callable[[], JournalIndex],
           notes: Notes) -> Optional[pp.Venue]:
    """The paper's venue, by the first rule that answers; None when none does.

    `asked` is the identifier OpenAlex was asked about the paper by, `work` the work it
    answered with (its record's raw `openalex`), `vor_doi` the version of record's DOI,
    `pubmed` PubMed's raw record and `journal` the record's journal name. `sources` finds
    an OpenAlex source by ISSN; `index` is NLM's journal list, read only when the rules
    reach `journal`. An ambiguous name, or one only close to a known one, is noted.
    """
    if work:
        # OpenAlex asks by DOI first, by PMID only for an identifier without one.
        doi = asked.asking_doi
        if doi is not None and doi == vor_doi:
            by = pp.VenueMatch.doi
        elif doi is None and asked.pmid is not None and asked.pmid == ids.pmid:
            by = pp.VenueMatch.pmid
        else:
            by = None
        found = source_of_work(work) if by else None
        if found:
            return pp.Venue(openalex_id=found, match=by)
    if ids.publication_status == pp.PublicationStatus.preprint:
        servers = {PREPRINT_SERVER_SOURCES.get(p.server) for p in ids.preprint_dois or []}
        if len(servers) == 1 and None not in servers:
            return pp.Venue(openalex_id=servers.pop(), match=pp.VenueMatch.preprint_server)
    held = {bare(s["id"]): s for s in filter(None, map(sources, pubmed_issns(pubmed or {})))}
    if len(held) == 1:
        return pp.Venue(openalex_id=next(iter(held)), match=pp.VenueMatch.issn)
    key = normalized(journal or "")
    if not key:
        return None
    titles = index()
    named = titles.named(key)
    if named:
        found = _one_source(named, sources, journal, notes)
        return pp.Venue(openalex_id=found, match=pp.VenueMatch.title) if found else None
    score, close = titles.closest(key)
    if score >= FUZZY_ACCEPT:
        found = _one_source(close, sources, journal, notes)
        return (pp.Venue(openalex_id=found, match=pp.VenueMatch.fuzzy, score=round(score, 1))
                if found else None)
    if close:
        notes.error(FIELD, f"journal {journal!r} is {score:.1f} like "
                    + ", ".join(f"{j.title!r} (NLM {j.nlm_id})" for j in close)
                    + ": for review")
    return None


def _one_source(journals: Sequence[Journal], sources: SourceLookup, journal: str,
                notes: Notes) -> Optional[str]:
    """The one OpenAlex source the journals' ISSNs belong to, or the one with `DOMINANT`
    times the works of the next; None, noted, when several are near alike."""
    held = {bare(s["id"]): s for j in journals for s in filter(None, map(sources, j.issns))}
    if len(held) <= 1:
        return next(iter(held), None)
    ranked = sorted(held.values(), key=lambda s: -(s.get("works_count") or 0))
    first, second = ranked[0].get("works_count") or 0, ranked[1].get("works_count") or 0
    if first >= DOMINANT * max(second, 1):
        return bare(ranked[0]["id"])
    notes.error(FIELD, f"journal {journal!r} names several sources: "
                + ", ".join(f"{bare(s['id'])} ({s.get('display_name')})" for s in ranked)
                + ": for review")
    return None
