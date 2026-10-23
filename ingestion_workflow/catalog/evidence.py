"""What NCBI and the DOI registries say of an article's identifiers.

The catalog's identity checks (registration, the invariants, a repair plan) read it
and nothing else. Each answer is asked once and kept for the life of the object.

A DOI is evidence of which paper a PubMed record is only when the registering agency
gives it that paper's title: PubMed sometimes gives a record another paper's DOI
(10.14670/HH-25-013 is a parotid tumour study, and PubMed gives it to an ocular
lymphoma review too), or a DOI with a typo that no agency has.
"""

from __future__ import annotations

import difflib
import html
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from ingestion_workflow.clients.doi_registry import DoiRegistry, RegisteredWork, Unreadable
from ingestion_workflow.clients.pubmed import PmcRecord, PubMedClient, PubMedSummary
from ingestion_workflow.utils.doi import normalize_doi

__all__ = ["Evidence", "Unreadable", "same_work"]

#: How alike two titles must be, letter by letter, to name one work. Publishers and
#: PubMed differ in case, punctuation, spelling and the odd word, not in more.
TITLE_LIKENESS = 0.9
#: How alike a word must be to one in the other title to count as a spelling of it
#: (behaviour, behavior), not another word (yes, no).
WORD_LIKENESS = 0.75
#: The fewest words a title may have and still be another's main title, before its
#: subtitle: "A default mode of brain function" is not enough on its own.
MAIN_TITLE_WORDS = 5

#: The fewest words a title may have and gain one more word, still the same title.
LONG_TITLE_WORDS = 8

#: Crossref relations joining a preprint and its version of record.
_VERSION_OF = ("is-preprint-of", "has-preprint")
#: The version number a publisher appends to a version's DOI.
_VERSIONED = re.compile(r"\.\d{1,2}$")

#: A notice about another work, or another kind of work under its name: it takes that
#: work's title into its own.
_NOTICE = re.compile(
    r"^\W*(errat(um|a)|corrigend(um|a)|corrections?\b\W*($|to\b|of\b|:)|retract(ion|ed)\b"
    r"|notice of retraction|withdrawn\b|expression of concern|addendum\b|reprint of\b"
    r"|(authors?'?s?\W+)?(reply|response)(\W+(to|on)\b|\W*:)|in reply\b|comments? on\b"
    r"|commentary on\b|on\s+[\"“'‘]|dataset\W*:)"
    r"|\b(corrigend(um|a)|errat(um|a))\W*$")
#: A word a title gains that makes it another work: a review's protocol.
_OTHER_WORK_WORDS = {"protocol"}
#: A PubMed author's suffix, after the initials: "Jack CR Jr", "Byrd TF 4th".
_SUFFIXES = {"jr", "sr", "2nd", "3rd", "4th", "ii", "iii", "iv"}
_NOTICE_TYPES = {"Published Erratum", "Retraction of Publication", "Retraction Notice",
                 "Expression of Concern"}
#: CSL types of works that are not a journal article or its preprint.
_NOT_ARTICLES = {"book", "chapter", "book-chapter", "book-section", "edited-book", "monograph",
                 "entry", "entry-dictionary", "entry-encyclopedia", "reference-entry",
                 "reference-book", "dataset", "component", "peer-review", "standard",
                 "proceedings-article", "paper-conference", "proceedings"}


@dataclass(frozen=True)
class _Work:
    words: Tuple[str, ...]
    #: The words with the punctuation between them, to find where a subtitle begins.
    tokens: Tuple[str, ...]
    author: Optional[str]
    year: Optional[int]
    notice: bool
    article: bool
    preprint: bool
    supplement: bool
    #: Which kind of record: a registry's title may lack the subtitle PubMed's has.
    registered: bool
    #: The DOI a PubMed record gives, or a registry's record is of.
    doi: Optional[str]
    issns: frozenset
    #: The journal's name before any subtitle ("Brain : a journal of neurology" is "brain").
    journal: str


def _text(title: Optional[str]) -> str:
    return re.sub(r"<[^>]+>", " ", html.unescape(title or "")).lower()


def _fold(text: str) -> str:
    ascii_ = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", ascii_.lower())


def _work(title: Optional[str], author: Optional[str], year: Optional[int],
          notice: bool, article: bool, preprint: bool, issns: Sequence[str],
          journal: Optional[str], supplement: bool, registered: bool,
          doi: Optional[str] = None) -> _Work:
    text = _text(title)
    return _Work(words=tuple(re.findall(r"[^\W_]+", text)),
                 tokens=tuple(re.findall(r"[^\W_]+|[:.?!;]", text)),
                 author=_fold(author) if author else None, year=year,
                 notice=notice or bool(_NOTICE.search(text)), article=article, preprint=preprint,
                 supplement=supplement, registered=registered,
                 doi=normalize_doi(doi) if doi else None,
                 issns=frozenset(re.sub(r"[^0-9X]", "", i.upper()) for i in issns),
                 journal=" ".join(re.findall(r"[^\W_]+", re.sub(
                     r"^the\s+|\s*:.*$", "", _text(journal).replace("&", " and ")))))


def _from_pubmed(summary: PubMedSummary) -> _Work:
    author = summary.first_author
    if author:
        # "Raichle ME", "Jack CR Jr": the family name before the initials and any suffix.
        parts = author.split()
        if len(parts) > 2 and parts[-1].lower() in _SUFFIXES:
            parts.pop()
        # initials in any script: "Özdemir İ"
        if len(parts) > 1 and len(parts[-1]) <= 3 and parts[-1].isalpha() and parts[-1].isupper():
            parts.pop()
        author = " ".join(parts)
    return _work(summary.title, author, summary.year,
                 bool(_NOTICE_TYPES & set(summary.pubtypes)), article=True,
                 preprint=summary.preprint, issns=summary.issns, journal=summary.journal,
                 supplement=summary.supplement, registered=False, doi=summary.doi)


def _from_registry(work: RegisteredWork) -> _Work:
    return _work(work.title, work.first_author, work.year, False,
                 article=work.type not in _NOT_ARTICLES, preprint=work.type == "posted-content",
                 issns=work.issns, journal=work.journal, supplement=work.supplement,
                 registered=True, doi=work.doi)


def _same_journal(a: _Work, b: _Work) -> bool:
    """One journal by name, though one record gives its print ISSN and the other its
    electronic one (Brain: 0006-8950 in PubMed, 1460-2156 at Crossref)."""
    short, long = sorted((a.journal, b.journal), key=len)
    return bool(short) and long.startswith(short)


def _same_author(a: str, b: str) -> bool:
    """One family name, though a registry may give the whole name ("Malcolm Wells"), an
    initial ("Gomez M."), or a letter it cannot encode ("Ulu?" for "Uluğ")."""
    short, long = sorted((a, b), key=len)
    return short == long or (len(short) >= 3 and (long.endswith(short)
                                                  or long.startswith(short)))


def _alike_words(a: Sequence[str], b: Sequence[str]) -> bool:
    """Whether each word of one title not in the other is a spelling of one there. A long
    title may also gain one word (an early release's "...in the ribosome" is the
    paper's "...in the ribosome tunnel"); a word in place of another (yes, no) is
    another title."""
    def spelling(x, y):
        # "women" is "men" with a word before it ("oestrogen" is a spelling of
        # "estrogen"), "T808" is "T807" by another number
        short, long = sorted((x, y), key=len)
        if (long.endswith(short) and len(long) - len(short) > 1
                or re.findall(r"\d+", x) != re.findall(r"\d+", y)):
            return False
        return difflib.SequenceMatcher(None, x, y).ratio() >= WORD_LIKENESS

    def spelled(x, others):
        return any(spelling(x, y) for y in others)
    only_a, only_b = set(a) - set(b), set(b) - set(a)
    lone_a = [x for x in only_a if not spelled(x, only_b)]
    lone_b = [y for y in only_b if not spelled(y, only_a)]
    extra = lone_a or lone_b
    return not (lone_a and lone_b) and (not extra or (
        len(extra) == 1 and min(len(a), len(b)) >= LONG_TITLE_WORDS
        and extra[0] not in _OTHER_WORK_WORDS))


def _main_title(short: _Work, long: _Work, series: bool) -> bool:
    """Whether `short` is `long`'s title before its subtitle; with `series`, not before a
    series' part ("...: Part II")."""
    head = list(short.tokens)
    while head and not re.match(r"\w", head[-1]):
        head.pop()
    rest = [t for t in long.tokens[len(head) + 1:] if re.match(r"\w", t)]
    return (len(short.words) >= MAIN_TITLE_WORDS and len(long.tokens) > len(head)
            and list(long.tokens[:len(head)]) == head and long.tokens[len(head)] in ":.?!;"
            and not (series and rest[:1] == ["part"]))


def _same_title(a: _Work, b: _Work) -> bool:
    if not a.words or not b.words:
        return False
    # the same letters, however punctuated: "5-HT1A" and "5-HT(1A)"
    if a.words == b.words or "".join(a.words) == "".join(b.words):
        return True
    short, long = sorted((a, b), key=lambda w: len(w.words))
    # Two records of one DOI differ in a word's spelling ("sigma", "sigma1"), a subtitle
    # or the series' part one gives; PubMed giving another paper's DOI gives another
    # title. Two records of one kind have no reason to differ in a subtitle, unless they
    # are two PubMed records of one DOI (35770780 and 37401785).
    one_doi = bool(a.doi) and a.doi == b.doi
    if (a.registered != b.registered or one_doi) and _main_title(short, long, not one_doi):
        return True
    return (difflib.SequenceMatcher(None, " ".join(a.words), " ".join(b.words)).ratio()
            >= TITLE_LIKENESS and (one_doi or _alike_words(a.words, b.words)))


Record = Union[PubMedSummary, RegisteredWork]


def _of(record: Record) -> _Work:
    return _from_pubmed(record) if isinstance(record, PubMedSummary) else _from_registry(record)


def same_work(first: Record, second: Record, *, versions: bool = False) -> bool:
    """Whether two records are one work, or with `versions`, one a preprint and the
    other its paper.

    Alike titles are not enough: the first authors must agree when both records give
    one, and one paper's records must be a year apart at most, in one journal by ISSN or
    name (a meeting abstract in a journal's supplement has its paper's title,
    authors and year); a preprint and its paper may be years and journals apart, and
    their authors reordered when the title is word for word the same. A
    notice (an erratum, a reply, a retraction) takes its work's title, so it is one
    work only with a notice of the same title and author, a year apart at most. A book
    chapter is never a journal article, nor a meeting abstract in a supplement issue its
    paper, unless the two records are of one DOI.
    """
    a, b = _of(first), _of(second)
    # PubMed and the registry differ on whether one DOI's issue is a supplement and its
    # work an article (an EMBC paper is a proceedings-article to Crossref)
    one_doi = a.doi is not None and a.doi == b.doi
    if not one_doi and (a.supplement != b.supplement or a.article != b.article):
        return False
    if a.notice or b.notice:
        return (a.notice and b.notice and a.words == b.words and a.author is not None
                and b.author is not None and _same_author(a.author, b.author)
                and _near(a.year, b.year))
    if not _same_title(a, b):
        return False
    authors_differ = a.author and b.author and not _same_author(a.author, b.author)
    if versions:
        return a.preprint != b.preprint and (not authors_differ or a.words == b.words)
    if authors_differ:
        return False
    if a.issns and b.issns and not a.issns & b.issns and not _same_journal(a, b):
        return False
    return _near(a.year, b.year)


def _near(a: Optional[int], b: Optional[int]) -> bool:
    """One work's records are a year apart at most: PubMed's year is the issue's, the
    registry's may be the online release's (19382289: 2009, 10.1093/brain/awn090: 2008)."""
    return a is None or b is None or abs(a - b) <= 1


class Evidence:
    def __init__(self, pubmed: PubMedClient, registry: DoiRegistry) -> None:
        self._pubmed = pubmed
        self._registry = registry
        self._summaries: Dict[str, Optional[PubMedSummary]] = {}
        self._pmc: Dict[str, Optional[PmcRecord]] = {}
        self._works: Dict[str, Union[RegisteredWork, Unreadable, None]] = {}
        self._versions: Dict[str, List[str]] = {}
        #: Each DOI as PubMed and PMC spell it, by its lowercase form.
        self._spellings: Dict[str, Set[str]] = {}
        self._title_versions: Dict[str, List[str]] = {}

    def summaries(self, pmids: Iterable[str]) -> Dict[str, PubMedSummary]:
        """PubMed's summary of each PMID PubMed has a record of."""
        pmids = list(dict.fromkeys(pmids))
        asked = [p for p in pmids if p not in self._summaries]
        if asked:
            found = self._pubmed.summaries(asked)
            self._summaries.update({p: found.get(p) for p in asked})
            self._spelt(s.doi for s in found.values())
        return {p: s for p in pmids if (s := self._summaries[p]) is not None}

    def pmc(self, pmcids: Iterable[str]) -> Dict[str, PmcRecord]:
        """The ID converter's record of each PMCID it knows."""
        pmcids = list(dict.fromkeys(pmcids))
        asked = [c for c in pmcids if c not in self._pmc]
        if asked:
            found = self._pubmed.pmc_records(asked)
            self._pmc.update({c: found.get(c) for c in asked})
            self._spelt(r.doi for r in found.values())
        return {c: r for c in pmcids if (r := self._pmc[c]) is not None}

    def _spelt(self, dois: Iterable[Optional[str]]) -> None:
        for doi in dois:
            if doi and (normal := normalize_doi(doi)):
                self._spellings.setdefault(normal, set()).add(doi)

    def pmids_for_dois(self, dois: Sequence[str]) -> List[str]:
        """Every PMID whose PubMed record gives one of `dois`."""
        return self._pubmed.pmids_for_dois(list(dois))

    def title_versions(self, pmid: str) -> List[str]:
        """The PMIDs of the other version of this record's work: its paper's records for
        a Preprint-typed record, its preprint's for any other. Crossref relates many
        preprints to their paper, not all; PubMed finds the rest by title."""
        if pmid not in self._title_versions:
            summary = self.summaries([pmid]).get(pmid)
            found = (self._pubmed.pmids_for_title(summary.title)
                     if summary and summary.title else [])
            others = self.summaries([p for p in found if p != pmid])
            self._title_versions[pmid] = [
                p for p, other in others.items()
                if same_work(summary, other, versions=True)]
        return self._title_versions[pmid]

    def work(self, doi: str) -> Optional[RegisteredWork]:
        """The registering agency's record of the DOI's work; None when no agency has the
        DOI. Raises Unreadable when the agency cannot be read for it."""
        if doi not in self._works:
            try:
                self._works[doi] = self._registry.work(doi)
            except Unreadable as exc:
                self._works[doi] = exc
        found = self._works[doi]
        if isinstance(found, Unreadable):
            raise found
        return found

    def same_paper(self, doi: Optional[str], summary: PubMedSummary) -> bool:
        """Whether the registering agency gives `doi` to the paper of PubMed's record.
        Raises Unreadable when the agency cannot be read for it."""
        work = self.work(doi) if doi else None
        return work is not None and same_work(work, summary)

    def same_dois(self, doi: str, other: str) -> bool:
        """Whether the registering agencies give the two DOIs one work. Raises Unreadable
        when an agency cannot be read for one."""
        first, second = self.work(doi), self.work(other)
        return first is not None and second is not None and same_work(first, second)

    def versions(self, doi: str) -> List[str]:
        """The DOIs Crossref records as this DOI's preprint or version of record, on either
        side of the relation (the other side asked by each spelling of the DOI PubMed and
        PMC have given: Crossref matches a related DOI's case, and bioRxiv relates its
        preprints to 10.7554/eLife.86439, 10.1523/JNEUROSCI.0665-22.2023), with
        each one's DOI without its version number when the registry gives both one work
        (eLife's reviewed version 10.7554/eLife.92563.3 is 10.7554/eLife.92563, which
        Crossref does not relate to the preprint). Raises Unreadable when the registry
        cannot be read for one of them."""
        if doi not in self._versions:
            relations = self._registry.relations(doi)
            relating = [self._registry.relating(spelling, _VERSION_OF) for spelling in
                        sorted({doi, *self._spellings.get(normalize_doi(doi) or doi, ())})]
            related = [r for kind in _VERSION_OF
                       for r in [*relations.get(kind, ()), *(r for found in relating
                                                             for r in found.get(kind, ()))]]
            concepts = [base for r in related if (base := _VERSIONED.sub("", r)) != r
                        and self.same_dois(base, r)]
            self._versions[doi] = list(dict.fromkeys(
                d for d in (normalize_doi(v) for v in related + concepts) if d))
        return self._versions[doi]
