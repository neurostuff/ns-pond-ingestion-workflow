"""Name articles from PDFs someone already downloaded.

A shared folder of PDFs arrives with no identifiers, so they are read off the
PDF: its document metadata first, then the text of its first two pages. Of
532 PDFs in one such folder, 502 printed their DOI there and none printed a
PMID or PMCID -- those come from `--enrich`. What is left falls back to a
title search on OpenAlex and Semantic Scholar, kept only when the match's
authors are named on the PDF itself.

Each PDF then becomes the article's `download/pdf` artifact, so `ingest run`
extracts the copy on disk instead of looking for one on the web.
"""

from __future__ import annotations

import hashlib
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence
from unicodedata import combining, normalize
from urllib.parse import quote

import requests

from ingestion_workflow.catalog import ArticleRef, Catalog, Outcome, Status
from ingestion_workflow.extractors.utils import build_downloaded_file
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    FileType,
    Identifier,
)
from ingestion_workflow.services import logging

logger = logging.get_logger(__name__)

PDF_MAGIC = b"%PDF-"
PAGES_READ = 2

_DOI = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+")
# A DOI the layout broke: after a hyphen at a line end ("10.1038/s41386-\n018-0019-7"),
# or anywhere before its suffix reached a digit ("10.1073/pnas. 1011455107").
_DOI_LINE_BREAK = re.compile(r"(10\.\d{4,9}/\S*-)\s*\r?\n\s*")
_DOI_UNFINISHED = re.compile(r"(10\.\d{4,9}/[^\s\d\"'<>]*[.\-])\s+(?=\S*\d)")
_TRAILING = ".,;:"
# Data and preprint-archive DOIs that a methods section prints beside the
# article's own: Dryad, figshare, Zenodo, OSF.
_DATA_PREFIXES = ("10.5061/", "10.6084/", "10.5281/", "10.17605/")
_SUPPLEMENT = re.compile(r"(?i)\bsupp")
_YEAR = re.compile(r"\b(19[5-9]\d|20\d\d)\b")
# "145.Ames SL,2014, Title" / "3. Garavan H, Title" / "478. Ding, et al. Title"
_LEADING_NUMBER = re.compile(r"^[\d.\s]+")
_AUTHOR_PREFIX = re.compile(
    r"^[A-Z][\w'\-‐]+(?:\s+[A-Z]{1,3})?,?\s*et all?\.?\s*,?\s*(?:(?:19|20)\d\d\s*,\s*)?"
    r"|^[A-Z][\w'\-‐]+(?:\s+[A-Z]{1,3})?\s*,\s*(?:(?:19|20)\d\d\s*,\s*)?",
    re.IGNORECASE,
)
_JUNK_TITLE = re.compile(r"(?i)^(microsoft word|untitled|pii:)|\.(docx?|fm|pdf)\b")


@dataclass
class LocalPdf:
    """What one PDF says about the article it holds."""

    path: Path
    sha256: str
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    found_by: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None
    supplement: bool = False
    error: Optional[str] = None
    candidates: List[str] = field(default_factory=list)
    #: The first pages, the Author field and the filename: where a title
    #: match's authors must appear for the match to be believed.
    front: str = field(default="", repr=False)

    @property
    def identifier(self) -> Optional[Identifier]:
        if not (self.doi or self.pmid or self.pmcid):
            return None
        return Identifier(doi=self.doi, pmid=self.pmid, pmcid=self.pmcid)


def find_pdfs(root: Path) -> List[Path]:
    """Every PDF under `root`, by content rather than extension.

    Shared folders lose extensions: two files in the first one ended in `_`.
    """
    found = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            if handle.read(len(PDF_MAGIC)) == PDF_MAGIC:
                found.append(path)
    return found


def read_pdf(path: Path) -> LocalPdf:
    import pypdfium2 as pdfium

    pdf = LocalPdf(
        path=path,
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        supplement=bool(_SUPPLEMENT.search(path.name)),
    )
    try:
        document = pdfium.PdfDocument(path)
        meta = {key: value for key, value in document.get_metadata_dict().items() if value}
        pages = [
            document[index].get_textpage().get_text_range()
            for index in range(min(PAGES_READ, len(document)))
        ]
    except Exception as exc:  # noqa: BLE001 - one unreadable file is reported, not fatal
        pdf.error = f"{type(exc).__name__}: {exc}"
        return pdf

    from_meta = dois_in(" ".join(meta.values()))
    pdf.candidates = list(dict.fromkeys(from_meta + dois_in("\n".join(pages))))
    pdf.doi = choose_doi(pdf.candidates)
    if pdf.doi:
        pdf.found_by = "pdf metadata" if pdf.doi in from_meta else "page text"
    pdf.title = _title_like(meta.get("Title")) or title_from_filename(path)
    pdf.front = "\n".join([*pages, meta.get("Author", ""), path.name])
    year = _YEAR.search(path.stem)
    pdf.year = int(year.group(1)) if year else None
    return pdf


def dois_in(text: str) -> List[str]:
    text = _DOI_UNFINISHED.sub(r"\1", _DOI_LINE_BREAK.sub(r"\1", text))
    return [doi for doi in (clean_doi(match) for match in _DOI.findall(text)) if doi]


def clean_doi(doi: str) -> Optional[str]:
    """Drop the sentence punctuation a DOI was printed against.

    A closing bracket is the DOI's own only when it opened one, as in
    `10.1016/S0924-9338(02)00676-4`.
    """
    while doi:
        if doi[-1] in _TRAILING:
            doi = doi[:-1]
        elif doi[-1] in ")]" and doi.count(doi[-1]) > doi.count("(" if doi[-1] == ")" else "["):
            doi = doi[:-1]
        else:
            break
    # A suffix with no digit is a fragment: `10.1172/jci` of `10.1172/jci.insight.182331`.
    suffix = doi.partition("/")[2]
    return doi if re.search(r"\d", suffix) and not suffix.endswith("-") else None


def choose_doi(candidates: Sequence[str]) -> Optional[str]:
    ranked = rank_dois(candidates)
    return ranked[0] if ranked else None


def rank_dois(candidates: Sequence[str]) -> List[str]:
    """The DOIs printed on a PDF's first pages, likeliest to be its own first.

    Candidates arrive metadata first, then in page order; the earliest wins,
    after two corrections seen in practice. A DOI that another candidate
    extends by a segment (`/5550917`, `.supp`) is the article's, and the
    extension is a landing-page path or a supplement. A data repository DOI
    is never the article's.
    """
    lowered = [doi.lower() for doi in candidates]
    kept = []
    for doi, low in zip(candidates, lowered):
        if low.startswith(_DATA_PREFIXES):
            continue
        if any(low != other and _extends(low, other) for other in lowered):
            continue
        if low not in (k.lower() for k in kept):
            kept.append(doi)
    return kept


def confirm_dois(pdfs: Iterable[LocalPdf], registered: Callable[[str], Optional[bool]]) -> None:
    """Keep the first candidate DOI that is actually registered.

    A PDF can print a typo (`cpn.2018.16.1.290` beside the real `16.3.290`),
    a DOI its text layer mangled (`10.1037/a0025l`), or one with a landing
    page's path still attached (`10.1093/brain/awae369/7895715`), so each
    candidate is tried as printed and then with trailing segments removed.
    A PDF none of whose DOIs is registered is left to the title search. When
    the registry cannot be asked, the first candidate stands.
    """
    for pdf in pdfs:
        if pdf.found_by not in ("pdf metadata", "page text"):
            continue
        confirmed: Optional[str] = None
        for doi in (form for doi in rank_dois(pdf.candidates) for form in _shortened(doi)):
            answer = registered(doi)
            if answer is None:
                confirmed = pdf.doi
                break
            if answer:
                confirmed = doi
                break
        if confirmed is None:
            logger.info("%s: no printed DOI is registered: %s", pdf.path.name, pdf.candidates)
            pdf.found_by = None
        pdf.doi = confirmed


def _shortened(doi: str) -> Iterator[str]:
    yield doi
    prefix, _, suffix = doi.partition("/")
    parts = suffix.split("/")
    for end in range(len(parts) - 1, 0, -1):
        shorter = "/".join(parts[:end])
        if re.search(r"\d", shorter):
            yield f"{prefix}/{shorter}"


def doi_registered(session: requests.Session) -> Callable[[str], Optional[bool]]:
    """Ask doi.org's handle API whether a DOI exists; None when it cannot say."""

    def registered(doi: str) -> Optional[bool]:
        try:
            response = session.get(f"https://doi.org/api/handles/{quote(doi)}", timeout=20)
        except requests.RequestException:
            return None
        if response.status_code == 404:
            return False
        if response.status_code != 200:
            return None
        return response.json().get("responseCode") == 1

    return registered


def _extends(longer: str, shorter: str) -> bool:
    return longer.startswith(shorter) and longer[len(shorter)] in "./"


def title_from_filename(path: Path) -> Optional[str]:
    stem = path.name
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    stem = re.sub(r"\s*-\s*Copy(?:\s*\(\d+\))?$", "", stem)
    stem = _LEADING_NUMBER.sub("", stem)
    stem = _AUTHOR_PREFIX.sub("", stem, count=1)
    return _title_like(stem.replace("_", " ").strip(" ._"))


def _title_like(text: Optional[str]) -> Optional[str]:
    if not text or _JUNK_TITLE.search(text):
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(re.findall(r"[A-Za-z]{3,}", text)) >= 4 else None


# -- title search --------------------------------------------------------------

#: Share of the PDF's title words a match's title must contain. Filenames
#: truncate titles and misspell words ("Cue- nduced"), so not all of them.
TITLE_OVERLAP = 0.8


def match_titles(pdfs: Iterable[LocalPdf], providers: Sequence) -> None:
    """Find ids for each PDF that printed no DOI, by searching its title.

    Each provider is a callable from title to `(Identifier, ArticleMetadata)`
    candidates, tried in order. A candidate is taken only when its title holds
    the PDF's and its authors are named on the PDF -- a title alone matches
    commentaries, errata and same-titled abstracts.
    """
    for pdf in pdfs:
        if pdf.identifier or pdf.supplement or not pdf.title:
            continue
        for name, search in providers:
            try:
                candidates = search(pdf.title)
            except Exception as exc:  # noqa: BLE001 - one failed search leaves one PDF unresolved
                logger.warning("%s title search failed for %r: %s", name, pdf.title, exc)
                continue
            match = next((c for c in candidates if verified(pdf, *c)), None)
            if match is not None:
                identifier = match[0]
                pdf.doi, pdf.pmid, pdf.pmcid = identifier.doi, identifier.pmid, identifier.pmcid
                pdf.found_by = f"title match ({name})"
                break


def title_searchers(settings) -> List[tuple]:
    """Each configured, credentialed provider's title search, in `metadata_providers` order."""
    from ingestion_workflow.clients.openalex import OpenAlexClient
    from ingestion_workflow.clients.pubmed import PubMedClient
    from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient

    factories = {
        "openalex": lambda: OpenAlexClient.from_settings(settings),
        "semantic_scholar": lambda: settings.semantic_scholar_api_key
        and SemanticScholarClient(settings.semantic_scholar_api_key),
        "pubmed": lambda: settings.pubmed_email
        and PubMedClient(
            email=settings.pubmed_email,
            api_key=settings.pubmed_api_key or None,
            tool=settings.pubmed_tool or "ingestion-workflow",
        ),
    }
    searchers = []
    for name in settings.metadata_providers:
        client = factories[name]() if name in factories else None
        if client:
            searchers.append((name, client.search_title))
    return searchers


def verified(pdf: LocalPdf, identifier: Identifier, metadata) -> bool:
    if not (identifier.doi or identifier.pmid or identifier.pmcid):
        return False
    year = metadata.publication_year
    if pdf.year and year and abs(int(year) - pdf.year) > 1:
        return False
    return titles_agree(pdf.title or "", metadata.title) and authors_named(
        [author.name for author in metadata.authors], pdf.front
    )


def titles_agree(local: str, remote: str) -> bool:
    wanted = set(_words(local))
    return bool(wanted) and len(wanted & set(_words(remote))) / len(wanted) >= TITLE_OVERLAP


def authors_named(authors: Sequence[str], text: str) -> bool:
    """The first author's surname, or two of the first five, appear in `text`.

    Surnames, because a page prints "J. Smith", "Smith J" or "SMITH, JOHN";
    two of five, because a provider can list an East Asian name either way
    round, which turns the "surname" into a given name.
    """
    present = set(_words(text, minimum=2))
    surnames = [words[-1] for words in (_words(name, minimum=2) for name in authors[:5]) if words]
    if not surnames:
        return False
    return surnames[0] in present or sum(name in present for name in surnames) >= 2


def _words(text: str, minimum: int = 3) -> List[str]:
    folded = "".join(c for c in normalize("NFKD", text) if not combining(c)).lower()
    return [word for word in re.findall(r"[a-z0-9]+", folded) if len(word) >= minimum]


# -- attaching -----------------------------------------------------------------


def attach(
    catalog: Catalog,
    pdfs: Sequence[LocalPdf],
    fingerprint: str,
    store: Optional[Path] = None,
    prefer_over: Sequence[str] = (),
) -> Dict[Path, str]:
    """Record each resolved PDF as its article's `download/pdf` artifact.

    With a `store`, the file is copied into it, named by its hash, so the
    catalog does not depend on the shared folder staying where it is. Without
    one the catalog points at the file where it lies, for a folder that is
    already the PDFs' permanent home.

    An article that already has a download keeps it and gets no PDF, unless
    every download it has is from a source in `prefer_over`; then it gets the
    PDF as well, and extraction, which takes sources in `download_sources`
    order, reads the PDF first. Returns each PDF's outcome, by path.
    """
    status: Dict[Path, str] = {}
    outcomes: List[Outcome] = []
    claimed: Dict[str, Path] = {}
    if store is not None:
        store.mkdir(parents=True, exist_ok=True)

    for pdf in pdfs:
        if pdf.supplement:
            status[pdf.path] = "supplement"
            continue
        identifier = pdf.identifier
        ref: Optional[ArticleRef] = catalog.resolve(identifier) if identifier else None
        if ref is None:
            status[pdf.path] = "unresolved"
            continue
        if ref.id in claimed:
            status[pdf.path] = f"duplicate of {claimed[ref.id].name}"
            continue
        claimed[ref.id] = pdf.path
        existing = catalog.artifacts([ref.id], "download").get(ref.id, {})
        held = sorted(s for s, a in existing.items() if a.status is Status.OK)
        if held and not set(held) <= set(prefer_over):
            status[pdf.path] = "already downloaded"
            continue

        target = pdf.path.resolve()
        if store is not None:
            target = store / f"{pdf.sha256}.pdf"
            if not target.exists():
                shutil.copyfile(pdf.path, target)
        result = DownloadResult(
            identifier=ref.identifier,
            source=DownloadSource.PDF,
            success=True,
            files=[build_downloaded_file(target, FileType.PDF, source=DownloadSource.PDF)],
        )
        outcomes.append(
            Outcome(
                article_id=ref.id,
                stage="download",
                source=DownloadSource.PDF.value,
                status=Status.OK,
                fingerprint=fingerprint,
                payload=result.to_dict(),
                summary={"files": 1, "types": ["pdf"], "from": str(pdf.path)},
            )
        )
        status[pdf.path] = f"attached over {','.join(held)}" if held else "attached"

    catalog.record(outcomes)
    return status


__all__ = [
    "LocalPdf",
    "attach",
    "choose_doi",
    "clean_doi",
    "confirm_dois",
    "doi_registered",
    "dois_in",
    "find_pdfs",
    "match_titles",
    "rank_dois",
    "read_pdf",
    "title_from_filename",
    "title_searchers",
]
