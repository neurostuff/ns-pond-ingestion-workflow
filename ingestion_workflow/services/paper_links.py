"""Every http(s) link an article gives, where it gives it, and which of them a probe can ask.

Read from four places (`FOUND_BY`): the source's markup (JATS `ext-link` and `uri`,
Elsevier `ce:inter-ref`, a publisher page's `a@href` whose text shows the link), a URL
pattern over the extracted text (a PDF, a link nothing marks), the supplementary material
JATS links to, and the reference list, whose URLs count and whose DOIs count only when
they are a dataset's (`DATA_DOI_PREFIXES`). A DOI is a link as `https://doi.org/<doi>`.

Each link is kept once, by its normalized url (`normalize_url`, study_schema's
`PaperLink.url`), with every occurrence in text order: its offsets into the text, its
section, how it was found, the entry of the references stage's list it was read from, and
the words near it that say it holds imaging data (`NIFTI_CONTEXT`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

from ingestion_workflow.utils.doi import DATA_DOI_PREFIXES, normalize_doi

FOUND_BY = ("markup", "text", "supplement", "reference")

#: Words near a link that say it holds images: NIfTI, a `.nii` file, statistical maps,
#: (f)MRI data. Matched in `CONTEXT_CHARS` either side of the link.
NIFTI_CONTEXT = re.compile(
    r"\bnifti\b|\.nii(?:\.gz)?\b"
    r"|\b(?:stat(?:istic(?:al)?|s)?|[tz][-\s]?(?:stat(?:istic)?)?)[-\s]maps?\b"
    r"|\bunthresholded\b"
    r"|\b(?:f?mri|imaging|neuroimaging)\s+data(?:sets?)?\b",
    re.IGNORECASE,
)
CONTEXT_CHARS = 200

_DOI_RESOLVER = re.compile(r"(?i)^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)(10\.\d{4,9}/\S+)$")
#: Where a link starts in running text: a scheme, `www.`, or the bare host of a repository a
#: data statement prints without one. A dataset DOI is found by `_DATA_DOI_START`.
_URL_START = re.compile(
    r"(?i)\bhttps?://|(?<![\w@/.-])www\.|(?<![\w@/.-])(?:osf\.io|openneuro\.org|neurovault\.org)/")
_DATA_DOI_START = re.compile(
    r"(?i)(?<![\w/.])(?:" + "|".join(re.escape(p) for p in DATA_DOI_PREFIXES) + ")")
#: What a link may hold, up to the first space or quote.
_URL_CHARS = re.compile(r"[^\s<>\"'“”‘’]+")
#: A line broken inside a link: the text so far ends where a link goes on, and the next
#: word goes on as a link does (`osf. io/4b9v8/`, `openneuro. ds007115.v1.0.0`), unlike a
#: word of the sentence (`and`, `e.g.`) or a heading a page's text glued on (`Results3.1`).
#: Docling joins a PDF's lines with a space.
_BROKEN_AT = "/.-_=?&#~"
_GOES_ON = re.compile(r"[a-z0-9_~%-][A-Za-z0-9_~%-]+[/.#?=&][A-Za-z0-9]")
_TRAILING = ".,;:!?*"
_CLOSERS = {")": "(", "]": "[", "}": "{", ">": "<"}
#: A section number a page's text glued to the link before it: `osf.io/w6khb.2.6. fMRI`.
_GLUED_NUMBER = re.compile(r"(?<=[A-Za-z])(?:\.\d{1,2}){2,3}$")

#: OSF route names five characters long, which are not a project's id.
_OSF_ROUTES = frozenset({"login", "users", "about", "files", "nodes", "guids", "legal", "admin"})
_OSF_ID = re.compile(r"^[a-z0-9]{5}$")
_OSF_DOI = re.compile(r"^10\.17605/osf\.io/([a-z0-9]{5})$")
_OPENNEURO_PATH = re.compile(r"^/datasets/(ds\d{6})(?:/versions/([\w.-]+))?(?:/|$)", re.IGNORECASE)
_OPENNEURO_DOI = re.compile(r"^10\.18112/openneuro\.(ds\d{6})(?:\.v([\w.-]+))?$")


# ------------------------------------------------------------------ normalizing
def doi_url(doi: str) -> Optional[str]:
    """A DOI as its link: `https://doi.org/` and the DOI with A-Z in lower case."""
    doi = normalize_doi(unquote(doi or ""))
    return f"https://doi.org/{doi}" if doi else None


def normalize_url(url: Optional[str]) -> Optional[str]:
    """`url` as study_schema's PaperLink.url: the scheme and host in lower case, no `www.`,
    no fragment and no trailing slash; a DOI, bare or at a resolver, as `doi_url`. A link
    printed without a scheme gets `https://`. None for what is not an http(s) link."""
    url = (url or "").strip()
    if not url:
        return None
    found = _DOI_RESOLVER.match(url.split("#", 1)[0])
    if found:
        return doi_url(found.group(1))
    if re.match(r"(?i)^10\.\d{4,9}/\S+$", url):
        return doi_url(url)
    if not re.match(r"(?i)^[a-z][a-z0-9+.-]*://", url):
        # `www.` and a host, or a host and a path: not a file name (`fnins-19-1661523.pdf`)
        if not re.match(r"(?i)^(?:www\.[a-z0-9-]+(?:\.[a-z0-9-]+)+(?:[/:?]|$)"
                        r"|[a-z0-9-]+(?:\.[a-z0-9-]+)+/)", url):
            return None
        url = "https://" + url
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.hostname:
        return None
    netloc = parts.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunsplit((scheme, netloc, parts.path.rstrip("/"), parts.query, "")) or None


def is_doi_link(url: str) -> bool:
    return url.startswith("https://doi.org/")


def is_data_doi_link(url: str) -> bool:
    return is_doi_link(url) and url[len("https://doi.org/"):].startswith(DATA_DOI_PREFIXES)


# ------------------------------------------------------------------- probe targets
@dataclass(frozen=True)
class Target:
    """The repository record a link names: an OSF guid or an OpenNeuro dataset."""

    repository: str  # "osf" or "openneuro"
    id: str
    #: OpenNeuro's snapshot tag, when the link names one; else the latest snapshot.
    version: Optional[str] = None
    #: An OSF view-only key the link carries, which opens a private project to its holder.
    view_only: Optional[str] = None


def probe_target(url: str) -> Optional[Target]:
    """The OSF project or OpenNeuro dataset a normalized link names; None for any other."""
    parts = urlsplit(url)
    host, path = parts.netloc, parts.path
    if host == "doi.org":
        doi = path.lstrip("/")
        if m := _OSF_DOI.match(doi):
            return Target("osf", m.group(1))
        if m := _OPENNEURO_DOI.match(doi):
            return Target("openneuro", m.group(1), m.group(2))
        return None
    if host == "osf.io":
        first = path.strip("/").split("/", 1)[0].lower()
        if not _OSF_ID.match(first) or first in _OSF_ROUTES:
            return None
        key = (parse_qs(parts.query).get("view_only") or [None])[0]
        return Target("osf", first, view_only=key)
    if host == "openneuro.org" and (m := _OPENNEURO_PATH.match(path)):
        return Target("openneuro", m.group(1).lower(), m.group(2))
    return None


# ------------------------------------------------------------------- occurrences
@dataclass
class Occurrence:
    url: str
    found_by: str
    #: As the source gave it: an href, or the text matched.
    raw: str
    start_char: Optional[int] = None
    end_char: Optional[int] = None
    section: Optional[str] = None
    #: The id of the references stage's entry it was read from (`ParsedPaper.references`).
    reference_id: Optional[str] = None
    context: Tuple[str, ...] = ()

    def placed(self) -> bool:
        return self.start_char is not None

    def to_dict(self) -> dict:
        out = {"found_by": self.found_by, "raw": self.raw, "start_char": self.start_char,
               "end_char": self.end_char, "section": self.section, "context": list(self.context)}
        if self.reference_id:
            out["reference_id"] = self.reference_id
        return out


def _trim(link: str) -> str:
    """Drop the punctuation a link was printed against, and a section number a sentence
    ending in it was glued to."""
    ended = False
    while link:
        if link[-1] in _TRAILING:
            ended = ended or link[-1] == "."
            link = link[:-1]
        elif link[-1] in _CLOSERS and link.count(link[-1]) > link.count(_CLOSERS[link[-1]]):
            link = link[:-1]
        elif ended and (glued := _GLUED_NUMBER.search(link)):
            link, ended = link[: glued.start()], False
        else:
            break
    return link


def _extent(text: str, start: int) -> int:
    """Where a link starting at `start` ends, going on across a line broken inside it."""
    end = start
    while True:
        run = _URL_CHARS.match(text, end)
        if run is None:
            return end
        end = run.end()
        if (end + 1 < len(text) and text[end] in " \n" and text[end - 1] in _BROKEN_AT
                and _GOES_ON.match(text, end + 1)):
            end += 1
            continue
        return end


def _joined(raw: str) -> str:
    return re.sub(r"[ \n]", "", raw)


def from_text(text: str) -> Iterator[Tuple[str, int, int]]:
    """(normalized url, start, end) of every link and dataset DOI printed in `text`."""
    taken: List[Tuple[int, int]] = []
    for pattern in (_URL_START, _DATA_DOI_START):
        for m in pattern.finditer(text):
            start = m.start()
            if any(a <= start < b for a, b in taken):
                continue
            end = _extent(text, start)
            raw = text[start:end]
            kept = _trim(raw)
            # `_trim` sees the link without the line breaks `_joined` drops, so the kept
            # length counts the broken text's own characters
            end = start + len(kept)
            url = normalize_url(_joined(kept))
            if url is None or (pattern is _DATA_DOI_START and not is_doi_link(url)):
                continue
            taken.append((start, end))
            yield url, start, end


def _context(text: str, start: int, end: int) -> Tuple[str, ...]:
    window = text[max(0, start - CONTEXT_CHARS): end + CONTEXT_CHARS]
    found = []
    for m in NIFTI_CONTEXT.finditer(window):
        word = re.sub(r"\s+", " ", m.group(0).lower())
        if word not in found:
            found.append(word)
    return tuple(found)


# ----------------------------------------------------------------------- markup
_XLINK = "{http://www.w3.org/1999/xlink}href"
_CE = "http://www.elsevier.com/xml/common/dtd"
_SB = "http://www.elsevier.com/xml/common/struct-bib/dtd"
_JATS_SUPPLEMENTS = ("supplementary-material", "inline-supplementary-material", "media")


def _local(el) -> str:
    from lxml import etree

    return etree.QName(el).localname if isinstance(el.tag, str) else ""


def _marked_url(href: Optional[str], kind: Optional[str] = None) -> Optional[str]:
    if not href:
        return None
    if (kind or "").lower() == "doi":
        return doi_url(href)
    return normalize_url(href)


def _jats(xml_path: Path, text: str, notes: Dict[str, int]) -> List[Occurrence]:
    from lxml import etree

    from ingestion_workflow.services.citations import jats_spans

    tree = etree.parse(str(xml_path))
    found: List[Tuple[object, Occurrence]] = []
    for el in tree.getroot().iter():
        name = _local(el)
        if name in ("ext-link", "uri"):
            href = el.get(_XLINK) or ("".join(el.itertext()) if name == "uri" else None)
            url = _marked_url(href, el.get("ext-link-type"))
        elif name in _JATS_SUPPLEMENTS:
            href = el.get(_XLINK)
            url = normalize_url(href) if href and re.match(r"(?i)^https?://", href) else None
        else:
            continue
        if url is None:
            continue
        ref = next((a for a in el.iterancestors() if _local(a) == "ref"), None)
        how = ("reference" if ref is not None
               else "supplement" if name in _JATS_SUPPLEMENTS else "markup")
        found.append((el, Occurrence(url, how, href.strip(),
                                     reference_id=ref.get("id") if ref is not None else None)))
    for ref in tree.getroot().iter("ref"):
        for pid in ref.iter("pub-id"):
            if (pid.get("pub-id-type") or "").lower() == "doi":
                value = "".join(pid.itertext()).strip()
                url = doi_url(value)
                if url:
                    found.append((None, Occurrence(url, "reference", value,
                                                   reference_id=ref.get("id"))))
    placed = [(el, occ) for el, occ in found if el is not None]
    spans = jats_spans(tree, xml_path, [el for el, _ in placed], text, notes)
    for i, (_, occ) in enumerate(placed):
        if i in spans:
            occ.start_char, occ.end_char = spans[i]
    return [occ for _, occ in found]


def _elsevier(xml_path: Path, text: str, notes: Dict[str, int]) -> List[Occurrence]:
    from lxml import etree

    from ingestion_workflow.services.citations import elsevier_spans

    root = etree.fromstring(xml_path.read_bytes())
    found: List[Tuple[object, Occurrence]] = []
    for el in root.iter(f"{{{_CE}}}inter-ref"):
        href = el.get(_XLINK)
        url = _marked_url(href)
        if url is None:
            continue
        ref = next((a for a in el.iterancestors() if a.tag == f"{{{_CE}}}bib-reference"), None)
        found.append((el, Occurrence(url, "markup" if ref is None else "reference", href.strip(),
                                     reference_id=ref.get("id") if ref is not None else None)))
    for ref in root.iter(f"{{{_CE}}}bib-reference"):
        for doi in ref.iter(f"{{{_CE}}}doi"):
            value = "".join(doi.itertext()).strip()
            url = doi_url(value)
            if url:
                found.append((None, Occurrence(url, "reference", value,
                                               reference_id=ref.get("id"))))
    placed = [(el, occ) for el, occ in found if el is not None]
    spans = elsevier_spans(root, [el for el, _ in placed], text, notes)
    for i, (_, occ) in enumerate(placed):
        if i in spans:
            occ.start_char, occ.end_char = spans[i]
    return [occ for _, occ in found]


def _html(html_path: Path, text: str) -> List[Occurrence]:
    """The page's anchors whose own text prints a link. An anchor of words ("PubMed",
    "Google Scholar", "Log in") is the publisher's furniture, not a link the paper gives."""
    from lxml import html as lhtml

    from ingestion_workflow.services.citations import _Letters

    parser = lhtml.HTMLParser(encoding="utf-8", huge_tree=True)
    root = lhtml.parse(str(html_path), parser).getroot()
    if root is None:
        return []
    for bad in root.xpath("//script|//style|//noscript"):
        bad.drop_tree()
    letters = _Letters(text)
    out, cursor = [], 0
    for a in root.iter("a"):
        href = (a.get("href") or "").strip()
        url = normalize_url(href) if re.match(r"(?i)^https?://", href) else None
        shown = re.sub(r"\s+", "", a.text_content())
        if url is None or not (_URL_START.match(shown) or _DATA_DOI_START.match(shown)
                               or _DOI_RESOLVER.match(shown)):
            continue
        occ = Occurrence(url, "markup", href)
        pos = letters.s.find(shown, cursor)
        if pos < 0:
            pos = letters.s.find(shown)
        if pos >= 0:
            occ.start_char = letters.idx[pos]
            occ.end_char = letters.idx[pos + len(shown) - 1] + 1
            cursor = pos + len(shown)
        out.append(occ)
    return out


def from_markup(source: str, download, text: str,
                notes: Dict[str, int]) -> Optional[List[Occurrence]]:
    """The links the source's markup gives, placed in `text` where it holds them; None for a
    source with no markup to read (a PDF) or whose main file is gone."""
    from ingestion_workflow.services.citations import JATS_SOURCES, READABLE_SOURCES, main_file

    if source not in READABLE_SOURCES:
        return None
    f = main_file(source, download)
    if f is None or not Path(f.file_path).exists():
        return None
    path = Path(f.file_path)
    if source in JATS_SOURCES:
        return _jats(path, text, notes)
    if source == "elsevier":
        return _elsevier(path, text, notes)
    return _html(path, text)


def from_reference_list(references: Sequence[dict]) -> List[Occurrence]:
    """The references stage's entries: each one's DOI, and the links its text prints."""
    out = []
    for entry in references:
        if entry.get("doi") and (url := doi_url(entry["doi"])):
            out.append(Occurrence(url, "reference", entry["doi"], reference_id=entry["id"]))
        for url, start, end in from_text(entry.get("text") or ""):
            out.append(Occurrence(url, "reference", entry["text"][start:end],
                                  reference_id=entry["id"]))
    return out


# -------------------------------------------------------------------- the article
def _kept(occ: Occurrence) -> bool:
    """A DOI in the reference list is a citation, not a link, unless a dataset's."""
    in_list = occ.found_by == "reference" or occ.section == "references"
    return not (in_list and is_doi_link(occ.url) and not is_data_doi_link(occ.url))


def read_links(text: str, markup: Optional[List[Occurrence]],
               listed: Optional[Sequence[dict]] = None) -> List[dict]:
    """Every link of the article: [{url, nifti_context, occurrences}], in order of first
    appearance, each one's occurrences in text order and those no span holds after them.

    `markup` is `from_markup`'s, `listed` the references stage's list, which sync writes as
    `ParsedPaper.references`: an occurrence names one of its entries or none, so markup
    in an entry the list does not hold, or with no list, names none.
    """
    from ingestion_workflow.services.paper_parse import section_at, sections

    spans = sections(text)
    ids = {entry["id"] for entry in listed or []}
    occurrences = list(markup or [])
    for occ in occurrences:
        if occ.reference_id not in ids:
            occ.reference_id = None
    marked = [(o.start_char, o.end_char) for o in occurrences if o.placed()]
    for url, start, end in from_text(text):
        # the markup's href is the link the text prints there; the text may have cut it
        if any(a < end and start < b for a, b in marked):
            continue
        occurrences.append(Occurrence(url, "text", text[start:end], start, end))
    occurrences += from_reference_list(listed or [])
    links: Dict[str, List[Occurrence]] = {}
    seen = set()
    for occ in occurrences:
        if occ.placed():
            section = section_at(spans, occ.start_char)
            occ.section = getattr(section.kind, "value", section.kind) if section else None
            occ.context = _context(text, occ.start_char, occ.end_char)
        key = (occ.url, occ.found_by, occ.start_char, occ.end_char, occ.reference_id)
        if key in seen or not _kept(occ):
            continue
        seen.add(key)
        links.setdefault(occ.url, []).append(occ)
    for found in links.values():
        found.sort(key=lambda o: (not o.placed(), o.start_char or 0))
    order = sorted(links, key=lambda url: min(
        (o.start_char for o in links[url] if o.placed()), default=len(text) + 1))
    return [{"url": url, "nifti_context": any(o.context for o in links[url]),
             "occurrences": [o.to_dict() for o in links[url]]} for url in order]
