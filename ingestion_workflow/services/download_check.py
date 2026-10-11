"""Tell a downloaded article page from a page that only stands where the article should be.

A publisher answers a scrape with a page whatever it has to give: an empty viewer
shell, a paywall notice over the abstract, an error page, or a citation tab. Each
parses as HTML and yields a few dozen characters of text that is not the article.
`check_page` names which of these a page is, so the download counts as failed with
that reason and the next source is tried.

A page is judged by its prose first. Running text of article length is the article,
whatever else the page says: publishers put "subscribe", "purchase access" and
"Not Found" widgets around full texts they serve, and print the abstract first.
Only a page without that much prose is read for the notices.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import lxml.html
from lxml import etree

from ingestion_workflow.models import DownloadResult, FileType

EMPTY_SHELL = "empty_shell"
PAYWALL = "paywall"
ERROR_PAGE = "error_page"
CITATION_STUB = "citation_stub"
REASONS = (EMPTY_SHELL, PAYWALL, ERROR_PAGE, CITATION_STUB)

#: Visible characters below which a page holds nothing to read (a PDF viewer's
#: `<embed>`, a bare `<body></body>`, a script-only shell).
MIN_VISIBLE_CHARS = 200
#: Words of running prose that make a page an article. An abstract runs to 150-400.
ARTICLE_WORDS = 800
#: Fewer words of prose than this and the page has no abstract either: a citation,
#: a metrics tab, a record display.
STUB_WORDS = 50
#: A block of fewer words is a link, a label or a menu item, not prose.
_PROSE_BLOCK_WORDS = 25

_BLOCKS = frozenset(
    "p div section article main li td dd blockquote figcaption h1 h2 h3 h4 h5 h6 ul ol dl table tr"
    .split()
)
_NOT_CONTENT = ("script", "style", "noscript", "template", "svg", "head")
_CHROME = frozenset(("nav", "footer", "aside", "header"))
# Consent dialogs run to hundreds of words of prose of their own. A page may
# also name its whole wrapper after a cookie banner it shows, so a container
# holding the article's title is kept.
_CONSENT = re.compile(r"cookie|consent|onetrust|gdpr", re.I)

# Each notice is a publisher's own wording, read from stored pages; none fires
# on a page that has an article's prose, since those are passed before this.
_PAYWALL = re.compile(
    r"abstract/extract view of this article is free"
    r"|full text of this article requires a subscription"
    r"|(?:purchase|buy|rent) (?:this|the) article"
    r"|pay per article"
    r"|purchase (?:short-term )?access"
    r"|access through your institution"
    r"|you (?:do not|don't) (?:currently )?have (?:full )?access"
    r"|to view (?:this item|the full text)"
    r"|preview of subscription content"
    r"|log ?in to (?:view|access|read) (?:the )?full",
    re.I,
)
# Read in the title and top headings, where an article's own title could say
# "error" or "not found": a bare word counts only as the whole heading's start.
_ERROR_HEADING = re.compile(
    r"^\s*(?:error|[45]\d\d)\s*(?:[-:|]|$)|^\s*not found\b"
    r"|page not found|\b404\b|currently unavailable|temporarily unavailable"
    r"|service unavailable|internal server error|bad gateway|gateway time-?out"
    r"|attention required|client challenge|dns error|(?:article locator|sciencedirect) error",
    re.I,
)
# A bibliographic database's record of the article: the citation and at most
# its abstract, however much else the page lists.
_RECORD_TITLE = re.compile(r"\bPubMed(?: - NCBI)?\s*$|^\s*PsycNET Record Display\s*$")

# PubMed's abstract page carries a hidden "temporarily unavailable" notice, so
# that phrase counts in a heading only.
_ERROR_BODY = re.compile(
    r"page not found|error 404|404 not found|currently unavailable"
    r"|internal server error|does not exist at this address",
    re.I,
)
_WS = re.compile(r"\s+")
# lxml refuses a str that declares its own encoding.
_XML_DECLARATION = re.compile(r"^\s*<\?xml[^>]*\?>")


@dataclass(frozen=True)
class BadPage:
    """Why a downloaded page is not the article."""

    reason: str
    detail: str

    @property
    def message(self) -> str:
        return f"bad page ({self.reason}): {self.detail}"


def check_page(html: Union[str, bytes]) -> Optional[BadPage]:
    """The reason this page is not an article, or None if it is one."""
    if isinstance(html, bytes):
        # Parsed as bytes, a page whose <meta> charset disagrees with its bytes
        # loses all but its head: 3,655 of 24,765 words on one stored article.
        html = html.decode("utf-8", errors="replace")
    # Some stored pages open with a run of NUL bytes, where lxml stops reading.
    html = html.replace("\x00", "")
    if not html.strip():
        return BadPage(EMPTY_SHELL, "page is blank")
    try:
        doc = lxml.html.document_fromstring(_XML_DECLARATION.sub("", html, count=1))
    except (etree.ParserError, ValueError):
        return BadPage(EMPTY_SHELL, "page does not parse as HTML")
    title = _text(doc.find(".//title"))
    if _RECORD_TITLE.search(title) or doc.xpath("//meta[@name='ncbi_db'][@content='pubmed']"):
        return BadPage(CITATION_STUB, f"database record page {title[-60:]!r}")
    for el in doc.xpath("|".join(f"//{tag}" for tag in _NOT_CONTENT)):
        el.drop_tree()
    for el in doc.xpath("//body//*[@class or @id][not(.//h1)]"):
        if el.getparent() is not None and _CONSENT.search(
            f"{el.get('class') or ''} {el.get('id') or ''}"
        ):
            el.drop_tree()

    body = doc.find("body")
    visible = _text(body)
    if len(visible) < MIN_VISIBLE_CHARS:
        return BadPage(EMPTY_SHELL, f"{len(visible)} characters of visible text")

    prose = _prose_words(body)
    if prose >= ARTICLE_WORDS:
        return None

    headings = [title] + [_text(h) for h in doc.xpath("//h1")[:3]]
    for heading in headings:
        if heading and _ERROR_HEADING.search(heading):
            return BadPage(ERROR_PAGE, f"heading {heading[:80]!r}")
    found = _ERROR_BODY.search(visible)
    if found:
        return BadPage(ERROR_PAGE, f"page says {found.group(0)!r}")
    found = _PAYWALL.search(visible)
    if found:
        return BadPage(PAYWALL, f"page says {found.group(0)!r}; {prose} words of prose")
    if prose < STUB_WORDS:
        return BadPage(CITATION_STUB, f"{prose} words of prose")
    return None


_MESSAGE = re.compile(r"bad page \((%s)\)" % "|".join(REASONS))


def reason_of(message: Optional[str]) -> Optional[str]:
    """The reason in a failure message written from a `BadPage`, if it is one."""
    found = _MESSAGE.search(message or "")
    return found.group(1) if found else None


def check_file(path: Path) -> Optional[BadPage]:
    data = Path(path).read_bytes()
    if not data.strip():
        return BadPage(EMPTY_SHELL, "file is empty")
    return check_page(data)


def check_download(result: DownloadResult) -> Optional[BadPage]:
    """The first HTML file of a download that is not an article, if any.

    XML and PDF payloads come from full-text services and are not pages; they pass.
    """
    for downloaded in result.files:
        if downloaded.file_type is FileType.HTML:
            bad = check_file(Path(downloaded.file_path))
            if bad is not None:
                return bad
    return None


def _text(el) -> str:
    return _WS.sub(" ", el.text_content()).strip() if el is not None else ""


def _prose_words(body) -> int:
    """Words in the page's text blocks long enough to be prose, outside page chrome.

    A block's own words exclude those of blocks nested in it, so a section that
    holds text beside its subsections is counted once.
    """
    total = 0
    for el in body.iter(*_BLOCKS):
        words = len(_own_text(el).split())
        if words < _PROSE_BLOCK_WORDS:
            continue
        if el.tag in _CHROME or any(a.tag in _CHROME for a in el.iterancestors()):
            continue
        total += words
    return total


def _own_text(el) -> str:
    parts = [el.text or ""]
    for child in el:
        if not (isinstance(child.tag, str) and child.tag in _BLOCKS):
            parts.append(child.text_content() if isinstance(child.tag, str) else "")
        parts.append(child.tail or "")
    return " ".join(parts)
