"""Tell a downloaded article page from a page that only stands where the article should be.

A publisher answers a scrape with a page whatever it has to give: an empty viewer
shell, a paywall notice over the abstract, an error page, or a citation tab. Each
parses as HTML and yields a few dozen characters of text that is not the article.
`check_page` names which of these a page is, so the download counts as failed with
that reason and the next source is tried.

A page is judged by its prose first. Running text of article length is the article,
whatever else the page says: publishers put "subscribe", "purchase access" and
"Not Found" widgets around full texts they serve, and print the abstract first.
Only a page without that much prose is read for the notices. Either way, a page that
shows an abstract and no body (no section headings and almost no running prose beside
the abstract, however long its reference list) is only the abstract.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Union

import lxml.html
from lxml import etree

from ingestion_workflow.models import DownloadResult, FileType

EMPTY_SHELL = "empty_shell"
PAYWALL = "paywall"
ERROR_PAGE = "error_page"
CITATION_STUB = "citation_stub"
ABSTRACT_ONLY = "abstract_only"
REASONS = (EMPTY_SHELL, PAYWALL, ERROR_PAGE, CITATION_STUB, ABSTRACT_ONLY)

#: Visible characters below which a page holds nothing to read (a PDF viewer's
#: `<embed>`, a bare `<body></body>`, a script-only shell).
MIN_VISIBLE_CHARS = 200
#: Words of running prose that make a page an article. An abstract runs to 150-400.
ARTICLE_WORDS = 800
#: Fewer words of prose than this and the page has no abstract either: a citation,
#: a metrics tab, a record display.
STUB_WORDS = 50
#: A heading that is only a status code ("ScienceDirect - 404") names an error page
#: only over fewer words of prose than this; the code alone could be part of a title.
ERROR_PAGE_WORDS = 200
#: A block of fewer words is a link, a label or a menu item, not prose.
_PROSE_BLOCK_WORDS = 25
#: Sentences of running prose, abstract included and the reference list's titles not,
#: below which a page that shows an
#: abstract and no body sections holds only the abstract. An abstract runs to 5-15;
#: the body of the shortest article with an abstract, to dozens more.
ABSTRACT_ONLY_SENTENCES = 20
#: Sentences an abstract must have for the page to count as showing one. A letter has
#: none, so it is never taken for an abstract page, however short.
ABSTRACT_SENTENCES = 3
#: Distinct body section headings that make a page an article's body.
BODY_SECTIONS = 2
#: Words of body prose beside the abstract that make a page an article, sections or
#: not: a commentary or an editorial runs to 500 and more. Of the stored pages that
#: are only an abstract, 6 in 5,000 have 300 or more beside it (a Karger page's cookie
#: box, copyright and disclaimer).
BODY_PROSE_WORDS = 400

_BLOCKS = frozenset(
    "p div section article main li td dd blockquote figcaption h1 h2 h3 h4 h5 h6 ul ol dl table tr"
    .split()
)
_NOT_CONTENT = ("script", "style", "noscript", "template", "svg", "head")
_CHROME = frozenset(("nav", "footer", "aside", "header"))
# Consent dialogs run to hundreds of words of prose of their own. A page may
# also name its whole wrapper after a cookie banner it shows, so a container
# holding the article's title is kept.
CONSENT_NAMES = re.compile(r"cookie|consent|onetrust|gdpr", re.I)

# Each notice is a publisher's own wording, read from stored pages; none fires
# on a page that has an article's prose, since those are passed before this.
# These say the reader cannot see the article.
_PAYWALL = re.compile(
    r"abstract/extract view of this article is free"
    r"|full text of this article requires a subscription"
    r"|you (?:do not|don't) (?:currently )?have (?:full )?access"
    r"|to view (?:this item|the full text)"
    r"|preview of subscription content"
    r"|log ?in to (?:view|access|read) (?:the )?full",
    re.I,
)
# The access box Atypon sites (AHA, Mary Ann Liebert, SAGE) and APA PsycNet print where
# the article's body would be, never beside one. Read before the prose gate, which such a page can
# pass on its references, abstract and boilerplate alone.
_ACCESS_BOX = re.compile(
    r"get full access to this article"
    r"|view all access and purchase options"
    r"|you currently have no access to this content"
    r"|your account doesn.t have access to this content",
    re.I,
)
# These only offer the article for sale, which JAMA pages also do beside a short
# article they serve in full under a reader's institutional access.
_STOREFRONT = re.compile(
    r"(?:purchase|buy|rent) (?:this|the) article"
    r"|pay per article"
    r"|purchase (?:short-term )?access",
    re.I,
)
# The banner alone proves nothing: AHA prints it site-wide, over its access box too.
_ACCESS_PROVIDED = re.compile(r"\baccess provided by\b", re.I)
#: Paragraphs of body text that, beside that banner, show the article is served.
SERVED_PARAGRAPHS = 3
# Containers whose paragraphs are not the article's body.
_NOT_BODY = re.compile(r"abstract|\bref|biblio|citation|footnote|letter|comment|related", re.I)
# Read in the title and top headings, where an article's own title could say
# "error" or "not found": a bare word counts only as the whole heading's start.
_ERROR_HEADING = re.compile(
    r"^\s*error\s*(?:[-:|]|$)|^\s*not found\b"
    r"|page not found|404 not found|error 404|currently unavailable|temporarily unavailable"
    r"|service unavailable|internal server error|bad gateway|gateway time-?out"
    r"|attention required|client challenge|dns error|(?:article locator|sciencedirect) error",
    re.I,
)
# A status code alone, opening the heading or closing it after a dash or bar. A
# citation title ends in its page number after a colon ("66 (3): 404"), or holds it
# mid-title, and neither counts.
_STATUS_HEADING = re.compile(r"^\s*[45]\d\d\s*(?:[-:|]|$)|[-|\u2013]\s*[45]\d\d\s*$")

# A bibliographic database's record of the article: the citation and at most
# its abstract, however much else the page lists.
_RECORD_TITLE = re.compile(r"\bPubMed(?: - NCBI)?\s*$|^\s*PsycNET Record Display\s*$")

# PubMed's abstract page carries a hidden "temporarily unavailable" notice, so
# that phrase counts in a heading only.
_ERROR_BODY = re.compile(
    r"page not found|error 404|404 not found|currently unavailable"
    r"|internal server error|does not exist at this address"
    r"|\bprotecting [^.]{1,60} from malicious requests caused by AI crawlers",
    re.I,
)
# A body section's heading, whole: "2. Materials and Methods", "RESULTS:".
_SECTION = re.compile(
    r"^\s*(?:[\dIVX]+\.?\s*)?(introduction|background|methods?|materials?(?: and methods?)?"
    r"|patients and methods|subjects and methods|participants|experimental procedures?"
    r"|procedures?|results?|discussion|conclusions?|results and discussion|case reports?"
    r"|case presentations?|case descriptions?|study design|statistical analysis)\s*:?\s*$",
    re.I,
)
# Publishers label sections with styled <div>s and <span>s as often as with headings.
_LABELS = ("h1", "h2", "h3", "h4", "h5", "h6", "div", "span", "p", "strong", "b", "dt",
           "label", "header")
_LABEL_CHARS = 60
_LABEL_CHILDREN = 3
_HEADINGS = ("h1", "h2", "h3", "h4", "h5", "h6")
_ABSTRACT_HEADING = re.compile(r"^\s*(?:abstract|summary)\s*:?\s*$", re.I)
_ABSTRACT_NAME = re.compile(r"abstract", re.I)
# Other papers' abstracts, listed beside this one ("Recommended articles", "Cited by").
_ELSEWHERE = re.compile(r"recommend|related|cited-?by|citing", re.I)
#: A container named for the abstract holding more words than this is the article's
#: wrapper, not its abstract.
_ABSTRACT_MAX_WORDS = 2000
# A sentence of running prose: a capital, 40 characters or more without a stop, and a
# stop after a word. Menus, labels and table rows have no such run.
_SENTENCE = re.compile(r"[A-Z][^.!?]{40,}?[a-z)\]%][.!?](?=\s|$)")
_SENTENCE_WORDS = 8
_YEAR = re.compile(r"\b(?:19|20)\d\d[a-z]?\b")
# A reference list entry reads as a sentence too; these mark it as one.
_CITING = re.compile(r"et al\b|doi\b|PubMed|Google Scholar|Crossref|\bVol\b|pp\.", re.I)
# Scripts written without spaces between words: a character counts as half a word.
_CJK = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]")
_STOP = re.compile(r"[.!?](?=\s|$)|[\u3002\uff01\uff1f]")
_PUNCTUATION = "\"'()[]{},.;:!?\u2018\u2019\u201c\u201d"
#: A block of running prose has a stop at least every this many words.
_WORDS_PER_STOP = 60
# A container of the reference list, as a whole class or id token or the end of one
# ("c-article-references", "ref-list"). A toggle's name does not count: Atypon's whole
# article body is `article__body show-references`.
_REFERENCE_TOKEN = re.compile(
    r"^(?!(?:show|hide|has|with|no)-)(?:.*[-_])?(?:refs?|references?|ref-?list"
    r"|reference-list|bibliography|biblio|citations?|footnotes?)$"
    # or an entry of it ("citation-content", Atypon's reference entry)
    r"|^(?:citation|reference|bib)[-_]", re.I)
# Containers of prose that is not the article's body however it reads: dialogs, an
# article preview's snippets, author lists and biographies.
_BESIDE_BODY = re.compile(r"modal|dialog|popup|snippet|preview|author|contrib|\bbio", re.I)
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
    doc = _parse(html)
    if doc is None:
        return BadPage(EMPTY_SHELL, "page does not parse as HTML" if _has_text(html)
                       else "page is blank")
    title = _text(doc.find(".//title"))
    if _RECORD_TITLE.search(title) or doc.xpath("//meta[@name='ncbi_db'][@content='pubmed']"):
        return BadPage(CITATION_STUB, f"database record page {title[-60:]!r}")
    _drop_non_content(doc)

    body = doc.find("body")
    visible = _text(body)
    if len(visible) < MIN_VISIBLE_CHARS:
        return BadPage(EMPTY_SHELL, f"{len(visible)} characters of visible text")

    found = _ACCESS_BOX.search(visible)
    if found:
        return BadPage(PAYWALL, f"page says {found.group(0)!r} in place of the article")

    prose = _prose_words(body)
    if prose >= ARTICLE_WORDS:
        return _abstract_only(doc, prose)

    headings = [title] + [_text(h) for h in doc.xpath("//h1")[:3]]
    for heading in headings:
        if heading and (_ERROR_HEADING.search(heading)
                        or (prose < ERROR_PAGE_WORDS and _STATUS_HEADING.search(heading))):
            return BadPage(ERROR_PAGE, f"heading {heading[:80]!r}")
    found = _ERROR_BODY.search(visible)
    if found:
        return BadPage(ERROR_PAGE, f"page says {found.group(0)!r}")
    served = (_ACCESS_PROVIDED.search(visible) is not None
              and _body_paragraphs(body) >= SERVED_PARAGRAPHS)
    found = _PAYWALL.search(visible) or (None if served else _STOREFRONT.search(visible))
    if found:
        return BadPage(PAYWALL, f"page says {found.group(0)!r}; {prose} words of prose")
    if prose < STUB_WORDS:
        return BadPage(CITATION_STUB, f"{prose} words of prose")
    return _abstract_only(doc, prose)


@dataclass(frozen=True)
class PageProse:
    """The running prose a page shows, in distinct sentences (`prose_sentences`)."""

    sentences: int
    words: int
    #: Of those, the sentences and words of the abstract.
    abstract_sentences: int
    abstract_words: int
    #: The body section headings the page has outside its abstract ("introduction",
    #: "methods", ...).
    sections: tuple
    #: Words of running prose outside the abstract, the reference list and the
    #: containers of `_BESIDE_BODY`, in any script (`_body_words`).
    body_words: int
    #: Of `sentences`, those read in the reference list: titles of cited papers.
    reference_sentences: int


def page_prose(html: Union[str, bytes]) -> Optional[PageProse]:
    """The page's running prose outside page chrome, consent dialogs and lists of other
    papers, or None if it does not parse. A sentence the page repeats (in a hidden
    pop-up, say) counts once."""
    doc = _parse(html)
    if doc is None:
        return None
    _drop_non_content(doc)
    return _page_prose(doc)


def prose_sentences(text: str) -> List[str]:
    """The distinct sentences of running prose in `text`, in order.

    Table rows, menus, labels and reference list entries are not prose, so a text and the
    page it came from can be compared on what both say of the article.
    """
    seen: Dict[str, None] = {}
    for found in _SENTENCE.finditer(_WS.sub(" ", text)):
        sentence = found.group(0)
        words = sentence.split()
        if len(words) < _SENTENCE_WORDS:
            continue
        if sum(w.isalpha() and w.islower() for w in words) < 0.6 * len(words):
            continue
        if len(_YEAR.findall(sentence)) >= 2 or _CITING.search(sentence):
            continue
        seen.setdefault(sentence, None)
    return list(seen)


def prose_words(text: str) -> int:
    return sum(len(sentence.split()) for sentence in prose_sentences(text))


def _abstract_only(doc, prose: int) -> Optional[BadPage]:
    found = _page_prose(doc)
    if (len(found.sections) >= BODY_SECTIONS or found.abstract_sentences < ABSTRACT_SENTENCES
            or found.sentences - found.reference_sentences >= ABSTRACT_ONLY_SENTENCES
            or found.body_words >= BODY_PROSE_WORDS):
        return None
    other = found.sentences - found.abstract_sentences - found.reference_sentences
    sections = ", ".join(found.sections) or "none"
    return BadPage(
        ABSTRACT_ONLY,
        f"an abstract of {found.abstract_words} words ({found.abstract_sentences} sentences) "
        f"and {other} other sentences of prose, besides {found.reference_sentences} in the "
        f"reference list; {found.body_words} words of body prose; body sections: {sections}; "
        f"{prose} words in prose blocks",
    )


def _page_prose(doc) -> PageProse:
    body = doc.find("body")
    in_abstract = set()
    for el in body.iter():
        if not isinstance(el.tag, str):
            continue
        if (_ABSTRACT_NAME.search(f"{el.get('class') or ''} {el.get('id') or ''}")
                and len(_text(el).split()) < _ABSTRACT_MAX_WORDS):
            in_abstract.update(el.iter())
        elif el.tag in _HEADINGS and _ABSTRACT_HEADING.match(_text(el)):
            for sibling in el.itersiblings():
                if sibling.tag in _HEADINGS:
                    break
                in_abstract.update(sibling.iter())
    sentences: Dict[str, bool] = {}
    cited = set()
    blocks = []
    for el in body.iter(*_BLOCKS):
        own = _own_text(el)
        if len(own.split()) < _PROSE_BLOCK_WORDS and _words(own) < _PROSE_BLOCK_WORDS:
            continue
        lineage = [el, *el.iterancestors()]
        if any(a.tag in _CHROME for a in lineage):
            continue
        names = [f"{a.get('class') or ''} {a.get('id') or ''}" for a in lineage]
        if any(_ELSEWHERE.search(name) for name in names):
            continue
        in_references = any(
            _REFERENCE_TOKEN.match(token) for name in names for token in name.split())
        if len(own.split()) >= _PROSE_BLOCK_WORDS:
            for sentence in prose_sentences(own):
                if sentence not in sentences and in_references:
                    cited.add(sentence)
                # A reference list right after the Abstract heading is read as under it.
                sentences.setdefault(sentence, el in in_abstract and not in_references)
        if (el not in in_abstract and not in_references
                and not any(_BESIDE_BODY.search(name) for name in names)):
            blocks.append(_WS.sub(" ", own).strip())
    abstract = [s for s, a in sentences.items() if a]
    # A structured abstract's "Methods" and "Results" head no body.
    sections = sorted({m.group(1).lower() for el in body.iter(*_LABELS)
                       if el not in in_abstract for m in [_label(el)] if m})
    return PageProse(
        sentences=len(sentences),
        words=sum(len(s.split()) for s in sentences),
        abstract_sentences=len(abstract),
        abstract_words=sum(len(s.split()) for s in abstract),
        sections=tuple(sections),
        body_words=_body_words(blocks, set(abstract)),
        reference_sentences=len(cited),
    )


def _body_words(blocks: List[str], abstract: set) -> int:
    """Words of the blocks that read as running prose, each block once.

    Judged by its shape, not its words, so a body in any language counts: a stop at
    least every `_WORDS_PER_STOP` words and, in a script with letter case, mostly
    lowercase words. A reference entry (a year among mostly capitalised words) and a
    block repeating the abstract do not count.
    """
    seen, total = set(), 0
    for text in blocks:
        words = _words(text)
        if text in seen or len(_STOP.findall(text)) < max(2, words / _WORDS_PER_STOP):
            continue
        cased = [w.strip(_PUNCTUATION) for w in _CJK.sub(" ", text).split()]
        if len(_CJK.findall(text)) < len(cased):
            if sum(w.isalpha() and w.islower() for w in cased) < 0.5 * len(cased):
                continue
            capitalised = sum(w[:1].isupper() or w[:1].isdigit() for w in cased)
            if _YEAR.search(text) and capitalised >= 0.4 * len(cased):
                continue
        own = prose_sentences(text)
        if own and sum(sentence in abstract for sentence in own) * 2 >= len(own):
            continue
        seen.add(text)
        total += words
    return total


def _words(text: str) -> int:
    return len(_CJK.sub(" ", text).split()) + len(_CJK.findall(text)) // 2


def _label(el):
    if el.tag not in _HEADINGS and len(el) > _LABEL_CHILDREN:
        return None  # a container, not a label; its text would be read again for each child
    text = _text(el)
    return _SECTION.match(text) if len(text) < _LABEL_CHARS else None


def _has_text(html: Union[str, bytes]) -> bool:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    return bool(html.replace("\x00", "").strip())


def _parse(html: Union[str, bytes]):
    """The page's document, or None if it is blank or does not parse."""
    if isinstance(html, bytes):
        # Parsed as bytes, a page whose <meta> charset disagrees with its bytes
        # loses all but its head: 3,655 of 24,765 words on one stored article.
        html = html.decode("utf-8", errors="replace")
    # Some stored pages open with a run of NUL bytes, where lxml stops reading.
    html = html.replace("\x00", "")
    if not html.strip():
        return None
    try:
        return lxml.html.document_fromstring(_XML_DECLARATION.sub("", html, count=1))
    except (etree.ParserError, ValueError):
        return None


def _drop_non_content(doc) -> None:
    for el in doc.xpath("|".join(f"//{tag}" for tag in _NOT_CONTENT)):
        el.drop_tree()
    for el in doc.xpath("//body//*[@class or @id][not(.//h1)]"):
        if el.getparent() is not None and CONSENT_NAMES.search(
            f"{el.get('class') or ''} {el.get('id') or ''}"
        ):
            el.drop_tree()


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


def _body_paragraphs(body) -> int:
    """Paragraphs long enough to be prose, outside page chrome and outside the abstract,
    the references and other containers that are not the article's body."""
    count = 0
    for p in body.iter("p"):
        if len(_own_text(p).split()) < _PROSE_BLOCK_WORDS:
            continue
        lineage = [p, *p.iterancestors()]
        if any(el.tag in _CHROME for el in lineage):
            continue
        names = (f"{el.get('class') or ''} {el.get('id') or ''}" for el in lineage)
        if any(_NOT_BODY.search(name) for name in names):
            continue
        count += 1
    return count


def _own_text(el) -> str:
    parts = [el.text or ""]
    for child in el:
        if not (isinstance(child.tag, str) and child.tag in _BLOCKS):
            parts.append(child.text_content() if isinstance(child.tag, str) else "")
        parts.append(child.tail or "")
    return " ".join(parts)
