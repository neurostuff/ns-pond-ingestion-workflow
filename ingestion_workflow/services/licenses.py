"""The licence of each copy of a paper, and of the paper, from what every source stated.

A statement is one source's licence for one version of the paper, or for the one copy at
a URL. A copy's licence is its own record's, else the first statement
`LICENSE_PRECEDENCE` lists for its version. A publisher's page held as a copy is that
copy's own record, and the last source for every other copy and for the paper. When none
was made it is all rights reserved, assumed. While a source that comes before the deciding
statement, or before an assumed licence, could not be read or did not answer, the licence
is undecided and the article is asked about again.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from lxml import etree
from lxml import html as lxml_html
from study_schema.models.paper_parse import (
    CopyRoute,
    CopyVersion,
    License,
    LicenseDetermination,
    LicenseId,
    MetadataSource,
)

from ingestion_workflow.models.download import DownloadSource

#: A held copy's own record: its file, or PMC's cloud record of it.
OWN = "own"
#: The own records of the other copies downloads hold, but for publishers' pages.
HELD = "held"
#: What the publisher's pages that downloads hold state.
PAGE = MetadataSource.publisher_page.value

#: For each version a copy can be, the sources whose statements speak for it, best first:
#: the source, the versions its statement must be about (None: it names no version), and
#: the routes of the copies it must be about (None: any, or no copy). A copy's own record
#: comes before all of them.
#:
#: - Another held copy of the same version states it in its own record.
#: - Crossref's deposit is the publisher's; a preprint DOI's deposits are the preprint's.
#: - OpenAlex speaks for a version only through its location at the publisher, or at the
#:   preprint server; a repository location, and Semantic Scholar's open-access PDF,
#:   describe only the copy at their URL.
#: - A manuscript's licence is never the version of record's.
#: - A publisher's page comes after every other source: its rights are read from markup
#:   that differs by publisher, theirs from records kept to state them.
LICENSE_PRECEDENCE: Mapping[
    Optional[CopyVersion], Tuple[Tuple[str, frozenset, Optional[frozenset]], ...]
] = {
    CopyVersion.vor: (
        (HELD, frozenset({CopyVersion.vor}), None),
        (MetadataSource.crossref.value, frozenset({CopyVersion.vor}), None),
        (MetadataSource.openalex.value, frozenset({CopyVersion.vor}),
         frozenset({CopyRoute.publisher})),
        (MetadataSource.crossref.value, frozenset({None}), None),
        (PAGE, frozenset({CopyVersion.vor}), None),
    ),
    CopyVersion.am: (
        (HELD, frozenset({CopyVersion.am}), None),
        (MetadataSource.crossref.value, frozenset({CopyVersion.am}), None),
        (PAGE, frozenset({CopyVersion.am}), None),
    ),
    CopyVersion.submitted: (
        (HELD, frozenset({CopyVersion.submitted}), None),
        (MetadataSource.crossref.value, frozenset({CopyVersion.submitted}), None),
        (MetadataSource.openalex.value, frozenset({CopyVersion.submitted}),
         frozenset({CopyRoute.preprint_server})),
    ),
    None: (),
}

#: The providers a licence statement can come from, besides the held copies' own records.
LICENSE_PROVIDERS = (MetadataSource.semantic_scholar, MetadataSource.crossref,
                     MetadataSource.openalex)


@dataclass(frozen=True)
class Statement:
    """A licence one source stated, for one version of the paper, or for the copy at `url`
    on `route`."""

    license: License
    version: Optional[CopyVersion] = None
    url: Optional[str] = None
    route: Optional[CopyRoute] = None


class Undecided(Exception):
    """A licence cannot be decided while `sources` (`OWN`, or provider names) have not
    answered; `outage` when each of them was only down or busy."""

    def __init__(self, message: str, sources: Sequence[str] = (), outage: bool = False) -> None:
        super().__init__(message)
        self.sources = tuple(sources)
        self.outage = outage


_CC_URL = re.compile(r"creativecommons\.org/licenses/([a-z-]+)")
_CC_PHRASE = re.compile(r"\bcc[ -]?by((?:[ -](?:nc|nd|sa))*)\b")
_COMPACT = {
    "ccby": LicenseId.cc_by,
    "ccbysa": LicenseId.cc_by_sa,
    "ccbync": LicenseId.cc_by_nc,
    "ccbyncsa": LicenseId.cc_by_nc_sa,
    "ccbysanc": LicenseId.cc_by_nc_sa,
    "ccbynd": LicenseId.cc_by_nd,
    "ccbyncnd": LicenseId.cc_by_nc_nd,
    "ccbyndnc": LicenseId.cc_by_nc_nd,
    "cc0": LicenseId.cc0,
    "cczero": LicenseId.cc0,
    "publicdomain": LicenseId.public_domain,
}
#: Terms that grant text and data mining and nothing more: Elsevier's TDM user licence and
#: TDM-REP licence, Springer's, Springer Nature's and Wiley's TDM URLs, PMC's text-mining
#: paragraph and its Cloud Service's `TDM` code.
_TDM = re.compile(r"text[ -]and[ -]data[ -]mining|text mining|\btdm\b|/tdm")


def _cc_from_parts(parts: str) -> Optional[LicenseId]:
    return _COMPACT.get("ccby" + re.sub(r"[^a-z]", "", parts))


def normalise_license(stated: str) -> LicenseId:
    """The licence a source's statement names: a URL, a short code or a paragraph."""
    text = " ".join(stated.lower().split())
    url = _CC_URL.search(text)
    if url:
        return _cc_from_parts(url.group(1).removeprefix("by")) or LicenseId.other
    if "creativecommons.org/publicdomain/zero" in text:
        return LicenseId.cc0
    if "creativecommons.org/publicdomain/mark" in text:
        return LicenseId.public_domain
    compact = re.sub(r"[^a-z0-9]", "", text).replace("creativecommons", "cc")
    if compact in _COMPACT:
        return _COMPACT[compact]
    phrase = _CC_PHRASE.search(text)
    if phrase:
        return _cc_from_parts(phrase.group(1)) or LicenseId.other
    if "creative commons" in text and "attribution" in text:
        parts = ""
        if re.search(r"non[ -]?commercial", text):
            parts += "nc"
        if re.search(r"share[ -]?alike", text):
            parts += "sa"
        if re.search(r"no[ -]?deriv", text):
            parts += "nd"
        return _cc_from_parts(parts) or LicenseId.other
    if re.search(r"\bcc0\b", text) or "public domain dedication" in text:
        return LicenseId.cc0
    if "public domain" in text:
        return LicenseId.public_domain
    if _TDM.search(text):
        return LicenseId.all_rights_reserved
    if "all rights reserved" in text or "all rights are reserved" in text:
        return LicenseId.all_rights_reserved
    return LicenseId.other


def stated(by: MetadataSource, text: Optional[str],
           license_id: Optional[LicenseId] = None) -> Optional[License]:
    """The licence `by` stated as `text`, or None when it stated nothing."""
    text = " ".join((text or "").split())
    if not text:
        return None
    return License(license_id=license_id or normalise_license(text),
                   determination=LicenseDetermination.stated, stated_by=by, stated_as=text)


def copyright_line(by: MetadataSource, line: Optional[str]) -> Optional[License]:
    """A copyright line with no licence beside it: the open licence it names, if any,
    else all rights reserved, stated."""
    found = stated(by, line)
    if found is None or found.license_id not in (LicenseId.other,
                                                 LicenseId.all_rights_reserved):
        return found
    return stated(by, line, LicenseId.all_rights_reserved)


ASSUMED = License(license_id=LicenseId.all_rights_reserved,
                  determination=LicenseDetermination.assumed)

_XLINK_HREF = "{http://www.w3.org/1999/xlink}href"
_ALI_LICENSE_REF = "{http://www.niso.org/schemas/ali/1.0/}license_ref"


def jats_license(root: etree._Element) -> Optional[License]:
    """The licence a JATS copy's `<permissions>` states for itself.

    Its statements are `ali:license_ref`s, then the `<license>`'s `@xlink:href`, the
    links in its text, its `@license-type` and its text. An open licence is taken before
    all rights reserved, and that before an `other`; then a `license_ref` whose
    `specific-use` is `textmining` yields to one for a version, such as `vor`; then the
    first wins. A `<copyright-statement>` with no licence is the licence it names, if
    any, else all rights reserved, stated.
    """
    permissions = root.find(".//article-meta/permissions")
    if permissions is None:
        return None
    # (text, specific-use): "" for a license_ref without one, None for the rest
    texts = [(ref.text, ref.get("specific-use") or "")
             for ref in permissions.iter(_ALI_LICENSE_REF)]
    licence = permissions.find("license")
    if licence is not None:
        texts += [(licence.get(_XLINK_HREF), None)]
        texts += [(link.get(_XLINK_HREF), None) for link in licence.iter("ext-link", "uri")]
        texts += [(licence.get("license-type"), None), (" ".join(licence.itertext()), None)]
    found = [(lic, use) for lic, use in ((stated(MetadataSource.jats, text), use)
                                         for text, use in texts) if lic]
    if found:
        version_ref = any(use not in (None, "textmining") for _, use in found)
        return min(found, key=lambda f: (_STRENGTH.get(f[0].license_id, 0),
                                         f[1] == "textmining" and version_ref))[0]
    line = permissions.findtext("copyright-statement")
    if not (line and line.strip()):
        holder = permissions.findtext("copyright-holder")
        year = permissions.findtext("copyright-year")
        if holder:
            line = " ".join(["©", *(part for part in (year, holder) if part)])
    return copyright_line(MetadataSource.jats, line)


#: Within one record, an open licence comes before all rights reserved, and that
#: before an `other`.
_STRENGTH = {LicenseId.all_rights_reserved: 1, LicenseId.other: 2}
#: The Creative Commons licences and dedication.
_CREATIVE_COMMONS = {lic.value for lic in (
    LicenseId.cc_by, LicenseId.cc_by_sa, LicenseId.cc_by_nc, LicenseId.cc_by_nc_sa,
    LicenseId.cc_by_nd, LicenseId.cc_by_nc_nd, LicenseId.cc0)}
#: The open licences: Creative Commons's and the public domain.
_OPEN = _CREATIVE_COMMONS | {LicenseId.public_domain.value}


def elsevier_license(content_xml: Path) -> Optional[License]:
    """The licence in an Elsevier full-text record's `coredata`: the user licence an
    open-access article names, else its copyright line."""
    root = etree.parse(str(content_xml)).getroot()
    coredata = next(iter(root.xpath("./*[local-name()='coredata']")), None)
    if coredata is None:
        return None

    def text(name: str) -> Optional[str]:
        found = coredata.xpath(f"./*[local-name()='{name}']")
        return " ".join(found[0].itertext()).strip() if found else None

    return (stated(MetadataSource.elsevier, text("openaccessUserLicense"))
            or copyright_line(MetadataSource.elsevier, text("copyright")))


#: What a publisher's page prints around every article, not about one: its footer, as a
#: tag or a class or id that contains `footer` (J Neurosci's `footer-group`, Psychiatria
#: Polska's `footer2`, Wiley's `footer-global__copyright`). An element that holds the
#: article's title, its `article` or `main`, or its footnotes is not a footer, whatever it
#: is called: HighWire classes its whole page `footer-borders`, and ScienceDirect keeps the
#: article's footnotes in `artFooterContent`. Figures (as tags, or Silverchair's `fig` and
#: `fig-caption`), references, research data cards and ScienceDirect's
#: supplementary files (`e-component`) are skipped too: the licences there are those of
#: what the article credits, cites or attaches.
_CHROME_TAGS = {"footer", "figure"}
_CHROME_WORD = re.compile(
    r"footer|(?:^|[\s_-])(?:fig|figure|caption|references?|bibliography|ref-list|research-data"
    r"|e-component)(?:$|[\s_-])",
    re.I)
_HOLDS_ARTICLE = ".//h1 | .//article | .//main | .//*[contains(@class, 'footnote')]"


_UPPER, _LOWER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
_CLASSES = f"concat(' ', translate(normalize-space(@class), '{_UPPER}', '{_LOWER}'), ' ')"
_CC_HOST = "creativecommons.org/"
#: The page's marks of the article's licence, in page order: a link to a Creative Commons
#: licence (an `href`, or a JATS `xlink:href` the page embeds), and the links
#: in a block classed `license`, which may name the publisher's own terms.
_PAGE_LICENSES = (
    f"//*[contains(@href, '{_CC_HOST}licenses')"
    f" or contains(@*[name() = 'xlink:href'], '{_CC_HOST}licenses')"
    f" or contains({_CLASSES}, ' license ')]"
)
#: A paragraph that grants a Creative Commons licence in words, as Springer's older
#: "Open Access" paragraph and ScienceDirect's older footnote do. Its sentences that grant
#: one to something else are not read: the article's data (BioMed Central's CC0 waiver for
#: "the data made available in this article", Scientific Data's for "the metadata files",
#: ScienceDirect's "This is open data under the CC BY license") or its accepted manuscript
#: (a funder's "CC BY public copyright license to any Author Accepted Manuscript").
_PAGE_PARAGRAPHS = "//*[self::p or (self::div and not(.//div or .//p))]"
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_ELSEWHERE_SCOPE = re.compile(r"\b(?:meta)?data(?:sets?)?\b|\baccepted manuscript\b", re.I)


def _about_the_article(text: str) -> str:
    """`text` without its sentences about the article's data or accepted manuscript."""
    return " ".join(sentence for sentence in _SENTENCE_END.split(text)
                    if not _ELSEWHERE_SCOPE.search(sentence))
#: Licences in the article records pages embed: Taylor & Francis's JSON-LD `license`,
#: ScienceDirect's `access.license`.
_EMBEDDED = re.compile(r'"license"\s*:\s*("https?:(?:[^"\\]|\\.)*")')
#: The article's copyright lines: its `dc.rights` and `prism.copyright` metadata, and what
#: an element classed `copyright` (or `copyright-line`, `ArticleCopyright`...) prints, when
#: it reads as one.
_PAGE_COPYRIGHTS = (
    "//meta[translate(@name, 'DCRIGHTSPRISMCOPY', 'dcrightsprismcopy') = 'dc.rights'"
    " or translate(@name, 'DCRIGHTSPRISMCOPY', 'dcrightsprismcopy') = 'prism.copyright']"
    f" | //*[contains(translate(@class, '{_UPPER}', '{_LOWER}'), 'copyright')]"
)
#: A copyright line has a mark, a year or a reservation, and a holder to name.
_COPYRIGHT_LINE = re.compile(r"©|\(c\)|\b(?:1[89]|20)\d\d\b|rights reserved", re.I)
_HOLDER = re.compile(r"[^\W\d_]{3}")

#: ACE saves pages as UTF-8, which libxml2 does not guess for a page that does not say so.
_UTF8_HTML = lxml_html.HTMLParser(encoding="utf-8")


def _strongest(found: Iterable[Optional[License]]) -> Optional[License]:
    """Within one record: an open licence, then all rights reserved, then an `other`; the
    first of them."""
    return min((lic for lic in found if lic),
               key=lambda lic: _STRENGTH.get(lic.license_id, 0), default=None)


def _page_root(page: bytes):
    return lxml_html.document_fromstring(page, parser=_UTF8_HTML)


def _article_marks(root, xpath: str) -> List:
    """The elements `xpath` finds outside the page's chrome."""
    chrome: Dict = {}

    def is_chrome(element) -> bool:
        if element not in chrome:
            named = (element.tag in _CHROME_TAGS
                     or _CHROME_WORD.search(f"{element.get('class') or ''} {element.get('id') or ''}"))
            chrome[element] = bool(named) and not element.xpath(_HOLDS_ARTICLE)
        return chrome[element]

    return [element for element in root.xpath(xpath)
            if not any(map(is_chrome, (element, *element.iterancestors())))]


def _copyright_lines(root) -> List[str]:
    lines = (element.get("content") if element.tag == "meta" else element.text_content()
             for element in _article_marks(root, _PAGE_COPYRIGHTS))
    return [line for line in lines if line and _COPYRIGHT_LINE.search(line) and _HOLDER.search(line)]


def page_license(page: bytes) -> Optional[License]:
    """The licence a publisher's page marks for the article it shows, outside its footer,
    figures, tables, references, research data and supplementary files.

    Its marks are its licence links and the licence of the article record it embeds, and
    the paragraphs and copyright lines that grant a Creative Commons licence in words. The
    words are the article's own licence text: a link that names another open licence is
    not read beside them (ScienceDirect's hidden `oaUserLicense`). When the words, or with
    none the links, name two different open licences, the page states none. Otherwise an
    open licence is taken, then all rights reserved, then an `other`, the first of each.
    """
    root = _page_root(page)
    by = MetadataSource.publisher_page
    texts: List[Optional[str]] = []
    for element in _article_marks(root, _PAGE_LICENSES):
        texts += [element.get("href"), element.get("xlink:href")]
        texts += [a.get("href") for a in element.iter("a") if a is not element]
    # A link to Creative Commons that names no licence ("published under a Creative Commons
    # License") is not one.
    links = [lic for lic in (stated(by, text) for text in texts)
             if lic and not (_CC_HOST in lic.stated_as and lic.license_id not in _CREATIVE_COMMONS)]
    links += [stated(by, json.loads(value)) for script in root.iter("script")
              for value in _EMBEDDED.findall(script.text or "")]
    granting = [p.text_content() for p in _article_marks(root, _PAGE_PARAGRAPHS)]
    words = [lic for lic in (stated(by, _about_the_article(text))
                             for text in granting + _copyright_lines(root))
             if lic and lic.license_id in _CREATIVE_COMMONS]
    named = {lic.license_id for lic in words} or {
        lic.license_id for lic in links if lic.license_id in _OPEN}
    if len(named) > 1:
        return None
    if named:
        return next(lic for lic in links + words if lic.license_id in named)
    return _strongest(links)


def page_copyright(page: bytes) -> Optional[License]:
    """The copyright line a publisher's page prints for the article: the licence it names,
    if any, else all rights reserved, stated."""
    return _strongest(copyright_line(MetadataSource.publisher_page, line)
                      for line in _copyright_lines(_page_root(page)))


#: ScienceDirect's self-archiving terms, in the article record its page embeds: the licence
#: an accepted manuscript may be shared under, from its start date.
_SELF_ARCHIVING = re.compile(r'"self-archiving":\{.*?\]\}')
_SA_FIELD = re.compile(r'"#name":"(sa-user-license|sa-start-date)","_":("(?:[^"\\]|\\.)*")')


def page_manuscript_license(page: bytes, now: Optional[datetime] = None) -> Optional[License]:
    """The licence a publisher's page names for the article's accepted manuscript:
    ScienceDirect's `sa-user-license`, once its `sa-start-date` has come."""
    terms = _SELF_ARCHIVING.search(page.decode("utf-8", errors="replace"))
    if terms is None:
        return None
    fields = {name: json.loads(value) for name, value in _SA_FIELD.findall(terms.group(0))}
    start = fields.get("sa-start-date")
    if start and datetime.fromisoformat(start.replace("Z", "+00:00")) > (
            now or datetime.now(timezone.utc)):
        return None
    return stated(MetadataSource.publisher_page, fields.get("sa-user-license"))


def cloud_license(record: Mapping) -> Optional[License]:
    """The licence PMC's Cloud Service records for its copy, its `license_code`."""
    return stated(MetadataSource.pmc_cloud, record.get("license_code"))


#: Crossref's `content-version`s, as copy versions. `tdm` deposits are a mining grant and
#: `stm-asf` ones the STM Article Sharing Framework's sharing policies; neither is a
#: licence, so neither is read.
_CROSSREF_VERSIONS = {"vor": CopyVersion.vor, "am": CopyVersion.am, "unspecified": None}
_EARLIEST = datetime.min.replace(tzinfo=timezone.utc)


def crossref_statements(entries: Optional[Sequence[Mapping]], now: Optional[datetime] = None,
                        preprint: bool = False) -> List[Statement]:
    """One statement per version Crossref's `license` deposits for one DOI cover now.

    An embargoed deposit, starting in the future, is not yet in force; one with no start
    always is. Among a version's deposits in force the one taken names a licence over an
    `other`, then starts latest, then has the smaller URL, so the choice never depends on
    deposit order. A preprint server's DOI is the preprint's, whatever version its
    deposits name, so with `preprint` each is about the submitted version.
    """
    now = now or datetime.now(timezone.utc)
    best: Dict[Optional[CopyVersion], Tuple] = {}
    for entry in entries or []:
        kind, url = entry.get("content-version"), entry.get("URL")
        if not url or kind not in _CROSSREF_VERSIONS:
            continue
        start = ((entry.get("start") or {}).get("date-time")) or ""
        try:
            started = datetime.fromisoformat(start.replace("Z", "+00:00"))
        except ValueError:
            started = _EARLIEST
        if started > now:
            continue
        found = stated(MetadataSource.crossref, str(url))
        key = (found.license_id == LicenseId.other, -started.timestamp(), found.stated_as)
        version = CopyVersion.submitted if preprint else _CROSSREF_VERSIONS[kind]
        if version not in best or key < best[version][0]:
            best[version] = (key, found)
    return [Statement(found, version) for version, (_, found) in best.items()]


def license_of(
    version: Optional[CopyVersion],
    own: Optional[License],
    statements: Mapping[str, Sequence[Statement]],
    unanswered: Set[str],
) -> License:
    """The licence of a copy of `version` whose own record states `own`.

    `statements` holds each source's statements (`HELD`, or a provider's name), and
    `unanswered` the sources (`OWN`, or a provider's name) that could not be read or did
    not answer. The first source in `LICENSE_PRECEDENCE` that stated one decides.
    Raises Undecided when a source before it, or before an assumed licence, is unanswered.
    """
    if OWN in unanswered:
        raise Undecided("its own record could not be read", [OWN])
    if own is not None:
        return own
    for source, versions, routes in LICENSE_PRECEDENCE[version]:
        if source in unanswered:
            raise Undecided(f"{source} did not answer", [source])
        for statement in statements.get(source, ()):
            if statement.version in versions and (routes is None or statement.route in routes):
                return statement.license
    return ASSUMED


def _page(source: str, files: Iterable[Path]) -> Optional[Path]:
    """The publisher's page an ACE download holds."""
    if source != DownloadSource.ACE.value:
        return None
    return next((p for p in files if p.suffix == ".html"), None)


def own_license(source: str, files: Iterable[Path]) -> Optional[License]:
    """The licence a held copy's own record states: PMC's cloud record of it, else its
    JATS or Elsevier full text, or the open licence the publisher's page ACE read marks.
    The page's other marks and its copyright line credit a holder or name terms; they are
    not its copy's own record, but rank last (`page_statements`). Raises when a record that
    is there cannot be read."""
    files = list(files)
    page = _page(source, files)
    if page is not None:
        marked = page_license(page.read_bytes())
        return marked if marked is not None and marked.license_id in _OPEN else None
    by_name = {path.name: path for path in files}
    cloud = by_name.get("pmc_cloud.json")
    if cloud is not None:
        found = cloud_license(json.loads(cloud.read_text(encoding="utf-8")))
        if found is not None:
            return found
    if "article.xml" in by_name:
        parser = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
        return jats_license(etree.parse(str(by_name["article.xml"]), parser).getroot())
    content = next((p for name, p in by_name.items() if name.startswith("content.")
                    and name.endswith(".xml")), None)
    if source == "elsevier" and content is not None:
        return elsevier_license(content)
    return None


def page_statements(source: str, files: Iterable[Path]) -> List[Statement]:
    """What the publisher's page ACE read states, for `PAGE` in `LICENSE_PRECEDENCE`: for the
    version of record the licence it marks, else its copyright line, and for the accepted
    manuscript its self-archiving licence."""
    page = _page(source, files)
    if page is None:
        return []
    data = page.read_bytes()
    found = [Statement(_strongest([page_license(data), page_copyright(data)]), CopyVersion.vor),
             Statement(page_manuscript_license(data), CopyVersion.am)]
    return [statement for statement in found if statement.license is not None]


__all__ = [
    "ASSUMED",
    "HELD",
    "LICENSE_PRECEDENCE",
    "LICENSE_PROVIDERS",
    "OWN",
    "PAGE",
    "Statement",
    "Undecided",
    "cloud_license",
    "copyright_line",
    "crossref_statements",
    "elsevier_license",
    "jats_license",
    "license_of",
    "normalise_license",
    "own_license",
    "page_copyright",
    "page_license",
    "page_manuscript_license",
    "page_statements",
    "stated",
]
