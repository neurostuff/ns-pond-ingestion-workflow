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
    if "cc0" in text or "public domain dedication" in text:
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


def _class_is(*names: str) -> str:
    """An XPath test for an element with one of the class tokens `names`, in any case."""
    upper, lower = "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"
    tokens = f"concat(' ', translate(normalize-space(@class), '{upper}', '{lower}'), ' ')"
    return " or ".join(f"contains({tokens}, ' {name.lower()} ')" for name in names)


#: Where a publisher's page names the article's licence: a `rel="license"` link (Springer
#: Nature, BioMed Central), Wiley's Creative Commons icon, the `license` blocks of
#: ScienceDirect, Oxford Academic and the PMC pages ACE reads, and older ScienceDirect's
#: `oaUserLicense`, in page order.
_PAGE_LICENSES = ("//a[@rel='license' or @aria-label='Creative Commons license information']"
                  f" | //*[{_class_is('license', 'oaUserLicense')}]")
#: Where it prints the article's copyright: its `dc.rights` and `prism.copyright` metadata,
#: ScienceDirect's `copyright-line`, Silverchair's and HighWire's `copyright-statement`,
#: Wiley's and older ScienceDirect's `p.copyright`, older Springer's `ArticleCopyright`.
#: The site's own copyright, in a page footer, is in none of them.
_PAGE_COPYRIGHTS = (
    "//meta[translate(@name, 'DCRIGHTSPRISMCOPY', 'dcrightsprismcopy')"
    " = 'dc.rights' or translate(@name, 'DCRIGHTSPRISMCOPY', 'dcrightsprismcopy')"
    " = 'prism.copyright']"
    f" | //*[{_class_is('copyright-line', 'copyright-statement', 'ArticleCopyright')}]"
    f" | //p[{_class_is('copyright')}]"
)


#: ACE saves pages as UTF-8, which libxml2 does not guess for a page that does not say so.
_UTF8_HTML = lxml_html.HTMLParser(encoding="utf-8")


def page_license(page: bytes) -> Optional[License]:
    """The licence a publisher's page states for the article it shows.

    Its licence links and blocks are taken first: an open licence before all rights
    reserved, and that before an `other`, then the first in the page. A link is read
    before its block's text. With none, its copyright lines are read the same way: each
    is the licence it names, if any, else all rights reserved, stated.
    """
    root = lxml_html.document_fromstring(page, parser=_UTF8_HTML)
    by = MetadataSource.publisher_page

    def strongest(found: Iterable[Optional[License]]) -> Optional[License]:
        return min((lic for lic in found if lic),
                   key=lambda lic: _STRENGTH.get(lic.license_id, 0), default=None)

    texts: List[Optional[str]] = []
    for element in root.xpath(_PAGE_LICENSES):
        if element.tag == "a":
            texts.append(element.get("href"))
        else:
            texts += [a.get("href") for a in element.iter("a")] + [element.text_content()]
    return (strongest(stated(by, text) for text in texts)
            or strongest(copyright_line(by, element.get("content") if element.tag == "meta"
                                        else element.text_content())
                         for element in root.xpath(_PAGE_COPYRIGHTS)))


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
    return next((p for p in files if p.suffix == ".html"), None) if source == "ace" else None


def own_license(source: str, files: Iterable[Path]) -> Optional[License]:
    """The licence a held copy's own record states: PMC's cloud record of it, else its
    JATS or Elsevier full text, or the publisher's page ACE read. Raises when a record that
    is there cannot be read."""
    files = list(files)
    page = _page(source, files)
    if page is not None:
        return page_license(page.read_bytes())
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


def manuscript_license(source: str, files: Iterable[Path]) -> Optional[License]:
    """The licence a held copy's record names for the accepted manuscript, which is not
    the copy: the self-archiving licence of the publisher's page ACE read."""
    page = _page(source, files)
    return page_manuscript_license(page.read_bytes()) if page is not None else None


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
    "manuscript_license",
    "normalise_license",
    "own_license",
    "page_license",
    "page_manuscript_license",
    "stated",
]
