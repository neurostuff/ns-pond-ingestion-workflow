"""The licence of each copy of a paper, and of the paper, from what every source stated.

A statement is one source's licence for one version of the paper, or for one copy at a
URL. A copy's licence is the first statement `LICENSE_PRECEDENCE` lists for its version
that a source made, a named licence before an `other`. When none was made it is all
rights reserved, assumed, but only if every source the precedence asks answered; while
one has not, the licence is undecided and the article is asked about again.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from lxml import etree
from study_schema.models.paper_parse import (
    CopyVersion,
    License,
    LicenseDetermination,
    LicenseId,
    MetadataSource,
)

#: A held copy's own record: its file, or PMC's cloud record of it.
OWN = "own"

#: For each version a copy can be, the sources whose statements speak for it, best first,
#: each with the versions its statement must be about (None: it names no version). A
#: copy's own statement comes before all of them. Crossref's version-of-record deposit is
#: the publisher's; OpenAlex and Semantic Scholar infer theirs; Crossref's deposits that
#: name no version come last. A manuscript's licence is never the version of record's.
LICENSE_PRECEDENCE: Mapping[
    Optional[CopyVersion], Tuple[Tuple[MetadataSource, frozenset], ...]
] = {
    CopyVersion.vor: (
        (MetadataSource.crossref, frozenset({CopyVersion.vor})),
        (MetadataSource.openalex, frozenset({CopyVersion.vor})),
        (MetadataSource.semantic_scholar, frozenset({None})),
        (MetadataSource.crossref, frozenset({None})),
    ),
    CopyVersion.am: (
        (MetadataSource.crossref, frozenset({CopyVersion.am})),
        (MetadataSource.openalex, frozenset({CopyVersion.am})),
    ),
    # Crossref's deposits are a preprint's only when the DOI asked is the preprint's.
    CopyVersion.submitted: (
        (MetadataSource.crossref, frozenset({CopyVersion.submitted})),
        (MetadataSource.openalex, frozenset({CopyVersion.submitted})),
    ),
    None: (),
}

#: The providers a licence statement can come from, besides a held copy's own record.
LICENSE_PROVIDERS = (MetadataSource.semantic_scholar, MetadataSource.crossref,
                     MetadataSource.openalex)


@dataclass(frozen=True)
class Statement:
    """A licence one source stated, for one version of the paper or one copy at `url`."""

    license: License
    version: Optional[CopyVersion] = None
    url: Optional[str] = None


class Undecided(Exception):
    """A licence would be assumed while a source that could state it has not answered."""


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
    if re.sub(r"\d+$", "", compact) in _COMPACT:  # "CC BY 4.0"
        return _COMPACT[re.sub(r"\d+$", "", compact)]
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


def cloud_license(record: Mapping) -> Optional[License]:
    """The licence PMC's Cloud Service records for its copy, its `license_code`."""
    return stated(MetadataSource.pmc_cloud, record.get("license_code"))


#: Crossref's `content-version`s, as copy versions. `tdm` deposits are a mining grant and
#: `stm-asf` ones the STM Article Sharing Framework's sharing policies; neither is a
#: licence, so neither is read.
_CROSSREF_VERSIONS = {"vor": CopyVersion.vor, "am": CopyVersion.am, "unspecified": None}
_EARLIEST = datetime.min.replace(tzinfo=timezone.utc)


def crossref_statements(entries: Optional[Sequence[Mapping]],
                        now: Optional[datetime] = None) -> List[Statement]:
    """One statement per version Crossref's `license` deposits cover now.

    An embargoed deposit, starting in the future, is not yet in force. Among a version's
    deposits in force the one taken names a licence over an `other`, then starts latest,
    then has the smaller URL, so the choice never depends on deposit order.
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
        version = _CROSSREF_VERSIONS[kind]
        if version not in best or key < best[version][0]:
            best[version] = (key, found)
    return [Statement(found, version) for version, (_, found) in best.items()]


def license_of(
    version: Optional[CopyVersion],
    own: Optional[License],
    statements: Mapping[MetadataSource, Sequence[Statement]],
    unanswered: Set[str],
) -> License:
    """The licence of a copy of `version` whose own record states `own`.

    `statements` holds each provider's statements about the paper's versions, and
    `unanswered` the sources (`OWN` or a MetadataSource value) that could not be read
    or did not answer. Raises Undecided rather than assume a licence one of them might
    have stated.
    """
    found: List[Tuple[bool, int, License]] = []
    if own is not None:
        found.append((own.license_id == LicenseId.other, 0, own))
    for tier, (source, versions) in enumerate(LICENSE_PRECEDENCE[version], start=1):
        for statement in statements.get(source, ()):
            if statement.version in versions:
                lic = statement.license
                found.append((lic.license_id == LicenseId.other, tier, lic))
                break
    if found:
        return min(found, key=lambda f: f[:2])[2]
    waited = {OWN} | {source.value for source, _ in LICENSE_PRECEDENCE[version]}
    missing = sorted(waited & unanswered)
    if missing:
        raise Undecided(f"{', '.join(missing)} did not answer")
    return ASSUMED


def own_license(source: str, files: Iterable[Path]) -> Optional[License]:
    """The licence a held copy's own record states: PMC's cloud record of it, else its
    JATS or Elsevier full text. Raises when a record that is there cannot be read."""
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


__all__ = [
    "ASSUMED",
    "LICENSE_PRECEDENCE",
    "LICENSE_PROVIDERS",
    "OWN",
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
    "stated",
]
