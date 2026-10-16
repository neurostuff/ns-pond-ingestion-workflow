"""Data models for article metadata."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


@dataclass
class Author:
    """
    Author information for an article.
    """

    # Author's full name
    name: str

    # Author's affiliation
    affiliation: Optional[str] = None

    # ORCID identifier
    orcid: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert Author to dictionary."""
        return {
            "name": self.name,
            "affiliation": self.affiliation,
            "orcid": self.orcid,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Author":
        """Create Author from dictionary."""
        return cls(
            name=str(data["name"]),
            affiliation=data.get("affiliation"),
            orcid=data.get("orcid"),
        )


class LicenseCode(str, Enum):
    """The licence an article is under, normalised from however it was stated."""

    CC_BY = "cc-by"
    CC_BY_SA = "cc-by-sa"
    CC_BY_NC = "cc-by-nc"
    CC_BY_NC_SA = "cc-by-nc-sa"
    CC_BY_ND = "cc-by-nd"
    CC_BY_NC_ND = "cc-by-nc-nd"
    CC0 = "cc0"
    PUBLIC_DOMAIN = "public-domain"
    #: A publisher's own terms granting text and data mining, such as Elsevier's user
    #: licence or PMC's "available for text mining": stated, but not an open licence.
    PUBLISHER_TDM = "publisher-tdm"
    ALL_RIGHTS_RESERVED = "all-rights-reserved"
    OTHER = "other"


class LicenseBasis(str, Enum):
    #: Someone said so: a licence, a TDM grant, or a copyright statement with no licence.
    STATED = "stated"
    #: Nobody stated anything, so all rights are taken as reserved.
    ASSUMED = "assumed"


class LicenseSource(str, Enum):
    """Who stated the licence."""

    JATS = "jats"
    ELSEVIER = "elsevier"
    CROSSREF = "crossref"
    OPENALEX = "openalex"
    SEMANTIC_SCHOLAR = "semantic_scholar"


#: Which statement wins when sources disagree, best first. The article's own file
#: describes the very copy that was downloaded; Crossref's version-of-record entry
#: is the publisher's deposit; OpenAlex and Semantic Scholar infer theirs; Crossref's
#: other entries cover an accepted manuscript or text mining, not the published copy.
LICENSE_PRECEDENCE = (
    ((LicenseSource.JATS, None), (LicenseSource.ELSEVIER, None)),
    ((LicenseSource.CROSSREF, "vor"),),
    ((LicenseSource.OPENALEX, None), (LicenseSource.SEMANTIC_SCHOLAR, None)),
    ((LicenseSource.CROSSREF, None),),
)

_CC_URL = re.compile(r"creativecommons\.org/licenses/([a-z-]+)")
_CC_PHRASE = re.compile(r"\bcc[ -]?by((?:[ -](?:nc|nd|sa))*)\b")
_COMPACT = {
    "ccby": LicenseCode.CC_BY,
    "ccbysa": LicenseCode.CC_BY_SA,
    "ccbync": LicenseCode.CC_BY_NC,
    "ccbyncsa": LicenseCode.CC_BY_NC_SA,
    "ccbysanc": LicenseCode.CC_BY_NC_SA,
    "ccbynd": LicenseCode.CC_BY_ND,
    "ccbyncnd": LicenseCode.CC_BY_NC_ND,
    "ccbyndnc": LicenseCode.CC_BY_NC_ND,
    "cc0": LicenseCode.CC0,
    "cczero": LicenseCode.CC0,
    "publicdomain": LicenseCode.PUBLIC_DOMAIN,
}
#: Publisher TDM terms as they appear: Elsevier's user licences and TDM-REP licence,
#: Springer's and Wiley's TDM URLs, PMC's text-mining paragraph for a manuscript.
_TDM = re.compile(r"text[ -]and[ -]data[ -]mining|text mining|\btdm|userlicense")


def _cc_from_parts(parts: str) -> Optional[LicenseCode]:
    return _COMPACT.get("ccby" + re.sub(r"[^a-z]", "", parts))


def normalise_license(stated: str) -> LicenseCode:
    """The code for a licence as some source wrote it: a URL, a short code or a paragraph."""
    text = " ".join(stated.lower().split())
    url = _CC_URL.search(text)
    if url:
        return _cc_from_parts(url.group(1).removeprefix("by")) or LicenseCode.OTHER
    if "creativecommons.org/publicdomain/zero" in text:
        return LicenseCode.CC0
    if "creativecommons.org/publicdomain/mark" in text:
        return LicenseCode.PUBLIC_DOMAIN
    compact = re.sub(r"[^a-z0-9]", "", text).replace("creativecommons", "cc")
    if compact in _COMPACT:
        return _COMPACT[compact]
    if re.sub(r"\d+$", "", compact) in _COMPACT:  # "CC BY 4.0"
        return _COMPACT[re.sub(r"\d+$", "", compact)]
    phrase = _CC_PHRASE.search(text)
    if phrase:
        return _cc_from_parts(phrase.group(1)) or LicenseCode.OTHER
    if "creative commons" in text and "attribution" in text:
        parts = ""
        if re.search(r"non-?commercial", text):
            parts += "nc"
        if "sharealike" in text or "share alike" in text or "share-alike" in text:
            parts += "sa"
        if re.search(r"no-?deriv|no deriv", text):
            parts += "nd"
        return _cc_from_parts(parts) or LicenseCode.OTHER
    if "cc0" in text or "public domain dedication" in text:
        return LicenseCode.CC0
    if "public domain" in text:
        return LicenseCode.PUBLIC_DOMAIN
    if _TDM.search(text):
        return LicenseCode.PUBLISHER_TDM
    if "all rights reserved" in text or "all rights are reserved" in text:
        return LicenseCode.ALL_RIGHTS_RESERVED
    return LicenseCode.OTHER


@dataclass(frozen=True)
class License:
    """An article's licence, with who stated it and how, or that nobody did."""

    code: LicenseCode
    basis: LicenseBasis
    stated_by: Optional[LicenseSource] = None
    #: The URL or text as the source gave it.
    stated_as: Optional[str] = None
    #: The version it covers when the source says: vor, am or submitted.
    version: Optional[str] = None

    @classmethod
    def stated(
        cls,
        by: LicenseSource,
        stated_as: Optional[str],
        version: Optional[str] = None,
        code: Optional[LicenseCode] = None,
    ) -> Optional["License"]:
        """The licence `by` stated, or None when it stated nothing.

        `code` is given when it is already known, or when the statement is not a
        licence at all: a copyright line is all-rights-reserved whatever it says.
        """
        stated_as = " ".join((stated_as or "").split())
        if not stated_as:
            return None
        return cls(
            code=code or normalise_license(stated_as),
            basis=LicenseBasis.STATED,
            stated_by=by,
            stated_as=stated_as,
            version=version,
        )

    @classmethod
    def assumed(cls) -> "License":
        return cls(code=LicenseCode.ALL_RIGHTS_RESERVED, basis=LicenseBasis.ASSUMED)

    @property
    def rank(self) -> int:
        """Its place in LICENSE_PRECEDENCE; lower wins."""
        for rank, tier in enumerate(LICENSE_PRECEDENCE):
            for source, version in tier:
                if self.stated_by is source and version in (None, self.version):
                    return rank
        return len(LICENSE_PRECEDENCE)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code.value,
            "basis": self.basis.value,
            "stated_by": self.stated_by.value if self.stated_by else None,
            "stated_as": self.stated_as,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "License":
        return cls(
            code=LicenseCode(data["code"]),
            basis=LicenseBasis(data["basis"]),
            stated_by=LicenseSource(data["stated_by"]) if data.get("stated_by") else None,
            stated_as=data.get("stated_as"),
            version=data.get("version"),
        )


def best_license_rank(source: LicenseSource) -> int:
    """The best rank a statement by `source` can have."""
    return next(rank for rank, tier in enumerate(LICENSE_PRECEDENCE)
                if any(by is source for by, _ in tier))


def preferred_license(*licenses: Optional[License]) -> Optional[License]:
    """The statement LICENSE_PRECEDENCE puts first; the earlier one on a tie."""
    found = [lic for lic in licenses if lic is not None]
    return min(found, key=lambda lic: lic.rank) if found else None


@dataclass
class ArticleMetadata:
    """
    Metadata for a scientific article.

    This model aggregates metadata from multiple sources
    (PubMed, Semantic Scholar, OpenAlex, parsed from PDF, etc.).
    """

    # Article title
    title: str

    # List of article authors
    authors: List[Author] = field(default_factory=list)

    # Article abstract
    abstract: Optional[str] = None

    # Journal or venue name
    journal: Optional[str] = None

    # Publication year
    publication_year: Optional[int] = None

    # Keywords or MeSH terms
    keywords: List[str] = field(default_factory=list)

    # The licence, as the best-ranked source stated it (or assumed)
    license: Optional[License] = None

    # Primary source of metadata (pubmed, semantic_scholar, etc.)
    source: Optional[str] = None

    # Whether the article is open access
    open_access: Optional[bool] = None

    # How it is open, kept apart from the licence: gold, hybrid, green, bronze,
    # diamond or closed. Bronze is free to read, not openly licensed.
    oa_status: Optional[str] = None

    # Direct link to an open-access PDF, when a provider reports one
    pdf_url: Optional[str] = None

    # Raw metadata from external sources for reference
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    def merge_from(self, other: ArticleMetadata) -> ArticleMetadata:
        """
        Merge metadata from another source, filling in missing fields.

        Values from `self` take precedence over values from `other`.

        Parameters
        ----------
        other : ArticleMetadata
            Metadata to merge from

        Returns
        -------
        ArticleMetadata
            New instance with merged metadata
        """
        # Merge authors: prefer the list with more authors
        merged_authors = self.authors if self.authors else other.authors
        if self.authors and other.authors and len(other.authors) > len(self.authors):
            merged_authors = other.authors

        # Merge abstract: prefer the longer one
        merged_abstract = self.abstract
        if not merged_abstract or (other.abstract and len(other.abstract) > len(merged_abstract)):
            merged_abstract = other.abstract

        # Merge keywords: combine unique keywords
        merged_keywords = list(self.keywords)
        for kw in other.keywords:
            if kw not in merged_keywords:
                merged_keywords.append(kw)

        # Merge raw_metadata
        merged_raw = {**other.raw_metadata, **self.raw_metadata}

        # Determine open_access value
        merged_open_access = (
            self.open_access if self.open_access is not None else other.open_access
        )

        return ArticleMetadata(
            title=self.title or other.title,
            authors=merged_authors,
            abstract=merged_abstract,
            journal=self.journal or other.journal,
            publication_year=self.publication_year or other.publication_year,
            keywords=merged_keywords,
            license=preferred_license(self.license, other.license),
            source=self.source or other.source,
            open_access=merged_open_access,
            oa_status=self.oa_status or other.oa_status,
            pdf_url=self.pdf_url or other.pdf_url,
            raw_metadata=merged_raw,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert ArticleMetadata to dictionary for caching."""
        return {
            "title": self.title,
            "authors": [author.to_dict() for author in self.authors],
            "abstract": self.abstract,
            "journal": self.journal,
            "publication_year": self.publication_year,
            "keywords": self.keywords,
            "license": self.license.to_dict() if self.license else None,
            "source": self.source,
            "open_access": self.open_access,
            "oa_status": self.oa_status,
            "pdf_url": self.pdf_url,
            "raw_metadata": self.raw_metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArticleMetadata":
        """Create ArticleMetadata from dictionary."""
        authors_data = data.get("authors", [])
        authors = [Author.from_dict(a) for a in authors_data]
        return cls(
            title=str(data["title"]),
            authors=authors,
            abstract=data.get("abstract"),
            journal=data.get("journal"),
            publication_year=data.get("publication_year"),
            keywords=data.get("keywords", []),
            license=License.from_dict(data["license"]) if data.get("license") else None,
            source=data.get("source"),
            open_access=data.get("open_access"),
            oa_status=data.get("oa_status"),
            pdf_url=data.get("pdf_url"),
            raw_metadata=data.get("raw_metadata", {}),
        )

    def to_neurostore_format(self) -> Dict[str, Any]:
        """
        Convert metadata to Neurostore API format.

        Returns
        -------
        dict
            Metadata formatted for Neurostore upload
        """
        raise NotImplementedError()


def merge_metadata_from_sources(
    metadata_list: List[ArticleMetadata],
) -> ArticleMetadata:
    """
    Merge metadata from multiple sources into a single record.

    Merging strategy:
    - Title: Use first non-None value
    - Authors: Prefer most complete list
    - Abstract: Use longest available abstract
    - License: the statement LICENSE_PRECEDENCE ranks first
    - Other fields: First non-None value wins

    Parameters
    ----------
    metadata_list : list of ArticleMetadata
        Metadata from different sources to merge

    Returns
    -------
    ArticleMetadata
        Merged metadata
    """
    if not metadata_list:
        raise ValueError("Cannot merge empty metadata list")

    if len(metadata_list) == 1:
        return metadata_list[0]

    # Start with the first metadata item
    merged = metadata_list[0]

    # Merge each subsequent item
    for metadata in metadata_list[1:]:
        merged = merged.merge_from(metadata)

    return merged


#: Fields that make metadata worth keeping. Deliberately not "all of them":
#: no provider returns keywords and open_access together, and treating their
#: absence as incomplete is what made every run refetch every article. A licence
#: is always set, assumed when nobody stated one.
ENOUGH = ("title", "abstract", "journal", "publication_year")


def is_sufficient(metadata: ArticleMetadata | None) -> bool:
    if metadata is None:
        return False
    if not (metadata.title and metadata.title.strip()):
        return False
    populated = sum(1 for name in ENOUGH if _present(getattr(metadata, name, None)))
    return populated >= 3 and bool(metadata.authors)


def _present(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return bool(value)
