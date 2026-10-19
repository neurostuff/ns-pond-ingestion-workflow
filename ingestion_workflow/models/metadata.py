"""Data models for article metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from study_schema.models.paper_parse import License, ReportedCopy


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

    # The provider the record came from, a study_schema MetadataSource value
    source: Optional[str] = None

    # The provider whose abstract `abstract` is; a record's own source unless merged
    abstract_provider: Optional[str] = None

    # Direct link to an open-access PDF, when a provider reports one
    pdf_url: Optional[str] = None

    # Raw metadata from external sources for reference
    raw_metadata: Dict[str, Any] = field(default_factory=dict)

    # The paper's licence, decided by the metadata service from every provider's answer
    license: Optional[License] = None

    # Each held copy's licence, by the download source that holds it
    copy_licenses: Dict[str, License] = field(default_factory=dict)

    # How the paper is open to read, a study_schema OaStatus value
    oa_status: Optional[str] = None

    # Copies the providers list that no download holds
    reported_copies: List[ReportedCopy] = field(default_factory=list)

    # When the first Crossref licence deposit left out for being embargoed starts to apply
    license_embargo_until: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.abstract and self.abstract_provider is None:
            self.abstract_provider = self.source

    def merge_from(self, other: ArticleMetadata) -> ArticleMetadata:
        """
        Merge metadata from another source, filling in missing fields.

        Values from `self` take precedence over values from `other`. The licence
        decisions are not merged: the metadata service makes them from every answer.

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

        # Merge abstract: prefer the longer one, and keep whose it is
        merged_abstract, abstract_provider = self.abstract, self.abstract_provider
        if not merged_abstract or (other.abstract and len(other.abstract) > len(merged_abstract)):
            merged_abstract, abstract_provider = other.abstract, other.abstract_provider

        # Merge keywords: combine unique keywords
        merged_keywords = list(self.keywords)
        for kw in other.keywords:
            if kw not in merged_keywords:
                merged_keywords.append(kw)

        # Merge raw_metadata
        merged_raw = {**other.raw_metadata, **self.raw_metadata}

        return ArticleMetadata(
            title=self.title or other.title,
            authors=merged_authors,
            abstract=merged_abstract,
            abstract_provider=abstract_provider if merged_abstract else None,
            journal=self.journal or other.journal,
            publication_year=self.publication_year or other.publication_year,
            keywords=merged_keywords,
            source=self.source or other.source,
            pdf_url=self.pdf_url or other.pdf_url,
            raw_metadata=merged_raw,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert ArticleMetadata to dictionary for caching."""
        return {
            "title": self.title,
            "authors": [author.to_dict() for author in self.authors],
            "abstract": self.abstract,
            "abstract_provider": self.abstract_provider,
            "journal": self.journal,
            "publication_year": self.publication_year,
            "keywords": self.keywords,
            "source": self.source,
            "pdf_url": self.pdf_url,
            "raw_metadata": self.raw_metadata,
            "license": _dumped(self.license),
            "copy_licenses": {name: _dumped(lic) for name, lic in self.copy_licenses.items()},
            "oa_status": self.oa_status,
            "reported_copies": [_dumped(copy) for copy in self.reported_copies],
            "license_embargo_until": (self.license_embargo_until.isoformat()
                                      if self.license_embargo_until else None),
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
            abstract_provider=data.get("abstract_provider"),
            journal=data.get("journal"),
            publication_year=data.get("publication_year"),
            keywords=data.get("keywords", []),
            source=data.get("source"),
            pdf_url=data.get("pdf_url"),
            raw_metadata=data.get("raw_metadata", {}),
            license=License.model_validate(data["license"]) if data.get("license") else None,
            copy_licenses={name: License.model_validate(lic)
                           for name, lic in (data.get("copy_licenses") or {}).items()},
            oa_status=data.get("oa_status"),
            reported_copies=[ReportedCopy.model_validate(copy)
                             for copy in data.get("reported_copies") or []],
            license_embargo_until=(datetime.fromisoformat(data["license_embargo_until"])
                                   if data.get("license_embargo_until") else None),
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


def _dumped(model) -> Optional[Dict[str, Any]]:
    return model.model_dump(mode="json", exclude_none=True) if model is not None else None


def merge_metadata_from_sources(
    metadata_list: List[ArticleMetadata],
) -> ArticleMetadata:
    """
    Merge metadata from multiple sources into a single record.

    Merging strategy:
    - Title: Use first non-None value
    - Authors: Prefer most complete list
    - Abstract: Use longest available abstract
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
#: no provider returns keywords for every article, and treating their absence
#: as incomplete is what made every run refetch every article.
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
