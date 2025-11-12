"""Data models for article metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
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

    # License information (e.g., CC-BY)
    license: Optional[str] = None

    # Primary source of metadata (pubmed, semantic_scholar, etc.)
    source: Optional[str] = None

    # Whether the article is open access
    open_access: Optional[bool] = None

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
        if (
            self.authors
            and other.authors
            and len(other.authors) > len(self.authors)
        ):
            merged_authors = other.authors
        
        # Merge abstract: prefer the longer one
        merged_abstract = self.abstract
        if not merged_abstract or (
            other.abstract and len(other.abstract) > len(merged_abstract)
        ):
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
            self.open_access
            if self.open_access is not None
            else other.open_access
        )
        
        return ArticleMetadata(
            title=self.title or other.title,
            authors=merged_authors,
            abstract=merged_abstract,
            journal=self.journal or other.journal,
            publication_year=self.publication_year or other.publication_year,
            keywords=merged_keywords,
            license=self.license or other.license,
            source=self.source or other.source,
            open_access=merged_open_access,
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
            "license": self.license,
            "source": self.source,
            "open_access": self.open_access,
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
            license=data.get("license"),
            source=data.get("source"),
            open_access=data.get("open_access"),
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
