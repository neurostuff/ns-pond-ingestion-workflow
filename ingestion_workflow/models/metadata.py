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
        raise NotImplementedError()

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
    raise NotImplementedError()
