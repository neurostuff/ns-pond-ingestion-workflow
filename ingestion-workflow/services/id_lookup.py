"""
This module will lookup article ids
using various id lookup services in this order:
1. semantic scholar
2. openalex
3. pubmed
"""

from ingestion_workflow.models import Identifiers


class IDLookupService:
    def __init__(self):
        pass

    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        """Lookup additional identifiers for the given article."""
        raise NotImplementedError(
            "IDLookupService find_identifiers method not implemented."
        )


class SemanticScholarIDLookupService(IDLookupService):
    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        """Lookup additional identifiers using Semantic Scholar."""
        raise NotImplementedError(
            "SemanticScholarIDLookupService find_identifiers method not implemented."
        )


class OpenAlexIDLookupService(IDLookupService):
    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        """Lookup additional identifiers using OpenAlex."""
        raise NotImplementedError(
            "OpenAlexIDLookupService find_identifiers method not implemented."
        )


class PubMedIDLookupService(IDLookupService):
    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        """Lookup additional identifiers using PubMed."""
        raise NotImplementedError(
            "PubMedIDLookupService find_identifiers method not implemented."
        )
