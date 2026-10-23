"""OpenAlex: the authorships it marks `is_corresponding`, by display name, never an email."""

from __future__ import annotations

from typing import List, Mapping

from study_schema.models.paper_parse import MetadataSource

from ingestion_workflow.models.metadata import CorrespondingAuthor

from .common import authors


def from_openalex(work: Mapping) -> List[CorrespondingAuthor]:
    return authors(
        (((a.get("author") or {}).get("display_name"), None)
         for a in work.get("authorships") or [] if a.get("is_corresponding")),
        MetadataSource.openalex,
    )
