"""Upload DTOs and outcomes for the upload stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ingestion_workflow.models import Analysis, Identifier


@dataclass
class BaseStudyPayload:
    """Subset of fields required to create or update a base study."""

    name: Optional[str] = None
    description: Optional[str] = None
    publication: Optional[str] = None
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    authors: Optional[str] = None
    year: Optional[int] = None
    is_oa: Optional[bool] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StudyPayload:
    """Subset of fields required to create or update a study."""

    name: Optional[str] = None
    description: Optional[str] = None
    publication: Optional[str] = None
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    authors: Optional[str] = None
    year: Optional[int] = None
    source: str = "llm"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TablePayload:
    """Table metadata to persist alongside analyses."""

    table_id: str
    caption: str = ""
    footer: str = ""
    title: str = ""
    label: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UploadWorkItem:
    """A full upload unit for a single article slug."""

    slug: str
    identifier: Optional[Identifier]
    base_study: BaseStudyPayload
    study: StudyPayload
    analyses: List["PreparedAnalysis"]


@dataclass
class PreparedAnalysis:
    """Analysis plus resolved table metadata."""

    table: TablePayload
    analysis: Analysis
    coordinate_space: Optional[str] = None


@dataclass
class UploadOutcome:
    """Result of processing a single article slug."""

    slug: str
    base_study_id: Optional[str] = None
    study_id: Optional[str] = None
    analysis_ids: List[str] = field(default_factory=list)
    success: bool = False
    error: Optional[str] = None
