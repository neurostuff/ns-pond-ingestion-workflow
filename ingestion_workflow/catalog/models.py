"""Value types the catalog stores and returns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from ingestion_workflow.models.ids import Identifier

#: The ids an article is named and found by, in the order a new article takes its id from
#: the first it has and a lookup tries them. `doi`, `pmid` and `pmcid` are the version of
#: record's; a preprint's and every other id of the paper's are kinds of their own, so no
#: query for a version of record's id can answer with another version's.
ALIAS_KINDS = ("pmcid", "pmid", "doi", "preprint_pmcid", "preprint_pmid", "preprint_doi",
               "neurostore", "alternative_pmcid", "alternative_pmid", "alternative_doi")

#: Each kind a paper holds beside its version of record's id, by the id it is a version of.
#: A lookup by an id finds the article holding it as any of these.
SECOND_KINDS = {"preprint_pmid": "pmid", "preprint_pmcid": "pmcid", "preprint_doi": "doi",
                "alternative_pmid": "pmid", "alternative_pmcid": "pmcid",
                "alternative_doi": "doi"}

#: The alias kinds an old catalog held the second kinds as, and the kind each became.
RENAMED_KINDS = {"alternate_doi": "alternative_doi", "duplicate_pmid": "alternative_pmid",
                 "duplicate_pmcid": "alternative_pmcid"}


def field_of(kind: str) -> str:
    """The Identifier field that holds the ids of an alias kind."""
    return kind + "s" if kind in SECOND_KINDS else kind


NO_SOURCE = ""

#: The exclusion table id that stands for every table of an article.
WHOLE_ARTICLE = "*"


class Status(str, Enum):
    OK = "ok"
    FAILED = "failed"          # transient; retried under backoff
    PERMANENT = "permanent"    # will never succeed; retried only with --refresh
    SKIPPED = "skipped"        # this source cannot handle this article

    @property
    def retryable(self) -> bool:
        return self is Status.FAILED


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class ArticleRef:
    """An article's stable id together with the identifiers known for it."""

    id: str
    identifier: Identifier

    def __str__(self) -> str:  # pragma: no cover - display only
        return self.id


@dataclass
class Artifact:
    """What a stage produced for one article from one source."""

    article_id: str
    stage: str
    source: str = NO_SOURCE
    status: Status = Status.OK
    fingerprint: str = ""
    blob: Optional[str] = None
    summary: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    updated_at: str = field(default_factory=utcnow)

    @property
    def ok(self) -> bool:
        return self.status is Status.OK


@dataclass
class Outcome:
    """A stage's report on one unit of work, ready to be recorded."""

    article_id: str
    stage: str
    source: str = NO_SOURCE
    status: Status = Status.OK
    fingerprint: str = ""
    payload: Any = None          # written to the blob store when not None
    summary: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @classmethod
    def failure(
        cls,
        article_id: str,
        stage: str,
        source: str,
        error: str,
        *,
        permanent: bool = False,
        fingerprint: str = "",
        summary: Optional[Dict[str, Any]] = None,
    ) -> "Outcome":
        return cls(
            article_id=article_id,
            stage=stage,
            source=source,
            status=Status.PERMANENT if permanent else Status.FAILED,
            fingerprint=fingerprint,
            summary=dict(summary or {}),
            error=error[:2000],
        )
