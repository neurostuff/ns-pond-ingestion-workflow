"""Value types the catalog stores and returns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from ingestion_workflow.models.ids import Identifier

#: Identifier kinds that can name an article, strongest first. A new article
#: takes its id from the first kind it has, which is why the order is fixed.
ALIAS_KINDS = ("pmcid", "pmid", "doi", "neurostore")

NO_SOURCE = ""


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
    ) -> "Outcome":
        return cls(
            article_id=article_id,
            stage=stage,
            source=source,
            status=Status.PERMANENT if permanent else Status.FAILED,
            fingerprint=fingerprint,
            error=error[:2000],
        )
