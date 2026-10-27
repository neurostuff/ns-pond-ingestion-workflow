"""Value types the catalog stores and returns."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set

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
class Notes:
    """What an article's output lacks and why, in the form every stage's summary keeps it:
    `errors` by the field each error left out, the `transient` ones among them (a provider
    busy or down, which the plans retry on their own), and the wanted fields `missing`."""

    errors: Dict[str, str] = field(default_factory=dict)
    transient: Set[str] = field(default_factory=set)
    missing: List[str] = field(default_factory=list)

    def error(self, fields: Iterable[str] | str, text: str, *, transient: bool = False) -> None:
        """Note `text` under each of `fields`; a field's first error is kept."""
        for name in [fields] if isinstance(fields, str) else fields:
            if name not in self.errors:
                self.errors[name] = text[:2000]
                if transient:
                    self.transient.add(name)

    def summary(self) -> Dict[str, Any]:
        """The summary's `errors`, `transient` and `missing`, each only when there is one."""
        return {name: value for name, value in (("errors", dict(self.errors)),
                                                ("transient", sorted(self.transient)),
                                                ("missing", list(self.missing))) if value}


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
