"""The stage contract. Stages produce artifacts; the scheduler decides when."""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, Iterator, List, Optional, Protocol, Sequence

from ingestion_workflow.catalog import (
    ArticleRef,
    Artifact,
    Catalog,
    Outcome,
    Status,
    is_retryable,
)
from ingestion_workflow.config import Settings

from .plan import StagePlan, Work


class Context:
    """What a stage is handed: settings, the catalog, and the freshness rule."""

    def __init__(
        self,
        settings: Settings,
        catalog: Catalog,
        *,
        refresh: Sequence[str] = (),
        max_attempts: int = 3,
        retry_after: timedelta = timedelta(days=1),
    ) -> None:
        self.settings = settings
        self.catalog = catalog
        self.refresh = {name.lower() for name in refresh}
        self.max_attempts = max_attempts
        self.retry_after = retry_after

    def refreshing(self, stage: str, source: str = "") -> bool:
        """Whether the operator asked for this work to be redone.

        `--refresh extract` covers the whole stage; `--refresh extract:ace`
        covers one source of it, which is what a single extractor changing
        calls for. Targeting matters for migrated artifacts especially: they
        carry no fingerprint, so a version bump cannot reach them and an
        explicit instruction is the only way.
        """
        if "all" in self.refresh or stage in self.refresh:
            return True
        return bool(source) and f"{stage}:{source}" in self.refresh

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        """Reusable iff it succeeded, its inputs are unchanged, and its blob survives."""
        if artifact is None or artifact.status is not Status.OK:
            return False
        if self.refreshing(artifact.stage, artifact.source):
            return False
        if expected and artifact.fingerprint and artifact.fingerprint != expected:
            return False
        if artifact.blob and not self.catalog.blobs.exists(artifact.blob):
            return False
        return True

    def should_attempt(
        self,
        artifact: Optional[Artifact],
        attempts: int,
        last_attempt: Optional[str],
        stage: str,
        source: str = "",
    ) -> bool:
        """Whether a non-fresh artifact is worth (re)trying now."""
        if self.refreshing(stage, source):
            return True
        if artifact is None:
            return True
        if artifact.status is Status.OK:
            return True  # stale: fingerprint changed
        if artifact.status in (Status.PERMANENT, Status.SKIPPED):
            return False
        return is_retryable(
            artifact,
            attempts,
            last_attempt,
            max_attempts=self.max_attempts,
            backoff=self.retry_after,
        )

    def payload(self, artifact: Optional[Artifact]):
        return self.catalog.payload(artifact)


class Stage(Protocol):
    """Produce artifacts for articles. Never decide whether to run."""

    name: str
    requires: Optional[str]

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        """Classify a batch of articles into work and non-work."""

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Do the work. One outcome per work item, success or failure."""
