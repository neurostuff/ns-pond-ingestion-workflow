"""The stage contract. Stages produce artifacts; the scheduler decides when."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
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

#: Why an artifact was taken back: every extraction of the article ran and found no text,
#: so what was made from its old text is no longer the article's. A plan whose input was
#: taken back takes back its own artifact in turn, down to neurostore and the corpus.
#: A failed or missing extraction is not that evidence: it only blocks.
NO_TEXT = "no extraction text"

#: What an upload records instead of retracting when a studyset holds the study: the
#: coordinates under someone's meta-analysis change only after a person has looked.
HELD_FOR_REVIEW = "held for review"


def taken_back(artifact: Optional[Artifact]) -> bool:
    return artifact is not None and artifact.status is Status.FAILED and artifact.error == NO_TEXT


def needs_taking_back(existing: Optional[Artifact]) -> bool:
    """Whether an artifact made from text that was taken back still has to be taken back.

    Whatever its status: a prose that last failed on a timeout still sits between the
    taken-back passages and a resolve made from them. A take-back that failed (say the
    neurostore connection dropped) is tried again; one that went through is not."""
    if existing is None:
        return False
    if existing.fingerprint != NO_TEXT:
        return True
    return existing.status is not Status.OK and existing.error != NO_TEXT


def take_back_or_block(plan: StagePlan, ref: ArticleRef, upstream: Optional[Artifact],
                       existing: Optional[Artifact]) -> None:
    """Plan an article whose input is not OK: it waits for the input, unless that input
    was taken back while this stage still holds something made from it."""
    if taken_back(upstream) and needs_taking_back(existing):
        plan.pending.append(Work(ref=ref, source="", fingerprint=NO_TEXT, upstream=None))
    else:
        plan.blocked += 1


def taking_back(stage: str, works: List[Work]):
    """The failures that take back the works planned by `take_back_or_block`, and the rest."""
    back = [Outcome.failure(w.article_id, stage, "", NO_TEXT, fingerprint=NO_TEXT)
            for w in works if w.upstream is None]
    return back, [w for w in works if w.upstream is not None]


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
        if taken_back(artifact):
            return True  # not a failed try: its input is back
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

    def recorded_path(self, path: Optional[str]) -> Optional[Path]:
        """A path an artifact recorded, made absolute without the process's cwd.

        Extractions run with a relative `cache_root` recorded relative paths
        (`.cache/extract/...`). They are relative to the directory holding the
        catalog, which is where the default `.catalog` and `.cache` sit side by
        side, so where a run is started from does not change what exists."""
        if not path:
            return None
        recorded = Path(path)
        if recorded.is_absolute():
            return recorded
        return Path(self.catalog.root).resolve().parent / recorded


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
