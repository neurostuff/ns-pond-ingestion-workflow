"""The stage contract. Stages produce artifacts; the scheduler decides when."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Protocol, Sequence

from ingestion_workflow.catalog import (
    ArticleRef,
    Artifact,
    Catalog,
    Outcome,
    Status,
    fingerprint,
)
from ingestion_workflow.catalog.store import NOT_FAILED, RETRY_DUE, retry_state
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.config import Settings
from ingestion_workflow.utils.isolation import error_text

from .plan import StagePlan, Work

logger = logging.getLogger(__name__)

#: Why an artifact was taken back: every extraction of the article ran and found no text,
#: so what was made from its old text is no longer the article's. A plan whose input was
#: taken back takes back its own artifact in turn, down to neurostore and the corpus.
#: A failed or missing extraction is not that evidence: it only blocks.
NO_TEXT = "no extraction text"
#: The other evidence: the prose was read again and holds nothing, and the tables hold
#: nothing either, so the analyses made from the earlier reading are no longer the
#: article's. Taken back like NO_TEXT, from resolve down.
NO_ANALYSES = "no analyses after re-read"
#: Why an artifact may be taken back. A take-back keeps NO_TEXT as its fingerprint, and
#: records its reason as its error.
TAKE_BACK_REASONS = (NO_TEXT, NO_ANALYSES)


def failure_of(stage: str, work: Work, exc: BaseException) -> Outcome:
    """The work's failure for an error it raised."""
    logger.warning("%s failed for %s: %s", stage, work.article_id, exc)
    return Outcome.failure(work.article_id, stage, work.source, error_text(exc),
                           fingerprint=work.fingerprint)


def alone(stage: str, work: Work, make: Callable[[], Outcome]) -> Outcome:
    """`make()`, or the work's failure when it raises: for a stage whose batch has done
    work too dear to redo, where the scheduler would narrow an error by running it again."""
    try:
        return make()
    except ModelUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - this article's failure, not the batch's
        return failure_of(stage, work, exc)


def publication_types(metadata: Mapping[str, Any]) -> List[str]:
    """MeSH publication types, from wherever PubMed's raw record put them.

    `xmltodict` gives a single type as a dict and several as a list, and each
    is `{"#text": ..., "@UI": ...}`, so both shapes have to be unwrapped.
    """
    article = (((metadata.get("raw_metadata") or {}).get("pubmed") or {})
               .get("MedlineCitation") or {}).get("Article") or {}
    listed = (article.get("PublicationTypeList") or {}).get("PublicationType")
    if listed is None:
        return []
    if not isinstance(listed, list):
        listed = [listed]
    out = []
    for item in listed:
        text = item.get("#text") if isinstance(item, dict) else item
        if isinstance(text, str):
            out.append(text)
    return out


#: The fields of an article's metadata a stage may read, each as read from the metadata
#: stage's payload. The metadata stage records a digest of each in its summary
#: (`digests`), so a plan fingerprints the fields its stage reads from the catalog row.
#: A field added here is missing from the rows written before: their readers block
#: (`NO_DIGESTS`) until the digests are recorded again.
METADATA_FIELDS: Mapping[str, Callable[[Mapping[str, Any]], Any]] = {
    "title": lambda payload: payload.get("title"),
    "abstract": lambda payload: payload.get("abstract"),
    "publication_types": publication_types,
}
#: Why a plan blocks an article whose OK metadata records no digests.
NO_DIGESTS = "metadata records no field digests: run scripts/rekey_metadata_reads.py"


def metadata_read(payload: Optional[Mapping[str, Any]], fields: Sequence[str]) -> Dict[str, Any]:
    """`fields` of the metadata stage's `payload`, as a stage reads them."""
    return {name: METADATA_FIELDS[name](payload or {}) for name in fields}


def metadata_digests(payload: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    """A digest of each of `METADATA_FIELDS` of `payload`, as the metadata stage records."""
    return {name: fingerprint("metadata field", json.dumps(value, sort_keys=True))
            for name, value in metadata_read(payload, list(METADATA_FIELDS)).items()}


_NOTHING_READ = metadata_digests(None)


def metadata_fingerprint(metadata: Optional[Artifact], fields: Sequence[str]) -> Optional[str]:
    """The fingerprint of `fields` of `metadata`, from the digests its row records.

    A metadata that is not OK holds no payload, so nothing is read of it. None when an
    OK one records no digest of one of `fields`."""
    digests = (_NOTHING_READ if metadata is None or not metadata.ok
               else (metadata.summary or {}).get("digests") or {})
    if any(name not in digests for name in fields):
        return None
    return fingerprint("metadata read", *[(name, digests[name]) for name in fields])


def taken_back(artifact: Optional[Artifact]) -> bool:
    return (artifact is not None and artifact.status is Status.FAILED
            and artifact.error in TAKE_BACK_REASONS)


def needs_taking_back(existing: Optional[Artifact]) -> bool:
    """Whether an artifact made from text that was taken back still has to be taken back.

    Whatever its status: a prose that last failed on a timeout still sits between the
    taken-back passages and a resolve made from them. A take-back that failed (say the
    neurostore connection dropped) is tried again; one that went through is not."""
    if existing is None:
        return False
    if existing.fingerprint != NO_TEXT:
        return True
    return existing.status is not Status.OK and existing.error not in TAKE_BACK_REASONS


def take_back_or_block(plan: StagePlan, ref: ArticleRef, upstream: Optional[Artifact],
                       existing: Optional[Artifact]) -> None:
    """Plan an article whose input is not OK: it waits for the input, unless that input
    was taken back while this stage still holds something made from it.

    A stage with nothing of its own (roles that never ran for want of a model) still
    records the take-back: the stages after it hold what was made from the same old text,
    and they learn of the take-back only from this stage's artifact."""
    if taken_back(upstream) and (existing is None or needs_taking_back(existing)):
        plan.pending.append(Work(ref=ref, source="", fingerprint=NO_TEXT, upstream=None,
                                 reason=upstream.error))
    else:
        plan.block(ref)


def is_take_back(work: Work) -> bool:
    """Whether `take_back_or_block` planned this work: no input, and the no-text fingerprint.

    Both, so a work built with no upstream for any other reason is executed as usual."""
    return work.upstream is None and work.fingerprint == NO_TEXT


def taking_back(stage: str, works: List[Work]):
    """The failures that take back the works planned by `take_back_or_block`, and the rest."""
    back = [Outcome.failure(w.article_id, stage, "", w.reason or NO_TEXT, fingerprint=NO_TEXT)
            for w in works if is_take_back(w)]
    return back, [w for w in works if not is_take_back(w)]


#: Whether `Context.freshness` reuses an artifact, and why not.
FRESH = "fresh"
NOT_OK = "not a success"
NO_FINGERPRINT = "has no fingerprint: `ingest repair` stores the one it should carry"
CHANGED = "its inputs or the stage's code changed since it was made"
PAYLOAD_GONE = "its stored payload is gone"
TRANSIENT_DUE = "kept with a provider error, and due to be asked again"

#: Why `Context.attempt_state` tries a non-fresh artifact again now...
REFRESHING = "asked to redo"
NEVER_TRIED = "never tried"
REMADE = "made from inputs or code since changed"
INPUT_BACK = "its input is back"
ATTEMPTED = (REFRESHING, NEVER_TRIED, REMADE, INPUT_BACK, RETRY_DUE)
#: ... and why not.
NOT_RETRIED = "a permanent failure or a skip, not tried again"


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
        now: Optional[datetime] = None,
    ) -> None:
        self.settings = settings
        self.catalog = catalog
        self.refresh = {name.lower() for name in refresh}
        self.max_attempts = max_attempts
        self.retry_after = retry_after
        #: The time every plan and run made from this context reads licences and
        #: embargoes at, fixed when it is built.
        self.now = now if now is not None else datetime.now(timezone.utc)

    @classmethod
    def from_settings(cls, settings: Settings, catalog: Catalog,
                      refresh: Sequence[str] = ()) -> "Context":
        """The context a run makes from its settings: their attempts and retry wait."""
        return cls(settings, catalog, refresh=refresh or (), max_attempts=settings.max_attempts,
                   retry_after=timedelta(hours=settings.retry_after_hours))

    def refreshing(self, stage: str, source: str = "") -> bool:
        """Whether the operator asked for this work to be redone.

        `--refresh extract` covers the whole stage; `--refresh extract:ace`
        covers one source of it, which is what a single extractor changing
        calls for.
        """
        if "all" in self.refresh or stage in self.refresh:
            return True
        return bool(source) and f"{stage}:{source}" in self.refresh

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        """Reusable iff it succeeded, its inputs are unchanged, and its blob survives."""
        return self.freshness(artifact, expected) == FRESH

    def freshness(self, artifact: Optional[Artifact], expected: str) -> str:
        """Whether an artifact is reusable as `expected`: FRESH, or why not.

        An artifact with no fingerprint says nothing about its inputs, so it is
        not reusable: `ingest repair` stores the fingerprint it should carry."""
        if artifact is None or artifact.status is not Status.OK:
            return NOT_OK
        if self.refreshing(artifact.stage, artifact.source):
            return REFRESHING
        if not artifact.fingerprint:
            return NO_FINGERPRINT
        if artifact.fingerprint != expected:
            return CHANGED
        if artifact.blob and not self.catalog.blobs.exists(artifact.blob):
            return PAYLOAD_GONE
        return TRANSIENT_DUE if self.due_again(artifact) else FRESH

    def due_again(self, artifact: Artifact) -> bool:
        """Whether an artifact kept despite an error noted `transient` (a provider busy or
        down) is due its retry: as a failure is, after `retry_after`. Any other noted error
        is redone only when asked (`--select errors`)."""
        if not (artifact.summary or {}).get("transient"):
            return False
        try:
            when = datetime.fromisoformat(artifact.updated_at)
        except (TypeError, ValueError):
            return True
        return self.now - when >= self.retry_after

    def should_attempt(
        self,
        artifact: Optional[Artifact],
        attempts: int,
        last_attempt: Optional[str],
        stage: str,
        source: str = "",
    ) -> bool:
        """Whether a non-fresh artifact is worth (re)trying now."""
        return self.attempt_state(artifact, attempts, last_attempt, stage, source) in ATTEMPTED

    def attempt_state(
        self,
        artifact: Optional[Artifact],
        attempts: int,
        last_attempt: Optional[str],
        stage: str,
        source: str = "",
    ) -> str:
        """Why a non-fresh artifact is, or is not, tried again now: one of `ATTEMPTED`,
        or `NOT_RETRIED`, `BACKING_OFF` or `RETRIES_SPENT`."""
        if self.refreshing(stage, source):
            return REFRESHING
        if artifact is None:
            return NEVER_TRIED
        if artifact.status is Status.OK:
            return REMADE
        if taken_back(artifact):
            return INPUT_BACK
        if artifact.status in (Status.PERMANENT, Status.SKIPPED):
            return NOT_RETRIED
        state = retry_state(artifact, attempts, last_attempt, max_attempts=self.max_attempts,
                            backoff=self.retry_after, now=self.now)
        return state if state != NOT_FAILED else NOT_RETRIED

    def payload(self, artifact: Optional[Artifact]):
        return self.catalog.payload(artifact)

    def recorded_path(self, path: Optional[str]) -> Optional[Path]:
        """A path an artifact recorded, made absolute without the process's cwd.

        Extractions run with a relative `cache_root` recorded relative paths
        (`.cache/extract/...`). They are relative to the directory holding the
        catalog, which is where the default `.catalog` and `.cache` sit side by
        side, so where a run is started from does not change what exists."""
        return recorded_path(self.catalog.root, path)


def recorded_path(catalog_root: Path | str, path: Optional[str]) -> Optional[Path]:
    """`Context.recorded_path`, for a reader that holds only the catalog's directory."""
    if not path:
        return None
    recorded = Path(path)
    if recorded.is_absolute():
        return recorded
    return Path(catalog_root).resolve().parent / recorded


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
