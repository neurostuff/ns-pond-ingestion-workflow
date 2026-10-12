"""Choosing which articles a run operates on."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Catalog, Status
from ingestion_workflow.models.ids import Identifier, Identifiers

logger = logging.getLogger(__name__)


class Select(str, Enum):
    """Which articles, relative to what has already been attempted."""

    PENDING = "pending"      # what a stage would do: new, stale, or due a retry (the default)
    NEW = "new"              # never attempted at any stage
    FAILED = "failed"        # has at least one retryable failure
    ALL = "all"


@dataclass
class Selection:
    """A resolved set of articles, plus how it was described."""

    refs: List[ArticleRef]
    description: str

    def __len__(self) -> int:
        return len(self.refs)


def from_manifest(catalog: Catalog, path: Path) -> Selection:
    """Articles named by a JSONL identifiers manifest, registering any new ones."""
    identifiers = Identifiers.load(path)
    refs = catalog.register_many(list(identifiers.identifiers))
    return Selection(refs, f"{len(refs):,} articles from {path.name}")


def from_identifiers(catalog: Catalog, identifiers: Sequence[Identifier]) -> Selection:
    refs = catalog.register_many(list(identifiers))
    return Selection(refs, f"{len(refs):,} articles")


def everything(catalog: Catalog) -> Selection:
    refs = [catalog.ref(article_id) for article_id in catalog.all_article_ids()]
    return Selection(refs, f"all {len(refs):,} articles in the catalog")


def narrow(
    catalog: Catalog,
    selection: Selection,
    mode: Select,
    stage: Optional[str],
    refresh: Sequence[str] = (),
    settings=None,
) -> Selection:
    """Restrict a selection by what the catalog already knows about it.

    Work the operator asked to redo counts as pending however finished it
    looks, otherwise `--refresh` and the default `--select pending` contradict
    each other: the artifacts named for redoing are exactly the ones that look
    done, so they would be dropped here and the refresh would do nothing.
    """
    selection = _gated(catalog, selection, stage, settings)
    if mode is Select.ALL or not selection.refs:
        return selection
    if mode is Select.PENDING and settings is not None:
        return _planned(catalog, selection, stage, refresh, settings)

    ids = [ref.id for ref in selection.refs]
    stages = [stage] if stage else _stages_present(catalog)
    per_stage = {name: catalog.artifacts(ids, name) for name in stages}
    wanted = {name.lower() for name in refresh}

    def redoing(artifact) -> bool:
        if "all" in wanted or artifact.stage in wanted:
            return True
        return f"{artifact.stage}:{artifact.source}" in wanted

    def keep(ref: ArticleRef) -> bool:
        artifacts = [
            artifact
            for name in stages
            for artifact in per_stage[name].get(ref.id, {}).values()
        ]
        if any(redoing(artifact) for artifact in artifacts):
            return True
        states = [artifact.status for artifact in artifacts]
        if mode is Select.NEW:
            return not states
        if mode is Select.FAILED:
            return Status.FAILED in states
        # PENDING without settings: no stage can be built to judge freshness,
        # so this is the status-only approximation (stale-but-OK is missed).
        return not states or any(state is not Status.OK for state in states)

    refs = [ref for ref in selection.refs if keep(ref)]
    return Selection(refs, f"{len(refs):,} {mode.value} of {selection.description}")


def _planned(
    catalog: Catalog,
    selection: Selection,
    stage: Optional[str],
    refresh: Sequence[str],
    settings,
) -> Selection:
    """Keep the articles some stage would do something about.

    The stage's own plan is the only judge of freshness: it compares the
    fingerprint of what it would build now against the stored one, and knows
    which failures are permanent and which are due a retry. Asking it here
    means a code or upstream change reaches finished-but-stale work, and this
    module has no second staleness rule to drift from the stages'.

    A stage that cannot plan from this configuration keeps its whole batch
    rather than dropping work on a guess; the run itself reports it.
    """
    from ingestion_workflow.pipeline.scheduler import _plan_batch, batched  # noqa: PLC0415
    from ingestion_workflow.pipeline.stage import Context  # noqa: PLC0415
    from ingestion_workflow.pipeline.stages import build  # noqa: PLC0415

    ctx = Context(
        settings,
        catalog,
        refresh=refresh,
        max_attempts=settings.max_attempts,
        retry_after=timedelta(hours=settings.retry_after_hours),
    )
    wanted = set()
    for stage_obj in build([stage] if stage else None, settings):
        for batch in batched(selection.refs):
            try:
                plan = _plan_batch(ctx, stage_obj, batch)
            except Exception as exc:    # noqa: BLE001
                logger.warning("%s: cannot tell what is pending (%s); keeping the batch",
                               stage_obj.name, exc)
                wanted.update(ref.id for ref in batch)
                continue
            wanted.update(work.article_id for work in plan.pending)
    refs = [ref for ref in selection.refs if ref.id in wanted]
    return Selection(refs, f"{len(refs):,} pending of {selection.description}")


def _gated(catalog: Catalog, selection: Selection, stage: Optional[str], settings=None) -> Selection:
    """Drop the articles a stage has declared it cannot work on.

    A stage may name an upstream summary field that must be truthy before it
    has anything to do -- `analyses` needs `triage` to have passed a table.
    Asking the catalog once is a query over an index; the alternative is
    planning every article to reach the same answer, which for one corpus run
    meant 477,625 artifacts recording that there was nothing to record.

    Only applied when exactly one stage is named, because that is the only
    time the gate is knowable: running `triage` and `analyses` together, the
    articles triage has not reached yet are precisely the ones to keep.

    Applied here rather than in the stage's `plan`, so it holds however the
    selection was built -- a manifest included.
    """
    if not stage or not selection.refs:
        return selection
    from ingestion_workflow.pipeline.stages import STAGE_TYPES     # noqa: PLC0415

    stage_type = STAGE_TYPES.get(stage)
    # Not `gate`: triage already has one, its coordinate gate.
    if settings is not None and hasattr(stage_type, "upstream_for"):
        upstream, flag = stage_type.upstream_for(settings)   # space follows prose_model
    else:
        upstream = getattr(stage_type, "requires", None)
        flag = getattr(stage_type, "requires_flag", None)
    if not (upstream and flag):
        return selection

    allowed = catalog.article_ids_where_summary(upstream, flag)
    refs = [ref for ref in selection.refs if ref.id in allowed]
    dropped = len(selection.refs) - len(refs)
    if dropped:
        logger.info("%s: %s has nothing for %d of %d articles",
                    stage, upstream, dropped, len(selection.refs))
    return Selection(
        refs, f"{len(refs):,} of {selection.description} that {upstream} passed")


def _stages_present(catalog: Catalog) -> List[str]:
    return sorted(catalog.status_counts().keys())
