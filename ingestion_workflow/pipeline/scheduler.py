"""Runs stages over a selection in batches, and records what happened."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Outcome, Status
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.utils.console import progress_bar

from .plan import StagePlan
from .stage import NO_TEXT, TAKE_BACK_REASONS, Context, Stage

logger = logging.getLogger(__name__)

#: Articles per planning batch. Bounds memory regardless of corpus size.
BATCH_SIZE = 500


@dataclass
class StageReport:
    stage: str
    planned: int = 0
    fresh: int = 0
    blocked: int = 0
    skipped: int = 0
    permanent: int = 0
    ok: int = 0
    failed: int = 0
    #: Of the failed: taken back for one of `stage.TAKE_BACK_REASONS`.
    taken_back: int = 0
    #: Why the stage stopped for the rest of the run (`ModelUnavailable`), or None.
    stopped: Optional[str] = None
    #: Of the selection, the articles a stop left without an outcome, planned or not.
    left_pending: int = 0
    #: What the stage has to say of the run beyond its counts (a source spent).
    notes: List[str] = field(default_factory=list)

    def absorb(self, plan: StagePlan) -> None:
        self.planned += len(plan.pending)
        self.fresh += plan.fresh
        self.blocked += plan.blocked
        self.skipped += plan.skipped
        self.permanent += plan.permanent

    def line(self, *, planned_only: bool = False) -> str:
        parts = [f"{self.stage:<16}"]
        if planned_only:
            parts.append(f"{self.planned:>7,} pending")
        else:
            parts.append(f"{self.ok:>7,} done")
            if self.failed:
                parts.append(f"{self.failed:>6,} failed")
            if self.taken_back:
                parts.append(f"{self.taken_back:>6,} taken back")
        parts.append(f"{self.fresh:>7,} fresh")
        if self.stopped:
            parts.append(f"STOPPED: {self.stopped}; {self.left_pending:,} article(s) left pending")
        for label, value in (
            ("blocked", self.blocked),
            ("skipped", self.skipped),
            ("permanent", self.permanent),
        ):
            if value:
                parts.append(f"{value:>6,} {label}")
        parts.extend(self.notes)
        return "   ".join(parts)


@dataclass
class RunReport:
    stages: Dict[str, StageReport] = field(default_factory=dict)

    def for_stage(self, name: str) -> StageReport:
        return self.stages.setdefault(name, StageReport(stage=name))

    def render(self, *, planned_only: bool = False) -> str:
        return "\n".join(
            report.line(planned_only=planned_only) for report in self.stages.values()
        )


def batched(items: Sequence[ArticleRef], size: int = BATCH_SIZE) -> Iterator[List[ArticleRef]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def run_stages(
    ctx: Context,
    stages: Sequence[Stage],
    refs: Sequence[ArticleRef],
    *,
    dry_run: bool = False,
) -> RunReport:
    """Advance a selection of articles through the given stages, in order."""
    report = RunReport()
    for stage in stages:
        stage_report = report.for_stage(stage.name)
        # One bar per stage, stepped per batch: these runs are unattended and
        # hours long, so silence is indistinguishable from a hang.
        bar = None if dry_run else progress_bar(
            ctx.settings, len(refs), stage.name, unit="article"
        )
        for batch in batched(refs):
            plan = _plan_batch(ctx, stage, batch)
            stage_report.absorb(plan)
            if bar is not None:
                bar.update(len(batch))
                bar.set_postfix_str(
                    f"{stage_report.ok:,} done, {stage_report.fresh:,} fresh", refresh=False
                )
            if dry_run or not plan.pending:
                continue
            try:
                _execute_batch(ctx, stage, plan, stage_report)
            except ModelUnavailable as exc:
                # The model is down or misconfigured: not these articles' failure, so none
                # is recorded for the articles it did not answer (that would spend each
                # one's retries), and no later batch of this stage is asked. They stay pending.
                stage_report.stopped = str(exc)
                stage_report.left_pending = len(refs) - (
                    stage_report.ok + stage_report.failed + stage_report.fresh
                    + stage_report.blocked + stage_report.skipped + stage_report.permanent)
                logger.error("%s stopped: %s; %d article(s) left pending",
                             stage.name, exc, stage_report.left_pending)
                break
        if bar is not None:
            bar.close()
        if not dry_run and hasattr(stage, "finish"):
            stage.finish()
        if hasattr(stage, "take_notes"):
            stage_report.notes.extend(stage.take_notes())
        logger.info("%s", stage_report.line(planned_only=dry_run))
    return report


def _execute_batch(ctx: Context, stage: Stage, plan: StagePlan, report: StageReport) -> None:
    """Run one planned batch and record its outcomes, those made before a stop included."""
    outcomes: List[Outcome] = []
    try:
        outcomes.extend(stage.execute(ctx, plan.pending))
    finally:
        ctx.catalog.record(outcomes)
        report.ok += sum(1 for o in outcomes if o.status is Status.OK)
        report.failed += sum(1 for o in outcomes if o.status is not Status.OK)
        report.taken_back += sum(1 for o in outcomes if o.error in TAKE_BACK_REASONS
                                 or (o.status is Status.OK and o.fingerprint == NO_TEXT))


def _plan_batch(ctx: Context, stage: Stage, batch: List[ArticleRef]) -> StagePlan:
    ids = [ref.id for ref in batch]
    artifacts = ctx.catalog.artifacts(ids, stage.name)
    upstream = ctx.catalog.artifacts(ids, stage.requires) if stage.requires else {}
    return stage.plan(ctx, batch, artifacts, upstream)


def outcomes_for_exception(
    works, stage: str, exc: BaseException
) -> Iterator[Outcome]:
    """Turn an executor-level blow-up into per-article failures, not a lost batch."""
    message = f"{type(exc).__name__}: {exc}"
    for work in works:
        yield Outcome.failure(work.article_id, stage, work.source, message)
