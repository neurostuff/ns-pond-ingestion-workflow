"""Runs stages over a selection in batches, and records what happened."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Outcome, Status
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.utils.console import progress_bar
from ingestion_workflow.utils.http import transient
from ingestion_workflow.utils.isolation import error_text

from .plan import StagePlan, Work
from .stage import NO_TEXT, TAKE_BACK_REASONS, Context, Stage, failure_of

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
    #: Why its plans blocked what they did, with how many.
    reasons: Dict[str, int] = field(default_factory=dict)

    def absorb(self, plan: StagePlan) -> None:
        self.planned += len(plan.pending)
        self.fresh += plan.fresh
        self.blocked += plan.blocked
        self.skipped += plan.skipped
        self.permanent += plan.permanent
        for reason, count in plan.reasons.items():
            self.reasons[reason] = self.reasons.get(reason, 0) + count

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
            plan = plan_batch(ctx, stage, batch)
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
        _execute_alone(ctx, stage, plan.pending, outcomes)
    finally:
        _record(ctx, outcomes)
        report.ok += sum(1 for o in outcomes if o.status is Status.OK)
        # A skip is a decision about the article, not a failure: it is counted with the
        # plans' skips, and is not retried.
        report.skipped += sum(1 for o in outcomes if o.status is Status.SKIPPED)
        report.failed += sum(1 for o in outcomes
                             if o.status not in (Status.OK, Status.SKIPPED))
        report.taken_back += sum(1 for o in outcomes if o.error in TAKE_BACK_REASONS
                                 or (o.status is Status.OK and o.fingerprint == NO_TEXT))


def _execute_alone(ctx: Context, stage: Stage, works: List[Work], outcomes: List[Outcome]) -> None:
    """Run `works`, adding their outcomes to `outcomes`, so that one work's error fails it
    alone: when the stage raises, the works it had not answered for are run again in halves
    until the one that raises is alone. A provider busy or down (`transient`) is no work's
    failure, so the works it left are left pending. A stop (`ModelUnavailable`) is raised."""
    start = len(outcomes)
    try:
        outcomes.extend(stage.execute(ctx, works))
        return
    except ModelUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - narrowed to the work that raised
        answered = {(o.article_id, o.source) for o in outcomes[start:]}
        rest = [w for w in works if (w.article_id, w.source) not in answered]
        if transient(exc):
            logger.warning("%s: %d article(s) left pending: %s", stage.name, len(rest), exc)
            return
        if len(rest) == 1:
            outcomes.append(failure_of(stage.name, rest[0], exc))
            return
        if not rest:
            logger.warning("%s raised after answering for its whole batch: %s", stage.name, exc)
            return
    half = len(rest) // 2
    for part in (rest[:half], rest[half:]):
        _execute_alone(ctx, stage, part, outcomes)


def _record(ctx: Context, outcomes: List[Outcome]) -> None:
    """Record the outcomes together; when that fails, each alone, and an outcome that cannot
    be recorded is recorded as its article's failure in its place."""
    try:
        ctx.catalog.record(outcomes)
        return
    except Exception as exc:  # noqa: BLE001 - narrowed to the outcome that cannot be
        logger.warning("recording %d outcomes failed (%s); recording each alone",
                       len(outcomes), exc)
    for index, outcome in enumerate(outcomes):
        try:
            ctx.catalog.record([outcome])
        except Exception as exc:  # noqa: BLE001 - this outcome's failure alone
            logger.warning("%s of %s could not be recorded: %s", outcome.stage,
                           outcome.article_id, exc)
            outcomes[index] = Outcome.failure(outcome.article_id, outcome.stage, outcome.source,
                                              f"record: {error_text(exc)}",
                                              fingerprint=outcome.fingerprint)
            ctx.catalog.record([outcomes[index]])


def plan_batch(ctx: Context, stage: Stage, batch: List[ArticleRef]) -> StagePlan:
    ids = [ref.id for ref in batch]
    artifacts = ctx.catalog.artifacts(ids, stage.name)
    upstream = ctx.catalog.artifacts(ids, stage.requires) if stage.requires else {}
    return stage.plan(ctx, batch, artifacts, upstream)
