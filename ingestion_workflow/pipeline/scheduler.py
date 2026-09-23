"""Runs stages over a selection in batches, and records what happened."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Outcome, Status

from .plan import StagePlan
from .stage import Context, Stage

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
        parts.append(f"{self.fresh:>7,} fresh")
        for label, value in (
            ("blocked", self.blocked),
            ("skipped", self.skipped),
            ("permanent", self.permanent),
        ):
            if value:
                parts.append(f"{value:>6,} {label}")
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
    progress=None,
) -> RunReport:
    """Advance a selection of articles through the given stages, in order."""
    report = RunReport()
    for stage in stages:
        stage_report = report.for_stage(stage.name)
        for batch in batched(refs):
            plan = _plan_batch(ctx, stage, batch)
            stage_report.absorb(plan)
            if dry_run or not plan.pending:
                continue
            outcomes = list(stage.execute(ctx, plan.pending))
            ctx.catalog.record(outcomes)
            stage_report.ok += sum(1 for o in outcomes if o.status is Status.OK)
            stage_report.failed += sum(1 for o in outcomes if o.status is not Status.OK)
            if progress is not None:
                progress(stage.name, stage_report)
        if not dry_run and hasattr(stage, "finish"):
            stage.finish()
        logger.info("%s", stage_report.line(planned_only=dry_run))
    return report


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
