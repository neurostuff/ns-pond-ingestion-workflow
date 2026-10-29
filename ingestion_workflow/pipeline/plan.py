"""What a stage would do, worked out before anything is executed."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact


@dataclass
class Work:
    """One unit a stage has been asked to produce."""

    ref: ArticleRef
    source: str
    fingerprint: str
    upstream: Optional[Artifact] = None
    #: Why a take-back work takes back, one of `stage.TAKE_BACK_REASONS`; NO_TEXT when unset.
    reason: Optional[str] = None
    #: When the stage records the article SKIPPED instead of making its output:
    #: (the error naming this article's case, the summary, whose `reason` it is counted by).
    skip: Optional[Tuple[str, Dict[str, Any]]] = None

    @property
    def article_id(self) -> str:
        return self.ref.id


#: What a plan decided for an article it does not put to work.
FRESH = "fresh"          # its artifact is current: nothing to do
BLOCKED = "blocked"      # the stage does not take it: its input has not passed, or why
SKIPPED = "skipped"      # nothing for the stage to do, or no source can address it
PERMANENT = "permanent"  # its artifact failed and is not tried again now (`Context.attempt_state`)
#: ... and for an article it does.
PENDING = "pending"


@dataclass
class StagePlan:
    """The outcome of planning one stage over one selection: per article, the work it
    is given or the verdict it is not, and the counts of each."""

    stage: str
    pending: List[Work] = field(default_factory=list)
    fresh: int = 0
    blocked: int = 0
    skipped: int = 0
    permanent: int = 0
    #: Why articles were blocked, when the stage can say: {reason: count}.
    reasons: Dict[str, int] = field(default_factory=dict)
    #: article_id -> (verdict, reason) of each article not put to work.
    verdicts: Dict[str, Tuple[str, Optional[str]]] = field(default_factory=dict)

    def add_fresh(self, ref) -> None:
        self.fresh += 1
        self.verdicts[ref.id] = (FRESH, None)

    def add_skipped(self, ref, reason: Optional[str] = None) -> None:
        self.skipped += 1
        self.verdicts[ref.id] = (SKIPPED, reason)

    def add_permanent(self, ref) -> None:
        self.permanent += 1
        self.verdicts[ref.id] = (PERMANENT, None)

    def block(self, ref, reason: Optional[str] = None) -> None:
        """Count one article blocked, for `reason` when the stage can say."""
        self.blocked += 1
        self.verdicts[ref.id] = (BLOCKED, reason)
        if reason is not None:
            self.reasons[reason] = self.reasons.get(reason, 0) + 1

    def by_article(self) -> Dict[str, Tuple[str, Optional[str]]]:
        """article_id -> (verdict, reason) for every article planned: PENDING when it
        has work."""
        out = dict(self.verdicts)
        out.update((work.article_id, (PENDING, None)) for work in self.pending)
        return out

    @property
    def total(self) -> int:
        return len(self.pending) + self.fresh + self.blocked + self.skipped + self.permanent

    def describe(self) -> str:
        bits = [f"{len(self.pending):>7,} pending", f"{self.fresh:>7,} fresh"]
        if self.blocked:
            bits.append(f"{self.blocked:>6,} blocked")
        if self.skipped:
            bits.append(f"{self.skipped:>6,} skipped")
        if self.permanent:
            bits.append(f"{self.permanent:>6,} permanent")
        bits.extend(f"({count:,} blocked: {reason})" for reason, count in self.reasons.items())
        return "   ".join(bits)
