"""What a stage would do, worked out before anything is executed."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from ingestion_workflow.catalog import ArticleRef, Artifact


@dataclass
class Work:
    """One unit a stage has been asked to produce."""

    ref: ArticleRef
    source: str
    fingerprint: str
    upstream: Optional[Artifact] = None

    @property
    def article_id(self) -> str:
        return self.ref.id


@dataclass
class StagePlan:
    """The outcome of planning one stage over one selection."""

    stage: str
    pending: List[Work] = field(default_factory=list)
    fresh: int = 0
    blocked: int = 0
    skipped: int = 0
    permanent: int = 0

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
        return "   ".join(bits)
