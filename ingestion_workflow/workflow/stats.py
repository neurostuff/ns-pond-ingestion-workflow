"""Shared stage metrics helpers for workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StageMetrics:
    """Lightweight counters for workflow stages."""

    produced: int = 0
    cache_hits: int = 0
    skipped: int = 0

    def record_produced(self, count: int) -> None:
        self.produced += max(0, count)

    def record_cache_hits(self, count: int) -> None:
        self.cache_hits += max(0, count)

    def record_skipped(self, count: int) -> None:
        self.skipped += max(0, count)


__all__ = ["StageMetrics"]
