"""Base classes shared by processing components."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ingestion_workflow.models import Identifier, ProcessedPaper


class ProcessorError(RuntimeError):
    """Raised when processing fails for a paper."""


@dataclass
class ProcessingContext:
    """Context for processing stages."""

    working_dir: Path
    identifier: Identifier

    def create_subdir(self, name: str) -> Path:
        path = self.working_dir / name
        path.mkdir(parents=True, exist_ok=True)
        return path


class Processor:
    """Simple base class for processor components."""

    name: str

    def process(self, context: ProcessingContext) -> ProcessedPaper:
        raise NotImplementedError
