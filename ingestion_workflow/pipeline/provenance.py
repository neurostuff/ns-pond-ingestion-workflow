"""Utilities for recording extractor provenance across pipeline runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class ProvenanceEntry:
    """Summary of identifier outcomes for a single extractor batch."""

    attempted: List[Dict[str, str]] = field(default_factory=list)
    coordinate_successes: List[Dict[str, str]] = field(default_factory=list)
    missing_coordinates: List[Dict[str, str]] = field(default_factory=list)
    passed_to_next: List[Dict[str, str]] = field(default_factory=list)
    notes: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "attempted": self.attempted,
            "coordinate_successes": self.coordinate_successes,
            "missing_coordinates": self.missing_coordinates,
            "passed_to_next": self.passed_to_next,
        }
        if self.notes:
            payload["notes"] = self.notes
        return payload


class ProvenanceLogger:
    """Persists provenance information per extractor batch to JSON."""

    def __init__(self, path: Path):
        self.path = path
        self._data: Dict[str, List[Dict[str, object]]] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                self._data = raw.get("extractors", {}) if isinstance(raw, dict) else {}
            except Exception:  # pragma: no cover - fallback to empty provenance
                self._data = {}

    def record_batch(self, extractor_name: str, entry: ProvenanceEntry) -> None:
        extractor_log = self._data.setdefault(extractor_name, [])
        extractor_log.append(entry.to_dict())
        self._flush()

    def as_dict(self) -> Dict[str, object]:
        return {"extractors": self._data}

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.as_dict(), indent=2), encoding="utf-8")
