"""Helpers for managing filesystem layout used by the workflow."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier


@dataclass
class PaperPaths:
    """Convenience container for common directories tied to an identifier."""

    hash_id: str
    root: Path
    staging_dir: Path
    processed_dir: Path
    source_dir: Path
    final_dir: Path

    def source_for(self, extractor_name: str) -> Path:
        return self.source_dir / extractor_name

    def processed_for(self, extractor_name: str) -> Path:
        return self.processed_dir / extractor_name


class StorageManager:
    """Coordinates staging, cache, and final storage paths."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.data_root = settings.data_root
        self.cache_root = settings.cache_root
        self.ns_pond_root = settings.ns_pond_root

    def prepare(self) -> None:
        """Ensure top-level directories exist."""
        for path in (self.data_root, self.cache_root, self.ns_pond_root):
            path.mkdir(parents=True, exist_ok=True)

    def paths_for(self, identifier: Identifier) -> PaperPaths:
        hash_id = identifier.hash_id
        root = self.data_root / hash_id
        staging_dir = root / "staging"
        processed_dir = root / "processed"
        source_dir = root / "source"
        final_dir = self.ns_pond_root / hash_id

        for path in (root, staging_dir, processed_dir, source_dir):
            path.mkdir(parents=True, exist_ok=True)

        return PaperPaths(
            hash_id=hash_id,
            root=root,
            staging_dir=staging_dir,
            processed_dir=processed_dir,
            source_dir=source_dir,
            final_dir=final_dir,
        )

    def cache_path(self, namespace: str, filename: str) -> Path:
        """Return a path inside the cache namespace."""
        namespace_dir = self.cache_root / namespace
        namespace_dir.mkdir(parents=True, exist_ok=True)
        return namespace_dir / filename

    def save_manifest(self, manifest_payload: Dict[str, object], path: Optional[Path] = None) -> Path:
        """Persist manifest JSON to disk."""
        import json

        if path is None:
            path = self.data_root / "run_manifest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(manifest_payload, handle, indent=2)
        return path
