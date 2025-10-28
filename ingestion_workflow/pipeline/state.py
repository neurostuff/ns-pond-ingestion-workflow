"""Manifest helpers for tracking pipeline progress."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from ingestion_workflow.config import Settings
from ingestion_workflow.models import RunManifest
from ingestion_workflow.storage import StorageManager


class ManifestStore:
    """Persist and retrieve run manifests."""

    def __init__(self, storage: StorageManager):
        self.storage = storage
        self.default_path = storage.data_root / "run_manifest.json"

    def create(self, settings: Settings) -> RunManifest:
        return RunManifest(
            created_at=datetime.utcnow(),
            settings={
                "data_root": str(settings.data_root),
                "cache_root": str(settings.cache_root),
                "extractor_order": settings.extractor_order,
            },
        )

    def save(self, manifest: RunManifest, path: Optional[Path] = None) -> Path:
        payload = manifest.to_dict()
        return self.storage.save_manifest(payload, path or self.default_path)

    def load(self, path: Path) -> Optional[RunManifest]:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        manifest = RunManifest(
            created_at=datetime.fromisoformat(payload["created_at"]),
            settings=payload.get("settings", {}),
            items=payload.get("items", {}),
        )
        return manifest

    def load_latest(self) -> Optional[RunManifest]:
        return self.load(self.default_path)
