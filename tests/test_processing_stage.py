import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier, RunManifest
from ingestion_workflow.processors.default_processor import DefaultProcessor
from ingestion_workflow.processors.runner import run_processing
from ingestion_workflow.storage import StorageManager

SAMPLE_HASH_DIR = Path("tests/data/sample_outputs/3qT3nzK9bLZ7")
IDENTIFIER = Identifier(
    pmid="26507433",
    doi="10.1016/j.dcn.2015.10.001",
    pmcid="PMC4691364",
)


def _prepare_storage(tmp_path):
    data_root = tmp_path / "data"
    sample_dest = data_root / IDENTIFIER.hash_id
    shutil.copytree(SAMPLE_HASH_DIR, sample_dest)
    settings = Settings(
        data_root=data_root,
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
    )
    storage = StorageManager(settings)
    storage.prepare()
    return storage


def test_default_processor_builds_processed_paper(tmp_path):
    storage = _prepare_storage(tmp_path)
    processor = DefaultProcessor(storage)

    paper = processor.process(IDENTIFIER, source="pubget")

    assert paper.metadata.title.startswith("Preliminary findings")
    assert paper.tables, "expected tables parsed from source"
    assert any(table.is_coordinate_table for table in paper.tables)
    assert paper.coordinates, "expected coordinate sets"
    assert paper.full_text_path.exists()


def test_run_processing_updates_manifest(tmp_path):
    storage = _prepare_storage(tmp_path)
    manifest = RunManifest(created_at=datetime.now(timezone.utc), settings={})
    manifest.register_identifier(IDENTIFIER)

    paths = storage.paths_for(IDENTIFIER)
    source_dir = paths.source_for("pubget")
    processed_dir = paths.processed_for("pubget")

    download_result = DownloadResult(
        identifier=IDENTIFIER,
        source="pubget",
        artifacts=[
            DownloadArtifact(path=next(source_dir.glob("*.xml")), kind="article_xml"),
            DownloadArtifact(path=processed_dir / "coordinates.csv", kind="coordinates_csv"),
        ],
        open_access=True,
    )
    manifest.register_download(download_result)

    processed = run_processing(manifest, storage)

    assert processed and processed[0].metadata.title.startswith("Preliminary")
    process_payload = manifest.items[IDENTIFIER.hash_id]["process"]
    assert process_payload["table_count"] == len(processed[0].tables)
    assert process_payload["coordinate_tables"]
