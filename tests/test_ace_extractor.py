import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ExtractorContext
from ingestion_workflow.extractors.ace_extractor import ACEExtractor
from ingestion_workflow.models import Identifier
from ingestion_workflow.storage import StorageManager


REPO_ROOT = Path(__file__).resolve().parents[1]
ACE_HTML_ROOT = REPO_ROOT / "tests" / "data" / "test_html"
ACE_METADATA_SOURCE = ACE_HTML_ROOT / "metadata_cache"
KNOWN_PMID = "25849988"


@pytest.fixture
def ace_extractor(tmp_path: Path) -> ACEExtractor:
    metadata_cache = tmp_path / "metadata_cache"
    metadata_cache.mkdir(parents=True, exist_ok=True)
    metadata_source_file = ACE_METADATA_SOURCE / KNOWN_PMID
    if metadata_source_file.exists():
        shutil.copy2(metadata_source_file, metadata_cache / KNOWN_PMID)

    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
        ace_html_root=ACE_HTML_ROOT,
        ace_metadata_root=metadata_cache,
        ace_use_readability=False,
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    return ACEExtractor(context)


def test_ace_extractor_parses_coordinates(ace_extractor: ACEExtractor):
    identifier = Identifier(pmid=KNOWN_PMID)
    results = ace_extractor.download([identifier])
    assert results
    result = results[0]

    paths = ace_extractor.context.storage.paths_for(identifier)
    processed_dir = paths.processed_for("ace")
    source_dir = paths.source_for("ace")

    coordinates_path = processed_dir / "coordinates.csv"
    tables_path = processed_dir / "tables.csv"
    metadata_path = processed_dir / "metadata.json"
    manifest_path = source_dir / "tables_manifest.json"
    html_path = source_dir / f"{KNOWN_PMID}.html"

    assert coordinates_path.exists()
    coords_df = pd.read_csv(coordinates_path)
    assert not coords_df.empty
    assert {"x", "y", "z", "region"}.issubset(coords_df.columns)

    tables_df = pd.read_csv(tables_path)
    assert not tables_df.empty
    assert {"table_id", "n_activations"}.issubset(tables_df.columns)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["pmid"] == KNOWN_PMID
    assert metadata.get("ace_source")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest
    assert html_path.exists()
    assert any(artifact.kind == "coordinates_csv" for artifact in result.artifacts)
