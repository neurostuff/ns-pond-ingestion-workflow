import json
import os
from pathlib import Path

import pandas as pd
import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ExtractorContext
from ingestion_workflow.extractors.pubget_extractor import (
    PubMedIdConverter,
    PubgetExtractor,
)
from ingestion_workflow.models import Identifier
from ingestion_workflow.storage import StorageManager

TESTS_DIR = Path(__file__).resolve().parent
PUBGET_CACHE_ROOT = TESTS_DIR / "data" / "pubget_cache"


@pytest.fixture
def extractor(tmp_path: Path) -> PubgetExtractor:
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    return PubgetExtractor(context)


@pytest.mark.vcr(record="once")
def test_pubmed_id_converter_resolves_pmcid():
    api_key = os.getenv("INGEST_PUBMED_API_KEY")
    if not api_key:
        pytest.skip("INGEST_PUBMED_API_KEY not configured")
    converter = PubMedIdConverter(email="pytest@example.com", api_key=api_key)
    identifier = Identifier(doi="10.1093/cercor/bht135")

    resolution = converter.resolve(identifier)

    assert resolution is not None
    assert resolution.pmcid_str == "PMC4207879"
    assert resolution.open_access is False


@pytest.mark.vcr(record="once")
def test_pubget_extractor_downloads_real_article(extractor: PubgetExtractor):
    identifier = Identifier(pmcid="PMC4691364")
    results = extractor.download([identifier])
    assert results
    result = results[0]

    storage = extractor.context.storage
    paths = storage.paths_for(identifier)
    processed_dir = paths.processed_for("pubget")
    source_dir = paths.source_for("pubget")

    coordinates_path = processed_dir / "coordinates.csv"
    assert coordinates_path.exists()
    coordinates_df = pd.read_csv(coordinates_path)
    assert not coordinates_df.empty
    assert {"x", "y", "z"}.issubset(coordinates_df.columns)

    manifest_path = source_dir / "tables_manifest.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert any(entry["table_id"] for entry in manifest)

    table_ids = {entry["table_id"] for entry in manifest}
    xml_files = {path.stem for path in (source_dir / "tables").glob("*.xml") if path.stem != "tables"}
    assert table_ids.issubset(xml_files)

    assert any(artifact.kind == "coordinates_csv" for artifact in result.artifacts)


def test_pubget_extractor_prefers_cached_articlesets(tmp_path: Path):
    if not PUBGET_CACHE_ROOT.exists():
        pytest.skip("pubget cache fixtures missing")

    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
        pubget_cache_root=PUBGET_CACHE_ROOT,
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    extractor = PubgetExtractor(context)

    working_dir = tmp_path / "work"
    working_dir.mkdir()
    source = extractor._locate_cached_articleset(9056519)
    assert source is not None
    destination = extractor._materialize_articleset(source, working_dir / "articlesets")
    assert destination.is_dir()
    cached_files = list(destination.glob("articleset_*.xml"))
    assert cached_files, "Expected cached articleset to be materialized."
