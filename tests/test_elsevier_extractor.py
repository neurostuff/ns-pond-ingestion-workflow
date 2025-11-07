import json
from pathlib import Path

import pandas as pd
import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ExtractorContext
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.models import Identifier
from ingestion_workflow.storage import StorageManager


TEST_DOI = "10.1016/j.dcn.2015.10.001"


@pytest.fixture
def elsevier_extractor(tmp_path: Path) -> ElsevierExtractor:
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    return ElsevierExtractor(context)


@pytest.mark.vcr(record="once")
def test_elsevier_extractor_downloads_article(elsevier_extractor: ElsevierExtractor):
    identifier = Identifier(doi=TEST_DOI)

    results = elsevier_extractor.download([identifier])

    assert results
    result = results[0]

    paths = elsevier_extractor.context.storage.paths_for(identifier)
    processed_dir = paths.processed_for("elsevier")
    source_dir = paths.source_for("elsevier")

    article_path = source_dir / "article.xml"
    coordinates_path = processed_dir / "coordinates.csv"
    metadata_path = processed_dir / "metadata.json"
    analyses_path = processed_dir / "analyses.json"

    assert article_path.exists()
    assert coordinates_path.exists()
    coords_df = pd.read_csv(coordinates_path)
    assert not coords_df.empty
    assert {"table_id", "analysis_name", "x", "y", "z", "space"}.issubset(coords_df.columns)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["doi"] == TEST_DOI
    assert metadata.get("title")

    analyses = json.loads(analyses_path.read_text(encoding="utf-8"))
    studies = analyses.get("studyset", {}).get("studies", [])
    assert studies and studies[0].get("analyses")

    assert any(artifact.kind == "coordinates_csv" for artifact in result.artifacts)
