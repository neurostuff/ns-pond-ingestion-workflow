from __future__ import annotations

import csv
import json
from pathlib import Path

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    ArticleExtractionBundle,
    ArticleMetadata,
    ExtractedContent,
    ExtractedTable,
    Identifier,
    UploadOutcome,
)
from ingestion_workflow.models.download import DownloadSource
from ingestion_workflow.workflow.orchastrator import PipelineState
from ingestion_workflow.workflow.sync import run_sync


def test_sync_writes_source_coordinates(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    cache_root = tmp_path / ".cache"
    ns_root = tmp_path / "ns"
    settings = Settings(
        data_root=data_root,
        cache_root=cache_root,
        ns_pond_root=ns_root,
    )

    coordinate_csv = tmp_path / "coords.csv"
    coordinate_csv.write_text("x,y,z,region\n1,2,3,frontal\n", encoding="utf-8")

    raw_table = tmp_path / "table.html"
    raw_table.write_text("<table>example</table>", encoding="utf-8")

    table = ExtractedTable(
        table_id="Table A",
        raw_content_path=raw_table,
        caption="Cap",
        coordinates=[],
        metadata={"coordinates_path": str(coordinate_csv)},
    )
    identifier = Identifier(pmid="12345")
    content = ExtractedContent(
        slug=identifier.slug,
        source=DownloadSource.ACE,
        identifier=identifier,
        tables=[table],
    )
    bundle = ArticleExtractionBundle(
        article_data=content,
        article_metadata=ArticleMetadata(title="Title"),
    )
    state = PipelineState(
        bundles=[bundle],
        analyses={},
        upload_outcomes=[UploadOutcome(slug=content.slug, base_study_id="BS1", success=True)],
    )

    run_sync(state, settings=settings)

    coord_out = ns_root / "BS1" / "processed" / content.source.value / "coordinates.csv"
    assert coord_out.exists()
    with coord_out.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert rows
    assert rows[0]["x"] == "1"
    assert rows[0]["y"] == "2"
    assert rows[0]["z"] == "3"
    # Ensure identifiers were written
    ident_out = ns_root / "BS1" / "identifiers.json"
    payload = json.loads(ident_out.read_text(encoding="utf-8"))
    assert payload["pmid"] == "12345"
