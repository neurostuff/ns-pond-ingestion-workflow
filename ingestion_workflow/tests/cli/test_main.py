from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner
import importlib

cli_module = importlib.import_module("ingestion_workflow.cli.main")
from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    ExtractedContent,
    ExtractedTable,
    Identifier,
)
from ingestion_workflow.models.download import DownloadSource

runner = CliRunner()


def _bundle_payload(tmp_path: Path) -> list[dict]:
    table_path = tmp_path / "table.html"
    table_path.write_text("<table></table>", encoding="utf-8")
    table = ExtractedTable(
        table_id="Table 1",
        raw_content_path=table_path,
    )
    content = ExtractedContent(
        slug="article-1",
        source=DownloadSource.ELSEVIER,
        identifier=Identifier(pmid="12345"),
        tables=[table],
    )
    metadata = ArticleMetadata(title="Example")
    bundle = ArticleExtractionBundle(article_data=content, article_metadata=metadata)
    return [bundle.to_dict()]


def test_cli_create_analyses_writes_output(monkeypatch, tmp_path):
    bundles_path = tmp_path / "bundles.json"
    bundles_path.write_text(
        json.dumps(_bundle_payload(tmp_path)),
        encoding="utf-8",
    )
    output_path = tmp_path / "analyses.json"

    collection = AnalysisCollection(
        slug="article-1::table-1",
        analyses=[Analysis(name="Example")],
    )
    expected_serialized = {"article-1": {"Table 1": collection.to_dict()}}
    expected_payload = {"article-1": {"Table 1": collection}}

    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda *_args, **_kwargs: Settings(llm_api_key="test"),
    )
    monkeypatch.setattr(
        cli_module.create_analyses_workflow,
        "run_create_analyses",
        lambda bundles, settings, extractor_name=None: expected_payload,
    )

    result = runner.invoke(
        cli_module.app,
        [
            "create-analyses",
            str(bundles_path),
            "--output",
            str(output_path),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(output_path.read_text(encoding="utf-8")) == expected_serialized
