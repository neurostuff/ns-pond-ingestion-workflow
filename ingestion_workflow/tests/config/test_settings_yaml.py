from __future__ import annotations

from pathlib import Path
import textwrap

from ingestion_workflow.config import load_settings


def test_load_settings_reads_yaml_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "basic.yaml"
    data_root = tmp_path / "data-root"
    cache_root = tmp_path / "cache-root"
    config_path.write_text(
        textwrap.dedent(
            f"""
            data_root: {data_root}
            cache_root: {cache_root}
            verbose: true
            max_workers: 5
            """
        ),
        encoding="utf-8",
    )

    settings = load_settings(yaml_path=config_path)

    assert settings.data_root == data_root
    assert settings.cache_root == cache_root
    assert settings.verbose is True
    assert settings.max_workers == 5


def test_load_settings_respects_lists_and_booleans(tmp_path: Path) -> None:
    config_path = tmp_path / "list-config.yaml"
    config_path.write_text(
        textwrap.dedent(
            """
            download_sources:
              - ace
              - pubget
            stages:
              - download
              - extract
            export: true
            show_progress: false
            """
        ),
        encoding="utf-8",
    )

    settings = load_settings(yaml_path=config_path)

    assert settings.download_sources == ["ace", "pubget"]
    assert settings.stages == ["download", "extract"]
    assert settings.export is True
    assert settings.show_progress is False
