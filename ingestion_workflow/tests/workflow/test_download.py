from pathlib import Path

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache
from ingestion_workflow.workflow.download import (
    EXTRACTOR_FACTORIES,
    run_downloads,
)


class _FakeElsevierExtractor(BaseExtractor):
    def __init__(self, *, success_dir: Path, success_targets: set[str]):
        self._success_dir = success_dir
        self._success_targets = success_targets

    def download(self, identifiers: Identifiers) -> list[DownloadResult]:
        results: list[DownloadResult] = []
        for identifier in identifiers.identifiers:
            if identifier.hash_id in self._success_targets:
                payload_path = (
                    self._success_dir / f"{identifier.hash_id}_content.txt"
                )
                payload_path.parent.mkdir(parents=True, exist_ok=True)
                payload_path.write_text("payload", encoding="utf-8")
                results.append(
                    DownloadResult(
                        identifier=identifier,
                        source=DownloadSource.ELSEVIER,
                        success=True,
                        files=[
                            DownloadedFile(
                                file_path=payload_path,
                                file_type=FileType.TEXT,
                                content_type="text/plain",
                                source=DownloadSource.ELSEVIER,
                            )
                        ],
                    )
                )
            else:
                results.append(
                    DownloadResult(
                        identifier=identifier,
                        source=DownloadSource.ELSEVIER,
                        success=False,
                        files=[],
                        error_message="simulated failure",
                    )
                )
        return results


def test_run_downloads_mixes_cached_and_new(monkeypatch, tmp_path):
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        download_sources=[DownloadSource.ELSEVIER.value],
    )

    identifiers = Identifiers(
        [
            Identifier(pmid="100"),
            Identifier(pmid="200"),
            Identifier(pmid="300"),
        ]
    )

    cached_dir = tmp_path / "cached"
    cached_dir.mkdir(parents=True, exist_ok=True)
    cached_path = cached_dir / "cached.txt"
    cached_path.write_text("cached", encoding="utf-8")

    cached_result = DownloadResult(
        identifier=identifiers.identifiers[0],
        source=DownloadSource.ELSEVIER,
        success=True,
        files=[
            DownloadedFile(
                file_path=cached_path,
                file_type=FileType.TEXT,
                content_type="text/plain",
                source=DownloadSource.ELSEVIER,
            )
        ],
    )

    cache.cache_download_results(
        settings,
        extractor_name=DownloadSource.ELSEVIER.value,
        results=[cached_result],
    )

    success_target = identifiers.identifiers[1].hash_id

    def fake_factory(_settings: Settings) -> BaseExtractor:
        return _FakeElsevierExtractor(
            success_dir=tmp_path / "success",
            success_targets={success_target},
        )

    monkeypatch.setitem(
        EXTRACTOR_FACTORIES,
        DownloadSource.ELSEVIER,
        fake_factory,
    )

    results = run_downloads(identifiers, settings=settings)

    assert len(results) == 3
    assert results[0].identifier is identifiers.identifiers[0]
    assert results[0].success
    assert results[1].identifier is identifiers.identifiers[1]
    assert results[1].success
    assert not results[2].success
    assert results[2].identifier is identifiers.identifiers[2]

    cached_after, missing_after = cache.partition_cached_downloads(
        settings,
        extractor_name=DownloadSource.ELSEVIER.value,
        identifiers=Identifiers(
            [identifiers.identifiers[1], identifiers.identifiers[2]]
        ),
    )

    assert len(cached_after) == 1
    assert cached_after[0].identifier is identifiers.identifiers[1]
    assert missing_after == [identifiers.identifiers[2]]
