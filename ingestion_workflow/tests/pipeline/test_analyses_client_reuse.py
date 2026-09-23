"""The analyses service is built once per run, not once per article.

Constructing it builds an `OpenAI` client, and every client builds its own
httpx pool, so one per article reuses no connection and pays a TLS handshake
per article. The batch runs on a thread pool, so the guard has to hold when
the first calls arrive together.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import ingestion_workflow.services.create_analyses as create_analyses
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage


class _CountingService:
    def __init__(self, settings) -> None:
        self.settings = settings


def test_service_is_built_once_under_concurrency(monkeypatch) -> None:
    built: list[object] = []

    def factory(settings):
        service = _CountingService(settings)
        built.append(service)
        return service

    monkeypatch.setattr(create_analyses, "CreateAnalysesService", factory)

    stage = AnalysesStage(settings=object())
    with ThreadPoolExecutor(max_workers=32) as pool:
        services = list(pool.map(lambda _: stage._service(), range(256)))

    assert len(built) == 1
    assert all(service is services[0] for service in services)


def test_service_is_not_built_until_used(monkeypatch) -> None:
    """Planning a run must not open a connection."""
    built: list[object] = []
    monkeypatch.setattr(
        create_analyses,
        "CreateAnalysesService",
        lambda settings: built.append(settings) or _CountingService(settings),
    )

    AnalysesStage(settings=object())

    assert built == []
