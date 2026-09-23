"""Turn downloaded bytes into text and tables, one source at a time."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import DownloadResult, DownloadSource, ExtractedContent

from ..plan import StagePlan, Work
from ..stage import Context
from .download import build_extractor

logger = logging.getLogger(__name__)

#: Bump when an extractor change should invalidate stored extractions.
EXTRACT_VERSION = 1


class ExtractStage:
    name = "extract"
    requires = "download"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._extractors: Dict[DownloadSource, object] = {}
        self._order = list(settings.download_sources)

    def _priority(self, sources: Sequence[str]) -> List[str]:
        """Configured order first; anything else after, so a source that has
        been dropped from the config is still reachable."""
        known = [s for s in self._order if s in sources]
        return known + sorted(set(sources) - set(known))

    def extractor(self, source: DownloadSource):
        if source not in self._extractors:
            self._extractors[source] = build_extractor(source, self.settings)
        return self._extractors[source]

    def fingerprint_for(self, source: str, upstream: Artifact) -> str:
        return fingerprint("extract", source, EXTRACT_VERSION, upstream=upstream.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        """Queue at most one extraction per article.

        An article downloaded from several sources only needs extracting once:
        the analyses stage consumes a single extraction, so doing the others is
        work whose result is thrown away. The configured `download_sources`
        order decides which one, and a source is only tried when every source
        above it has been ruled out.
        """
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        attempts_cache: Dict[str, Dict[str, tuple]] = {}

        def attempts(source: str) -> Dict[str, tuple]:
            if source not in attempts_cache:
                attempts_cache[source] = ctx.catalog.attempt_counts(ids, self.name, source)
            return attempts_cache[source]

        for ref in refs:
            downloads = {
                source: artifact
                for source, artifact in upstream.get(ref.id, {}).items()
                if artifact.status is Status.OK
            }
            if not downloads:
                plan.blocked += 1
                continue

            existing = artifacts.get(ref.id, {})
            if any(
                ctx.is_fresh(existing.get(source), self.fingerprint_for(source, download))
                for source, download in downloads.items()
            ):
                plan.fresh += 1
                continue

            chosen = None
            for source in self._priority(list(downloads)):
                count, last = attempts(source).get(ref.id, (0, None))
                if ctx.should_attempt(existing.get(source), count, last, self.name):
                    chosen = (source, downloads[source])
                    break

            if chosen is None:
                plan.permanent += 1
                continue

            source, download = chosen
            plan.pending.append(
                Work(
                    ref=ref,
                    source=source,
                    fingerprint=self.fingerprint_for(source, download),
                    upstream=download,
                )
            )
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        by_source: Dict[str, List[Work]] = {}
        for work in works:
            by_source.setdefault(work.source, []).append(work)

        for source_name, group in by_source.items():
            source = DownloadSource(source_name)
            usable, unusable = self._split_usable(ctx, group)
            yield from unusable
            if not usable:
                continue
            downloads = [download for _, download in usable]
            try:
                results = self.extractor(source).extract(downloads)
            except Exception as exc:
                logger.warning("extract[%s] batch failed: %s", source_name, exc)
                for work, _ in usable:
                    yield Outcome.failure(
                        work.article_id,
                        self.name,
                        source_name,
                        f"{type(exc).__name__}: {exc}",
                        fingerprint=work.fingerprint,
                    )
                continue
            if len(results) != len(usable):
                logger.error(
                    "extract[%s] returned %d results for %d inputs",
                    source_name,
                    len(results),
                    len(usable),
                )
            for (work, _), content in zip(usable, results):
                yield self._outcome(work, source_name, content)

    def _split_usable(self, ctx: Context, works: Sequence[Work]):
        """Drop work whose downloaded files have gone missing, reporting each."""
        usable, rejected = [], []
        for work in works:
            payload = ctx.payload(work.upstream)
            if payload is None:
                rejected.append(
                    Outcome.failure(
                        work.article_id,
                        self.name,
                        work.source,
                        "download payload missing from blob store",
                        fingerprint=work.fingerprint,
                    )
                )
                continue
            download = DownloadResult.from_dict(payload)
            missing = [f.file_path for f in download.files if not f.file_path.exists()]
            if missing or not download.files:
                rejected.append(
                    Outcome.failure(
                        work.article_id,
                        self.name,
                        work.source,
                        f"downloaded files missing on disk: {len(missing)}",
                        fingerprint=work.fingerprint,
                    )
                )
                continue
            usable.append((work, download))
        return usable, rejected

    def _outcome(self, work: Work, source: str, content: ExtractedContent) -> Outcome:
        if content is None or content.error_message:
            return Outcome.failure(
                work.article_id,
                self.name,
                source,
                (content.error_message if content else "extractor returned nothing"),
                fingerprint=work.fingerprint,
            )
        tables = content.tables or []
        with_coords = sum(1 for table in tables if table.coordinates)
        return Outcome(
            article_id=work.article_id,
            stage=self.name,
            source=source,
            status=Status.OK,
            fingerprint=work.fingerprint,
            payload=content.to_dict(),
            summary={
                "tables": len(tables),
                "tables_with_coordinates": with_coords,
                "has_text": bool(content.full_text_path),
            },
        )
