"""Article metadata, fetched once per article rather than once per run.

Split out of `extract` because it is a per-article fact, not a per-source one:
re-extracting a PDF should not refetch the abstract, and a cached extraction
should not refetch anything at all.

Every provider is asked by identifier, so an extraction is not needed: an
article whose tables never parsed but whose prose holds coordinates gets its
metadata too, which is why `metadata` runs after `passages` -- and before
`prose`, whose model reads the title and abstract.

The licences are decided here too, one for each copy the article's ok downloads hold,
so a new or changed download makes the artifact stale.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import DownloadResult, DownloadSource, ExtractedContent
from ingestion_workflow.models.metadata import ArticleMetadata, is_sufficient

from ..plan import StagePlan, Work
from ..stage import Context, metadata_digests
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .sync import _copy_of

logger = logging.getLogger(__name__)

METADATA_VERSION = 3

#: Kept in the catalog row for display and for the upload stage.
SUMMARY_FIELDS = ("title", "journal", "publication_year", "oa_status")

def held_dois(catalog, article_id: str) -> List[str]:
    """Every DOI the catalog holds for the article as its own: the version of record's and
    each preprint's. Crossref and OpenAlex are asked by each; an alternative is not asked."""
    return catalog.aliases(article_id, "doi") + catalog.aliases(article_id, "preprint_doi")


class MetadataStage:
    name = "metadata"
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._service = None

    @property
    def service(self):
        if self._service is None:
            from ingestion_workflow.services.metadata import MetadataService

            self._service = MetadataService(self.settings)
        return self._service

    def fingerprint_for(self, downloads: Dict[str, Artifact]) -> str:
        """The version, the providers, and the article's ok downloads, whose copies
        each get a licence."""
        held = sorted((source, artifact.fingerprint) for source, artifact in downloads.items()
                      if artifact.status is Status.OK)
        return fingerprint("metadata", METADATA_VERSION, tuple(self.settings.metadata_providers),
                           tuple(held))

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        downloads = ctx.catalog.artifacts([ref.id for ref in refs], "download")
        # With prose on, an article without an extraction is reached through
        # its prose, when that holds a passage for `prose` to read.
        with_passages = {}
        if getattr(self.settings, "prose_model", None):
            passages = ctx.catalog.artifacts([ref.id for ref in refs], "passages")
            for article_id, by in passages.items():
                found = by.get("")
                if (found is not None and found.status is Status.OK
                        and found.summary.get("passages")):
                    with_passages[article_id] = found
        for ref in refs:
            existing = artifacts.get(ref.id, {}).get("")
            fp = self.fingerprint_for(downloads.get(ref.id, {}))
            if ctx.is_fresh(existing, fp) and not _embargo_lifted(existing, ctx.now):
                plan.fresh += 1
                continue
            extractions = [a for a in upstream.get(ref.id, {}).values() if a.status is Status.OK]
            if not extractions and ref.id in with_passages:
                if read_from_bad_page(with_passages[ref.id], downloads.get(ref.id, {}),
                                      upstream.get(ref.id, {})):
                    plan.block(f"passages {READ_FROM_BAD_PAGE}")  # left as they are
                    continue
                extractions = [with_passages[ref.id]]
            if not extractions:
                plan.blocked += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if existing is not None and (existing.summary or {}).get("unanswered"):
                # A provider's outage is not the article's failure: it waits out the
                # retry delay but spends none of its tries.
                count = 0
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(
                Work(ref=ref, source="", fingerprint=fp, upstream=extractions[0])
            )
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        contents: List[ExtractedContent] = []
        by_slug: Dict[str, Work] = {}
        for work in works:
            if work.upstream.stage == "passages":
                # Only the identifier is asked for; passages have no extraction to lend.
                content = ExtractedContent(slug=work.ref.identifier.slug,
                                           identifier=work.ref.identifier,
                                           source=DownloadSource(ctx.payload(work.upstream)["source"]))
                contents.append(content)
                by_slug[content.slug] = work
                continue
            payload = ctx.payload(work.upstream)
            if payload is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "extraction payload missing",
                    fingerprint=work.fingerprint,
                )
                continue
            content = ExtractedContent.from_dict(payload)
            content.identifier = work.ref.identifier
            content.slug = work.ref.identifier.slug
            contents.append(content)
            by_slug[content.slug] = work

        if not contents:
            return

        from ingestion_workflow.services.metadata import HeldDownload

        downloads = ctx.catalog.artifacts([work.article_id for work in by_slug.values()],
                                          "download")
        held: Dict[str, List[HeldDownload]] = {}
        dois: Dict[str, List[str]] = {}
        # In the order the sources are tried, so the first held copy's record speaks first.
        order = {source: i for i, source in enumerate(self.settings.download_sources)}
        for slug, work in by_slug.items():
            dois[slug] = held_dois(ctx.catalog, work.article_id)
            for source, download in sorted(downloads.get(work.article_id, {}).items(),
                                           key=lambda item: (order.get(item[0], len(order)),
                                                             item[0])):
                if download.status is not Status.OK:
                    continue
                payload = ctx.payload(download)
                files = DownloadResult.from_dict(payload).files if payload else []
                held.setdefault(slug, []).append(HeldDownload(
                    source, _copy_of(download),
                    [ctx.recorded_path(str(f.file_path)) for f in files]))

        # Outside the batch's own failures: no article can be decided until it is configured.
        self.service.require_licence_providers()
        try:
            found = self.service.enrich_metadata(contents, held, dois, ctx.now)
        except Exception as exc:
            logger.warning("metadata batch failed: %s", exc)
            for work in by_slug.values():
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
            return

        for slug, work in by_slug.items():
            metadata = found.get(slug)
            if slug in self.service.conflicts:
                yield Outcome.failure(
                    work.article_id, self.name, "", self.service.conflicts[slug],
                    fingerprint=work.fingerprint,
                )
                continue
            if metadata is None:
                yield Outcome.failure(
                    work.article_id, self.name, "", "no provider returned metadata",
                    fingerprint=work.fingerprint,
                )
                continue
            if slug in self.service.undecided:
                undecided = self.service.undecided[slug]
                yield Outcome.failure(
                    work.article_id, self.name, "", str(undecided),
                    fingerprint=work.fingerprint,
                    summary={"unanswered": list(undecided.sources)} if undecided.outage else None,
                )
                continue
            payload = metadata.to_dict()
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                payload=payload,
                summary={
                    name: getattr(metadata, name, None) for name in SUMMARY_FIELDS
                } | _licenses(metadata)
                | {"authors": len(metadata.authors), "sufficient": is_sufficient(metadata),
                   "digests": metadata_digests(payload)},
            )


def _licenses(metadata: ArticleMetadata) -> Dict:
    """The paper's licence and determination, and each held copy's licence, for the report."""
    return {
        "license": metadata.license.license_id,
        "license_determination": metadata.license.determination,
        "copy_licenses": {source: lic.license_id
                          for source, lic in metadata.copy_licenses.items()},
        "license_embargo_until": (metadata.license_embargo_until.isoformat()
                                  if metadata.license_embargo_until else None),
    }


def _embargo_lifted(artifact: Optional[Artifact], now: datetime) -> bool:
    """Whether a deposit the artifact's licences left out, for not yet being in force, is
    in force now. The inputs the fingerprint holds did not change, but the decision did."""
    until = ((artifact.summary or {}).get("license_embargo_until") if artifact else None)
    return bool(until) and datetime.fromisoformat(until) <= now
