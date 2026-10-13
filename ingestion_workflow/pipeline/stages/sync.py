"""Materialise articles into the ns-pond tree pondie reads, with the parse upload sends."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    DownloadResult,
    ExtractedContent,
)
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.services import nspond, paper_parse

from .. import exclusions as excl
from ..plan import StagePlan, Work
from ..stage import NO_TEXT, Context, is_take_back, take_back_or_block
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .roles import refuse_unassigned, with_roles

logger = logging.getLogger(__name__)

#: 2: stage1 points carry `sign` and `is_subpeak`.
#: 3: parse/parsed_paper.json and parse/coordinate_parse.json beside stage1.
#: 4: the sign split follows study_schema.statistics and is declared, not named.
SYNC_VERSION = 4

#: Why an article neurostore holds no base study for waits for sync: the ingester finds
#: or creates one by doi, pmid or pmcid, and the article has none of them.
NO_IDENTIFIER = "no neurostore base study id, and no doi, pmid or pmcid to create one by"

#: Beside the ns-pond tree: `<ns_pond_root>-new/<article_id>/` holds the copy of an article
#: neurostore holds no base study for yet. Upload sends its parse and the ingester creates
#: the base study; the next sync writes the article into the tree under that id and
#: removes this copy. pondie reads only the tree, so it sees no directory not named by one.
NEW_ARTICLES = "-new"

#: Why a paper is not synced: PubMed lists it as a retraction notice, not a study.
RETRACTION_NOTICE = "retraction notice"

#: Why an article is not synced: another catalog article names the same base study. The
#: ingester keeps one current parse per base study and supersedes what a newer one lacks,
#: so each upload would replace the other's analyses. When one of them is a preprint and
#: exactly one is not, that one is synced; otherwise none is, until a person decides.
SHARED_BASE_STUDY = "shares its neurostore base study with another article"
#: The stages whose summaries name the base study an article is written under.
BASE_STUDY_STAGES = ("upload", "sync")


def new_articles_root(settings) -> Path:
    root = Path(settings.ns_pond_root)
    return root.parent / f"{root.name}{NEW_ARTICLES}"


def shared_base_studies(ctx: Context, bases: Dict[str, str]) -> Dict[str, Tuple[str, List[str]]]:
    """{article_id: (base study, the other articles naming it)} for the articles in
    `bases` ({article_id: base study}) that are not synced for SHARED_BASE_STUDY.

    The other articles are any in the catalog whose own base study is the same one, by
    `known_base_study_ids`."""
    if not bases:
        return {}
    named = ctx.catalog.articles_naming_base_studies(list(bases.values()), BASE_STUDY_STAGES)
    others = sorted(set().union(*named.values()) - set(bases))
    found = ctx.catalog.identifiers(others)
    known = {**known_base_study_ids(
        ctx, [ArticleRef(id=i, identifier=found[i]) for i in others]), **bases}
    groups: Dict[str, List[str]] = {}
    for article_id, base in known.items():
        groups.setdefault(base, []).append(article_id)
    groups = {base: sorted(ids) for base, ids in groups.items() if len(ids) > 1}
    preprints = read_from_preprint(ctx, [i for ids in groups.values() for i in ids])
    out = {}
    for base, ids in groups.items():
        published = [i for i in ids if i not in preprints]
        kept = published[0] if len(published) == 1 else None
        for article_id in ids:
            if article_id in bases and article_id != kept:
                out[article_id] = (base, [i for i in ids if i != article_id])
    return out


def read_from_preprint(ctx: Context, ids: Sequence[str]) -> set:
    """The articles whose text sync writes was downloaded as a submitted version."""
    from ingestion_workflow.extractors.pdf_extractor import PREPRINT_VERSION

    if not ids:
        return set()
    extracted = ctx.catalog.artifacts(list(ids), "extract")
    downloads = ctx.catalog.artifacts(list(ids), "download")
    triaged = ctx.catalog.artifacts(list(ids), "triage")
    out = set()
    for article_id in ids:
        found = downloads.get(article_id, {})
        extraction = _synced_extraction(ctx, extracted.get(article_id, {}), found,
                                        triaged.get(article_id, {}).get(""))
        download = found.get(extraction.source) if extraction is not None else None
        if download is not None and (download.summary or {}).get(
                "article_version") == PREPRINT_VERSION:
            out.add(article_id)
    return out


def known_base_study_ids(ctx: Context, refs: Sequence[ArticleRef]) -> Dict[str, str]:
    """{article_id: base study id} for the articles neurostore holds a base study for.

    The one an earlier upload wrote to, else the article's neurostore alias, else the
    one an earlier sync named its directory by.
    """
    ids = [ref.id for ref in refs]
    uploaded = ctx.catalog.artifacts(ids, "upload")
    synced = ctx.catalog.artifacts(ids, "sync")
    out = {}
    for ref in refs:
        upload = uploaded.get(ref.id, {}).get("")
        sync = synced.get(ref.id, {}).get("")
        found = (
            ((upload.summary or {}).get("base_study_id")
             if upload is not None and upload.status in (Status.OK, Status.SKIPPED) else None)
            or ref.identifier.neurostore
            or ((sync.summary or {}).get("base_study_id") if sync is not None else None)
        )
        if found:
            out[ref.id] = found
    return out


class SyncStage:
    """Write the corpus copy and the coordinate parse from `space`, before upload sends it.

    Nothing here waits for an upload. The directory and the parse's header name the base
    study neurostore holds for the article (`known_base_study_ids`). An article it holds
    none for is written under `new_articles_root` with no base study in its header, so the
    ingester creates one from its identifiers and its ParsedPaper's bibliography.
    """

    name = "sync"
    requires = "space"
    #: An article whose analyses found nothing is a legitimate `ok` in space, and
    #: selection leaves it out rather than writing nothing.
    requires_flag = "tables"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._synced: List[Tuple[str, ArticleExtractionBundle]] = []
        self._retracted: List[str] = []
        self._written_before: Dict[str, Optional[str]] = {}

    def fingerprint_for(self, upstream: Artifact, base_study_id: Optional[str] = None,
                        excluded=None, notices: Optional[Artifact] = None) -> str:
        # The base study is in the parse's header, and the notices are the parsed paper's
        # corrections. A person's exclusions change what is written, so marking a table
        # re-syncs and upload then sends the parse without it.
        marked = excl.digest(excluded or {})
        return fingerprint(
            "sync", SYNC_VERSION, paper_parse.PAPER_PARSE_VERSION,
            (upstream.summary or {}).get("role_records"), "before upload", base_study_id,
            _answered(notices).fingerprint if _answered(notices) else None,
            *([marked] if marked else []),
            upstream=upstream.fingerprint,
        )

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        roled = with_roles(ctx, ids)
        prose = ctx.catalog.artifacts(ids, "prose")
        downloads = ctx.catalog.artifacts(ids, "download")
        extracted = ctx.catalog.artifacts(ids, "extract")
        excluded = ctx.catalog.exclusions(ids)
        noticed = ctx.catalog.artifacts(ids, "notices")
        bases = known_base_study_ids(ctx, refs)
        shared = shared_base_studies(ctx, bases)
        for ref in refs:
            spaced = upstream.get(ref.id, {}).get("")
            existing = artifacts.get(ref.id, {}).get("")
            if spaced is None or spaced.status is not Status.OK:
                take_back_or_block(plan, ref, spaced, existing)
                continue
            if ref.id not in roled:
                plan.blocked += 1
                continue
            base_study_id = bases.get(ref.id)
            identifier = ref.identifier
            notices = _answered(noticed.get(ref.id, {}).get(""))
            if notices is not None and (notices.summary or {}).get("retraction_notice"):
                # A notice can reprint the table it retracts; it is not a study.
                _skip(plan, ref, spaced, existing, RETRACTION_NOTICE, base_study_id)
                continue
            if not base_study_id and not (identifier.doi or identifier.pmid or identifier.pmcid):
                _skip(plan, ref, spaced, existing, NO_IDENTIFIER, None)
                continue
            if ref.id in shared:
                base, others = shared[ref.id]
                _skip(plan, ref, spaced, existing, SHARED_BASE_STUDY, base,
                      f"shares base study {base} with {', '.join(others)}", shared_with=others)
                continue
            if read_from_bad_page(prose.get(ref.id, {}).get(""), downloads.get(ref.id, {}),
                                  extracted.get(ref.id, {})):
                plan.block(f"prose {READ_FROM_BAD_PAGE}")
                continue
            fp = self.fingerprint_for(spaced, base_study_id, excluded.get(ref.id), notices)
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=spaced))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        ids = [work.article_id for work in works]
        extractions = ctx.catalog.artifacts(ids, "extract")
        metadata = ctx.catalog.artifacts(ids, "metadata")
        # The analyses as uploaded: `space` is `analyses` with unknown spaces read in.
        analyses = ctx.catalog.artifacts(ids, "space")
        downloads = ctx.catalog.artifacts(ids, "download")
        triaged = ctx.catalog.artifacts(ids, "triage")
        passages = ctx.catalog.artifacts(ids, "passages")
        # For the coordinate parse: table readings, prose passages, restatements.
        read = ctx.catalog.artifacts(ids, "analyses")
        prose = ctx.catalog.artifacts(ids, "prose")
        resolved = ctx.catalog.artifacts(ids, "resolve")
        noticed = ctx.catalog.artifacts(ids, "notices")

        excluded = ctx.catalog.exclusions(ids)
        bases = self.base_study_ids(ctx, works)
        for work in works:
            base_study_id = bases.get(work.article_id)
            if self.retracting(work):
                yield self._retract(work, base_study_id)
                continue
            if work.skip is not None:
                error, summary = work.skip
                yield Outcome(article_id=work.article_id, stage=self.name, source="",
                              status=Status.SKIPPED, fingerprint=work.fingerprint, error=error,
                              summary=summary)
                continue
            spaced = analyses.get(work.article_id, {}).get("")
            refused = refuse_unassigned(
                self.name, work, ctx.payload(spaced) if spaced is not None else None
            )
            if refused is not None:
                yield refused
                continue
            try:
                bundle, per_table, files = self._assemble(
                    ctx, work, extractions, metadata, analyses, downloads, triaged,
                    excluded.get(work.article_id, {}),
                )
            except LookupError as exc:
                yield Outcome.failure(
                    work.article_id, self.name, "", str(exc), fingerprint=work.fingerprint
                )
                continue
            root, name = self.location(work, base_study_id)
            try:
                target = nspond.write_article(
                    root,
                    name,
                    bundle,
                    per_table,
                    files,
                    overwrite=self.settings.sync_overwrite,
                    stage1=False,
                )
            except Exception as exc:
                logger.warning("sync failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id, self.name, "", f"{type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            inputs = _parse_inputs(
                ctx, work, base_study_id, excluded.get(work.article_id, {}),
                {stage: found.get(work.article_id, {}) for stage, found in (
                    ("extract", extractions), ("metadata", metadata), ("space", analyses),
                    ("triage", triaged), ("analyses", read), ("prose", prose),
                    ("resolve", resolved), ("passages", passages), ("notices", noticed))},
                bundle.article_data.source.value,
            )
            if base_study_id:
                self._synced.append((base_study_id, bundle))
            # stage1 follows the parse so its splits name their originals by the
            # parse's keys; it is written whether or not the parse is.
            splits: dict = {}
            try:
                parse = paper_parse.write(target, bundle, per_table, inputs,
                                          overwrite=self.settings.sync_overwrite,
                                          splits=splits)
            except Exception as exc:  # noqa: BLE001 - stage1 is written; the parse is retried
                nspond.write_stage1(target, per_table, self.settings.sync_overwrite)
                logger.warning("parse files failed for %s: %s", work.article_id, exc)
                yield Outcome.failure(
                    work.article_id, self.name, "", f"parse: {type(exc).__name__}: {exc}",
                    fingerprint=work.fingerprint,
                )
                continue
            nspond.write_stage1(target, per_table, self.settings.sync_overwrite, splits)
            yield Outcome(
                article_id=work.article_id,
                stage=self.name,
                source="",
                status=Status.OK,
                fingerprint=work.fingerprint,
                summary={"base_study_id": base_study_id, "path": str(target), **parse,
                         **self.written(work, target)},
            )

    def base_study_ids(self, ctx: Context, works: Sequence[Work]) -> Dict[str, Optional[str]]:
        """{article_id: the base study its directory and parse header are named by}."""
        prior = ctx.catalog.artifacts([work.article_id for work in works], self.name)
        self._written_before = {
            article_id: (found[""].summary or {}).get("path")
            for article_id, found in prior.items() if "" in found}
        return known_base_study_ids(ctx, [work.ref for work in works])

    def location(self, work: Work, base_study_id: Optional[str]) -> Tuple[Path, str]:
        """(root, directory name) the article is written under."""
        if base_study_id:
            return Path(self.settings.ns_pond_root), base_study_id
        return new_articles_root(self.settings), work.article_id

    def written(self, work: Work, target: Path) -> Dict[str, object]:
        """Marks a copy of an article neurostore holds no base study for; once it has one,
        the earlier copy outside the tree goes, since the tree now holds the article."""
        new = new_articles_root(self.settings)
        if Path(target).parent == new:
            return {"new_to_neurostore": True}
        before = Path(self._written_before.get(work.article_id) or target)
        if before != Path(target) and before.parent == new and before.exists():
            shutil.rmtree(before)
            return {"replaces": str(before)}
        return {}

    def retracting(self, work: Work) -> bool:
        """Whether the work takes the article out of the corpus rather than writing it."""
        return is_take_back(work)

    def _move_aside(self, base_study_id: str) -> Optional[Path]:
        """Move the article's directory out of the tree; where it went, if it was there.

        Moved, not deleted: the corpus is not reproducible from the catalog
        alone, so the directory goes to a sibling `<root>-retracted/` tree,
        stamped with the time, where it can be put back by hand.
        """
        root = Path(self.settings.ns_pond_root)
        source = root / base_study_id
        moved = None
        if source.exists():
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            moved = root.parent / f"{root.name}-retracted" / f"{base_study_id}-{stamp}"
            moved.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(moved))
        self._retracted.append(base_study_id)
        return moved

    def _retract(self, work: Work, base_study_id: Optional[str]) -> Outcome:
        """Take the copy of an article whose analyses were taken back out of the tree.

        No parse is written for it. The copy in the tree is moved aside as `SyncStage`
        moves one; a copy outside it, which neurostore never named, is removed. Recorded
        with the reason the stages before it recorded, so upload sees the take-back.
        """
        reason = work.reason or NO_TEXT
        moved = self._move_aside(base_study_id) if base_study_id else None
        new = new_articles_root(self.settings) / work.article_id
        removed = None
        if new.exists():
            shutil.rmtree(new)
            removed = str(new)
        return Outcome.failure(
            work.article_id, self.name, "", reason, fingerprint=NO_TEXT,
            summary={"base_study_id": base_study_id, "retracted": True, "reason": reason,
                     "moved_to": str(moved) if moved else None,
                     **({"removed": removed} if removed else {})},
        )

    def _assemble(self, ctx, work, extractions, metadata, analyses, downloads, triaged,
                  excluded=None):
        extraction = _synced_extraction(
            ctx,
            extractions.get(work.article_id, {}),
            downloads.get(work.article_id, {}),
            triaged.get(work.article_id, {}).get(""),
        )
        if extraction is None:
            raise LookupError("no successful extraction to sync")
        payload = ctx.payload(extraction)
        if payload is None:
            raise LookupError("extraction payload missing from blob store")
        content = ExtractedContent.from_dict(payload)
        source = extraction.source
        content.identifier = work.ref.identifier
        content.slug = work.ref.identifier.slug

        meta_payload = ctx.payload(metadata.get(work.article_id, {}).get(""))
        article_metadata = (
            ArticleMetadata.from_dict(meta_payload)
            if meta_payload
            else ArticleMetadata(title=content.slug)
        )

        analysis_payload = excl.kept(
            ctx.payload(analyses.get(work.article_id, {}).get("")), excluded or {}
        )
        per_table = {
            table_id: AnalysisCollection.from_dict(blob)
            for table_id, blob in analysis_payload.items()
        }

        files: List[DownloadResult] = []
        download = downloads.get(work.article_id, {}).get(source)
        download_payload = ctx.payload(download)
        if download_payload:
            files.append(DownloadResult.from_dict(download_payload))

        return ArticleExtractionBundle(content, article_metadata), per_table, files

    def finish(self) -> None:
        """Write the corpus-level manifest once the run is over."""
        if not self._synced and not self._retracted:
            return
        nspond.write_corpus_manifest(
            self.settings.ns_pond_root / "pmids.tsv", self._synced, drop=self._retracted
        )
        self._synced.clear()
        self._retracted.clear()


def _skip(plan: StagePlan, ref: ArticleRef, spaced: Artifact, existing: Optional[Artifact],
          reason: str, base_study_id: Optional[str], error: Optional[str] = None,
          **summary) -> None:
    """Plan an article sync records SKIPPED for `reason`, unless that is recorded already.

    The summary keeps the base study, so the article still names it to
    `shared_base_studies`."""
    error = error or reason
    if existing is not None and existing.status is Status.SKIPPED and existing.error == error:
        plan.block(reason)
        return
    plan.pending.append(Work(ref=ref, source="", fingerprint="", upstream=spaced, skip=(
        error, {"reason": reason, "base_study_id": base_study_id, **summary})))


def _answered(notices: Optional[Artifact]) -> Optional[Artifact]:
    """The notices artifact when PubMed has answered for the paper; the catalog keeps
    the last OK answer through a failed refresh, so anything else means never answered."""
    return notices if notices is not None and notices.status is Status.OK else None


def _parse_inputs(
    ctx: Context, work: Work, base_study_id, excluded, found, source
) -> paper_parse.ParseInputs:
    """What the parse files need from the catalog beyond the bundle."""
    def ok(stage, key=""):
        artifact = found[stage].get(key)
        return artifact if artifact is not None and artifact.status is Status.OK else None

    def payload(stage, key=""):
        artifact = ok(stage, key)
        return ctx.payload(artifact) if artifact is not None else None

    analyses = ok("analyses")
    summary = analyses.summary if analyses is not None else {}
    passages = payload("passages")
    resolved = ok("resolve")
    picked = {"extract": ok("extract", source), "metadata": ok("metadata"), "space": ok("space"),
              "triage": ok("triage")}
    return paper_parse.ParseInputs(
        article_id=work.article_id,
        base_study_id=base_study_id,
        triage=payload("triage"),
        excluded=excluded or {},
        readings=summary.get("readings"),
        unread=summary.get("unread"),
        prose=payload("prose"),
        passages_kept=len(passages.get("passages") or []) if passages else None,
        passages_omitted=len(passages["omitted"]) if passages and "omitted" in passages else None,
        restated=(resolved.summary or {}).get("restated") if resolved is not None else None,
        restatements=((resolved.summary or {}).get("restated_points") or ())
        if resolved is not None else (),
        fingerprints={k: a.fingerprint for k, a in picked.items() if a and a.fingerprint},
        corrections=(payload("notices") or {}).get("corrections") if ok("notices") else None,
    )


def _synced_extraction(
    ctx: Context,
    extractions: Dict[str, Artifact],
    downloads: Dict[str, Artifact],
    triage: Artifact | None,
) -> Artifact | None:
    """The extraction triage judged, which is the one the analyses came from.

    Table ids are unique only within an extraction, so writing another one
    beside these analyses would pair them with different tables. An article
    triage has not reached takes the one triage would judge, whose text its
    passages index.
    """
    from .triage import judged_extraction

    judged = extractions.get((triage.summary or {}).get("source", "")) if triage else None
    if judged is not None and judged.status is Status.OK:
        return judged
    return judged_extraction(ctx, extractions, downloads)
