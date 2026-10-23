"""Store the fingerprint each artifact would be given now, without making it again.

An artifact with no fingerprint is not fresh. Most of them were imported from the
pre-catalog caches and were made from the inputs a run would use now; what each
stage's plan expects of them is stored instead. The artifacts made from them chain
on their empty fingerprint, so those carry over to the new one too, and so does a
fingerprint another artifact copied of them. Every stage is
planned twice, as `ingest run --dry-run` plans it: once against the catalog as it
stands, to learn what was fresh, and once with the new fingerprints, to learn what
each plan expects now.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Dict, Mapping, NamedTuple, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Catalog, Status

from .scheduler import RunReport, run_stages
from .stage import NO_TEXT, Context, Stage
from .stages.extract import extraction_inputs, unusable
from .stages.passages import text_of

Key = Tuple[str, str, str]

#: Summary fields holding a copy of another of the article's artifacts' fingerprint,
#: by the stage writing them, naming that artifact's stage. Roles compares the copy
#: with the artifact, so the copy is rewritten with it.
COPIED_FINGERPRINTS: Mapping[str, Mapping[str, str]] = {
    "prose": {"passages_read": "passages"},
    "resolve": {"prose_read": "prose", "basis": "analyses"},
}


class Rewrite(NamedTuple):
    """An artifact as stored, as it is to be stored, and the payload replacing its blob."""

    old: Artifact
    new: Artifact
    payload: Optional[Any] = None


def _key(artifact: Artifact) -> Key:
    return artifact.article_id, artifact.stage, artifact.source


class _Rewritten:
    """The catalog as it reads once `rewrites` are stored."""

    def __init__(self, catalog: Catalog, rewrites: Dict[Key, Rewrite]) -> None:
        self._catalog = catalog
        self._rewrites = rewrites

    def __getattr__(self, name):
        return getattr(self._catalog, name)

    def _now(self, artifact: Artifact) -> Artifact:
        done = self._rewrites.get(_key(artifact))
        return done.new if done else artifact

    def artifact(self, article_id: str, stage: str, source: str = "") -> Optional[Artifact]:
        found = self._catalog.artifact(article_id, stage, source)
        return self._now(found) if found else None

    def artifacts(self, article_ids: Sequence[str], stage: str) -> Dict[str, Dict[str, Artifact]]:
        found = self._catalog.artifacts(article_ids, stage)
        for by_source in found.values():
            for source, artifact in by_source.items():
                by_source[source] = self._now(artifact)
        return found


class _Before(Context):
    """Plans the catalog as it stands, an empty fingerprint passing as the one expected,
    and notes every artifact that is fresh with a fingerprint of its own."""

    def __init__(self, base: Context, fresh: Set[Key]) -> None:
        super().__init__(base.settings, base.catalog, max_attempts=base.max_attempts,
                         retry_after=base.retry_after)
        self.fresh = fresh

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        if artifact is not None and not artifact.fingerprint:
            return super().is_fresh(replace(artifact, fingerprint=expected), expected)
        fresh = super().is_fresh(artifact, expected)
        if fresh:
            self.fresh.add(_key(artifact))
        return fresh


class _After(Context):
    """Plans with the fingerprints rewritten so far. An artifact a plan asks about that
    was fresh, or had no fingerprint, is rewritten to the fingerprint asked for when that
    makes it fresh."""

    def __init__(self, base: Context, fresh_before: Set[Key]) -> None:
        self.rewrites: Dict[Key, Rewrite] = {}
        super().__init__(base.settings, _Rewritten(base.catalog, self.rewrites),
                         max_attempts=base.max_attempts, retry_after=base.retry_after)
        self.fresh_before = fresh_before
        self.still_fresh: Set[Key] = set()
        self.asked: Set[Key] = set()
        self.empty_asked: Set[Key] = set()

    def _copied(self, artifact: Artifact) -> dict:
        summary = dict(artifact.summary or {})
        for name, stage in COPIED_FINGERPRINTS.get(artifact.stage, {}).items():
            done = self.rewrites.get((artifact.article_id, stage, ""))
            if done and summary.get(name) and summary[name] == done.old.fingerprint:
                summary[name] = done.new.fingerprint
        return summary

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        if artifact is None:
            return False
        key = _key(artifact)
        self.asked.add(key)
        if key in self.rewrites or (artifact.fingerprint and key not in self.fresh_before):
            return super().is_fresh(artifact, expected)
        if artifact.stage == "extract" and artifact.ok and not artifact.fingerprint \
                and artifact.summary.get("has_text", True) and not unusable(artifact) \
                and text_of(self, artifact)[0] is None:
            # An import that kept no text and was not set aside claims some: stored as
            # current it would be chosen over a source that has the text, and no plan
            # could read it.
            self.empty_asked.add(key)
            return False
        now = replace(artifact, fingerprint=expected, summary=self._copied(artifact))
        fresh = super().is_fresh(now, expected)
        if not fresh:
            if not artifact.fingerprint and artifact.ok:
                self.empty_asked.add(key)
        elif now != artifact:
            self.rewrites[key] = Rewrite(artifact, now)
        else:
            self.still_fresh.add(key)
        return fresh


def _read_from(artifact: Artifact) -> Optional[str]:
    """The extraction source the passages or triage verdict `artifact` was read from."""
    if artifact.stage in ("passages", "triage"):
        return (artifact.summary or {}).get("source") or None
    return None


@dataclass
class StageCounts:
    """One stage's artifacts, by what storing the fingerprints does to them."""

    #: Empty fingerprints filled.
    filled: int = 0
    #: Fingerprints rewritten because the artifact they were made from was.
    carried: int = 0
    #: Empty fingerprints a plan asked about and could not fill, their blob being gone:
    #: these run again, as they would have without this change.
    empty_rerun: int = 0
    #: Empty fingerprints no plan asks about: no run reads them as they stand.
    empty_unplanned: int = 0
    #: Fresh before, and not fresh after: these run again.
    lost: int = 0
    #: Made from an artifact that is rewritten, and not asked about, so left to go
    #: stale. A stage whose model is not served blocks, and asks about nothing.
    unasked: int = 0
    #: Extractions left stale that no plan can ask about, the article having no download
    #: of their source. Reported; a run never reads them, so they are no reason to refuse.
    unreachable: int = 0
    #: Passages and triage verdicts read from an extraction that has since failed: kept as
    #: they are, with or without this repair. Reported; no reason to refuse.
    failed_read: int = 0


@dataclass
class Refingerprint:
    """The rewrites to store, and what they do, by stage."""

    rewrites: Dict[Key, Rewrite]
    stages: Dict[str, StageCounts] = field(default_factory=dict)
    #: The plans with the rewrites made: why each stage blocked what it did.
    plan: RunReport = field(default_factory=RunReport)

    def rerun(self) -> Dict[str, int]:
        """By stage, the artifacts that would run again once this is stored."""
        return {stage: n.empty_rerun + n.lost + n.unasked for stage, n in self.stages.items()
                if n.empty_rerun + n.lost + n.unasked}


def refingerprint(ctx: Context, stages: Sequence[Stage], refs: Sequence[ArticleRef],
                  empty: Mapping[str, int]) -> Refingerprint:
    """What storing the fingerprints `stages` expect of the artifacts of `refs` would do.

    `empty` counts the artifacts with no fingerprint, by stage, which `ctx.catalog`
    gives as `empty_fingerprints()`. Nothing is written."""
    fresh_before: Set[Key] = set()
    run_stages(_Before(ctx, fresh_before), stages, refs, dry_run=True)
    after = _After(ctx, fresh_before)
    plan = run_stages(after, stages, refs, dry_run=True)

    rewrites = after.rewrites
    counts: Dict[str, StageCounts] = {}
    for (_, stage, _), (old, _, _) in rewrites.items():
        found = counts.setdefault(stage, StageCounts())
        if old.fingerprint:
            found.carried += 1
        else:
            found.filled += 1
    for _, stage, _ in after.empty_asked - rewrites.keys():
        counts.setdefault(stage, StageCounts()).empty_rerun += 1
    for _, stage, _ in fresh_before - after.still_fresh - rewrites.keys():
        counts.setdefault(stage, StageCounts()).lost += 1
    made: Dict[str, Set[str]] = {}
    for article_id, stage, _ in rewrites:
        made.setdefault(stage, set()).add(article_id)
    for stage in stages:
        stale = ctx.catalog.artifacts(sorted(made.get(stage.requires, ())), stage.name)
        downloads = ctx.catalog.artifacts(sorted(stale), "download")
        extractions = ctx.catalog.artifacts(sorted(stale), "extract")
        for article_id, by_source in stale.items():
            readable = extraction_inputs(downloads.get(article_id, {}))
            for artifact in by_source.values():
                key = _key(artifact)
                if (artifact.ok and artifact.fingerprint not in ("", NO_TEXT)
                        and key not in after.asked and key not in fresh_before):
                    read = extractions.get(article_id, {}).get(_read_from(artifact))
                    if read is not None and read.status is not Status.OK:
                        # The verdict stays while its extraction is failed, with or
                        # without this repair: no plan reads it, and nothing chains on it.
                        counts.setdefault(stage.name, StageCounts()).failed_read += 1
                        continue
                    found = counts.setdefault(stage.name, StageCounts())
                    if artifact.stage == "extract" and artifact.source not in readable:
                        found.unreachable += 1
                    else:
                        found.unasked += 1
    for stage, n in empty.items():
        found = counts.setdefault(stage, StageCounts())
        found.empty_unplanned = n - found.filled - found.empty_rerun
    return Refingerprint(rewrites, counts, plan)
