"""Rekey analyses, and what is made from them, to what analyses reads.

Analyses fingerprinted triage's whole fingerprint, which holds the publication types
triage reads to mark a meta-analysis. Analyses never reads them, yet a change to them
made it stale. It now fingerprints what it reads: triage's verdict (the extraction it
judged and the tables it passed, `source` and `passed_tables` in triage's summary), that
extraction, and the title and abstract (`METADATA_READ`). This, once:

* records `passed_tables` (and `source`) in each OK triage artifact that has none, from
  its payload;
* gives each OK analyses artifact the former formula made from the triage in `--earlier`
  (a copy of the catalog from before triage and the metadata ran again) the current
  formula's fingerprint over what it read: that triage's verdict, and the extraction
  and the metadata as `--earlier` holds them. It is not matched against the triage that
  stands now, which may have run again since. Where the verdict, the extraction, the
  title or the abstract changed after, it is stale, as it should be; otherwise fresh.
  One whose triage was stale in `--earlier` is left: what it read is not what
  `--earlier` holds;
* carries what was made from those over to the fingerprint expected of it now, as
  `ingest repair` carries what was made from an artifact it fills. An artifact with no
  fingerprint is never given one: that is `ingest repair`'s. Sync is not carried: it
  fingerprints the triage it reads, and syncs again.

An analyses artifact the former formula does not match was stale already and is left as
it is; so is an empty fingerprint, which is `ingest repair`'s. Blobs are addressed by
content, so `--earlier`'s payloads are read from this catalog's blob store. Run it with
the config the runs use and its models served (a stage blocked for want of its model asks
about nothing, and what it holds would go stale: --apply then refuses), before analyses
runs again:

    python scripts/rekey_analyses_reads.py --config settings.yaml --earlier <copy>
    python scripts/rekey_analyses_reads.py --config settings.yaml --earlier <copy> \\
        --apply --backup <path>

A dry run opens both catalogs read-only and prints, by stage, what it would do. --apply
first copies the catalog to `--backup`, a path not yet taken, then changes only
fingerprints and summaries: no status, payload or `updated_at` moves. A row written
meanwhile is left alone. A second run finds nothing left to do.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional, Sequence, Set

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import ArticleRef, Artifact, Catalog, fingerprint  # noqa: E402
from ingestion_workflow.config import load_settings  # noqa: E402
from ingestion_workflow.pipeline import Context  # noqa: E402
from ingestion_workflow.pipeline.refingerprint import Key, Rewrite, _After, _Before, _key  # noqa: E402
from ingestion_workflow.pipeline.scheduler import RunReport, batched, run_stages  # noqa: E402
from ingestion_workflow.pipeline.selection import everything  # noqa: E402
from ingestion_workflow.pipeline.stage import NO_TEXT, Stage, metadata_fingerprint  # noqa: E402
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, build  # noqa: E402
from ingestion_workflow.pipeline.stages import analyses as analyses_module  # noqa: E402

COUNTS = ("verdicts", "rekeyed", "rekeyed_stale", "carried", "fresh", "stale", "unmatched",
          "triage_stale_then", "no_digests", "no_upstream", "unasked", "lost")
#: Why a rekeyed analyses artifact is stale: what it read that has changed since.
CHANGED = ("verdict", "extraction", "title_or_abstract")


def former_fingerprint(stage: Stage, triage_fingerprint: str) -> str:
    """What analyses fingerprinted before: triage's whole fingerprint."""
    native = bool(getattr(stage.settings, "llm_native_schema", False))
    return fingerprint(
        "analyses",
        analyses_module.COORDINATE_PARSING_PROMPT_VERSION,
        analyses_module.EXTRACTION_VERSION,
        stage.settings.llm_model,
        str(native),
        *(() if native else (analyses_module.PROMPT_RULES_VERSION,)),
        upstream=triage_fingerprint,
    )


def verdict(payload: Optional[dict]) -> dict:
    """Triage's verdict as its summary now records it, from its payload: what analyses
    read of that payload."""
    payload = payload or {}
    return {"source": payload.get("source", ""),
            "passed_tables": [v["table_id"] for v in payload.get("tables", [])
                              if v.get("passes")]}


class _Verdicts:
    """The catalog with the verdicts to be recorded in its triage artifacts' summaries."""

    def __init__(self, catalog: Catalog, verdicts: Dict[str, dict]) -> None:
        self._catalog = catalog
        self._verdicts = verdicts

    def __getattr__(self, name):
        return getattr(self._catalog, name)

    def _now(self, artifact: Artifact) -> Artifact:
        found = self._verdicts.get(artifact.article_id) if artifact.stage == "triage" else None
        return (replace(artifact, summary={**(artifact.summary or {}), **found})
                if found is not None else artifact)

    def artifact(self, article_id: str, stage: str, source: str = "") -> Optional[Artifact]:
        found = self._catalog.artifact(article_id, stage, source)
        return self._now(found) if found else None

    def artifacts(self, article_ids: Sequence[str], stage: str) -> Dict[str, Dict[str, Artifact]]:
        found = self._catalog.artifacts(article_ids, stage)
        if stage == "triage":
            for by_source in found.values():
                for source, artifact in by_source.items():
                    by_source[source] = self._now(artifact)
        return found


class _Rekeying(_After):
    """Plans with the rekeyed analyses, noting which of them are fresh. An artifact with no
    fingerprint is planned as `_Before` plans it, and is never given one."""

    def __init__(self, base: Context, fresh_before: Set[Key], rekeyed: Dict[Key, Rewrite]):
        super().__init__(base, fresh_before)
        self.rewrites.update(rekeyed)
        self.rekeyed = set(rekeyed)
        self.rekeyed_fresh: Set[Key] = set()

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        if artifact is not None and not artifact.fingerprint:
            return Context.is_fresh(self, replace(artifact, fingerprint=expected), expected)
        fresh = super().is_fresh(artifact, expected)
        if fresh and _key(artifact) in self.rekeyed:
            self.rekeyed_fresh.add(_key(artifact))
        return fresh


@dataclass
class Rekey:
    rewrites: Dict[Key, Rewrite]
    #: {stage: {count: n}}, the counts named in `COUNTS`.
    stages: Dict[str, Dict[str, int]] = field(default_factory=dict)
    #: Of the rekeyed analyses that are stale, how many changed in each of `CHANGED`.
    changed: Dict[str, int] = field(default_factory=dict)
    plan: RunReport = field(default_factory=RunReport)
    #: The stages blocked for want of a model that is not served.
    unserved: Set[str] = field(default_factory=set)

    def rerun(self) -> Dict[str, int]:
        """By stage, what would be made again only because this is stored: what was fresh
        and is not, and what a stage whose model is not served could not be asked about."""
        out = {stage: n["lost"] + (n["unasked"] if stage in self.unserved else 0)
               for stage, n in self.stages.items()}
        return {stage: n for stage, n in out.items() if n}


def _verdicts(ctx: Context, refs: Sequence[ArticleRef]) -> Dict[str, dict]:
    """The verdict each OK triage artifact without one is to record, by article."""
    out: Dict[str, dict] = {}
    for batch in batched(refs):
        for article_id, by_source in ctx.catalog.artifacts([r.id for r in batch],
                                                           "triage").items():
            artifact = by_source.get("")
            if (artifact is not None and artifact.ok
                    and "passed_tables" not in (artifact.summary or {})):
                payload = ctx.catalog.payload(artifact)
                if payload is not None:
                    out[article_id] = verdict(payload)
    return out


def _read(stage: Stage, triage: Optional[Artifact], extractions: Dict[str, Artifact],
          metadata: Optional[Artifact]):
    """What `stage` reads, given these: (verdict, extraction, title and abstract)."""
    summary = (triage.summary or {}) if triage is not None else {}
    extraction = stage._extraction_for(extractions, triage) if triage is not None else None
    return ((summary.get("source", ""), summary.get("passed_tables")),
            extraction.fingerprint if extraction is not None else None,
            metadata_fingerprint(metadata, analyses_module.METADATA_READ))


def _rekeyed(ctx: Context, earlier: Catalog, stage: Stage, triage_stage: Stage,
             refs: Sequence[ArticleRef], n: Dict[str, int]):
    """The analyses to rewrite, and for each, what of what it read has changed since.

    An analysis the former formula matches was made from that triage, so it read what
    that triage read when the triage was fresh: the extraction and metadata `earlier`
    holds."""
    rekeyed: Dict[Key, Rewrite] = {}
    changed: Dict[Key, Set[str]] = {}
    for batch in batched(refs):
        ids = [r.id for r in batch]
        now = {name: ctx.catalog.artifacts(ids, name) for name in ("triage", "extract",
                                                                   "metadata")}
        then = {name: earlier.artifacts(ids, name) for name in ("triage", "extract",
                                                                "metadata")}
        for article_id, by_source in ctx.catalog.artifacts(ids, stage.name).items():
            artifact = by_source.get("")
            if artifact is None or not artifact.ok:
                continue
            triage = then["triage"].get(article_id, {}).get("")
            if (triage is None or not triage.ok
                    or artifact.fingerprint != former_fingerprint(stage, triage.fingerprint)):
                n["unmatched"] += 1
                continue
            triage = replace(triage, summary={**(triage.summary or {}),
                                              **verdict(earlier.payload(triage))})
            extractions = then["extract"].get(article_id, {})
            extraction = stage._extraction_for(extractions, triage)
            metadata = then["metadata"].get(article_id, {}).get("")
            read = metadata_fingerprint(metadata, analyses_module.METADATA_READ)
            judged = triage_stage.metadata_fingerprint(metadata)
            if read is None or judged is None:
                n["no_digests"] += 1
                continue
            if (extraction is None
                    or triage_stage.fingerprint_for(judged, extraction) != triage.fingerprint):
                n["triage_stale_then"] += 1
                continue
            key = _key(artifact)
            rekeyed[key] = Rewrite(artifact, replace(
                artifact, fingerprint=stage.fingerprint_for(triage, extraction, read)))
            was = _read(stage, triage, extractions, metadata)
            is_ = _read(stage, now["triage"].get(article_id, {}).get(""),
                        now["extract"].get(article_id, {}),
                        now["metadata"].get(article_id, {}).get(""))
            changed[key] = {name for name, a, b in zip(CHANGED, was, is_) if a != b}
    return rekeyed, changed


def rekey(ctx: Context, earlier: Catalog, stages: Sequence[Stage],
          refs: Sequence[ArticleRef]) -> Rekey:
    """What rekeying the artifacts of `refs` would store, reading what the analyses read
    from `earlier`. Nothing is written."""
    stored = ctx.catalog
    verdicts = _verdicts(ctx, refs)
    ctx = Context(ctx.settings, _Verdicts(stored, verdicts), max_attempts=ctx.max_attempts,
                  retry_after=ctx.retry_after)
    fresh: Set[Key] = set()
    run_stages(_Before(ctx, fresh), stages, refs, dry_run=True)
    counts = {stage.name: dict.fromkeys(COUNTS, 0) for stage in stages}
    analyses, triage = (next(stage for stage in stages if stage.name == name)
                        for name in ("analyses", "triage"))
    rekeyed, changed = _rekeyed(ctx, earlier, analyses, triage, refs, counts["analyses"])
    after = _Rekeying(ctx, fresh, rekeyed)
    plan = run_stages(after, stages, refs, dry_run=True)
    rewrites = after.rewrites

    for batch in batched(refs):
        for article_id, by_source in stored.artifacts([r.id for r in batch], "triage").items():
            artifact = by_source.get("")
            if artifact is None or article_id not in verdicts:
                continue
            key = _key(artifact)
            done = rewrites.get(key)
            now = done.new if done else artifact
            rewrites[key] = Rewrite(artifact, replace(now, summary={
                **now.summary, **verdicts[article_id]}), done.payload if done else None)
            counts["triage"]["verdicts"] += 1
    moved = {(article_id, stage) for (article_id, stage, _), done in rewrites.items()
             if done.old.fingerprint != done.new.fingerprint}
    stale_because = dict.fromkeys(CHANGED, 0)
    for stage in stages:
        for batch in batched(refs):
            for by_source in stored.artifacts([r.id for r in batch], stage.name).values():
                for artifact in by_source.values():
                    if not artifact.ok or artifact.fingerprint in ("", NO_TEXT):
                        continue
                    key, n = _key(artifact), counts[stage.name]
                    done = rewrites.get(key)
                    if key in after.rekeyed:
                        if key in after.rekeyed_fresh:
                            n["rekeyed"] += 1
                        elif key in after.asked:
                            n["rekeyed_stale"] += 1
                            for name in changed[key]:
                                stale_because[name] += 1
                        else:
                            n["unasked"] += 1
                    elif done and done.old.fingerprint != done.new.fingerprint:
                        n["carried"] += 1
                    elif key in after.still_fresh:
                        n["fresh"] += 1
                    elif key in fresh:
                        n["lost"] += 1  # fresh before, and not once rekeyed
                    elif key in after.asked:
                        n["stale"] += 1
                    elif (artifact.article_id, stage.requires) in moved:
                        n["unasked"] += 1  # its stage blocked: it would go stale
                    else:
                        n["no_upstream"] += 1
    unserved = {stage.name for stage in stages if hasattr(stage, "model_state")
                and any(stage.model_state(o)[0] is None for o in stage.origins())}
    return Rekey(rewrites, counts, stale_because, plan, unserved)


def render(found: Rekey) -> str:
    lines = ["stage\t" + "\t".join(COUNTS)]
    for stage in STAGE_ORDER:
        if stage in found.stages:
            lines.append(stage + "\t" + "\t".join(str(found.stages[stage][c]) for c in COUNTS))
    lines.append("analyses rekeyed_stale, by what changed since: " + ", ".join(
        f"{name} {n}" for name, n in found.changed.items()))
    return "\n".join(lines)


def _read_only(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def run(settings, earlier: Path, *, apply: bool = False, backup: Optional[Path] = None,
        out=sys.stdout) -> Rekey:
    root = Path(settings.catalog_root)
    path = root / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if not Path(earlier).exists():
        raise SystemExit(f"no catalog at {earlier}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    if apply:
        conn = sqlite3.connect(path, isolation_level=None)
        conn.row_factory = sqlite3.Row
    else:
        conn = _read_only(path)
    then = _read_only(earlier)
    try:
        # Not Catalog.open, which would migrate an older catalog first.
        catalog = Catalog(conn, root)
        ctx = Context(settings, catalog, max_attempts=settings.max_attempts,
                      retry_after=timedelta(hours=settings.retry_after_hours))
        stages = build([n for n in STAGE_ORDER if settings.prose_model or n not in PROSE_STAGES],
                       settings)
        found = rekey(ctx, Catalog(then, root), stages, everything(catalog).refs)
        print(render(found), file=out)
        rerun = found.rerun()
        for stage in rerun:
            for reason, n in found.plan.stages[stage].reasons.items():
                print(f"{stage}: {n} blocked: {reason}", file=out)
        if not apply:
            print("dry run, nothing written", file=out)
            return found
        if rerun:
            raise SystemExit("refusing to write: " + ", ".join(
                f"{n} {stage}" for stage, n in rerun.items()) + " fresh now would run again; "
                "serve the models the config names and run it again")
        print(f"catalog backed up to {catalog.backup(backup)}", file=out)
        stored = catalog.store_fingerprints(list(found.rewrites.values()))
        print(f"stored {stored} rewrites; {len(found.rewrites) - stored} artifacts were "
              "written meanwhile", file=out)
        return found
    finally:
        conn.close()
        then.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", type=Path, required=True, help="the runs' settings file")
    parser.add_argument("--earlier", type=Path, required=True,
                        help="a copy of the catalog from before triage and the metadata ran "
                        "again, holding what the analyses read")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    parser.add_argument("--backup", type=Path, help="where --apply copies the catalog first")
    args = parser.parse_args(argv)
    run(load_settings(args.config), args.earlier, apply=args.apply, backup=args.backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
