"""Rekey triage and prose, and what is made from them, to what they read of the metadata.

Triage and prose fingerprinted the metadata artifact's whole fingerprint, so a change
to any field of it (a licence) made them stale, and everything made from them. They now
fingerprint only what they read of it (`metadata_read` in each). This gives each stored
artifact that the former formula made from the inputs it has now the current formula's
fingerprint, its output accepted, and carries what was made from it over to the
fingerprint expected of it now, as `ingest repair` carries what was made from an
artifact it fills. An artifact the former formula does not match was stale already and
is left as it is; so is one no plan asks about, its input missing or not OK. Empty
fingerprints are left for `ingest repair`.

Prose records with each passage's answer the metadata it was read with, which was the
metadata artifact's fingerprint and is now the fingerprint of what the model read of
it; an answer recorded with the article's metadata as it stands is relabelled, so a
passage whose text is unchanged keeps its answer.

The former formula is checked against the metadata as it stands, so run it once,
before the metadata stage or `ingest repair` changes a metadata artifact, with the
config the runs use and its models served (a stage blocked for want of its model asks
about nothing, and what it holds would go stale: --apply then refuses):

    python scripts/rekey_metadata_reads.py --config settings.yaml               # dry run
    python scripts/rekey_metadata_reads.py --config settings.yaml --apply --backup <path>

A dry run opens the catalog read-only and prints, by stage, what it would do. --apply
first copies the catalog to `--backup`, a path not yet taken, then changes only
fingerprints, the fingerprints summaries copy, and the relabelled prose payloads: no
status or `updated_at` moves. A row written meanwhile is left alone. A second run
finds nothing left to rekey.
"""

from __future__ import annotations

import argparse
import copy
import sqlite3
import sys
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from typing import Dict, Optional, Sequence, Set

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import ArticleRef, Artifact, Catalog  # noqa: E402
from ingestion_workflow.config import load_settings  # noqa: E402
from ingestion_workflow.pipeline import Context  # noqa: E402
from ingestion_workflow.pipeline.refingerprint import (  # noqa: E402
    Key,
    Rewrite,
    _After,
    _Before,
    _key,
)
from ingestion_workflow.pipeline.scheduler import RunReport, batched, run_stages  # noqa: E402
from ingestion_workflow.pipeline.selection import everything  # noqa: E402
from ingestion_workflow.pipeline.stage import NO_TEXT, Stage  # noqa: E402
from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, build  # noqa: E402

#: The stages whose formula changed.
READERS = ("triage", "prose")
COUNTS = ("rekeyed", "carried", "fresh", "stale", "no_upstream", "unasked", "lost",
          "relabelled")


def _whole_metadata(ctx: Context, metadata: Optional[Artifact]) -> Optional[str]:
    return metadata.fingerprint if metadata is not None else None


def former(stage: Stage) -> Stage:
    """`stage` as it fingerprinted before: with the metadata artifact's whole fingerprint."""
    old = copy.copy(stage)
    old.metadata_fingerprint = _whole_metadata
    return old


class _Rekeying(_After):
    """Plans with the rekeyed fingerprints. An artifact with no fingerprint is planned as
    `_Before` plans it, and is never given one."""

    def is_fresh(self, artifact: Optional[Artifact], expected: str) -> bool:
        if artifact is not None and not artifact.fingerprint:
            return Context.is_fresh(self, replace(artifact, fingerprint=expected), expected)
        return super().is_fresh(artifact, expected)


@dataclass
class Rekey:
    rewrites: Dict[Key, Rewrite]
    #: {stage: {count: n}}, the counts named in `COUNTS`.
    stages: Dict[str, Dict[str, int]] = field(default_factory=dict)
    plan: RunReport = field(default_factory=RunReport)
    #: The stages blocked for want of a model that is not served.
    unserved: Set[str] = field(default_factory=set)

    def rerun(self) -> Dict[str, int]:
        """By stage, what would be made again only because this is stored: what was fresh
        and is not, and what a stage whose model is not served could not be asked about.
        What any other stage blocks waits on its own inputs, and is made again when they
        change, rekeyed or not."""
        out = {stage: n["lost"] + (n["unasked"] if stage in self.unserved else 0)
               for stage, n in self.stages.items()}
        return {stage: n for stage, n in out.items() if n}


def _relabel(payload: dict, old: str, new: str) -> bool:
    changed = False
    for passage in payload.get("passages") or []:
        for recorded in passage.get("inputs") or []:
            if recorded.get("artifact_kind") == "metadata" and recorded.get("fingerprint") == old:
                recorded["fingerprint"] = new
                changed = True
    return changed


def _prose(ctx: Context, prose: Stage, refs: Sequence[ArticleRef],
           rewrites: Dict[Key, Rewrite], counts: Dict[str, Dict[str, int]]) -> None:
    """Relabel the metadata each OK prose's answers record, and give a rewritten prose
    the `read_fingerprint` its new fingerprint was made from."""
    for batch in batched(refs):
        ids = [ref.id for ref in batch]
        metadata = ctx.catalog.artifacts(ids, "metadata")
        passages = ctx.catalog.artifacts(ids, "passages")
        for article_id, by_source in ctx.catalog.artifacts(ids, prose.name).items():
            artifact = by_source.get("")
            meta = metadata.get(article_id, {}).get("")
            if artifact is None or not artifact.ok or meta is None or not meta.ok:
                continue
            key = _key(artifact)
            done = rewrites.get(key)
            now, payload = (done.new, done.payload) if done else (artifact, None)
            read = prose.metadata_fingerprint(ctx, meta)
            if done and "read_fingerprint" in now.summary:
                parent = passages.get(article_id, {}).get("")
                moved = rewrites.get(_key(parent)) if parent is not None else None
                parent = moved.new if moved else parent
                if parent is not None and prose.fingerprint_for(parent, read) == now.fingerprint:
                    now = replace(now, summary={
                        **now.summary, "read_fingerprint": prose.read_fingerprint(parent, read)})
            stored = ctx.catalog.payload(artifact)
            if stored and _relabel(stored, meta.fingerprint, read):
                payload = stored
                counts[prose.name]["relabelled"] += 1
            if payload is not None or (done and now is not done.new):
                rewrites[key] = Rewrite(artifact, now, payload)


def rekey(ctx: Context, stages: Sequence[Stage], refs: Sequence[ArticleRef]) -> Rekey:
    """What rekeying the artifacts of `refs` would store. Nothing is written."""
    fresh: Set[Key] = set()
    run_stages(_Before(ctx, fresh), stages, refs, dry_run=True)
    fresh_former: Set[Key] = set()
    run_stages(_Before(ctx, fresh_former), [former(s) for s in stages if s.name in READERS],
               refs, dry_run=True)
    rekeying = fresh_former - fresh
    fresh |= rekeying
    after = _Rekeying(ctx, fresh)
    plan = run_stages(after, stages, refs, dry_run=True)
    rewrites = after.rewrites
    counts = {stage.name: dict.fromkeys(COUNTS, 0) for stage in stages}

    prose = next((stage for stage in stages if stage.name == "prose"), None)
    if prose is not None:
        _prose(ctx, prose, refs, rewrites, counts)
    moved = {(article_id, stage) for (article_id, stage, _), done in rewrites.items()
             if done.old.fingerprint != done.new.fingerprint}
    for stage in stages:
        for batch in batched(refs):
            for by_source in ctx.catalog.artifacts([r.id for r in batch], stage.name).values():
                for artifact in by_source.values():
                    if not artifact.ok or artifact.fingerprint in ("", NO_TEXT):
                        continue
                    key, n = _key(artifact), counts[stage.name]
                    done = rewrites.get(key)
                    if done and done.old.fingerprint != done.new.fingerprint:
                        n["rekeyed" if key in rekeying else "carried"] += 1
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
    return Rekey(rewrites, counts, plan, unserved)


def render(found: Rekey) -> str:
    lines = ["stage\t" + "\t".join(COUNTS)]
    for stage in STAGE_ORDER:
        if stage in found.stages:
            lines.append(stage + "\t" + "\t".join(str(found.stages[stage][c]) for c in COUNTS))
    return "\n".join(lines)


def run(settings, *, apply: bool = False, backup: Optional[Path] = None,
        out=sys.stdout) -> Rekey:
    root = Path(settings.catalog_root)
    path = root / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    conn = (sqlite3.connect(path, isolation_level=None) if apply
            else sqlite3.connect(f"file:{path}?mode=ro", uri=True))
    conn.row_factory = sqlite3.Row
    try:
        # Not Catalog.open, which would migrate an older catalog first.
        catalog = Catalog(conn, root)
        ctx = Context(settings, catalog, max_attempts=settings.max_attempts,
                      retry_after=timedelta(hours=settings.retry_after_hours))
        stages = build([n for n in STAGE_ORDER if settings.prose_model or n not in PROSE_STAGES],
                       settings)
        found = rekey(ctx, stages, everything(catalog).refs)
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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", type=Path, required=True, help="the runs' settings file")
    parser.add_argument("--apply", action="store_true", help="write; the default is a dry run")
    parser.add_argument("--backup", type=Path, help="where --apply copies the catalog first")
    args = parser.parse_args(argv)
    run(load_settings(args.config), apply=args.apply, backup=args.backup)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
