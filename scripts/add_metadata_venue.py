"""Give each stored metadata record its venue, leaving every other field as it is.

METADATA_VERSION 7 adds the venue (`services.venues`). Left to the stage, every metadata
artifact would be made again from the providers' records, and any field a provider now
answers differently would change with it. This, once, for each OK metadata artifact with
no venue that is fresh under version 6:

* decides its venue as the stage does, from the stored record and the cached OpenAlex
  work (one not cached is not asked for); the sources of the ISSNs PubMed gives are asked
  for in batches of 100 and cached when not held, and NLM's journal list once;
* adds `venue` to the payload, and the files that decision read to the summary's
  `records`, with any note on it;
* gives it the version 7 fingerprint of those inputs.

The field digests that triage, analyses and prose read stay as they are, so only what
carries the bibliography (sync, then upload) is made again. An artifact stale already,
or whose venue lookup hit an outage, is left for the stage. Run it before the metadata
stage runs under version 7:

    python scripts/add_metadata_venue.py --config settings.yaml               # dry run
    python scripts/add_metadata_venue.py --config settings.yaml --apply --backup <path>

A dry run opens the catalog read-only and prints the counts; it needs a catalog at the
current schema. --apply first copies the catalog to `--backup`, a path not yet taken,
then opens it (an older schema is brought up to date) and changes only these artifacts'
fingerprints, summaries and payloads: no status or `updated_at` moves. A row written
meanwhile is left alone. A second run finds nothing left to do.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from contextlib import closing
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import List, Optional

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog  # noqa: E402
from ingestion_workflow.catalog.backup import backup as backup_to  # noqa: E402
from ingestion_workflow.catalog.schema import SCHEMA_VERSION  # noqa: E402
from ingestion_workflow.config import load_settings  # noqa: E402
from ingestion_workflow.models.metadata import ArticleMetadata  # noqa: E402
from ingestion_workflow.pipeline.refingerprint import Rewrite  # noqa: E402
from ingestion_workflow.pipeline.scheduler import batched  # noqa: E402
from ingestion_workflow.pipeline.selection import everything  # noqa: E402
from ingestion_workflow.pipeline.stages.metadata import (  # noqa: E402
    MetadataStage,
    _digest,
    held_dois,
    record_digests,
)

#: The version whose artifacts are given a venue.
FORMER_VERSION = 6


@dataclass
class Added:
    rewrites: List[Rewrite] = field(default_factory=list)
    #: `venue <match>` / `no venue` for each rewrite; `stale`, `transient`, `has venue`
    #: for what is left.
    counts: Counter = field(default_factory=Counter)


def add_venues(catalog: Catalog, settings) -> Added:
    """What adding the venue to each stored metadata record would store. Only the caches
    are written: an ISSN's source and NLM's list, when not held."""
    stage = MetadataStage(settings)
    root = settings.get_cache_dir("metadata")
    found = Added()
    for batch in batched(everything(catalog).refs):
        ids = [ref.id for ref in batch]
        metadata = catalog.artifacts(ids, "metadata")
        downloads = catalog.artifacts(ids, "download")
        extracted = catalog.artifacts(ids, "extract")
        held = []
        for ref in batch:
            artifact = metadata.get(ref.id, {}).get("")
            if artifact is None or not artifact.ok:
                continue
            payload = catalog.payload(artifact)
            if payload is None:  # its blob is gone: the stage makes it again
                found.counts["no payload"] += 1
                continue
            if "venue" in payload:
                found.counts["has venue"] += 1
                continue
            records = (artifact.summary or {}).get("records") or {}
            dois = held_dois(catalog, ref.id)
            args = ((ref.identifier.pmid, dois), downloads.get(ref.id, {}),
                    extracted.get(ref.id, {}))
            now = {path: _digest(root / path) for path in records}
            if stage.fingerprint_for(*args, now, version=FORMER_VERSION) != artifact.fingerprint:
                found.counts["stale"] += 1
                continue
            held.append((ref, artifact, payload, records, dois, args,
                         ArticleMetadata.from_dict(payload)))
        stage.service.cache_sources(record for *_, record in held)
        for ref, artifact, payload, records, dois, args, record in held:
            read, notes = stage.service.venue_from_cache(
                ref.identifier, dois, catalog.aliases(ref.id, "preprint_doi"), record)
            if notes.transient:
                found.counts["transient"] += 1
                continue
            records = {**records, **record_digests(settings, read)}
            summary = {**artifact.summary, "records": records}
            if notes.errors:
                summary["errors"] = {**(summary.get("errors") or {}), **notes.errors}
            venue = record.to_dict()["venue"]
            found.rewrites.append(Rewrite(
                artifact, replace(artifact, fingerprint=stage.fingerprint_for(*args, records),
                                  summary=summary),
                {**payload, "venue": venue}))
            found.counts[f"venue {venue['match']}" if venue else "no venue"] += 1
    return found


def run(settings, *, apply: bool = False, backup: Optional[Path] = None,
        out=sys.stdout) -> Added:
    root = Path(settings.catalog_root)
    path = root / "catalog.sqlite"
    if not path.exists():
        raise SystemExit(f"no catalog at {path}")
    if apply and (backup is None or Path(backup).exists()):
        raise SystemExit("--apply needs --backup <path>, a path not yet taken")
    if apply:
        # The copy first: opening brings an older catalog to SCHEMA_VERSION.
        with closing(sqlite3.connect(path)) as reader:
            print(f"catalog backed up to {backup_to(reader, Path(backup))}", file=out)
        catalog = Catalog.open(root)
    else:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row is None or int(row["value"]) < SCHEMA_VERSION:
            conn.close()
            raise SystemExit(f"the catalog is schema {row['value'] if row else '?'}: "
                             f"--apply brings it to {SCHEMA_VERSION} after its backup")
        catalog = Catalog(conn, root)
    try:
        found = add_venues(catalog, settings)
        for name, n in sorted(found.counts.items()):
            print(f"{name}\t{n}", file=out)
        if not apply:
            print("dry run, nothing written", file=out)
            return found
        stored = catalog.store_fingerprints(found.rewrites)
        print(f"stored {stored} venues; {len(found.rewrites) - stored} artifacts were "
              "written meanwhile", file=out)
        return found
    finally:
        catalog.close()


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
