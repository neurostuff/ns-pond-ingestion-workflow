"""Rekey each metadata artifact to the fingerprint of the inputs it was made from.

The metadata stage fingerprinted the version, the list of providers in the settings (a list
the stage never read), the article's downloads and its PDF extractions. It now fingerprints
the version, the ids it asks by (the PMID, and the DOIs held as the article's own and its
preprints'), the downloads and extractions, and a digest of each provider's cached record of
the article. This, once, gives each metadata artifact (OK or failed) whose stored fingerprint
the former formula makes from the inputs it has now the new formula's fingerprint, without
asking a provider and without moving its status or `updated_at`. An artifact the former
formula does not make was stale already and is left as it is, to be made again.

Nothing downstream of the metadata stage reads the metadata artifact's own fingerprint:
triage and prose fingerprint the field digests its summary records, sync its blob, and
notices the article's ids. Run it before the lookup that files preprint DOIs, which should
make the metadata of the articles it changes stale and no other:

    python scripts/rekey_metadata_fingerprints.py --config settings.yaml               # dry run
    python scripts/rekey_metadata_fingerprints.py --config settings.yaml --apply --backup <path>

A dry run opens the catalog read-only and prints what it would do. --apply first copies the
catalog to `--backup`, a path not yet taken; a row written meanwhile is left alone. A second
run finds nothing left to do. The former formula is here only; delete the script once it has
run.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, List, Optional

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog, Status, fingerprint  # noqa: E402
from ingestion_workflow.config import load_settings  # noqa: E402
from ingestion_workflow.pipeline.stages.metadata import (  # noqa: E402
    MetadataStage,
    cached_records,
    held_dois,
)
from ingestion_workflow.services.corresponding_authors import PDF_SOURCES  # noqa: E402

#: `METADATA_VERSION` when the former formula was current.
FORMER_VERSION = 6
BATCH = 900


def former_fingerprint(providers, downloads, extractions) -> str:
    held = sorted((source, a.fingerprint) for source, a in downloads.items()
                  if a.status is Status.OK)
    texts = sorted((source, a.fingerprint) for source, a in extractions.items()
                   if source in PDF_SOURCES and a.status is Status.OK)
    return fingerprint("metadata", FORMER_VERSION, tuple(providers), tuple(held),
                       *([tuple(texts)] if texts else []))


@dataclass
class Rekey:
    #: (stored artifact, the artifact as it is to be stored)
    rewrites: List[tuple] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=lambda: dict.fromkeys(
        ("rekeyed", "already_new", "stale", "no_fingerprint"), 0))


def rekey(settings, catalog: Catalog) -> Rekey:
    """What rekeying the catalog's metadata artifacts would store. Nothing is written."""
    stage = MetadataStage(settings)
    found = Rekey()
    ids = list(catalog.all_article_ids())
    for start in range(0, len(ids), BATCH):
        batch = ids[start:start + BATCH]
        metadata = catalog.artifacts(batch, "metadata")
        if not metadata:
            continue
        downloads = catalog.artifacts(list(metadata), "download")
        extractions = catalog.artifacts(list(metadata), "extract")
        identifiers = catalog.identifiers(list(metadata))
        for article_id, by_source in metadata.items():
            artifact = by_source.get("")
            if artifact is None:
                continue
            if not artifact.fingerprint:
                found.counts["no_fingerprint"] += 1
                continue
            have = (downloads.get(article_id, {}), extractions.get(article_id, {}))
            identifier = identifiers[article_id]
            new = stage.fingerprint_for(
                (identifier.pmid, held_dois(catalog, article_id)), *have,
                cached_records(settings, identifier))
            if artifact.fingerprint == new:
                found.counts["already_new"] += 1
            elif artifact.fingerprint == former_fingerprint(
                    settings.metadata_providers, *have):
                found.rewrites.append((artifact, replace(artifact, fingerprint=new), None))
                found.counts["rekeyed"] += 1
            else:
                found.counts["stale"] += 1
    return found


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
        found = rekey(settings, catalog)
        print("\t".join(f"{name} {n:,}" for name, n in found.counts.items()), file=out)
        if not apply:
            print("dry run, nothing written", file=out)
            return found
        print(f"catalog backed up to {catalog.backup(backup)}", file=out)
        stored = catalog.store_fingerprints(found.rewrites)
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
