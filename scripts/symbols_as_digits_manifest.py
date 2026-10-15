"""List the articles whose stored PDF text maps math symbols to digits, as a manifest.

The pdf extractor sets such a text aside (`pdf_extractor.symbols_as_digits`), but only
when it runs. Extractions stored before that keep their misread text and tables, and
their fingerprints are unchanged, so this finds them by running the same check over
their stored texts, for a targeted refresh:

    python scripts/symbols_as_digits_manifest.py <catalog root> <manifest.jsonl>
    ingest run -m <manifest.jsonl> --stage extract --refresh extract:pdf
    ingest run -m <manifest.jsonl>

The catalog is opened read-only. Extractions already set aside hold no text, so a
second run after the refresh lists none of them.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ingestion_workflow.catalog import Catalog, Status  # noqa: E402
from ingestion_workflow.extractors.pdf_extractor import symbols_as_digits  # noqa: E402
from ingestion_workflow.models.ids import Identifier, Identifiers  # noqa: E402


def misread_articles(catalog_root: Path) -> List[Identifier]:
    """The identifiers of the articles whose OK pdf extraction's stored text is misread."""
    root = Path(catalog_root).resolve()
    conn = sqlite3.connect((root / "catalog.sqlite").as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    catalog = Catalog(conn, root)
    try:
        ids = list(catalog.all_article_ids())
        found = []
        for article_id, by_source in catalog.artifacts(ids, "extract").items():
            extraction = by_source.get("pdf")
            if extraction is None or extraction.status is not Status.OK:
                continue
            text_path = (catalog.payload(extraction) or {}).get("full_text_path")
            if not text_path:
                continue
            # A relative path is relative to the directory holding the catalog
            # (`Context.recorded_path`).
            path = Path(text_path) if Path(text_path).is_absolute() else root.parent / text_path
            if path.is_file() and symbols_as_digits(path.read_text(encoding="utf-8")):
                found.append(article_id)
        return list(catalog.identifiers(found).values())
    finally:
        conn.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("catalog_root", type=Path, help="Directory holding catalog.sqlite.")
    parser.add_argument("manifest", type=Path, help="JSONL identifiers manifest to write.")
    args = parser.parse_args(argv)
    found = misread_articles(args.catalog_root)
    Identifiers(found).save(args.manifest)
    print(f"{len(found)} articles -> {args.manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
