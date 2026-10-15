"""List the articles whose stored PDF text the pdf extractor now reads differently.

The extractor restores a minus its text layer gave as a NUL (`normalize_text_tokens`)
and sets aside a text that maps `=` to 5 (`pdf_extractor.symbols_as_digits`), but only
when it runs. Extractions stored before that keep their old text and tables, and their
fingerprints are unchanged, so this finds them by running the same code over their
stored texts, for a targeted refresh:

    python scripts/symbols_as_digits_manifest.py <catalog root> <manifest.jsonl>
    ingest run -m <manifest.jsonl> --stage extract --refresh extract:pdf
    ingest run -m <manifest.jsonl>

The catalog is opened read-only. A re-extracted text is already repaired, and one set
aside holds no text, so a second run after the refresh lists none of them.
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
from ingestion_workflow.extractors.docling_convert import normalize_text_tokens  # noqa: E402
from ingestion_workflow.extractors.pdf_extractor import symbols_as_digits  # noqa: E402
from ingestion_workflow.models.ids import Identifier, Identifiers  # noqa: E402
from ingestion_workflow.pipeline.stage import recorded_path  # noqa: E402


def read_differently(text: str) -> bool:
    """Whether the extractor would now repair `text` or set it aside."""
    repaired = normalize_text_tokens(text)
    return repaired != text or symbols_as_digits(repaired) is not None


def misread_articles(catalog_root: Path) -> List[Identifier]:
    """The identifiers of the articles whose OK pdf extraction's stored text the
    extractor would now read differently."""
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
            path = recorded_path(root, (catalog.payload(extraction) or {}).get("full_text_path"))
            if path is not None and path.is_file() and read_differently(
                    path.read_text(encoding="utf-8")):
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
