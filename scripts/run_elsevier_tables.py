#!/usr/bin/env python3
"""Quick script to fetch Elsevier articles and dump extracted tables."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

if sys.version_info < (3, 9):
    raise SystemExit("Please run this script with Python 3.9 or newer.")

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from elsevier_coordinate_extraction.download.api import download_articles
from elsevier_coordinate_extraction.table_extraction import (
    extract_tables_from_article,
)
from elsevier_coordinate_extraction.types import ArticleContent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Elsevier articles from a manifest and extract tables."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("playground/manifests/manifest_n10.jsonl"),
        help="Path to manifest JSONL file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/elsevier_tables"),
        help="Directory to store extracted tables/XML (default: /tmp/elsevier_tables).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of identifiers to process.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional seed for deterministic sampling.",
    )
    return parser.parse_args()


def load_manifest(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")
    records: List[Dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        doi = payload.get("doi")
        pmid = payload.get("pmid")
        if not doi and not pmid:
            continue
        record = {}
        if doi:
            record["doi"] = doi
        if pmid:
            record["pmid"] = pmid
        records.append(record)
    return records


def slugify_identifier(record: Dict[str, str], metadata: Dict[str, object]) -> str:
    if "identifier_hash" in metadata:
        return str(metadata["identifier_hash"]).replace("/", "_")
    for key in ("pmcid", "pmid", "doi"):
        value = metadata.get(key) or record.get(key)
        if value:
            return str(value).replace("/", "_")
    return "article"


def save_article_payload(article: ArticleContent, output_dir: Path, stem: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / f"{stem}.xml"
    destination.write_bytes(article.payload)
    return destination


def export_tables(
    article: ArticleContent,
    record: Dict[str, str],
    output_dir: Path,
) -> int:
    slug = slugify_identifier(record, article.metadata or {})
    article_dir = output_dir / slug
    save_article_payload(article, article_dir, "content")
    tables = extract_tables_from_article(article.payload)
    for idx, (metadata, dataframe) in enumerate(tables):
        table_path = article_dir / f"table_{idx:02d}.csv"
        dataframe.to_csv(table_path, index=False)
        meta_path = article_dir / f"table_{idx:02d}_metadata.json"
        meta_path.write_text(json.dumps(asdict(metadata), indent=2), encoding="utf-8")
    return len(tables)


def main() -> None:
    args = parse_args()
    records = load_manifest(args.manifest)
    if not records:
        raise SystemExit("Manifest did not contain any identifiers with DOI/PMID.")

    if args.limit:
        rng = random.Random(args.seed)
        records = rng.sample(records, k=min(args.limit, len(records)))

    print(f"Requesting {len(records)} articles from Elsevier…")
    articles = asyncio.run(download_articles(records))
    print(f"Downloaded {len(articles)} articles.")

    total_tables = 0
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for article, record in zip(articles, records):
        count = export_tables(article, record, args.output_dir)
        print(f"{slugify_identifier(record, article.metadata or {})}: {count} tables")
        total_tables += count

    print(f"Done. Extracted {total_tables} tables into {args.output_dir}")


if __name__ == "__main__":
    main()
