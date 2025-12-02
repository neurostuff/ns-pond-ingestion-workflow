#!/usr/bin/env python3
"""Generate a JSONL manifest by sampling identifiers from index.json."""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import asdict
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if sys.version_info < (3, 9):
    raise SystemExit("Python 3.9 or newer is required to run this script.")

from ingestion_workflow.models.ids import Identifier


DEFAULT_INDEX_PATH = Path("ingestion_workflow/tests/data/manifests/index_with_coordinates.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sample identifiers from the manifest index and write a JSONL "
            "file formatted for the ingestion pipeline."
        )
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        required=False,
        help="Number of identifiers to sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible sampling.",
    )
    parser.add_argument(
        "--index-path",
        type=Path,
        default=DEFAULT_INDEX_PATH,
        help=f"Path to index.json (default: {DEFAULT_INDEX_PATH}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write the manifest file (default: current directory).",
    )
    return parser.parse_args()


def load_identifiers(index_path: Path) -> List[Identifier]:
    if not index_path.exists():
        raise FileNotFoundError(f"Index file not found: {index_path}")

    payload = json.loads(index_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Index file must contain an object keyed by IDs.")

    identifiers: List[Identifier] = []
    for key, value in payload.items():
        identifier_payload = _coerce_identifier_payload(key, value)
        identifiers.append(Identifier(**identifier_payload))
    return identifiers


def _coerce_identifier_payload(
    neurostore_key: str,
    entry: object,
) -> dict:
    if isinstance(entry, dict):
        payload = dict(entry)
    elif isinstance(entry, list):
        if len(entry) < 3:
            raise ValueError(
                "List-based identifier entries must contain pmid, doi, and pmcid."
            )
        pmid, doi, pmcid = entry[:3]
        payload = {
            "pmid": pmid,
            "doi": doi,
            "pmcid": pmcid,
        }
    else:
        raise ValueError(
            "Each identifier entry must be a JSON object or list."
        )

    payload.setdefault("neurostore", neurostore_key)
    for field in ("neurostore", "pmid", "doi", "pmcid"):
        value = payload.get(field)
        if value is not None:
            payload[field] = str(value)
    return payload


def write_manifest(
    identifiers: List[Identifier],
    output_dir: Path,
    count: int,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"manifest_n{count}.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for identifier in identifiers:
            handle.write(json.dumps(asdict(identifier)))
            handle.write("\n")
    return output_path


def main() -> None:
    args = parse_args()

    identifiers = load_identifiers(args.index_path)

    if not args.num or args.num <= 0:
        args.num = len(identifiers)
    if args.num > len(identifiers):
        raise ValueError(
            f"Requested {args.num} identifiers, but only "
            f"{len(identifiers)} entries are available."
        )

    rng = random.Random(args.seed)
    sampled = rng.sample(identifiers, args.num)

    output_dir = args.output_dir or Path.cwd()
    output_path = write_manifest(sampled, output_dir, args.num)
    print(f"Wrote {args.num} identifiers to {output_path}")


if __name__ == "__main__":
    main()
