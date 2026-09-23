"""The ns-pond record as a declared layout, readable as well as writable.

The shapes are a contract with pondie, so the bytes are pinned here rather than
left to a library default: a trailing newline or an escaped non-ASCII character
makes a different file. pyarty 0.1 picks a codec from the field's annotation
and `at()` takes no override, so every field is declared `File[bytes]` and the
encoding lives in this module where it can be held to the existing tree.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from pyarty import Dir, File, at, bundle, read_bundle, write_bundle

# -- encodings, matched to what the tree already holds ------------------------


def encode_pretty_json(value: Any) -> bytes:
    """`identifiers.json`, `processed/<source>/metadata.json`."""
    return json.dumps(value, indent=2).encode("utf-8")


def encode_stage1_json(value: Any) -> bytes:
    """`stage1/analyses.json`, which pondie reads."""
    return (json.dumps(value, indent=1, ensure_ascii=False) + "\n").encode("utf-8")


def encode_jsonl(rows: Sequence[Dict[str, Any]]) -> bytes:
    """`tables.jsonl`, `analyses.jsonl`. One object per line, always newline
    terminated -- including the empty case, which writes an empty file."""
    if not rows:
        return b""
    return ("\n".join(json.dumps(row) for row in rows) + "\n").encode("utf-8")


def encode_csv(rows: Sequence[Dict[str, Any]], headers: Sequence[str]) -> bytes:
    """`coordinates.csv`. Headers are passed in because they vary by extractor:
    the file carries whichever columns the source produced."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(headers))
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in headers})
    return buffer.getvalue().encode("utf-8")


def decode_json(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


def decode_jsonl(raw: bytes) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]


def decode_csv(raw: bytes) -> List[Dict[str, Any]]:
    if not raw.strip():
        return []
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8"), newline="")))


# -- the layout ---------------------------------------------------------------


@bundle
class ProcessedSource:
    """`processed/<source>/` -- what extraction and parsing produced."""

    source: str
    metadata: File[bytes] = at("metadata.json")
    tables: File[bytes] = at("tables.jsonl")
    analyses: File[bytes] = at("analyses.jsonl")
    coordinates: File[bytes] = at("coordinates.csv")
    text: Optional[File[bytes]] = at("text.txt")


@bundle
class Stage1:
    """`stage1/` -- the coordinate parse in the shape pondie reads."""

    analyses: File[bytes] = at("analyses.json")


@bundle
class NsPondRecord:
    """One article under `ns-pond/<base_study_id>/`.

    `source/<source>/` is deliberately outside the bundle: it holds whatever
    files the extractor downloaded, under names nothing declares, so it is
    copied rather than described.
    """

    identifiers: File[bytes] = at("identifiers.json")
    processed: Dir[list[ProcessedSource]] = at("processed/{source}")
    stage1: Dir[Stage1] = at("stage1")


# -- the typed view -----------------------------------------------------------


@dataclass
class ProcessedView:
    """One source's processed outputs, decoded."""

    source: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    analyses: List[Dict[str, Any]] = field(default_factory=list)
    coordinates: List[Dict[str, Any]] = field(default_factory=list)
    text: Optional[str] = None


@dataclass
class ArticleView:
    """An ns-pond record, decoded. What `read_record` returns."""

    base_study_id: str
    identifiers: Dict[str, Any] = field(default_factory=dict)
    processed: Dict[str, ProcessedView] = field(default_factory=dict)
    stage1: Dict[str, Any] = field(default_factory=dict)


def read_record(root: Path | str, base_study_id: str) -> ArticleView:
    """Read `root/<base_study_id>/` back into typed objects.

    There was no reader before this; the tree was write-only.
    """
    record = read_bundle(NsPondRecord, Path(root) / base_study_id)
    return ArticleView(
        base_study_id=base_study_id,
        identifiers=decode_json(record.identifiers),
        processed={
            item.source: ProcessedView(
                source=item.source,
                metadata=decode_json(item.metadata) if item.metadata else {},
                tables=decode_jsonl(item.tables),
                analyses=decode_jsonl(item.analyses),
                coordinates=decode_csv(item.coordinates),
                text=item.text.decode("utf-8") if item.text is not None else None,
            )
            for item in record.processed
        },
        stage1=decode_json(record.stage1.analyses),
    )


def write_record(root: Path | str, record: NsPondRecord, base_study_id: str) -> Path:
    """Write an already-encoded record. Callers own the bytes."""
    target = Path(root) / base_study_id
    write_bundle(record, target, overwrite=True)
    return target


__all__ = [
    "ArticleView",
    "NsPondRecord",
    "ProcessedSource",
    "ProcessedView",
    "Stage1",
    "decode_csv",
    "decode_json",
    "decode_jsonl",
    "encode_csv",
    "encode_jsonl",
    "encode_pretty_json",
    "encode_stage1_json",
    "read_record",
    "write_record",
]
