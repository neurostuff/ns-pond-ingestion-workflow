"""Records from each catalogue, merged into one dictionary of data sources and aliases.

The curated TSV is reviewed by hand, and it alone makes the dictionary: matched unreviewed,
the catalogues' entries are mostly software, atlases and institutions (PR-190-Data-Sources).
A record from another catalogue joins the curated entry sharing its RRID, its re3data id or
one of its names; one that joins none is a candidate for the TSV.
"""

from __future__ import annotations

import csv
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import fingerprint

from .matching import TIERS, key

CURATED = Path(__file__).with_name("curated.tsv")
#: The curated TSV's alias column for each tier.
TIER_COLUMNS = {
    "name": "full_names",
    "safe": "acronyms_safe",
    "context": "acronyms_context",
    "defined": "acronyms_defined",
    "qualified": "qualified_names",
}
#: The providers, in the order their records are read.
PROVIDERS = ("curated", "re3data", "scicrunch", "jpnd")
#: The tiers of an entry's aliases a record's name joins it by: a full name, or an acronym
#: that names nothing else. A record joins by its own full names only, never its acronyms.
JOINED_BY = ("name", "safe")
#: A key this short joins nothing: two-letter names are not one data source's.
MIN_JOIN_KEY = 3


@dataclass
class Record:
    """What one catalogue says of one data source."""

    provider: str
    record_id: str
    name: str
    #: (alias, tier)
    aliases: List[Tuple[str, str]] = field(default_factory=list)
    kinds: List[str] = field(default_factory=list)
    modalities: List[str] = field(default_factory=list)
    url: str = ""
    #: Where the url came from: the curated TSV's checked kind, else the provider.
    url_source: str = ""
    rrids: List[str] = field(default_factory=list)
    re3data_ids: List[str] = field(default_factory=list)
    #: The curated id of the data source hosting this one.
    part_of: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Record":
        return cls(**{**data, "aliases": [tuple(a) for a in data.get("aliases") or []]})


@dataclass
class DataSource:
    id: str
    name: str
    kinds: List[str]
    modalities: List[str]
    url: str
    url_source: str
    rrids: List[str]
    re3data_ids: List[str]
    part_of: str
    #: The providers whose records it was merged from.
    providers: List[str]


def split(cell: Optional[str]) -> List[str]:
    return [x.strip() for x in (cell or "").split("|") if x.strip()]


def scraped_aliases(names: Iterable[str], acronyms: Iterable[str]) -> List[Tuple[str, str]]:
    """The tiers proposed for a record's aliases, which nobody reviewed: a name of two words or
    more is a `name`; a one-word name is `qualified`; an acronym is `defined`. A record joins a
    curated entry by its `name` aliases, and a candidate is shown with all of them."""
    out = []
    for name in names:
        name = re.sub(r"\s+", " ", name).strip()
        if len(key(name)) >= MIN_JOIN_KEY:
            out.append((name, "name" if " " in name else "qualified"))
    for acronym in acronyms:
        acronym = acronym.strip()
        if len(key(acronym)) >= 2 and " " not in acronym:
            out.append((acronym, "defined"))
    return list(dict.fromkeys(out))


def curated_records(path: Path = CURATED) -> List[Record]:
    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return [
        Record(
            provider="curated",
            record_id=row["id"],
            name=row["name"],
            aliases=[(alias, tier) for tier, column in TIER_COLUMNS.items()
                     for alias in split(row[column])],
            kinds=split(row["roles"]),
            modalities=split(row["modality"]),
            url=row["url"],
            url_source=row["url_source"],
            rrids=split(row["rrid"]),
            re3data_ids=split(row["re3data_id"]),
            part_of=row["hosted_by"],
        )
        for row in rows
    ]


def merge(records: Sequence[Record]) -> Tuple[List[DataSource], List[Tuple[str, str, str]],
                                             List[Record]]:
    """(data sources, aliases as (data source id, alias, tier), candidates), each sorted.

    Only the curated TSV makes entries and aliases: a record from another catalogue that
    joins a curated entry adds its ids and provenance, and one that joins none is a
    candidate for a person to add to the TSV."""
    order = {name: i for i, name in enumerate(PROVIDERS)}
    records = sorted(records, key=lambda r: (order.get(r.provider, len(order)), r.record_id))
    sources: Dict[str, DataSource] = {}
    aliases: Dict[str, Dict[Tuple[str, str], None]] = {}
    by_id: Dict[Tuple[str, str], str] = {}
    candidates: List[Record] = []

    for record in (r for r in records if r.provider == "curated"):
        if record.record_id in sources:
            raise ValueError(f"two data sources are given the id {record.record_id!r}")
        sources[record.record_id] = DataSource(
            id=record.record_id, name=record.name, kinds=sorted(set(record.kinds)),
            modalities=sorted(set(record.modalities)), url=record.url,
            url_source=record.url_source, rrids=sorted(set(record.rrids)),
            re3data_ids=sorted(set(record.re3data_ids)), part_of=record.part_of,
            providers=["curated"])
        aliases[record.record_id] = dict.fromkeys(record.aliases)
        for rrid in record.rrids:
            by_id.setdefault(("rrid", rrid), record.record_id)
        for r3d in record.re3data_ids:
            by_id.setdefault(("re3data", r3d), record.record_id)
        for name in [record.name, *(a for a, tier in record.aliases if tier in JOINED_BY)]:
            if len(key(name)) >= MIN_JOIN_KEY:
                by_id.setdefault(("key", key(name)), record.record_id)

    for record in (r for r in records if r.provider != "curated"):
        joined = next(
            (by_id[k] for k in [*(("rrid", r) for r in record.rrids),
                                *(("re3data", r) for r in record.re3data_ids),
                                ("key", key(record.name)),
                                *(("key", key(a)) for a, tier in record.aliases
                                  if tier == "name")]
             if k in by_id),
            None,
        )
        if joined is None:
            candidates.append(record)
            continue
        source = sources[joined]
        source.providers = sorted({*source.providers, record.provider})
        source.rrids = sorted({*source.rrids, *record.rrids})
        source.re3data_ids = sorted({*source.re3data_ids, *record.re3data_ids})

    unknown = {s.part_of for s in sources.values() if s.part_of} - set(sources)
    if unknown:
        raise ValueError(f"part_of names no data source: {sorted(unknown)}")
    rows = sorted((source_id, alias, tier) for source_id, held in aliases.items()
                  for alias, tier in held)
    return [sources[k] for k in sorted(sources)], rows, candidates


def digest(aliases: Sequence[Tuple[str, str, str]]) -> str:
    """What a match depends on of the dictionary: every alias and its tier."""
    return fingerprint("data source aliases", TIERS, tuple(sorted(aliases)))
