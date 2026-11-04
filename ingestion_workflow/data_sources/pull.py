"""Pull catalogues into the catalog and rebuild the merged dictionary from every record held."""

from __future__ import annotations

from dataclasses import asdict
from typing import Callable, Dict, Iterable, Mapping, Sequence, Tuple

from .merge import PROVIDERS, Record, digest, merge


def pull(catalog, pullers: Mapping[str, Callable[[], Iterable[Record]]],
         providers: Sequence[str]) -> Tuple[Dict[str, int], int]:
    """Pull `providers`, keep the records the catalog holds of the others, and store the
    dictionary merged from all of them. The curated TSV is read on every pull: it is the
    repo's, and a dictionary built from an older copy of it would not be the reviewed one.
    ({provider: records} of what is held after, how many records are candidates)."""
    asked = list(dict.fromkeys(["curated", *providers]))
    unknown = [p for p in asked if p not in pullers]
    if unknown:
        raise ValueError(f"cannot pull {', '.join(unknown)}: known are {', '.join(pullers)}")
    fetched = {name: [r.to_dict() for r in pullers[name]()] for name in asked}
    held = {name: catalog.data_source_records(name) for name in PROVIDERS if name not in fetched}
    records = [Record.from_dict(r) for rows in [*fetched.values(), *held.values()] for r in rows]
    sources, aliases, candidates = merge(records)
    catalog.replace_data_sources(fetched, [asdict(s) for s in sources], aliases, digest(aliases))
    counts = {name: len(rows) for name, rows in {**fetched, **held}.items() if rows}
    return counts, len(candidates)
