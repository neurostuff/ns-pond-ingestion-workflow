"""The `modality` stage's readings as one TSV row per Neurostore base study, for the command
that loads `base_studies.modalities`.

Several articles can name one base study. A base study takes the readings of its best
source: a record over the methods text, the methods text over the full text. Readings of
one source are joined.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

COLUMNS = ("base_study_id", "modalities", "source", "papers")
#: Best first. Neurostore's loader (neurostore#1962) refuses a TSV with any other source.
SOURCES = ("record", "methods", "full_text", "none")
CHUNK = 900


def rows(catalog) -> List[Dict[str, str]]:
    best: Dict[str, Dict] = {}
    ids = list(catalog.all_article_ids())
    for start in range(0, len(ids), CHUNK):
        found = catalog.artifacts(ids[start:start + CHUNK], "modality")
        for article_id, by_source in found.items():
            artifact = by_source.get("")
            if artifact is None or not artifact.ok:
                continue
            summary = artifact.summary or {}
            base, source = summary.get("base_study_id"), summary.get("source", "none")
            if not base:
                continue
            held = best.get(base)
            rank = SOURCES.index(source)
            if held is None or rank < held["rank"]:
                best[base] = {"rank": rank, "source": source, "modalities": set(), "papers": set()}
                held = best[base]
            if rank == held["rank"]:
                held["modalities"].update(summary.get("modalities") or [])
                held["papers"].add(article_id)
    return [{"base_study_id": base, "modalities": "|".join(sorted(held["modalities"])),
             "source": held["source"], "papers": "|".join(sorted(held["papers"]))}
            for base, held in sorted(best.items())]


def export(catalog, out: Path) -> int:
    """Write the TSV to `out`; the number of base studies."""
    written = rows(catalog)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(written)
    return len(written)
