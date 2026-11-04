"""The dictionary and the matches, as the TSVs neurostore's `manage data-sources` reads:
`data_sources.tsv` for `load`, `paper_entities.tsv` for `link-matches`; and
`candidates.tsv`, the catalogue records that join no curated entry, for review."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

SOURCES_COLUMNS = ("id", "name", "roles", "modality", "hosted_by", "rrid", "re3data_id", "url",
                   "url_source")
CANDIDATE_COLUMNS = ("provider", "record_id", "name", "aliases", "kinds", "modalities", "url",
                     "rrids", "re3data_ids")
MATCH_COLUMNS = ("paper", "pmid", "doi", "year", "source", "entity", "status", "n_hits",
                 "sections", "forms")
CHUNK = 900


def _write(path: Path, columns, rows) -> int:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def source_rows(catalog) -> List[Dict[str, str]]:
    return [{"id": s["id"], "name": s["name"], "roles": "|".join(s["kinds"]),
             "modality": "|".join(s["modalities"]), "hosted_by": s["part_of"],
             "rrid": "|".join(s["rrids"]), "re3data_id": "|".join(s["re3data_ids"]),
             "url": s["url"], "url_source": s["url_source"]}
            for s in catalog.data_sources()]


def candidate_rows(catalog) -> List[Dict[str, str]]:
    from .merge import Record, merge

    candidates = merge([Record.from_dict(r) for r in catalog.data_source_records()])[2]
    return [{"provider": r.provider, "record_id": r.record_id, "name": r.name,
             "aliases": "|".join(f"{alias}:{tier}" for alias, tier in r.aliases),
             "kinds": "|".join(r.kinds), "modalities": "|".join(r.modalities), "url": r.url,
             "rrids": "|".join(r.rrids), "re3data_ids": "|".join(r.re3data_ids)}
            for r in candidates]


def match_rows(ctx) -> List[Dict[str, str]]:
    """A row per data source each paper names, for the papers sync wrote and did not retract,
    read from the datasources artifact of the extraction triage judges."""
    from ingestion_workflow.pipeline.stages.datasources import synced
    from ingestion_workflow.pipeline.stages.triage import judged_extraction

    catalog = ctx.catalog
    rows: List[Dict[str, str]] = []
    ids = list(catalog.all_article_ids())
    for start in range(0, len(ids), CHUNK):
        chunk = ids[start:start + CHUNK]
        found = catalog.artifacts(chunk, "datasources")
        if not found:
            continue
        held = list(found)
        syncs = catalog.artifacts(held, "sync")
        extracted = catalog.artifacts(held, "extract")
        downloads = catalog.artifacts(held, "download")
        metadata = catalog.artifacts(held, "metadata")
        identifiers = catalog.identifiers(held)
        for article_id in held:
            if not synced(syncs.get(article_id, {}).get("")):
                continue
            judged = judged_extraction(ctx, extracted.get(article_id, {}),
                                       downloads.get(article_id, {}))
            artifact = found[article_id].get(judged.source) if judged is not None else None
            if artifact is None or not artifact.ok:
                continue
            identifier = identifiers[article_id]
            meta = metadata.get(article_id, {}).get("")
            year = (meta.summary or {}).get("publication_year") if meta is not None else None
            for match in (ctx.payload(artifact) or {}).get("matches", []):
                rows.append({
                    "paper": article_id, "pmid": identifier.pmid or "",
                    "doi": identifier.doi or "", "year": year or "", "source": artifact.source,
                    "entity": match["data_source_id"], "status": match["status"],
                    "n_hits": match["n_hits"],
                    "sections": ";".join(f"{s}:{n}" for s, n in match["sections"].items()),
                    "forms": ";".join(match["forms"]),
                })
    return rows


def export(ctx, out: Path) -> Dict[str, int]:
    """Write the three TSVs into `out`; {file: rows}."""
    out.mkdir(parents=True, exist_ok=True)
    return {
        "data_sources.tsv": _write(out / "data_sources.tsv", SOURCES_COLUMNS,
                                   source_rows(ctx.catalog)),
        "paper_entities.tsv": _write(out / "paper_entities.tsv", MATCH_COLUMNS, match_rows(ctx)),
        "candidates.tsv": _write(out / "candidates.tsv", CANDIDATE_COLUMNS,
                                 candidate_rows(ctx.catalog)),
    }
