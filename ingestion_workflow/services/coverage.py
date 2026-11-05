"""What has run: articles by stage, source, run, model and status, and what changed since
upload. `ingest report coverage` prints it; it and every offload import write it as JSON
for the "Pipeline coverage" dashboard page.

Read-only, from a catalog of schema v4 (no runs) or later.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .funnel_report import open_readonly

#: The stages a change of makes an uploaded study out of date, in pipeline order.
UPLOADED_FROM = ("sync", "record", "evidence", "normalize")
#: Where the JSON is written, under the catalog's directory, unless named.
COVERAGE_JSON = "coverage.json"

_COUNTS = """
SELECT a.stage, a.source, {run} AS run_id, {model} AS model, a.status, COUNT(*) AS articles
FROM artifacts a JOIN articles ar ON ar.id = a.article_id AND ar.merged_into IS NULL
{join}
GROUP BY 1, 2, 3, 4, 5
"""

#: The uploaded articles' artifacts that changed after their upload: a different parse for
#: sync, a later OK artifact of any source for the others.
_CHANGED = """
FROM artifacts u JOIN artifacts s ON s.article_id = u.article_id
WHERE u.stage = 'upload' AND u.status = 'ok' AND s.status = 'ok' AND s.stage IN ({marks})
  AND CASE WHEN s.stage = 'sync'
           THEN json_extract(s.summary, '$.parse_id') IS NOT json_extract(u.summary, '$.parse_id')
           ELSE s.updated_at > u.updated_at END
"""


def _tables(conn: sqlite3.Connection) -> set:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def coverage(catalog_root: Path | str) -> Dict[str, Any]:
    """The coverage of the catalog at `catalog_root`, as the JSON export holds it."""
    with closing(open_readonly(catalog_root)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(artifacts)")}
        has_runs = "run_id" in columns and "runs" in _tables(conn)
        sql = _COUNTS.format(
            run="a.run_id" if has_runs else "NULL", model="r.model" if has_runs else "NULL",
            join="LEFT JOIN runs r ON r.run_id = a.run_id" if has_runs else "")
        stages = [dict(zip(("stage", "source", "run_id", "model", "status", "articles"), row))
                  for row in conn.execute(sql)]
        runs: List[Dict[str, Any]] = []
        if has_runs:
            conn.row_factory = sqlite3.Row
            for row in conn.execute("SELECT * FROM runs ORDER BY created_at, run_id"):
                run = dict(row)
                run["config"] = json.loads(run["config"] or "{}")
                run["articles"] = {s["status"]: s["articles"] for s in stages
                                   if s["run_id"] == run["run_id"]}
                runs.append(run)
            conn.row_factory = None
        marks = ",".join(f"'{stage}'" for stage in UPLOADED_FROM)
        changed = _CHANGED.format(marks=marks)
        by_stage = dict(conn.execute(
            f"SELECT s.stage, COUNT(DISTINCT s.article_id) {changed} GROUP BY s.stage"))
        changed_any = conn.execute(
            f"SELECT COUNT(DISTINCT s.article_id) {changed}").fetchone()[0]
        uploaded = conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE stage = 'upload' AND status = 'ok'").fetchone()[0]
        version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        live = conn.execute("SELECT COUNT(*) FROM articles WHERE merged_into IS NULL").fetchone()[0]
    stages.sort(key=lambda s: (_order(s["stage"]), s["source"], s["run_id"] or "", s["status"]))
    return {
        "catalog": str(Path(catalog_root).resolve()),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "schema_version": int(version[0]) if version else None,
        "articles": live,
        "stages": stages,
        "runs": runs,
        "changed_since_upload": {
            "uploaded": uploaded,
            "any": changed_any,
            "by_stage": {stage: by_stage.get(stage, 0) for stage in UPLOADED_FROM},
        },
    }


def _order(stage: str) -> int:
    from ingestion_workflow.pipeline.stages import STAGE_ORDER

    return STAGE_ORDER.index(stage) if stage in STAGE_ORDER else len(STAGE_ORDER)


def write_json(found: Dict[str, Any], path: Path | str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(found, indent=1), "utf-8")
    tmp.replace(path)
    return path


def default_json(catalog_root: Path | str) -> Path:
    return Path(catalog_root) / COVERAGE_JSON


def render(found: Dict[str, Any]) -> List[str]:
    """The report as lines: one per stage, source, run, model and status."""
    lines = [f"catalog: {found['articles']:,} articles, schema v{found['schema_version']}", "",
             f"{'stage':<13}{'source':<12}{'run':<28}{'model':<34}{'status':<11}{'articles':>10}"]
    for s in found["stages"]:
        lines.append(f"{s['stage']:<13}{s['source'] or '-':<12}{s['run_id'] or '-':<28}"
                     f"{(s['model'] or '-')[:33]:<34}{s['status']:<11}{s['articles']:>10,}")
    changed = found["changed_since_upload"]
    lines += ["", f"changed since upload: {changed['any']:,} of {changed['uploaded']:,} "
                  "uploaded studies, by stage: "
              + ", ".join(f"{stage} {n:,}" for stage, n in changed["by_stage"].items())]
    return lines


__all__ = ["COVERAGE_JSON", "UPLOADED_FROM", "coverage", "default_json", "render", "write_json"]
