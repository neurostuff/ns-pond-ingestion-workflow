"""Build the labelling units: one per table or passage, holding its coordinate sets.

Run on beast, where the data lives:

    PYTHONPATH=<this checkout> python experiments/role_classifier/build_sets.py OUT_DIR \
        [--wild-passages 3000]

Writes OUT_DIR/table_units.jsonl and OUT_DIR/prose_units.jsonl
(`services.set_roles.labeling` reads them):

- Table units are nu-v21's real training rows (curated, real-positive,
  hand-judged; /home/james/train-data/train_v21.jsonl), one per table with
  analyses. Each target analysis is a set, and the row itself is kept as
  `base_row`, so `export.nu_v21_rows` returns it in its own format with roles.
  The article's sentences citing the table are read from its extracted text.
- Prose units are the prose dataset's rows (jk-prose-coords v3), one per
  passage, sets in order of first appearance, the row kept as `base_row`.
  Synthetic rows carry their generator's roles (`labels_from: dataset`) and
  are not sent to a labeller. Optionally, passages the prose stage read from
  the corpus (`--wild-passages`), for the encoder only (no `base_row`).

Each unit gets a `stratum` from its cues, which the labelling job takes in
turn so rare roles are labelled early.
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import sqlite3
from pathlib import Path

from ingestion_workflow.prompts.prose_coordinates import study_schema_role
from ingestion_workflow.services.set_roles.common import (
    _ANCHOR_CUES,
    _CITATION,
    _DISPLAY_CUES,
    _PRIOR_CUES,
    citing_sentences,
)

TRAIN_V21 = Path("/home/james/train-data/train_v21.jsonl")
REAL_TABLE_ORIGINS = {"curated", "real-positive", "hand-judged"}
PROSE_V3 = Path("/data/alejandro/jk-prose-coords/datasets/v3-20261006")
NS_POND = Path("/data/alejandro/projects/ns-pond")
CATALOG = NS_POND / "catalog"


#: Non-result proposals, rarest first, as `role_key` names them.
RARE_FIRST = ("display", "stimulation_target", "reference", "seed", "roi", "not_coordinates")


def study_schema_points(points):
    """Points with the prose model's own role (the prose dataset's too) as study_schema's
    role fields, through the prose stage's legacy adapter."""
    return [
        {**{k: v for k, v in p.items() if k != "role"}, **study_schema_role(p.get("role"))}
        if isinstance(p, dict)
        else p
        for p in points
    ]


def with_role(name, points):
    """A set of already converted points, with its first point's role fields."""
    first = next((p for p in points if isinstance(p, dict)), {})
    return {
        "name": name,
        "points": points,
        **{k: first.get(k) for k in ("role", "anchor_kind", "from_prior_study")},
    }


def role_key(s) -> str:
    """A set's proposal for stratifying: an anchor by its kind."""
    return s.get("anchor_kind") or s.get("role") or "not_coordinates"


def stratum(text: str, proposed=(), strong: str = "") -> str:
    """The rarest thing a unit points at: a non-result proposal, then cues in `strong` (a
    table's caption and footer), then cues anywhere in `text`."""
    for key in RARE_FIRST:
        if key in proposed:
            return f"proposed:{key}"
    for where, body in (("caption", strong), ("text", text)):
        if _DISPLAY_CUES.search(body):
            return f"{where}:display"
        if _PRIOR_CUES.search(body):
            return f"{where}:prior"
        if _ANCHOR_CUES.search(body):
            return f"{where}:anchor"
    return "citation" if _CITATION.search(text) else "plain"


class Corpus:
    """A training row's article in the catalog: its extracted text and its tables' labels."""

    def __init__(self):
        from ingestion_workflow.catalog.blobs import BlobStore  # noqa: PLC0415

        self.conn = sqlite3.connect(f"file:{CATALOG / 'catalog.sqlite'}?mode=ro", uri=True)
        self.blobs = BlobStore(CATALOG / "blobs")

    def _article(self, slug: str):
        if self.conn.execute("select 1 from articles where id=?", (slug,)).fetchone():
            return slug
        pmid = slug.split("-", 1)[0]
        row = (
            self.conn.execute(
                "select article_id from aliases where kind='pmid' and value=?", (pmid,)
            ).fetchone()
            if pmid.isdigit()
            else None
        )
        return row[0] if row else None

    def lookup(self, slug: str, source: str, table_id: str):
        """(article text, table label), either None where it cannot be found."""
        article = self._article(slug)
        if article is None:
            return None, None
        rows = self.conn.execute(
            "select source, blob from artifacts where stage='extract' and status='ok' "
            "and article_id=?",
            (article,),
        ).fetchall()
        rows.sort(key=lambda r: r[0] != source)
        for _, blob in rows:
            payload = self.blobs.get(blob) or {}
            table = next(
                (t for t in payload.get("tables") or [] if t.get("table_id") == table_id), None
            )
            if table is None and len(rows) > 1:
                continue
            path = payload.get("full_text_path")
            text = (
                Path(path).read_text(encoding="utf-8", errors="replace")
                if path and Path(path).is_file()
                else None
            )
            meta = (table or {}).get("metadata") or {}
            label = meta.get("table_label") or (
                f"Table {table['table_number']}" if table and table.get("table_number") else None
            )
            return text, label
        return None, None


def table_units():
    corpus = Corpus()
    seen = set()
    found = collections.Counter()
    with open(TRAIN_V21, encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            key = (row.get("article_id"), row.get("table_id"))
            if row.get("origin") not in REAL_TABLE_ORIGINS or not all(key) or key in seen:
                continue
            analyses = json.loads(row["target_json"]).get("analyses") or []
            if not analyses:
                continue
            seen.add(key)
            text, label = corpus.lookup(
                row["article_id"], row.get("source") or "", row["table_id"]
            )
            caption = row.get("caption") or ""
            if not label and caption.lower().startswith(("table", "supplementary table")):
                label = caption.split(".")[0]
            label = label or ""
            citing = citing_sentences(text, label) if text and label else []
            found["with_text"] += text is not None
            found["with_citing"] += bool(citing)
            found["units"] += 1
            cue = " ".join(
                [
                    row.get("caption") or "",
                    row.get("footer") or "",
                    *citing,
                    *(a.get("name") or "" for a in analyses),
                ]
            )
            yield {
                "unit_id": f"t:{row['article_id']}:{row['table_id']}",
                "origin": "table",
                "article_id": row["article_id"],
                "source": row.get("source"),
                "table_id": row["table_id"],
                "table_label": label,
                "title": row.get("title"),
                "abstract": row.get("abstract"),
                "caption": row.get("caption"),
                "footer": row.get("footer"),
                "table_serialised": row.get("table_serialised"),
                "citing": citing,
                "sets": [
                    {"name": a.get("name"), "points": a.get("points") or []} for a in analyses
                ],
                "base_row": row,
                "stratum": stratum(
                    cue, strong=f"{row.get('caption') or ''} {row.get('footer') or ''}"
                ),
            }
    print("table units", dict(found))


def _sets_from_points(points):
    groups = collections.OrderedDict()
    for p in points:
        groups.setdefault(p.get("analysis"), []).append(p)
    return [with_role(name, pts) for name, pts in groups.items()]


def prose_units(wild: int):
    found = collections.Counter()
    for split in ("train", "val", "test"):
        wide = {}
        path = PROSE_V3 / f"wide-context-{split}.jsonl"
        if path.exists():
            wide = {r["id"]: r for r in map(json.loads, open(path, encoding="utf-8"))}
        for line in open(PROSE_V3 / f"{split}.jsonl", encoding="utf-8"):
            row = json.loads(line)
            if not row.get("points"):
                continue
            w = wide.get(row["id"]) or {}
            synthetic = row.get("dataset", "").startswith("synthetic")
            row = {**row, "points": study_schema_points(row["points"])}
            sets = _sets_from_points(row["points"])
            unit = {
                "unit_id": f"p:{row['id']}",
                "origin": "text",
                "article_id": row.get("article_id") or row["id"],
                "dataset": row.get("dataset"),
                "dataset_split": split,
                "heading": row.get("heading") or w.get("heading"),
                "text": row.get("text"),
                "before": row.get("before") or w.get("before"),
                "after": row.get("after") or w.get("after"),
                "sets": sets,
                "base_row": row,
                "stratum": stratum(row.get("text") or "", {role_key(s) for s in sets}),
            }
            if synthetic:
                unit["labels_from"] = "dataset"
            found[(split, "synthetic" if synthetic else "real")] += 1
            yield unit
    if wild:
        yield from wild_passages(wild, found)
    print("prose units", dict(found))


def wild_passages(n: int, found):
    """Passages the prose stage read from the corpus, sampled evenly over their proposed roles."""
    from ingestion_workflow.catalog.blobs import BlobStore  # noqa: PLC0415

    conn = sqlite3.connect(f"file:{CATALOG / 'catalog.sqlite'}?mode=ro", uri=True)
    blobs = BlobStore(CATALOG / "blobs")
    rng = random.Random(20261009)
    rows = conn.execute(
        "select article_id, blob from artifacts where stage='prose' and status='ok'"
        " and blob is not null"
    ).fetchall()
    rng.shuffle(rows)
    by_role = collections.defaultdict(list)
    want = max(1, n // 6)
    for article_id, blob in rows[: n * 6]:
        payload = blobs.get(blob) or {}
        passages = conn.execute(
            "select blob from artifacts where stage='passages' and status='ok' and article_id=?",
            (article_id,),
        ).fetchone()
        context = (blobs.get(passages[0]) or {}).get("passages", []) if passages else []
        for i, passage in enumerate(payload.get("passages") or []):
            sets = [
                with_role(a.get("name"), study_schema_points(a["points"]))
                for a in passage.get("analyses") or []
                if a.get("points")
            ]
            if not sets:
                continue
            roles = {role_key(s) for s in sets}
            key = next(
                (r for r in RARE_FIRST if r in roles),
                "result",
            )
            if len(by_role[key]) >= want:
                continue
            ctx = context[i] if i < len(context) else {}
            by_role[key].append(
                {
                    "unit_id": f"w:{article_id}:{i}",
                    "origin": "text",
                    "article_id": article_id,
                    "dataset": "wild",
                    "heading": passage.get("heading") or ctx.get("heading"),
                    "text": passage.get("text"),
                    "before": ctx.get("before"),
                    "after": ctx.get("after"),
                    "sets": sets,
                    "base_row": None,
                    "stratum": stratum(passage.get("text") or "", roles),
                }
            )
        if sum(map(len, by_role.values())) >= n:
            break
    for key, units in sorted(by_role.items()):
        found[("wild", key)] = len(units)
        yield from units


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--wild-passages", type=int, default=0)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for name, units in (
        ("table_units.jsonl", table_units()),
        ("prose_units.jsonl", prose_units(args.wild_passages)),
    ):
        with open(args.out / name, "w", encoding="utf-8") as handle:
            for unit in units:
                handle.write(json.dumps(unit, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
