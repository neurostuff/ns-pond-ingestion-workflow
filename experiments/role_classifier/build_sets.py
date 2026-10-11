"""Build the labelling units: one per table or passage, holding its coordinate sets.

Run on beast, where the data lives:

    PYTHONPATH=<this checkout> python experiments/role_classifier/build_sets.py OUT_DIR \
        [--wild-passages 3000]

Writes OUT_DIR/table_units.jsonl and OUT_DIR/prose_units.jsonl
(`services.set_roles.labeling` reads them), and OUT_DIR/build_report.json: the articles
left out because their text or spans could not be read, each with its reason, and counts
that add up. A run asked for wild passages that builds none exits non-zero.


- Table units are nu-v21's real training rows (curated, real-positive,
  hand-judged; /home/james/train-data/train_v21.jsonl), one per table with
  analyses. Each target analysis is a set, and the row itself is kept as
  `base_row`.
  The article's sentences citing the table are read from its extracted text.
- Prose units are the prose dataset's rows (jk-prose-coords v3), one per
  passage, sets in order of first appearance, the row kept as `base_row`.
  Synthetic rows carry their generator's roles (`labels_from: dataset`) and
  are not sent to a labeller. Optionally, passages the prose stage read from
  the corpus (`--wild-passages`), for the encoder only (no `base_row`); their text is
  read from the article's text by the spans the passages stage stored.

Each unit gets a `stratum` from its cues, which the labelling job takes in
turn so rare roles are labelled early.
"""

from __future__ import annotations

import argparse
import collections
import json
import random
import sqlite3
import sys
from pathlib import Path

from ingestion_workflow.pipeline.stages.passages import passage_from, read_text
from ingestion_workflow.services.set_roles.common import (
    _ANCHOR_CUES,
    _CITATION,
    _PRIOR_CUES,
    _SLICE_CUES,
    citing_sentences,
)
from study_schema.models.paper_parse import AnchorKind, CoordinateRole

TRAIN_V21 = Path("/home/james/train-data/train_v21.jsonl")
REAL_TABLE_ORIGINS = {"curated", "real-positive", "hand-judged"}
PROSE_V3 = Path("/data/alejandro/jk-prose-coords/datasets/v3-20261006")
NS_POND = Path("/data/alejandro/projects/ns-pond")
CATALOG = NS_POND / "catalog"


#: The prose datasets' own role words (the current nu-prose model's too), read as
#: study_schema's fields. They are the labels of a dataset's synthetic rows, which
#: no labeller sees, and what corpus passages are sampled by; nothing in the
#: pipeline reads them. `other` was "anything else", mostly numbers that are not
#: coordinates; `figure` names no role.
_DATASET_ROLES = {
    "result": (CoordinateRole.result.value, None, False),
    "roi": (CoordinateRole.anchor.value, AnchorKind.roi.value, False),
    "seed": (CoordinateRole.anchor.value, AnchorKind.seed.value, False),
    "target": (CoordinateRole.anchor.value, AnchorKind.stimulation_target.value, False),
    "prior_study": (CoordinateRole.reference.value, None, True),
}


def dataset_role(role):
    """A prose dataset's role word as `role`, `anchor_kind` and `from_prior_study`."""
    fields = _DATASET_ROLES.get(role or "result", (None, None, False))
    return dict(zip(("role", "anchor_kind", "from_prior_study"), fields))


#: Roles other than result, rarest first, as `role_key` names them.
RARE_FIRST = ("stimulation_target", "reference", "seed", "roi", "not_coordinates")


def study_schema_points(points):
    """Points with the prose dataset's own role word as study_schema's role fields."""
    return [
        {**{k: v for k, v in p.items() if k != "role"}, **dataset_role(p.get("role"))}
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
    """A set's role for stratifying: an anchor by its kind."""
    return s.get("anchor_kind") or s.get("role") or "not_coordinates"


def stratum(text: str, roles=(), strong: str = "") -> str:
    """The rarest thing a unit points at: a role other than result its source row gives,
    then cues in `strong` (a table's caption and footer), then cues anywhere in `text`."""
    for key in RARE_FIRST:
        if key in roles:
            return f"role:{key}"
    for where, body in (("caption", strong), ("text", text)):
        if _SLICE_CUES.search(body):
            return f"{where}:slice"
        if _PRIOR_CUES.search(body):
            return f"{where}:prior"
        if _ANCHOR_CUES.search(body):
            return f"{where}:anchor"
    return "citation" if _CITATION.search(text) else "plain"


class Corpus:
    """A training row's article in the catalog: its extracted text and its tables' labels."""

    def __init__(self):
        from ingestion_workflow.catalog.blobs import BlobStore  # noqa: PLC0415

        #: Texts read with no hash recorded to check them against.
        self.unverified = 0

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

    def _text(self, path, recorded_sha):
        """The extraction text, checked against the hash its extraction recorded."""
        from ingestion_workflow.services.offsets import sha256  # noqa: PLC0415

        file = _Recorded(CATALOG).recorded_path(path)
        try:
            text = file.read_bytes().decode("utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise SpanError(
                "text_unreadable", f"the text cannot be read: {type(exc).__name__}: {exc}"
            ) from exc
        if recorded_sha is None:
            self.unverified += 1
        elif sha256(text) != recorded_sha:
            raise SpanError("text_changed", "the text the extraction recorded has changed")
        return text

    def lookup(self, slug: str, source: str, table_id: str):
        """(article text, table label), either None where it cannot be found.

        Raises SpanError when the extraction records a text that is gone or has changed."""
        article = self._article(slug)
        if article is None:
            return None, None
        rows = self.conn.execute(
            "select source, blob, summary from artifacts where stage='extract' and status='ok' "
            "and article_id=?",
            (article,),
        ).fetchall()
        rows.sort(key=lambda r: r[0] != source)
        for _, blob, summary in rows:
            payload = self.blobs.get(blob) or {}
            table = next(
                (t for t in payload.get("tables") or [] if t.get("table_id") == table_id), None
            )
            if table is None and len(rows) > 1:
                continue
            path = payload.get("full_text_path")
            text = (
                self._text(path, (json.loads(summary or "{}")).get("text_sha256"))
                if path
                else None
            )
            meta = (table or {}).get("metadata") or {}
            label = meta.get("table_label") or (
                f"Table {table['table_number']}" if table and table.get("table_number") else None
            )
            return text, label
        return None, None


def table_units(failures=None, tally=None):
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
            try:
                text, label = corpus.lookup(
                    row["article_id"], row.get("source") or "", row["table_id"]
                )
            except SpanError as exc:
                print(f"WARNING {row['article_id']} {row['table_id']}: {exc}", file=sys.stderr)
                if failures is not None:
                    failures.append(
                        {
                            "stage": "table",
                            "article_id": row["article_id"],
                            "table_id": row["table_id"],
                            "kind": exc.kind,
                            "reason": str(exc),
                        }
                    )
                continue
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
    if tally is not None:
        tally["text_unverified"] = corpus.unverified
    print("table units", dict(found))


def _sets_from_points(points):
    groups = collections.OrderedDict()
    for p in points:
        groups.setdefault(p.get("analysis"), []).append(p)
    return [with_role(name, pts) for name, pts in groups.items()]


def prose_units(wild: int, failures=None, tally=None):
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
        yield from wild_passages(wild, found, failures, tally)
    print("prose units", dict(found))


class _Recorded:
    """What `read_text` asks of a context: a recorded path made absolute against the catalog."""

    def __init__(self, root: Path):
        self.root = root

    def recorded_path(self, path):
        if not path:
            return None
        recorded = Path(path)
        return recorded if recorded.is_absolute() else self.root.resolve().parent / recorded


class SpanError(Exception):
    """Text that cannot be read back from the spans or hash that index it.

    `kind` names the reason, for the build report to count."""

    def __init__(self, kind: str, reason: str):
        super().__init__(reason)
        self.kind = kind


def read_passages(payload: dict, read: dict, root: Path):
    """The article's passages as `passage_from` reads them from its text.

    The text is the one the passages payload indexes, so a passage's words and its
    offsets come from the same place. Raises SpanError, with the reason, when the text
    is gone or has changed, or when a passage the prose stage read no longer sits
    where the passages payload says."""
    stored = payload.get("passages") or []
    ours = read.get("passages") or []
    if not stored and not ours:
        return []
    if not payload.get("text_sha256") or any("span" not in p for p in stored):
        raise SpanError(
            "passages_predate_spans",
            "the passages payload predates spans (it holds passage text, no spans or text hash);"
            " the passages stage must be re-run on this article",
        )
    if any("span" not in p for p in ours):
        raise SpanError(
            "prose_predates_spans",
            "the prose payload predates spans (its passages carry no span);"
            " the prose stage must be re-run on this article",
        )
    try:
        text = read_text(_Recorded(root), payload)
    except LookupError as exc:
        raise SpanError("text_changed", str(exc)) from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise SpanError(
            "text_unreadable", f"the text cannot be read: {type(exc).__name__}: {exc}"
        ) from exc
    if len(ours) != len(stored):
        raise SpanError(
            "passage_count",
            f"prose read {len(ours)} passages, passages indexes {len(stored)}",
        )
    out = []
    for i, (mine, theirs) in enumerate(zip(ours, stored)):
        if mine.get("span") != theirs.get("span"):
            raise SpanError(
                "span_mismatch",
                f"passage {i}: prose span {mine.get('span')} != indexed {theirs.get('span')}",
            )
        try:
            passage = passage_from(theirs, text)
        except Exception as exc:  # noqa: BLE001 - a span outside the text is that article's problem
            raise SpanError(
                "span_outside_text", f"passage {i}: {type(exc).__name__}: {exc}"
            ) from exc
        if not passage.text.strip():
            raise SpanError("span_empty", f"passage {i}: span {theirs.get('span')} holds no text")
        out.append(passage)
    return out


def wild_passages(n: int, found, failures=None, tally=None):
    """Passages the prose stage read from the corpus, sampled evenly over its model's roles.

    Their text, context and heading are read from the article's text by the spans the
    passages stage stored. An article whose spans cannot be read is left out and recorded
    in `failures` with the reason; `tally` counts articles scanned, read and used."""
    tally = tally if tally is not None else collections.Counter()
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
        tally["scanned"] += 1
        payload = blobs.get(blob) or {}
        stored = conn.execute(
            "select blob from artifacts where stage='passages' and status='ok' and article_id=?",
            (article_id,),
        ).fetchone()
        try:
            if not stored:
                raise SpanError("no_passages_artifact", "no passages artifact")
            context = read_passages(blobs.get(stored[0]) or {}, payload, CATALOG)
        except SpanError as exc:
            print(f"WARNING {article_id}: {exc}", file=sys.stderr)
            if failures is not None:
                failures.append(
                    {
                        "stage": "wild",
                        "article_id": article_id,
                        "kind": exc.kind,
                        "reason": str(exc),
                    }
                )
            continue
        tally["read"] += 1
        built = sum(map(len, by_role.values()))
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
            ctx = context[i]
            by_role[key].append(
                {
                    "unit_id": f"w:{article_id}:{i}",
                    "origin": "text",
                    "article_id": article_id,
                    "dataset": "wild",
                    "heading": ctx.heading,
                    "text": ctx.text,
                    "before": ctx.before,
                    "after": ctx.after,
                    "sets": sets,
                    "base_row": None,
                    "stratum": stratum(ctx.text, roles),
                }
            )
        total = sum(map(len, by_role.values()))
        tally["used"] += total > built
        if total >= n:
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
    failures, tally, counts = [], collections.Counter(), {}
    for name, units in (
        ("table_units.jsonl", table_units(failures, tally)),
        ("prose_units.jsonl", prose_units(args.wild_passages, failures, tally)),
    ):
        n = 0
        with open(args.out / name, "w", encoding="utf-8") as handle:
            for unit in units:
                handle.write(json.dumps(unit, ensure_ascii=False) + "\n")
                n += 1
        counts[name] = n
    wild = [f for f in failures if f["stage"] == "wild"]
    tables = [f for f in failures if f["stage"] == "table"]
    report = {
        "units": counts,
        # scanned = left_out + read; used is the read articles that gave at least one unit
        "wild_passages": {
            "scanned": tally["scanned"],
            "left_out": len(wild),
            "read": tally["read"],
            "used": tally["used"],
            "left_out_by_reason": dict(collections.Counter(f["kind"] for f in wild)),
        },
        "table_units": {
            "built": counts["table_units.jsonl"],
            "left_out": len(tables),
            "left_out_by_reason": dict(collections.Counter(f["kind"] for f in tables)),
            "text_read_without_hash": tally["text_unverified"],
        },
        "left_out": failures,
    }
    (args.out / "build_report.json").write_text(
        json.dumps(report, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(
        f"wild: {tally['scanned']} scanned, {len(wild)} left out, {tally['used']} used;"
        f" tables: {len(tables)} left out (build_report.json)"
    )
    if args.wild_passages and not tally["used"]:
        sys.exit(
            f"--wild-passages built no unit: of {tally['scanned']} articles sampled,"
            f" {len(wild)} were left out and {tally['read']} held no usable passage;"
            " see build_report.json for the reasons"
        )


if __name__ == "__main__":
    main()
