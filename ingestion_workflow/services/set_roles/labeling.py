"""Labelling coordinate sets with a language model, slowly and resumably.

A unit is what one call labels: a table with all its sets, or a passage with
all of its. Table and prose units are read from different evidence, so each
origin has its own instructions and prompt; the answer is the same strict
schema (`label_schema`). Units come from `experiments/role_classifier/build_sets.py`;
labels feed the encoder and both extractors' training rows (`export`).

The job writes one JSONL per batch, one row per set, and skips units already
in any batch, so it can be stopped and started again. Each call's tokens go
to `ledger.jsonl`. Units are taken stratum by stratum in turn, so the rare
cues (a citation beside a seed, a display word) are labelled early instead of
in proportion to how rarely they occur.
"""

from __future__ import annotations

import collections
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from . import prose_context, table_context
from .label_schema import LABEL_VERSION, SCHEMA, check, label_of

#: (system, prompt, schema, unit id) -> (answer, cost). `codex_caller` builds one.
Caller = Callable[[str, str, Dict[str, Any], str], Tuple[Dict[str, Any], Dict[str, Any]]]

_ROLES = """\
Labels, for each coordinate set:
- result: a finding of THIS study -- peaks of an effect it tested, whatever the statistic.
- anchor: a location the study placed or defined and then used: anchor_kind roi (a region of
  interest or sphere), seed (connectivity, PPI), stimulation_target (TMS, tDCS, DBS, focused
  ultrasound), node (a network node or parcel centre).
- localization: where electrodes, optodes or sources were placed or recorded.
- reference: coordinates quoted from other publications for comparison, not used as anchors.
- display: slice, crosshair or view positions of a figure.
- other: none of these.
from_prior_study is true when the coordinates were taken from another publication (a
meta-analysis, an earlier study, an atlas paper), whatever the role: a seed taken from a
meta-analysis is anchor/seed with from_prior_study true. An ROI built from this study's own
results, or from a localizer it ran, is not from a prior study. A reference is always from a
prior study. anchor_kind is null unless the role is anchor.
evidence: the numbers of the given sentences that show the role or the prior study (a
citation, "a 6 mm sphere centred on", "as reported by"); empty when the table or passage
alone shows it. Answer every set, by its number."""

TABLE_SYSTEM = f"""\
You label the coordinate sets read from one table of a neuroimaging article: for each set (an
analysis the extractor read from the table), what the coordinates are for. The table's caption,
footer and header usually say it; the sentences citing the table, and the table's other
analyses, settle the rest. A results table whose rows include a seed region, or a column of
peak coordinates from another study beside this study's, needs the rows themselves.

{_ROLES}"""

PROSE_SYSTEM = f"""\
You label the coordinate sets read from one passage of a neuroimaging article: for each set
(coordinates the extractor grouped under one name), what they are for. The sentences around the
coordinates, the section heading and any citation beside them say it: "peaks reported by Lee
et al. (2008)" quotes another study; "a 6 mm sphere centred on" places an ROI; "TMS was
applied at" names a stimulation target; "electrodes were located at" is a localization.

{_ROLES}"""


def _numbered(sentences: Sequence[str]) -> str:
    return "\n".join(f"[{i}] {s}" for i, s in enumerate(sentences)) or "(none)"


def _clip(text: Optional[str], limit: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def contexts(unit: Mapping[str, Any]) -> List[Any]:
    """The context of each of a unit's sets, by the builder of the unit's origin."""
    sets = unit["sets"]
    if unit["origin"] == "table":
        out = []
        for i, s in enumerate(sets):
            ctx = table_context.build(
                s,
                index=i,
                siblings=sets,
                table_text=unit.get("table_serialised"),
                caption=unit.get("caption") or "",
                footer=unit.get("footer") or "",
                table_label=unit.get("table_label") or "",
            )
            ctx.citing = list(unit.get("citing") or [])
            out.append(ctx)
        return out
    passage = {k: unit.get(k) for k in ("heading", "text", "before", "after")}
    return [prose_context.build(s, passage=passage) for s in sets]


def serialize(unit: Mapping[str, Any], context: Any) -> str:
    """The encoder's input for one set of a unit."""
    module = table_context if unit["origin"] == "table" else prose_context
    return module.serialize(context)


def context_version_field(origin: str) -> Tuple[str, int]:
    """The (field, version) a row records for the builder that read its set."""
    if origin == "table":
        return "table_context_version", table_context.TABLE_CONTEXT_VERSION
    return "prose_context_version", prose_context.PROSE_CONTEXT_VERSION


def render(unit: Mapping[str, Any]) -> Tuple[str, str, List[str]]:
    """(system prompt, prompt, the numbered sentences) for one unit."""
    found = contexts(unit)
    sentences = found[0].evidence_sentences() if found else []
    head = [f"Title: {unit.get('title') or ''}"]
    if unit.get("abstract"):
        head.append(f"Abstract: {_clip(unit['abstract'], 1500)}")
    if unit["origin"] == "table":
        rows = []
        for i, (s, ctx) in enumerate(zip(unit["sets"], found)):
            rows.append(
                f"#{i} {s.get('name') or '(unnamed)'} -- {len(ctx.points)} points; "
                f"rows: {' / '.join(ctx.rows[:6]) or '(not found)'}"
            )
        body = [
            f"{unit.get('table_label') or 'Table'} caption: {unit.get('caption') or ''}",
            f"Footer: {unit.get('footer') or ''}",
            "",
            "Table:",
            _clip(unit.get("table_serialised"), 6000),
            "",
            "Sentences (the caption, the footer, and the article's sentences citing the table):",
            _numbered(sentences),
            "",
            "Sets read from this table:",
            *rows,
        ]
        return TABLE_SYSTEM, "\n".join(head + [""] + body), sentences
    rows = []
    for i, ctx in enumerate(found):
        points = "; ".join(f"({p.get('x')}, {p.get('y')}, {p.get('z')})" for p in ctx.points[:8])
        rows.append(f"#{i} {ctx.name or '(unnamed)'} -- {points}")
    body = [
        f"Section heading: {unit.get('heading') or '(none)'}",
        "",
        "Sentences (the paragraph before, the passage, the paragraph after):",
        _numbered(sentences),
        "",
        "Sets read from this passage:",
        *rows,
    ]
    return PROSE_SYSTEM, "\n".join(head + [""] + body), sentences


def read_units(path: Path) -> List[Dict[str, Any]]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def stratified(units: Sequence[Mapping[str, Any]], seed: int = 0) -> List[Mapping[str, Any]]:
    """Units taken one per stratum in turn, each stratum shuffled: rare strata come early."""
    rng = random.Random(seed)
    strata: Dict[str, List[Mapping[str, Any]]] = collections.defaultdict(list)
    for unit in units:
        strata[unit.get("stratum") or "plain"].append(unit)
    for members in strata.values():
        rng.shuffle(members)
    queues = [collections.deque(strata[k]) for k in sorted(strata)]
    out = []
    while queues:
        for queue in list(queues):
            out.append(queue.popleft())
            if not queue:
                queues.remove(queue)
    return out


def done_units(out_dir: Path) -> set:
    done = set()
    for path in sorted(Path(out_dir).glob("batch-*.jsonl")):
        with open(path, encoding="utf-8") as handle:
            done.update(json.loads(line)["unit_id"] for line in handle if line.strip())
    return done


def read_labels(out_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Every set's label in a job directory, by set id."""
    out = {}
    for path in sorted(Path(out_dir).glob("batch-*.jsonl")):
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    row = json.loads(line)
                    out[row["set_id"]] = row
    return out


def label_rows(
    unit: Mapping[str, Any],
    answer: Mapping[str, Any],
    sentences: Sequence[str],
    *,
    model: str,
    effort: str,
) -> List[Dict[str, Any]]:
    """One row per set of a unit, in the label schema's words plus provenance."""
    field, version = context_version_field(unit["origin"])
    at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for a in sorted(answer["sets"], key=lambda a: a["set"]):
        evidence = sorted(set(a.get("evidence") or []))
        rows.append(
            {
                "unit_id": unit["unit_id"],
                "set_id": f"{unit['unit_id']}#{a['set']}",
                "origin": unit["origin"],
                "article_id": unit.get("article_id"),
                "name": unit["sets"][a["set"]].get("name"),
                "role": a["role"],
                "anchor_kind": a.get("anchor_kind") if a["role"] == "anchor" else None,
                "label": label_of(a),
                "from_prior_study": bool(a["from_prior_study"]) or a["role"] == "reference",
                "evidence": evidence,
                "evidence_text": [sentences[e] for e in evidence],
                "model": model,
                "effort": effort,
                "label_version": LABEL_VERSION,
                field: version,
                "at": at,
            }
        )
    return rows


def run(
    units: Iterable[Mapping[str, Any]],
    out_dir: Path,
    caller: Caller,
    *,
    model: str,
    effort: str,
    limit: int = 0,
    batch_size: int = 25,
    pace: float = 0.0,
    log=sys.stderr,
) -> Dict[str, int]:
    """Label the units not yet in `out_dir`, `batch_size` units per batch file."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    done = done_units(out_dir)
    todo = [u for u in units if u["unit_id"] not in done]
    if limit:
        todo = todo[:limit]
    batch_no = len(list(out_dir.glob("batch-*.jsonl")))
    counts = collections.Counter()
    handle, in_batch = None, batch_size
    for i, unit in enumerate(todo):
        system, prompt, sentences = render(unit)
        try:
            answer, cost = caller(system, prompt, SCHEMA, unit["unit_id"])
            problem = check(answer, len(unit["sets"]), len(sentences))
        except Exception as exc:  # noqa: BLE001 - recorded, the unit is retried next run
            answer, cost, problem = None, {}, f"{type(exc).__name__}: {str(exc)[:300]}"
        with open(out_dir / "ledger.jsonl", "a", encoding="utf-8") as ledger:
            ledger.write(
                json.dumps(
                    {
                        "unit_id": unit["unit_id"],
                        "origin": unit["origin"],
                        "model": model,
                        "effort": effort,
                        "sets": len(unit["sets"]),
                        "ok": problem is None,
                        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        **cost,
                    }
                )
                + "\n"
            )
        if problem is not None:
            counts["failed"] += 1
            with open(out_dir / "errors.jsonl", "a", encoding="utf-8") as errors:
                errors.write(json.dumps({"unit_id": unit["unit_id"], "error": problem}) + "\n")
            print(f"{unit['unit_id']}: {problem}", file=log, flush=True)
        else:
            if in_batch >= batch_size:  # a new file per `batch_size` labelled units
                if handle:
                    handle.close()
                batch_no += 1
                handle, in_batch = (
                    open(out_dir / f"batch-{batch_no:05d}.jsonl", "a", encoding="utf-8"),
                    0,
                )
            in_batch += 1
            for row in label_rows(unit, answer, sentences, model=model, effort=effort):
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                counts["sets"] += 1
            handle.flush()
            counts["units"] += 1
        if (i + 1) % 10 == 0:
            print(f"{i + 1}/{len(todo)} units, {dict(counts)}", file=log, flush=True)
        if pace and i + 1 < len(todo):
            time.sleep(pace)
    if handle:
        handle.close()
    return dict(counts)


def codex_caller(model: str, effort: str, attempts: int = 3) -> Caller:
    """A `Caller` through pondie's `CodexCaller` (`codex exec` on the `codex login` account).

    Needs a pondie that waits out a spent usage limit (`CodexUsageLimit`, pondie #10).
    """
    from pondie.extraction import llm  # noqa: PLC0415 - only the labelling job needs pondie
    from pondie.extraction.models import ModelCall  # noqa: PLC0415

    if not hasattr(llm, "CodexUsageLimit"):
        raise RuntimeError(
            "this pondie predates #10 and fails on a usage limit instead of waiting"
        )
    codex = llm.CodexCaller()

    def call(system: str, prompt: str, schema: Dict[str, Any], unit_id: str):
        reply = codex(
            ModelCall(
                model=model,
                prompt=prompt,
                system=system,
                effort=effort,
                attempts=attempts,
                json_schema=schema,
            ),
            paper=unit_id,
            stage="set-roles",
        )
        return reply.payload, reply.cost.model_dump()

    return call


def _kappa(pairs: Sequence[Tuple[str, str]]) -> Optional[float]:
    if not pairs:
        return None
    n = len(pairs)
    observed = sum(a == b for a, b in pairs) / n
    left, right = (
        collections.Counter(a for a, _ in pairs),
        collections.Counter(b for _, b in pairs),
    )
    expected = sum(left[k] * right[k] for k in left) / (n * n)
    return None if expected == 1 else round((observed - expected) / (1 - expected), 3)


def agreement(
    a: Mapping[str, Mapping[str, Any]], b: Mapping[str, Mapping[str, Any]]
) -> Dict[str, Any]:
    """How two labellers agree on the sets both labelled, per origin."""
    out = {}
    shared = sorted(set(a) & set(b))
    for origin in sorted({a[k]["origin"] for k in shared}):
        keys = [k for k in shared if a[k]["origin"] == origin]
        labels = [(a[k]["label"], b[k]["label"]) for k in keys]
        prior = [(str(a[k]["from_prior_study"]), str(b[k]["from_prior_study"])) for k in keys]
        out[origin] = {
            "sets": len(keys),
            "label_agreement": round(sum(x == y for x, y in labels) / len(keys), 3),
            "label_kappa": _kappa(labels),
            "role_agreement": round(
                sum(x.split(":")[0] == y.split(":")[0] for x, y in labels) / len(keys), 3
            ),
            "prior_agreement": round(sum(x == y for x, y in prior) / len(keys), 3),
            "prior_kappa": _kappa(prior),
            "labels_a": dict(collections.Counter(x for x, _ in labels)),
            "labels_b": dict(collections.Counter(y for _, y in labels)),
            "disagreements": dict(collections.Counter(f"{x} | {y}" for x, y in labels if x != y)),
        }
    return out


def ledger_totals(out_dir: Path) -> Dict[str, Any]:
    """Calls and tokens spent by a job, per model."""
    totals: Dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    path = Path(out_dir) / "ledger.jsonl"
    if path.exists():
        for line in open(path, encoding="utf-8"):
            row = json.loads(line)
            t = totals[row["model"]]
            t["calls"] += 1
            t["ok"] += row.get("ok", False)
            for key in (
                "input_tokens",
                "cached_tokens",
                "output_tokens",
                "reasoning_tokens",
                "seconds",
            ):
                t[key] += row.get(key) or 0
    return {model: dict(t) for model, t in totals.items()}


def iter_set_rows(
    units: Iterable[Mapping[str, Any]],
) -> Iterator[Tuple[Mapping[str, Any], int, Any]]:
    """(unit, set index, context) for every set of every unit."""
    for unit in units:
        for i, ctx in enumerate(contexts(unit)):
            yield unit, i, ctx
