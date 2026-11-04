"""Turn the sets collected before `ingest offload` existed into runs and results to import.

Three sets on beast, each made outside ns-pond:

* `record`: the 27B's records (`ns-collect/set-20261101`, pondie `vista/collect_ns.py`),
  one run per wave of Vista jobs. `extraction_metadata` there is the placeholder
  `vista-compact` / `0`; import stamps the run instead.
* `evidence`: those records with ev1-full's evidence (`ns-collect/set-20261101-ev1`,
  pondie `vista/ns_ev1_merge.py`).
* `normalize`: the normalization build `astra_norm/out_all/current`, per study the rows
  of its derived tables. Only the studies it built from a 27B record are written; the
  rest were Astra's, and the catalog holds no Astra record.

The runs are rebuilt from pondie's job configs and `VISTA-JOURNAL.md` (RUNS below). The
text a record was made from is the `Text:` of the prompt its shard sent: when it is the
parse's text file, decoded as the prompt builder read it, the row carries the file's
sha256, which is the parse's `text_sha256`; otherwise the prompt text's own, and import
refuses it. Evidence and normalization rows carry `record_digest` of the record they
read: the collected record, and the build's `records_all` copy.

    python scripts/offload_backfill.py --catalog COPY --out DIR [--only record evidence normalize]

writes DIR/<run_id>.run.json and DIR/<run_id>.jsonl.gz for each run, then

    ingest offload import DIR/<run_id>.run.json DIR/<run_id>.jsonl.gz --catalog COPY

The catalog is opened read-only, for each article's parse directory.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple

from ingestion_workflow.pipeline.stages.offloaded import record_digest

AGENTS = Path("/data/james/agents/astra-distill")
SET = AGENTS / "ns-collect/set-20261101"
SET_EV1 = AGENTS / "ns-collect/set-20261101-ev1"
NS = AGENTS / "vista-out/ns"
NORM = Path("/home/james/astra_norm")

#: vLLM settings of every ns-27b-e shard (tacc/configs/bench-27b-e.json, setup e).
VLLM_E = {"guided": False, "workers": 48, "gpu_mem": 0.92,
          "vllm_args": ["--kv-cache-dtype", "fp8", "--quantization", "fp8", "--speculative-config",
                        '{"method": "qwen3_5_mtp", "num_speculative_tokens": 2}']}
RECORD_RUN = dict(
    kind="model", stage="record", source="ns-27b", pipeline="pondie astra-distill vista",
    version="ns-27b-e", model="Qwen/Qwen3.8-27B",
    # QLoRA nf4 r=64, 1 epoch on train v5, step 776; merged on Vista $SCRATCH as
    # merged/r3-27b-bench. Not on the Hub yet, so it has no revision.
    adapter="r3-27b-full/adapter-final",
    prompt_version="astra-distill v5 paper prompt (vista/tasks.py prompt_of, bodies_ns.py)",
    schema_version="extraction-record (pondie migrate/in, compact.from_compact)",
    code_commit="pondie@a963c94", machine="vista gh (GH200)")
#: (run_id, first shard, last shard, created, max_len, jobs and notes), VISTA-JOURNAL.md.
RECORD_WAVES = (
    ("ns-27b-e-20261028-s000", 0, 0, "2026-10-28", 49152,
     {"note": "smoke shard, 498 papers; 4 cut at the context limit, so the rest ran at 65,536"}),
    ("ns-27b-e-20261028", 1, 84, "2026-10-28", 65536, {"bodies": "shards-20261028"}),
    ("ns-27b-e-20261030", 85, 103, "2026-10-30", 65536,
     {"bodies": "run-20261030/shards-20261030", "jobs": "1110622-1110640; s100 again as 1111029",
      "input": "staging/sync-parse-20261030/vista-ready.tsv, ns-pond main 76ae0fc"}),
    ("ns-27b-e-20261031", 104, 136, "2026-10-31", 65536,
     {"bodies": "run-20261031/shards-20261031",
      "jobs": "1112034-1112063; s134-s136 as slots freed", "input": "run-20261031/articles.tsv"}),
)
SHARD_DIRS = (NS / "shards-20261028", NS / "run-20261030/shards-20261030",
              NS / "run-20261031/shards-20261031")
EVIDENCE_RUN = dict(
    run_id="ns-ev1-20261102", kind="model", stage="evidence", source="ns-27b",
    pipeline="pondie astra-distill ev1", version="ev1-full", model="numind/NuExtract3",
    adapter="ev1-full/adapter-final",
    prompt_version="evidence by unit index (vista/evidence.py, ns_ev1_bodies.py)",
    schema_version="extraction-evidence (evidence sets, source retriever)",
    code_commit="pondie@58a8d36", machine="vista gh (GH200)", created_at="2026-11-02",
    config={"shards": "s000-s032 (ns-ev1-s000..s032, 23.4 gh node-h, max 52k input tokens)",
            "redo": "ns-ev1-redo, gh-dev 1116687: 306 papers, max_len 131072, max_tokens 40000",
            "merge": str(SET_EV1), "base": str(SET)})


def _shard(name: str) -> int:
    return int(name[1:])


def _wave(shard: int) -> str:
    return next(run_id for run_id, first, last, *_ in RECORD_WAVES if first <= shard <= last)


def _write(out: Path, run: dict, rows: Iterator[dict]) -> Counter:
    (out / f"{run['run_id']}.run.json").write_text(json.dumps(run, indent=1))
    counts: Counter = Counter()
    with gzip.open(out / f"{run['run_id']}.jsonl.gz", "wt", encoding="utf-8",
                   compresslevel=3) as handle:
        for row in rows:
            counts[row["status"]] += 1
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return counts


class Papers:
    """Each article's parse directory, from the catalog's sync artifact (read-only)."""

    def __init__(self, catalog: Path) -> None:
        uri = (catalog.resolve() / "catalog.sqlite").as_uri() + "?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True)

    def directory(self, article_id: str) -> Optional[Path]:
        for _ in range(10):
            row = self._conn.execute("SELECT merged_into FROM articles WHERE id=?",
                                     (article_id,)).fetchone()
            if row is None or row[0] is None:
                break
            article_id = row[0]
        row = self._conn.execute(
            "SELECT json_extract(summary, '$.path') FROM artifacts "
            "WHERE article_id=? AND stage='sync' AND source='' AND status='ok'",
            (article_id,)).fetchone()
        return Path(row[0]) if row and row[0] else None


def _read_as_prompted(raw: bytes) -> str:
    """The text as `bodies_ns.py` read it: `Path.read_text()`, universal newlines."""
    return raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")


def _prompt_text(prompt: str) -> str:
    start = prompt.index("\n\nText:\n") + len("\n\nText:\n")
    return prompt[start:prompt.rindex("\n\nCoordinate sets:\n")]


HEAD = re.compile(r'^\{"sid": "([^"]+)"')


def _prompts(wanted: Dict[str, str]) -> Iterator[Tuple[str, str]]:
    """(sid, prompt text) of each wanted sid, from the shard it was kept from."""
    for directory in SHARD_DIRS:
        for path in sorted(directory.glob("s[0-9][0-9][0-9].bodies.jsonl")):
            shard = path.name.split(".")[0]
            with open(path, encoding="utf-8") as handle:
                for line in handle:
                    sid = HEAD.match(line)[1]
                    if wanted.get(sid) == shard:
                        yield sid, _prompt_text(json.loads(line)["prompt"])


def text_hashes(papers: Papers, manifest: list) -> Dict[str, Tuple[str, str]]:
    """{sid: (text_sha256, how)} for each manifest row, by the rule in the docstring."""
    by_sid = {row["sid"]: row for row in manifest}
    found: Dict[str, Tuple[str, str]] = {}
    for sid, text in _prompts({row["sid"]: row["shard"] for row in manifest}):
        prompted = hashlib.sha256(text.encode("utf-8")).hexdigest()
        paper = papers.directory(by_sid[sid]["article_id"])
        how = "no parse"
        if paper is not None:
            try:
                parsed = json.loads((paper / "parse" / "parsed_paper.json").read_text("utf-8"))
                raw = (paper / parsed["text_path"]).read_bytes()
                if _read_as_prompted(raw) == text:
                    found[sid] = (hashlib.sha256(raw).hexdigest(), "parse text")
                    continue
                how = "another text"
            except (OSError, ValueError, KeyError, UnicodeDecodeError):
                how = "parse unreadable"
        found[sid] = (prompted, how)
    return found


def _manifest(path: Path) -> list:
    lines = path.read_text("utf-8").splitlines()
    head = lines[0].split("\t")
    return [dict(zip(head, line.split("\t"))) for line in lines[1:]]


def record_runs(out: Path, papers: Papers) -> Dict[str, Tuple[str, str]]:
    manifest = _manifest(SET / "manifest.tsv")
    hashes = text_hashes(papers, manifest)
    print("text:", dict(Counter(how for _, how in hashes.values())))
    for run_id, first, last, created, max_len, extra in RECORD_WAVES:
        run = dict(RECORD_RUN, run_id=run_id, created_at=created,
                   config=dict(VLLM_E, max_len=max_len, shards=f"s{first:03d}-s{last:03d}",
                               collect=str(SET), **extra))

        def rows(run_id=run_id):
            for row in manifest:
                if _wave(_shard(row["shard"])) != run_id:
                    continue
                sid, article_id = row["sid"], row["article_id"]
                out_row = {"article_id": article_id, "base_study_id": sid,
                           "text_sha256": hashes.get(sid, (None,))[0]}
                if row["status"] == "no_prediction":
                    out_row.update(status="skipped", reason=f"no prediction: {row['note']}")
                elif row["status"] in ("valid", "invalid"):
                    record = json.loads((SET / "records" / f"{sid}.extraction.json").read_text())
                    out_row.update(status="ok", record=record)
                else:
                    out_row.update(status="failed", reason=f"{row['status']}: {row['note']}")
                yield out_row
        print(run_id, dict(_write(out, run, rows())))
    return hashes


def evidence_run(out: Path, hashes: Dict[str, Tuple[str, str]]) -> None:
    by_sid = {row["sid"]: row for row in _manifest(SET / "manifest.tsv")}

    def rows():
        for row in _manifest(SET_EV1 / "manifest.tsv"):
            sid = row["sid"]
            out_row = {"article_id": by_sid[sid]["article_id"], "base_study_id": sid,
                       "text_sha256": hashes.get(sid, (None,))[0]}
            if row["evidence"] != "ev1":
                out_row.update(status="skipped",
                               reason="no evidence: over the input cap, cut at max_tokens, "
                                      "or no text")
            else:
                base = json.loads((SET / "records" / f"{sid}.extraction.json").read_text())
                out_row.update(status="ok", record_digest=record_digest(base),
                               record=json.loads(
                                   (SET_EV1 / "records" / f"{sid}.extraction.json").read_text()))
            yield out_row
    print(EVIDENCE_RUN["run_id"], dict(_write(out, EVIDENCE_RUN, rows())))


def normalize_run(out: Path) -> None:
    import pyarrow.parquet as pq

    build = NORM / "out_all/current"
    manifest = json.loads((build / "manifest.json").read_text())
    tables = {name: pq.read_table(build / "derived" / f"{name}.parquet").to_pylist()
              for name in ("analyses", "tasks", "conditions", "groups_medical")}
    per_study: Dict[str, Dict[str, list]] = {}
    for name, rows in tables.items():
        for row in rows:
            per_study.setdefault(row["study"], {}).setdefault(name, []).append(row)
    sid_to_article = {row["sid"]: row["article_id"] for row in _manifest(SET / "manifest.tsv")}
    run = dict(
        run_id="astra-norm-20261031", kind="model", stage="normalize", source="ns-27b",
        pipeline="taskCategories astra_build_combined.sh", version=manifest["built"],
        model="SapBERT + MiniLM + MONDO + taskcat", code_commit=f"pondie@{manifest['pondie_commit']}",
        machine="beast (CPU)", created_at=manifest["built"],
        config={"vocabulary": manifest["vocab"] | {k: manifest["inputs"][k] for k in
                                                    ("task_labels", "contrast_sets",
                                                     "medical_decisions")},
                "parameters": manifest["parameters"], "code_sha256": manifest["code_sha256"],
                "build": str(build), "records": manifest["inputs"]["record_dir"]})
    skipped = Counter()

    def rows():
        for line in (build / "record_hashes.tsv").read_text().splitlines()[1:]:
            sid = line.split("\t")[0]
            record = json.loads((NORM / "records_all" / f"{sid}.extraction.json").read_text())
            if (record.get("extraction_metadata") or {}).get("extractor_version") != "ns-27b-e":
                skipped["astra record"] += 1
                continue
            if sid not in sid_to_article:
                skipped["not in set-20261101"] += 1
                continue
            yield {"article_id": sid_to_article[sid], "base_study_id": sid, "status": "ok",
                   "record_digest": record_digest(record), "record": per_study.get(sid, {})}
    print(run["run_id"], dict(_write(out, run, rows())), "left out:", dict(skipped))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--catalog", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--only", nargs="+", default=["record", "evidence", "normalize"])
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    hashes = record_runs(a.out, Papers(a.catalog)) if "record" in a.only else {}
    if "evidence" in a.only:
        if not hashes:
            hashes = text_hashes(Papers(a.catalog), _manifest(SET / "manifest.tsv"))
        evidence_run(a.out, hashes)
    if "normalize" in a.only:
        normalize_run(a.out)


if __name__ == "__main__":
    main()
