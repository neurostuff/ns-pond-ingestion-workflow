"""What the normalize stage hands `ingestion_workflow.normalization`, the run it records, the
task-design model's answers and the store that keeps them.

The run is the model and the code: `version` hashes the served name, this package's
normalization code and data (the prompt's prefix among them), the vocabularies and pondie's
commit, so a new model or new code is a new run and every normalization made under the old
one is stale. Answers are stored by served name, template and prompt (`design.key_of`), so a
rerun under new assembly code asks the model nothing it was asked before.

Before the model is asked, what code decides is decided again (`renormalize`): the siblings
study_schema#56 removed (`age_unit_normalized`, `category_normalized`) are dropped; pondie's
deterministic fixes (`repair`) make the record's references and shapes agree with its schema
and its parse; pondie's `apply_derived` fills the derived slots, replacing what the model
answered there (`groups[].is_healthy` among them, but for the model's `true` that pondie
would make `false`, which the medical step settles); and its `apply_normalized` writes each
field's `normalized` sidecar. The record then holds to the schema pondie and this package
pin, and the medical step reads the derived `is_healthy`."""

from __future__ import annotations

import functools
import json
import socket
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ingestion_workflow.catalog import Artifact, Run, RunKind
from ingestion_workflow.pipeline.stage import recorded_path
from ingestion_workflow.pipeline.stages.offloaded import drop_siblings

#: The pipeline a normalize run made in-process is filed under.
PIPELINE = "ns-pond normalize"
#: Bumped when `renormalize` changes what it does; part of the run's version.
RENORMALIZE_VERSION = 5
#: How an answer that is not the model's last word is kept: the finish reason.
CUT, OVER = "length", "over_context"

#: pondie's fixes that `repair` leaves out, because they change what the record says rather
#: than how it says it: `mirrored` writes a sign-reversed copy of an analysis the paper
#: reported once (411 analyses over 191 of 3,000 ns-pond records), and `unreachable` deletes
#: entities the paper states that no analysis points at (an sMRI acquisition, a scanner).
SKIPPED_REPAIRS = frozenset({"mirrored", "unreachable"})


def describe(model: str) -> Dict[str, Any]:
    """The run version for served model `model`, and what it hashes."""
    from ingestion_workflow.normalization import api, design

    vocabulary = api.vocab_versions()
    assigner = {"model": model, "prefix_sha256": design.MODELS[model][1],
                "code_sha256": api.digest(api.code_hashes()), "vocab": vocabulary,
                "pondie_commit": api.pondie_commit(), "renormalize": RENORMALIZE_VERSION}
    return {"version": "design-" + api.digest(assigner)[:12], "model": model,
            "vocabulary": vocabulary, "assigner": assigner}


def run_for(source: str, model: str, code_commit: str,
            about: Optional[Dict[str, Any]] = None) -> Run:
    """The run a normalize stage makes for `source` with served model `model`."""
    about = about or describe(model)
    made = Run("", RunKind.MODEL, "normalize", source, pipeline=PIPELINE,
               version=about["version"], model=model,
               prompt_version=about["assigner"]["prefix_sha256"],
               schema_version="extraction-record", code_commit=code_commit,
               machine=socket.gethostname(),
               config={"vocabulary": about["vocabulary"], **about["assigner"]})
    return Run(**{**made.__dict__, "run_id": made.minted_id})


def text_of(catalog_root: Path, sync: Artifact) -> str:
    """The parse's text, the one the record's evidence offsets index, as the ingester reads it."""
    study = recorded_path(catalog_root, sync.summary["path"])
    paper = json.loads((study / "parse" / "parsed_paper.json").read_text("utf-8"))
    # Not read_text: it would turn a CRLF into LF, and the offsets would move.
    return (study / paper["text_path"]).read_bytes().decode("utf-8")


def parse_of(catalog_root: Path, sync: Artifact) -> Optional[Path]:
    """The paper's coordinate parse, which the record's `source_table_analysis` keys name."""
    parse = recorded_path(catalog_root, sync.summary["path"]) / "parse" / "coordinate_parse.json"
    return parse if parse.is_file() else None


def _skipped(body: Dict[str, Any], ctx: Any) -> list:
    return []


def repair(record: Dict[str, Any], parse: Optional[Path], text: str) -> List[str]:
    """pondie's deterministic fixes over `record`, in place, but `SKIPPED_REPAIRS`; the names
    of those that changed something.

    Each decides nothing a paper must settle: it repoints a cell at the same-named term its
    model reaches, scopes a term name two models share, names an analysis by its parse key,
    writes a reference a name settles. Over 3,000 ns-pond records they take the errors pondie's
    checks find from 1,722 to 973, and on the 27B's val records raise cell F1 against Astra's
    by 0.44 points."""
    from dataclasses import replace

    from pondie.extraction.record import fix
    from pondie.formats import text_index

    sequence = tuple(
        replace(step, apply=_skipped) if step.name in SKIPPED_REPAIRS else step
        for step in fix.build_sequence())
    ctx = fix.Context(schema=_extraction_schema(), parse=parse, table_map=None,
                      text=text_index.normalize(text) if text else "")
    log = fix.apply_all(record, ctx, sequence=sequence)
    return log.fired()


@functools.lru_cache(maxsize=1)
def _extraction_schema():
    from pondie import schema
    from pondie.schema import reader

    return reader.load(schema.EXTRACTION)


def apply_derived(record: Dict[str, Any]) -> None:
    """pondie's derived-slot fills (`is_healthy`, `population_characteristics`), in place.

    A group the model called healthy stays healthy where pondie would say otherwise: pondie
    reads a head out of any value outside its wellness phrases ("Normal hearing",
    "Neurotypical"), and only linking can tell such a head from a diagnosis. The medical step
    decides it (`normalization.medical.settle_healthy`)."""
    from pondie.formats import values
    from pondie.normalization import apply_derived as apply
    from pondie.normalization._records import value_of

    groups = [g for g in record.get("groups") or [] if isinstance(g, dict)]
    said = [value_of(g.get("is_healthy")) for g in groups]
    apply(record)
    for group, answer in zip(groups, said):
        if answer is True and value_of(group.get("is_healthy")) is False:
            group["is_healthy"] = values.wrap(True, source="generated", evidence="not_found")


def apply_normalized(record: Dict[str, Any]) -> None:
    """pondie's per-field normalizers, each writing its field's `normalized` sidecar."""
    from pondie.normalization import apply_normalized as apply

    apply(record)


def renormalize(record: Dict[str, Any], parse: Optional[Path] = None,
                text: str = "") -> Dict[str, Any]:
    """`record` repaired, with its derived slots and per-field normalizations made again, in
    place. In this order: the fixes read the record the model wrote, and the fit's medical
    step reads the derived `is_healthy`."""
    drop_siblings(record)
    repair(record, parse, text)
    apply_derived(record)
    apply_normalized(record)
    return record


def _prepare_one(job) -> Tuple[Optional[Tuple[Dict[str, Any], Dict[str, str]]], Optional[str]]:
    """((the record, renormalized and linked, {task id: prompt}), None), or (None, why it
    failed)."""
    from ingestion_workflow.normalization import api

    record, text, article_id, parse, model, version = job
    try:
        record = renormalize(record, Path(parse) if parse else None, text)
        api.link(record, text, article_id, version)
        return (record, api.prompts(record, text, model)), None
    except Exception as exc:  # noqa: BLE001 - this article's failure
        return None, f"{type(exc).__name__}: {exc}"


def prepare_all(jobs: Iterable[Tuple[Any, Optional[tuple]]], workers: int,
                no_input: str) -> Iterator[Tuple[Any, Tuple[Optional[tuple], Optional[str]]]]:
    """(key, (prepared, error)) for each (key, job) of `jobs`, in order; a job of None fails
    with `no_input`. `workers` > 1 prepares in forked processes: each inherits the vocabularies
    the first job loaded here, rather than loading its own (spawn would, per process)."""
    import multiprocessing

    held = iter(jobs)
    if workers <= 1:
        for key, job in held:
            yield key, ((None, no_input) if job is None else _prepare_one(job))
        return
    for key, job in held:  # the first one here, so the forked workers share what it loaded
        yield key, ((None, no_input) if job is None else _prepare_one(job))
        break
    pending = list(held)
    with multiprocessing.get_context("fork").Pool(workers) as pool:
        answers = pool.imap(_prepare_one, [job for _, job in pending if job is not None],
                            chunksize=4)
        for key, job in pending:
            yield key, ((None, no_input) if job is None else next(answers))


def no_answer(finish: str) -> str:
    said = {CUT: "no answer: cut at the length limit",
            OVER: "no answer: the prompt is over the model's context"}
    return said.get(finish, f"no answer: {finish}")


class AnswerStore:
    """The model's answers by `design.key_of`: what it was asked once is never asked again.
    Only final answers are kept (finished, cut at the length limit, over the context); a
    request that failed is not."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(self.path), timeout=60)
        self._db.execute("pragma journal_mode=wal")
        self._db.execute("""create table if not exists answers (key text primary key,
                            model text not null, finish text not null, content text,
                            prompt_tokens integer, completion_tokens integer,
                            made_at text not null, source text)""")
        self._db.commit()

    def get(self, key: str) -> Optional[Tuple[str, Optional[str]]]:
        """(finish reason, content), or None when the prompt was never answered."""
        row = self._db.execute("select finish, content from answers where key=?",
                               (key,)).fetchone()
        return (row[0], row[1]) if row else None

    def put(self, key: str, model: str, finish: str, content: Optional[str], prompt_tokens=None,
            completion_tokens=None, source: Optional[str] = None) -> None:
        made = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._db.execute("insert or replace into answers values (?, ?, ?, ?, ?, ?, ?, ?)",
                         (key, model, finish, content, prompt_tokens, completion_tokens, made,
                          source))
        self._db.commit()

    def __len__(self) -> int:
        return self._db.execute("select count(*) from answers").fetchone()[0]

    def close(self) -> None:
        self._db.close()


class RequestFailed(RuntimeError):
    """A request that may succeed when asked again: the server down, an error of its own."""


class Client:
    """The served task-design model, asked one prompt at a time (`design.request`)."""

    def __init__(self, url: str, model: str, timeout_s: int = 1800) -> None:
        self.url, self.model, self.timeout_s = url.rstrip("/"), model, timeout_s

    def served(self) -> List[str]:
        """The served names, from GET /models. Raises ModelUnavailable when nothing answers."""
        from ingestion_workflow.clients.model_server import ModelUnavailable

        try:
            with urllib.request.urlopen(self.url + "/models", timeout=30) as handle:
                return [m.get("id") for m in json.load(handle).get("data") or []]
        except (OSError, ValueError) as exc:
            raise ModelUnavailable(f"{self.url} does not answer: {exc}") from exc

    def ask(self, prompt: str) -> Tuple[str, Optional[str], Dict[str, Any]]:
        """(finish reason, content, usage). Raises RequestFailed when the server answers with
        an error of its own, and ModelUnavailable when it does not answer."""
        from ingestion_workflow.clients.model_server import ModelUnavailable
        from ingestion_workflow.normalization import design

        body = json.dumps(design.request(prompt, self.model)).encode("utf-8")
        q = urllib.request.Request(self.url + "/chat/completions", data=body,
                                   headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(q, timeout=self.timeout_s) as handle:
                got = json.load(handle)
        except urllib.error.HTTPError as exc:
            said = exc.read().decode("utf-8", "replace")[:600]
            if exc.code == 400 and any(w in said.lower()
                                       for w in ("context length", "maximum context")):
                return OVER, None, {}
            raise RequestFailed(f"http {exc.code}: {said[:300]}") from exc
        except OSError as exc:
            raise ModelUnavailable(f"{self.url} stopped answering: {exc}"[:300]) from exc
        except ValueError as exc:
            raise RequestFailed(f"not JSON: {exc}"[:300]) from exc
        choice = got["choices"][0]
        return (choice.get("finish_reason") or "stop", choice["message"].get("content"),
                got.get("usage") or {})


def answer_of(finish: str, content: Optional[str]):
    """What `normalization.api.finish` reads for one task: the content, or why there is none."""
    from ingestion_workflow.normalization.api import NoAnswer

    return content if finish == "stop" and content is not None else NoAnswer(no_answer(finish))


def load_parse(parse: Optional[Path]) -> Optional[dict]:
    try:
        return json.loads(Path(parse).read_text("utf-8")) if parse else None
    except (OSError, ValueError):
        return None


__all__ = ["AnswerStore", "CUT", "Client", "OVER", "PIPELINE", "RequestFailed", "SKIPPED_REPAIRS",
           "answer_of", "apply_derived", "apply_normalized", "describe", "drop_siblings",
           "load_parse", "no_answer", "parse_of", "prepare_all", "renormalize", "repair",
           "run_for", "text_of"]
