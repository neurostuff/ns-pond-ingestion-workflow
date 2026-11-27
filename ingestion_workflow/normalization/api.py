"""One article at a time: its medical links and modalities, each task's design from the task-design model, and
each analysis's structure and contrast identity, all written into the record's `normalized` sidecars.

    prompts(record, text, model) -> {task id: the prompt}
    link(record, text, article_id, version) -> tally          medical conditions and modalities
    finish(record, answers, parse, model, version) -> tally   designs, structure, contrast identity

`answers` maps each task with conditions to the model's answer text, or to a str saying why it has none
(`NoAnswer`). Nothing is fitted over the corpus: given the model, the vocabulary and this code, an article's
normalization depends on the article alone.
"""
import hashlib
import json
from pathlib import Path

from . import design, identity, medical, modality, structure, vocabulary

HERE = Path(__file__).resolve().parent


class NoAnswer(str):
    """Why a task has no answer to read: cut at the length limit, over the context, not JSON."""


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def code_hashes():
    """sha256 of this package's code and data: what a normalization is made by besides the model."""
    out = {}
    for p in sorted(HERE.rglob("*")):
        if p.is_file() and p.suffix in (".py", ".json", ".tsv", ".txt") and "__pycache__" not in p.parts:
            out[str(p.relative_to(HERE))] = sha(p)
    return out


def pondie_commit():
    """The pondie commit installed: pip's record of a git install, else the checkout's HEAD."""
    from importlib import metadata

    try:
        url = json.loads(metadata.distribution("pondie").read_text("direct_url.json") or "{}")
        if (url.get("vcs_info") or {}).get("commit_id"):
            return url["vcs_info"]["commit_id"]
    except metadata.PackageNotFoundError:
        pass
    import subprocess

    import pondie

    where = Path(next(iter(pondie.__path__))).parent
    got = subprocess.run(["git", "-C", str(where), "rev-parse", "HEAD"], capture_output=True, text=True)
    return got.stdout.strip() or None


def vocab_versions():
    """The vocabularies read: MONDO's release, pondie's negation parser and abbreviation miner, and the condition
    vocabulary."""
    try:
        from pondie.vocabularies.abbreviations import detector

        miner = "scispacy" if detector() is not None else "builtin"
    except Exception:  # noqa: BLE001
        miner = None
    return {**medical.versions(), "abbreviation_miner": miner, "condition_vocabulary": vocabulary.load()["version"]}


def body_of(document):
    """The extraction record inside a document that wraps it as {"study": record}, or the record itself."""
    inner = document.get("study") if isinstance(document, dict) else None
    return inner if isinstance(inner, dict) and "tasks" not in document else document


def _tid(task):
    return str(design.v(task.get("local_id")))


def prompts(record, text, model):
    """Each task with conditions -> its prompt for `model`."""
    r = body_of(record)
    return {_tid(t): design.prompt_of(design.item_of(r, t, text), model) for t in design.tasks_of(r)}


def link(record, text, article_id, version, *, allow_no_parser=False):
    """Medical conditions (MONDO, ONVOC) and acquisition modalities, in place."""
    from . import sidecars

    r = body_of(record)
    if not allow_no_parser and not medical.versions()["negation_parser"]:
        # without spaCy's en_core_web_sm pondie cannot read negation scope: "Dementia free" links to dementia
        raise RuntimeError("pondie's negation parser is missing: python -m spacy download en_core_web_sm")
    store = medical.abbreviations(text or "", article_id)
    links = medical.settle_healthy(r, medical.links(r, article_id, text or "", store))
    tally = {f"medical:{k}": n for k, n in sidecars.write_medical(r, links, version).items()}
    tally.update({f"modality:{k}": n for k, n in sidecars.write_modalities(r, modality.read(r), version).items()})
    return tally


def designs_of(answers):
    """task id -> its design, or the reason it has none."""
    out = {}
    for tid, answer in answers.items():
        if isinstance(answer, NoAnswer):
            out[tid] = str(answer)
            continue
        try:
            out[tid] = design.read(answer)
        except ValueError as exc:   # json.JSONDecodeError is one
            out[tid] = f"no answer: not a JSON object ({str(exc)[:80]})"
    return out


def finish(record, answers, parse, model, version):
    """The tasks' designs and the analyses' contrast identities, in place. `parse` is the loaded coordinate parse
    (for the statistics' sign), or None."""
    from . import sidecars

    r = body_of(record)
    designs = designs_of(answers)
    read = {tid: d for tid, d in designs.items() if isinstance(d, dict)}
    kinds = {c["id"]: c["kind"] for d in read.values() for c in d["conditions"] if c["kind"]}
    structures = structure.assemble(r, parse, kinds=kinds)
    found = identity.identities(r, structures, read)
    tally = {f"tasks:{k}": n for k, n in sidecars.write_tasks(
        r, designs, model, vocabulary.load()["version"], version).items()}
    tally.update({f"contrasts:{k}": n for k, n in sidecars.write_contrasts(r, found, version).items()})
    return tally


def normalize_record(record, text, answers, *, model, version, article_id, parse=None, allow_no_parser=False):
    """`link` and `finish` over one record; the record, its sidecars written."""
    link(record, text, article_id, version, allow_no_parser=allow_no_parser)
    finish(record, answers, parse, model, version)
    return record


__all__ = ["NoAnswer", "body_of", "code_hashes", "designs_of", "digest", "finish", "link", "normalize_record",
           "pondie_commit", "prompts", "sha", "vocab_versions"]
