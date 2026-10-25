"""A reviewed plan of identity repairs, read from JSON lines and applied as one write.

Each line is one operation, named by `op`, with the arguments of the catalog
method it calls; any other key (`why`, the evidence) is carried for the reader.

    {"op": "add", "article": A, "kind": "pmid", "value": "123"}
    {"op": "reassign", "moves": [{"kind": "pmid", "value": "123", "from": A, "to": B}]}
    {"op": "remove", "article": A, "kind": "doi", "value": "10.1/x"}
    {"op": "split", "article": A, "aliases": [["doi", "10.1/y"]],
     "artifacts": [["download", "elsevier"]], "add": [["pmid", "456"]]}
    {"op": "move_artifacts", "from": A, "to": B, "artifacts": [["download", "pubget"]]}
    {"op": "swap_artifacts", "articles": [A, B], "artifacts": [["download", "ace"]]}
    {"op": "forget_artifacts", "article": A, "artifacts": [["extract", "pubget"]]}
    {"op": "merge", "articles": [A, B], "drop": [["pmid", "37401785"]],
     "rekind": [["pmid", "37808793", "preprint_pmid"]]}
    {"op": "rekind", "article": A, "kind": "pmid", "value": "37808793", "as": "preprint_pmid"}

A plan in which an identifier comes back to an article and kind it left is refused
whole: no operation of a reviewed plan undoes another. `add` takes only an identifier
no article holds; `split`'s `add` attaches more
such identifiers to the new article. `rekind` holds a PMID or PMCID as the
preprint's (`preprint_pmid`, `preprint_pmcid`), or a DOI as an alternate DOI or a
preprint's (`alternative_doi`, `preprint_doi`): the kind is always named.

An article whose extractions a plan forgets, moves or splits off, and which is left
with none, has its passages taken back as `ingest run` takes them back when no
extractor finds text: every stage built from that text then takes back its own.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stage import NO_TEXT

from .evidence import Evidence
from .invariants import Violation, index_downloads, neighbourhood, suspects, violations
from .models import ALIAS_KINDS, SECOND_KINDS
from .models import Outcome as StageOutcome
from .store import Catalog, IdentityError

Operation = Dict[str, Any]

_FIELDS = {
    "add": ("article", "kind", "value"),
    "reassign": ("moves",),
    "remove": ("article", "kind", "value"),
    "split": ("article", "aliases"),
    "move_artifacts": ("from", "to", "artifacts"),
    "swap_artifacts": ("articles", "artifacts"),
    "forget_artifacts": ("article", "artifacts"),
    "merge": ("articles",),
    "rekind": ("article", "kind", "value", "as"),
}


def load(path: Path) -> List[Tuple[int, Operation]]:
    """Each operation with its line number. Raises ValueError on a malformed line."""
    operations = []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            operation = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"line {number}: not JSON: {exc}") from exc
        fields = _FIELDS.get(operation.get("op")) if isinstance(operation, dict) else None
        if fields is None:
            raise ValueError(f"line {number}: op must be one of {', '.join(_FIELDS)}")
        missing = [name for name in fields if name not in operation]
        if missing:
            raise ValueError(f"line {number}: {operation['op']} needs {', '.join(missing)}")
        operations.append((number, operation))
    _refuse_returns(operations)
    return operations


def _refuse_returns(operations: List[Tuple[int, Operation]]) -> None:
    """Raise ValueError when an identifier comes back to a holder and kind it left: an
    operation undoing an earlier one, which a reviewed plan does not hold. Each value's
    history starts at the holder and kind its first operation takes it from."""
    states: Dict[str, List[Tuple[Optional[Tuple[str, str]], int]]] = {}

    def move(value, left, state, number):
        seen = states.setdefault(str(value), [(left, number)])
        if seen[-1][0] == state:
            return
        if any(s == state for s, _ in seen):
            first = next(n for s, n in seen if s == state)
            where = f"{state[1]} of {state[0]}" if state else "no article"
            raise ValueError(
                f"line {number}: {value} comes back to {where} it left after line {first}")
        seen.append((state, number))

    for number, o in operations:
        if o["op"] == "add":
            move(o["value"], None, (o["article"], o["kind"]), number)
        elif o["op"] == "remove":
            move(o["value"], (o["article"], o["kind"]), None, number)
        elif o["op"] == "rekind":
            move(o["value"], (o["article"], o["kind"]), (o["article"], o["as"]), number)
        elif o["op"] == "reassign":
            for m in o["moves"]:
                move(m["value"], (m["from"], m["kind"]), (m["to"], m["kind"]), number)
        elif o["op"] == "split":
            # the new article has no id until the split runs
            for kind, value in o["aliases"]:
                move(value, (o["article"], kind), (f"the article split from {o['article']}", kind),
                     number)


@dataclass
class Outcome:
    """What a plan did, or would do."""

    applied: Counter = field(default_factory=Counter)
    #: (line number, the operation, why it was refused)
    refused: List[Tuple[int, Operation, str]] = field(default_factory=list)
    new_articles: List[str] = field(default_factory=list)
    #: The identifiers of every live article an operation touched, as they are afterwards.
    touched: List[Identifier] = field(default_factory=list)
    #: The violations the plan makes: how the articles it touched, those linked to
    #: them, and every article holding two of a kind break the catalog's invariants
    #: once it has run and did not before.
    violations: List[Violation] = field(default_factory=list)
    #: The violations among the same articles that were there before the plan.
    preexisting: List[Violation] = field(default_factory=list)
    #: Preprint and paper pairs kept apart, each with a base study (`Violation.accepted`).
    accepted: List[Violation] = field(default_factory=list)
    #: Articles whose passages were taken back: the plan left them no extraction.
    taken_back: List[str] = field(default_factory=list)
    committed: bool = False


class _Discard(Exception):
    """Leaves the plan's transaction, rolling back everything it wrote."""


def apply(
    catalog: Catalog,
    operations: List[Tuple[int, Operation]],
    evidence: Evidence,
    *,
    backup: Optional[Path] = None,
) -> Outcome:
    """Run every operation inside one transaction; commit only with `backup`, no
    refusal, and no violation the plan makes.

    Each operation is checked against the catalog as the ones before it left
    it, and a refused one writes nothing, so a dry run reports every refusal. A
    plan is committed whole or not at all. With `backup`, the plan is first run as
    a dry run, and only one that would commit is run again behind the backup: the
    backup is taken under the write lock, so nothing is written between the copy
    and the plan.

    The articles checked are the plan's scope: those it touches, their neighbourhood
    and every article holding two of a kind, as they are after the plan, with every
    article it names. They are checked before the plan and after it; a violation only
    the after has is the plan's, and one both have is reported as there before. So a
    plan can be committed around a case left for a person, and cannot make the catalog
    worse.
    """
    index_downloads(catalog)
    outcome = _attempt(catalog, operations, evidence, None)
    if backup is None or outcome.refused or outcome.violations:
        return outcome
    return _attempt(catalog, operations, evidence, backup)


def _attempt(catalog: Catalog, operations: List[Tuple[int, Operation]], evidence: Evidence,
             backup: Optional[Path]) -> Outcome:
    outcome = Outcome()
    try:
        with catalog.transaction():
            if backup is not None:
                catalog.backup(backup)
            scope = _scope(catalog, operations, evidence)
            before = violations(catalog, scope, evidence)
            touched, lost_text = _run(catalog, operations, outcome)
            outcome.taken_back = _take_back(catalog, lost_text)
            live = sorted({catalog.current_id(article_id) for article_id in touched})
            outcome.touched = [catalog.identifier(article_id) for article_id in live]
            after = violations(catalog, neighbourhood(catalog, live, evidence) | suspects(catalog)
                               | {catalog.current_id(a) for a in scope}, evidence)
            # an accepted pair the plan breaks is a violation the plan makes
            was = {_key(catalog, v) for v in before if not v.accepted}
            outcome.accepted = [v for v in after if v.accepted]
            after = [v for v in after if not v.accepted]
            outcome.violations = [v for v in after if _key(catalog, v) not in was]
            outcome.preexisting = [v for v in after if _key(catalog, v) in was]
            if backup is None or outcome.refused or outcome.violations:
                raise _Discard
    except _Discard:
        return outcome
    outcome.committed = True
    return outcome


def _run(catalog: Catalog, operations: List[Tuple[int, Operation]],
         outcome: Outcome) -> Tuple[Set[str], Set[str]]:
    """Run the operations; return the articles they touched and those they took an
    extraction from."""
    touched: Set[str] = set()
    lost_text: Set[str] = set()
    for number, operation in operations:
        try:
            touched.update(_RUN[operation["op"]](catalog, operation, outcome))
        except IdentityError as exc:
            outcome.refused.append((number, operation, str(exc)))
            continue
        outcome.applied[operation["op"]] += 1
        lost_text.update(_text_from(operation))
    return touched, lost_text


def _scope(
    catalog: Catalog, operations: List[Tuple[int, Operation]], evidence: Evidence
) -> Set[str]:
    """The articles the plan's result is checked over, found by running it and rolling
    it back, with every article it names (each one merged away is live before it)."""
    found: Set[str] = set()
    try:
        with catalog.transaction():
            touched, _ = _run(catalog, operations, Outcome())
            live = {catalog.current_id(article_id) for article_id in touched}
            found = neighbourhood(catalog, live, evidence) | suspects(catalog)
            raise _Discard
    except _Discard:
        pass
    return found | {a for _, operation in operations for a in _named(operation)}


def _named(operation: Operation) -> List[str]:
    named = [operation.get(k) for k in ("article", "from", "to")]
    named += list(operation.get("articles") or ())
    named += [m.get(k) for m in operation.get("moves") or () for k in ("from", "to")]
    return [str(a) for a in named if a]


_ARTICLE_ID = re.compile(r"\b[a-z2-7]{12}\b")


def _key(catalog: Catalog, violation: Violation) -> Tuple[str, str, str]:
    """A violation by the articles as they are after the plan: one it merged an article
    of into another is the same violation."""
    detail = _ARTICLE_ID.sub(lambda m: catalog.current_id(m.group(0)), violation.detail)
    return catalog.current_id(violation.article_id), violation.rule, detail


def _pairs(rows) -> List[Tuple[str, str]]:
    return [(str(a), str(b)) for a, b in rows]


def _text_from(operation: Operation) -> List[str]:
    """The articles an operation took an extraction from."""
    keys = operation.get("artifacts") or ()
    if operation["op"] == "swap_artifacts" or not any(stage == "extract" for stage, _ in keys):
        return []
    return [operation.get("from") or operation["article"]]


def _take_back(catalog: Catalog, article_ids: Set[str]) -> List[str]:
    """Take back the passages of each article left with no ok extraction."""
    back = []
    for article_id in sorted(catalog.current_id(a) for a in article_ids):
        extracts = catalog.artifacts([article_id], "extract").get(article_id, {})
        passages = catalog.artifact(article_id, "passages", "")
        if any(a.ok for a in extracts.values()) or passages is None or (
                passages.fingerprint == NO_TEXT and not passages.ok):
            continue
        back.append(article_id)
    catalog.record([StageOutcome.failure(a, "passages", "", NO_TEXT, fingerprint=NO_TEXT)
                    for a in back])
    return back


def _add(catalog: Catalog, article_id: str, kind: str, value: str) -> None:
    if kind not in ALIAS_KINDS and kind not in SECOND_KINDS:
        raise IdentityError(f"{kind} is not an alias kind")
    if not catalog.live(article_id):
        raise IdentityError(f"{article_id} is not a live article")
    holder = catalog.holder(SECOND_KINDS.get(kind, kind), value)
    if holder is not None:
        raise IdentityError(f"{kind} {value} is held by {holder}, not by no article")
    catalog.add_aliases([(article_id, kind, value)])


def _run_add(catalog, op, outcome):
    _add(catalog, op["article"], op["kind"], str(op["value"]))
    return [op["article"]]


def _run_reassign(catalog, op, outcome):
    moves = [(m["kind"], str(m["value"]), m["from"], m["to"]) for m in op["moves"]]
    catalog.reassign_aliases(moves)
    return [article for _, _, src, dst in moves for article in (src, dst)]


def _run_remove(catalog, op, outcome):
    catalog.remove_aliases([(op["kind"], str(op["value"]), op["article"])])
    return [op["article"]]


def _run_split(catalog, op, outcome):
    # The new article's id is known only once it is made, so its extra
    # identifiers are added in the same write.
    with catalog.transaction():
        new_id = catalog.split(op["article"], _pairs(op["aliases"]),
                               _pairs(op.get("artifacts", ())))
        for kind, value in _pairs(op.get("add", ())):
            _add(catalog, new_id, kind, value)
    outcome.new_articles.append(new_id)
    return [op["article"], new_id]


def _run_move_artifacts(catalog, op, outcome):
    catalog.move_artifacts(op["from"], op["to"], _pairs(op["artifacts"]))
    return [op["from"], op["to"]]


def _run_swap_artifacts(catalog, op, outcome):
    first, second = op["articles"]
    catalog.swap_artifacts(first, second, _pairs(op["artifacts"]))
    return [first, second]


def _run_forget_artifacts(catalog, op, outcome):
    catalog.forget_artifacts(op["article"], _pairs(op["artifacts"]))
    return [op["article"]]


def _run_merge(catalog, op, outcome):
    return [catalog.merge(op["articles"], _pairs(op.get("drop", ())),
                          [(str(k), str(v), str(a)) for k, v, a in op.get("rekind", ())])]


def _run_rekind(catalog, op, outcome):
    catalog.rekind_aliases([(op["kind"], str(op["value"]), op["article"], op["as"])])
    return [op["article"]]


_RUN: Dict[str, Callable[[Catalog, Operation, Outcome], List[str]]] = {
    "add": _run_add,
    "reassign": _run_reassign,
    "remove": _run_remove,
    "split": _run_split,
    "move_artifacts": _run_move_artifacts,
    "swap_artifacts": _run_swap_artifacts,
    "forget_artifacts": _run_forget_artifacts,
    "merge": _run_merge,
    "rekind": _run_rekind,
}
