"""A reviewed plan of identity repairs, read from JSON lines and applied as one write.

Each line is one operation, named by `op`, with the arguments of the catalog
method it calls; any other key (`why`, the evidence) is carried for the reader.

    {"op": "add", "article": A, "kind": "pmid", "value": "123"}
    {"op": "reassign", "moves": [{"kind": "pmid", "value": "123", "from": A, "to": B}]}
    {"op": "remove", "article": A, "kind": "doi", "value": "10.1/x"}
    {"op": "split", "article": A, "aliases": [["doi", "10.1/y"]],
     "artifacts": [["download", "elsevier"]], "add": [["pmid", "456"]]}
    {"op": "move_artifacts", "from": A, "to": B, "artifacts": [["download", "pubget"]]}
    {"op": "forget_artifacts", "article": A, "artifacts": [["extract", "pubget"]]}
    {"op": "merge", "articles": [A, B], "drop": [["pmid", "37401785"]]}
    {"op": "mark_preprint_pmid", "article": A, "value": "37808793"}

`add` takes only an identifier no article holds; `split`'s `add` attaches more
such identifiers to the new article.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ingestion_workflow.models.ids import Identifier

from .invariants import Violation, violations
from .models import ALIAS_KINDS
from .store import Catalog, IdentityError

Operation = Dict[str, Any]

_FIELDS = {
    "add": ("article", "kind", "value"),
    "reassign": ("moves",),
    "remove": ("article", "kind", "value"),
    "split": ("article", "aliases"),
    "move_artifacts": ("from", "to", "artifacts"),
    "forget_artifacts": ("article", "artifacts"),
    "merge": ("articles",),
    "mark_preprint_pmid": ("article", "value"),
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
    return operations


@dataclass
class Outcome:
    """What a plan did, or would do."""

    applied: Counter = field(default_factory=Counter)
    #: (line number, the operation, why it was refused)
    refused: List[Tuple[int, Operation, str]] = field(default_factory=list)
    new_articles: List[str] = field(default_factory=list)
    #: The identifiers of every live article an operation touched, as they are afterwards.
    touched: List[Identifier] = field(default_factory=list)
    #: How the touched articles break the catalog's invariants once the plan has run.
    violations: List[Violation] = field(default_factory=list)
    committed: bool = False


class _Discard(Exception):
    """Leaves the plan's transaction, rolling back everything it wrote."""


def apply(
    catalog: Catalog,
    operations: List[Tuple[int, Operation]],
    *,
    registered: Callable[[str], Optional[bool]],
    backup: Optional[Path] = None,
) -> Outcome:
    """Run every operation inside one transaction; commit only with `backup`, no
    refusal, and no touched article breaking the invariants afterwards.

    Each operation is checked against the catalog as the ones before it left
    it, and a refused one writes nothing, so a dry run reports every refusal. A
    plan is committed whole or not at all. The backup is taken under the write
    lock, so nothing is written between the copy and the plan. `registered` is
    for the invariants' check of PubMed's DOIs.
    """
    outcome = Outcome()
    try:
        with catalog.transaction():
            if backup is not None:
                catalog.backup(backup)
            touched: Set[str] = set()
            for number, operation in operations:
                try:
                    touched.update(_RUN[operation["op"]](catalog, operation, outcome))
                except IdentityError as exc:
                    outcome.refused.append((number, operation, str(exc)))
                    continue
                outcome.applied[operation["op"]] += 1
            live = sorted({catalog.current_id(article_id) for article_id in touched})
            outcome.touched = [catalog.identifier(article_id) for article_id in live]
            outcome.violations = violations(catalog, live, registered)
            if backup is None or outcome.refused or outcome.violations:
                raise _Discard
    except _Discard:
        return outcome
    outcome.committed = True
    return outcome


def _pairs(rows) -> List[Tuple[str, str]]:
    return [(str(a), str(b)) for a, b in rows]


def _add(catalog: Catalog, article_id: str, kind: str, value: str) -> None:
    if kind not in ALIAS_KINDS:
        raise IdentityError(f"{kind} is not an alias kind")
    if not catalog.live(article_id):
        raise IdentityError(f"{article_id} is not a live article")
    holder = catalog.resolve(Identifier(**{kind: value}))
    if holder is not None:
        raise IdentityError(f"{kind} {value} is held by {holder.id}, not by no article")
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


def _run_forget_artifacts(catalog, op, outcome):
    catalog.forget_artifacts(op["article"], _pairs(op["artifacts"]))
    return [op["article"]]


def _run_merge(catalog, op, outcome):
    return [catalog.merge(op["articles"], _pairs(op.get("drop", ())))]


def _run_mark_preprint_pmid(catalog, op, outcome):
    catalog.mark_preprint_pmids([(str(op["value"]), op["article"])])
    return [op["article"]]


_RUN: Dict[str, Callable[[Catalog, Operation, Outcome], List[str]]] = {
    "add": _run_add,
    "reassign": _run_reassign,
    "remove": _run_remove,
    "split": _run_split,
    "move_artifacts": _run_move_artifacts,
    "forget_artifacts": _run_forget_artifacts,
    "merge": _run_merge,
    "mark_preprint_pmid": _run_mark_preprint_pmid,
}
