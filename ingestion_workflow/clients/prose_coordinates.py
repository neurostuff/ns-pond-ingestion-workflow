"""LLM client for coordinates written in an article's prose."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence

from study_schema.spaces import normalize_space

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.statistics import normalize_statistic_kind
from ingestion_workflow.prompts.prose_coordinates import (
    INSTRUCTION,
    ROLES,
    TEMPLATE,
    WITH_CONTEXT,
    schema,
)
from ingestion_workflow.services.create_analyses import build_document
from ingestion_workflow.services.prose_passages import MINUS, Passage

#: P: the passage alone. H: with its section heading. W: with the sentences
#: around it. HW: both -- what the fine-tune was trained on and the stage sends.
CONTEXTS = ("P", "H", "W", "HW")
CONTEXT = "HW"


def passage_body(passage: Passage, context: str = "HW") -> str:
    """The text in the document's table slot: the passage and the context asked for."""
    parts = []
    if "H" in context and passage.heading:
        parts.append(f"Section: {passage.heading}")
    if "W" in context and (passage.before or passage.after):
        if passage.before:
            parts.append(f"Context before: {passage.before}")
        parts.append(f"Passage: {passage.text}")
        if passage.after:
            parts.append(f"Context after: {passage.after}")
    else:
        parts.append(passage.text)
    return "\n\n".join(parts)


def numbers_in(text: str) -> List[float]:
    """The passage's numbers in the order written, without their signs."""
    flat = re.sub(rf"[{MINUS}]", "-", text)
    flat = re.sub(r"-\s+(?=\d)", "-", flat)
    return [abs(float(v)) for v in re.findall(r"-?\d+(?:\.\d+)?", flat)]


#: Most numbers between x and y, or y and z: "x = 4, y = 30" has none,
#: "(−30, −81, 30 and 36, −75, 33)" none, a cluster size or a statistic one.
GAP = 3


def written(xyz: Sequence[float], numbers: List[float]) -> bool:
    """Whether x, y and z are written in that order, close together.

    The model, given numbers that are not a coordinate, makes one of them:
    "(BA 9, 47)" read as (47, 47, 47), "t(14) = 2.96" as (2.96, 2.96, 2.96).
    Of 14,265 labelled points, 3 fail this, each a glyph or label error.
    """
    x, y, z = (abs(v) for v in xyz)
    for i, v in enumerate(numbers):
        if v != x:
            continue
        for j in range(i + 1, min(i + 1 + GAP, len(numbers))):
            if numbers[j] == y and z in numbers[j + 1:j + 1 + GAP]:
                return True
    return False


def clean_answer(answer: Dict[str, Any], passage_text: str) -> Dict[str, Any]:
    """The model's answer, kept to what the passage says.

    A repeated analysis is a loop, not a second result. A point not written
    in the passage came from the surrounding context or nowhere.
    """
    nums = numbers_in(passage_text)
    seen, analyses = set(), []
    for a in (answer or {}).get("analyses") or []:
        key = (a.get("name"), json.dumps(a.get("points"), sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        points = []
        for p in a.get("points") or []:
            try:
                xyz = [float(p[k]) for k in "xyz"]
            except (KeyError, TypeError, ValueError):
                continue
            if not written(xyz, nums):
                continue
            role = p.get("role") if p.get("role") in ROLES else "other"
            points.append({"x": xyz[0], "y": xyz[1], "z": xyz[2],
                           "statistic": normalize_statistic_kind(p.get("statistic")),
                           "value": p.get("value"), "cluster_size": p.get("cluster_size"),
                           "role": role})
        if points:
            analyses.append({"name": (a.get("name") or "").strip() or None,
                             "measure": a.get("measure"), "points": points})
    space = (answer or {}).get("space")
    return {"space": normalize_space(space), "analyses": analyses}


class ProseCoordinateClient(GenericLLMClient):
    """Reads one passage at a time with base NuExtract3's structured mode."""

    MAX_TOKENS = 6144

    def __init__(self, settings: Optional[Settings] = None) -> None:
        super().__init__(
            settings,
            # A local vLLM server takes any key; a hosted one needs llm_api_key.
            api_key=(getattr(settings, "llm_api_key", None) or "none"),
            base_url=getattr(settings, "prose_api_base", None),
            default_model=getattr(settings, "prose_model", None),
        )
        self.context = CONTEXT

    def request(self, passage: Passage, *, title: str = "", abstract: str = "") -> Dict[str, Any]:
        wide = "W" in self.context and bool(passage.before or passage.after)
        document = build_document(title=title, abstract=abstract, caption=None, footer=None,
                                  table_text=passage_body(passage, self.context))
        return {
            "model": self.default_model,
            "messages": [{"role": "user",
                          "content": INSTRUCTION + (WITH_CONTEXT if wide else "") + "\n\n" + document}],
            "temperature": 0.0,
            "max_completion_tokens": self.MAX_TOKENS,
            "extra_body": {
                "chat_template_kwargs": {"template": TEMPLATE, "enable_thinking": False},
                "structured_outputs": {"json": schema()},
            },
        }

    def extract(self, passage: Passage, *, title: str = "", abstract: str = "") -> Dict[str, Any]:
        response = self.client.chat.completions.create(**self.request(passage, title=title, abstract=abstract))
        return clean_answer(json.loads(response.choices[0].message.content or "{}"), passage.text)


__all__ = ["CONTEXT", "CONTEXTS", "ProseCoordinateClient", "clean_answer", "numbers_in", "passage_body", "written"]
