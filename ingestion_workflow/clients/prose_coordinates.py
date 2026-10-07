"""LLM client for coordinates written in an article's prose."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Set

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


def numbers_in(text: str) -> Set[float]:
    flat = re.sub(rf"[{MINUS}]", "-", text)
    flat = re.sub(r"-\s+(?=\d)", "-", flat)
    values = {float(v) for v in re.findall(r"-?\d+(?:\.\d+)?", flat)}
    return values | {abs(v) for v in values}


def clean_answer(answer: Dict[str, Any], passage_text: str) -> Dict[str, Any]:
    """The model's answer, kept to what the passage says.

    A repeated analysis is a loop, not a second result. A point whose numbers
    are not all in the passage came from the surrounding context or nowhere.
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
            if not all(v in nums or abs(v) in nums for v in xyz):
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
    return {"space": space if space in ("MNI", "TAL") else None, "analyses": analyses}


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


__all__ = ["CONTEXT", "CONTEXTS", "ProseCoordinateClient", "clean_answer", "numbers_in", "passage_body"]
