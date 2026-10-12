"""LLM client for coordinates written in an article's prose.

A server that does not answer is asked again by the SDK. When it still does
not answer, the server's model listing is asked: if it lists the model, the
server is up and fails on this input alone, which is that passage's failure;
otherwise `ProseModelUnavailable` is raised, and the client stays `down`
without asking again.
"""

from __future__ import annotations

import itertools
import json
import threading
from typing import Any, Dict, Optional, Sequence

import openai
from study_schema.spaces import normalize_space

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models.statistics import normalize_statistic_kind
from ingestion_workflow.prompts.prose_coordinates import (
    INSTRUCTION,
    TEMPLATE,
    WITH_CONTEXT,
    schema,
)
from ingestion_workflow.services import coordinate_text
from ingestion_workflow.services.create_analyses import build_document
from ingestion_workflow.services.nuextract_payload import UnreadableAnswer
from ingestion_workflow.services.prose_passages import Passage

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


def printed(xyz: Sequence[float], text: str) -> bool:
    """Whether `text` prints x, y and z in that order, with these signs or others.

    The digits are `coordinate_text`'s to match. A sign is not: "±50, +8, −26"
    holds the left hemisphere's peak too, and a point printed only with
    another sign is still the passage's, its sign for the parse to record.
    """
    return any(coordinate_text.find_all(tuple(s * v for s, v in zip(signs, xyz)), text)
               for signs in itertools.product((1, -1), repeat=3)
               if all(s > 0 or v for s, v in zip(signs, xyz)))


def clean_answer(answer: Dict[str, Any], passage_text: str) -> Dict[str, Any]:
    """The model's answer, kept to what the passage says.

    A repeated analysis is a loop, not a second result. A point the passage
    does not print (`printed`) came from the surrounding context or
    nowhere; each analysis counts its own as `unwritten`. Points repeated
    within a set are resolve's to merge (`resolve`), where the set is whole.
    A named analysis the model gave no point is kept: a contrast the passage
    reports nothing for. One whose every point was unwritten, and an unnamed
    one left empty, are listed in `omitted` with the count and the reason.
    """
    seen, analyses, omitted = set(), [], []
    for a in (answer or {}).get("analyses") or []:
        key = (a.get("name"), json.dumps(a.get("points"), sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        points, unwritten = [], 0
        for p in a.get("points") or []:
            xyz = coordinate_text.triple(p)
            if xyz is None or not printed(xyz, passage_text):
                unwritten += 1
                continue
            points.append({"x": xyz[0], "y": xyz[1], "z": xyz[2],
                           "statistic": normalize_statistic_kind(p.get("statistic")),
                           "value": p.get("value"), "cluster_size": p.get("cluster_size")})
        name = (a.get("name") or "").strip() or None
        if points or (name and not unwritten):
            analyses.append({"name": name, "measure": a.get("measure"), "points": points,
                             "unwritten": unwritten})
        else:
            reason = "points not in passage" if unwritten else "no name and no points"
            omitted.append({"name": name, "unwritten": unwritten, "reason": reason})
    space = (answer or {}).get("space")
    return {"space": normalize_space(space), "analyses": analyses, "omitted": omitted}


class ProseModelUnavailable(RuntimeError):
    """The served model did not answer after every try: no passage is read."""


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
        self._down: Optional[str] = None
        self._lock = threading.Lock()

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

    def read(self, passage: Passage, *, title: str = "", abstract: str = "") -> Dict[str, Any]:
        """The model's answer as it gave it.

        Raises `UnreadableAnswer` unless the answer is a whole object with its list of
        analyses: only `{"analyses": []}` says the passage holds no coordinate. Nothing
        at all, an answer cut off at the token limit, or anything else is a failed read.
        """
        response = self._answer(self.request(passage, title=title, abstract=abstract))
        choice = response.choices[0]
        content = choice.message.content
        if choice.finish_reason == "length":
            raise UnreadableAnswer("the prose answer was cut off at the token limit")
        if not isinstance(content, str) or not content.strip():
            raise UnreadableAnswer("the prose model returned nothing at all")
        answer = json.loads(content)
        if not isinstance(answer, dict) or not isinstance(answer.get("analyses"), list):
            raise UnreadableAnswer("the prose answer is not an object with its analyses")
        return answer

    @property
    def down(self) -> Optional[str]:
        """Why the server is taken to be down for the rest of this run, or None."""
        return self._down

    def _healthy(self) -> bool:
        """Whether the server answers its model listing and lists this model."""
        try:
            listed = self.client.with_options(max_retries=0).models.list()
            return any(m.id == self.default_model for m in listed.data)
        except Exception:  # noqa: BLE001 - any failure to answer is an outage
            return False

    def _answer(self, request: Dict[str, Any]):
        if self._down:
            raise ProseModelUnavailable(self._down)
        try:
            return self.client.chat.completions.create(**request)
        except (openai.APIConnectionError, openai.InternalServerError,
                openai.RateLimitError) as exc:
            # timeouts, refused and dropped connections, 5xx and 429, after the SDK's tries
            last = f"{type(exc).__name__}: {exc}"
        except openai.BadRequestError:
            # The server refused this input (one too long for the model, say): this passage only.
            raise
        except openai.APIStatusError as exc:
            raise ProseModelUnavailable(
                f"{self.base_url} answered HTTP {exc.status_code} for {self.default_model}: {exc}"
            ) from exc
        tries = self.client.max_retries + 1
        if self._healthy():
            # Up, and failing on this input every time: this passage's failure, not an
            # outage, so the articles after it are still read.
            raise RuntimeError(f"server error on this input after {tries} tries: {last}")
        with self._lock:
            self._down = self._down or (
                f"{self.default_model} at {self.base_url} did not answer in {tries} tries: {last}"
            )
        raise ProseModelUnavailable(self._down)

    def extract(self, passage: Passage, *, title: str = "", abstract: str = "") -> Dict[str, Any]:
        return clean_answer(self.read(passage, title=title, abstract=abstract), passage.text)


__all__ = [
    "CONTEXT", "CONTEXTS", "ProseCoordinateClient", "ProseModelUnavailable", "clean_answer",
    "passage_body", "printed",
]
