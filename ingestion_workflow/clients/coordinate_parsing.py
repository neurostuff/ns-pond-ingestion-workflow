"""LLM client for coordinate parsing tasks."""

from __future__ import annotations

import dataclasses
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import CoordinatePoint, ParseAnalysesOutput
from ingestion_workflow.models.statistics import (
    STATISTIC_KINDS,
    normalize_statistic_kind,
)
from ingestion_workflow.services.nuextract_payload import parse_payload


logger = logging.getLogger(__name__)

_POINT_FIELDS = frozenset(field.name for field in dataclasses.fields(CoordinatePoint))


class CoordinateParsingClient(GenericLLMClient):
    """Client responsible for parsing coordinate tables via LLM."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None,
    ) -> None:
        super().__init__(
            settings,
            api_key=api_key,
            base_url=base_url,
            default_model=default_model,
        )

    # NuExtract's structured mode. The schema is a *chat template variable*,
    # not a system message: the template renders it between 【template_start】
    # and 【template_end】, and the fine-tune has only ever seen it there.
    NATIVE_TEMPLATE = json.dumps(
        {
            "space": ["MNI", "TAL"],
            "analyses": [
                {
                    "name": "verbatim-string",
                    "measure": ["voxels", "mm^3"],
                    "points": [
                        [
                            "number",
                            "number",
                            "number",
                            # Not a list written here: the kinds the rest of
                            # the system accepts. Written here, it offered six
                            # where the reader took eight, and a model trained
                            # to report Cohen's d was forbidden it by its own
                            # prompt.
                            list(STATISTIC_KINDS),
                            "number",
                            "integer",
                        ]
                    ],
                }
            ],
        },
        ensure_ascii=False,
    )

    #: What the server will hold. Asking for output that does not fit beside
    #: the prompt is refused outright, not truncated, so the budget has to be
    #: worked out before the call rather than discovered from a 400.
    CONTEXT_WINDOW = 16384

    @classmethod
    def native_schema(cls) -> Dict[str, Any]:
        """The template as a JSON schema, for grammar-constrained decoding.

        Derived from `NATIVE_TEMPLATE` rather than written beside it. Written
        beside it, the statistic slot was declared a free string while the
        template declared an enum, and the model answered `"T=5.53"` -- legal
        under the grammar, so the type and the value fused into the type slot
        and every statistic was dropped without an error. A grammar forbids
        exactly what the schema forbids, so the schema has to come from the
        same characters the model is shown.

        `minItems` 3 keeps the short point the reader already accepts, where
        the statistic tail is absent.
        """
        template = json.loads(cls.NATIVE_TEMPLATE)
        point = template["analyses"][0]["points"][0]

        def slot(declared: Any) -> Dict[str, Any]:
            if isinstance(declared, list):
                # A closed set in the template is a closed set in the grammar,
                # plus null for the point that carries no statistic.
                return {"enum": [*declared, None]}
            if declared == "integer":
                return {"type": ["integer", "null"]}
            return {"type": ["number", "null"]}

        return {
            "type": "object",
            "properties": {
                "space": {"enum": [*template["space"], None]},
                "analyses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": ["string", "null"]},
                            "measure": slot(template["analyses"][0]["measure"]),
                            "points": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "prefixItems": [
                                        {"type": "number"},
                                        {"type": "number"},
                                        {"type": "number"},
                                        *[slot(d) for d in point[3:]],
                                    ],
                                    "minItems": 3,
                                    "maxItems": len(point),
                                },
                            },
                        },
                        "required": ["name", "points"],
                    },
                },
            },
            "required": ["space", "analyses"],
        }

    def native_request(
        self,
        document: str,
        *,
        model: Optional[str] = None,
        max_tokens: int = 8192,
        context_window: Optional[int] = None,
        constrain: bool = False,
    ) -> Dict[str, Any]:
        """The request the native path sends, as keyword arguments.

        Everything that decides what the model sees is computed here and
        nowhere else: the template, the budget, the clip, the temperature.
        A benchmark that rebuilds the call instead of using this one measures
        a prompt production never sends -- which is what happened: a hand
        copied template declared the statistic slot a number where this one
        declares an enum, and every figure taken under it was void.
        """
        window = context_window or self.CONTEXT_WINDOW
        # Three characters per token is the conservative ratio for serialised
        # tables, which are mostly digits and separators; the margin absorbs
        # the template and the chat scaffolding the server adds.
        floor = 256
        room = (window - floor - 512) * 3
        if len(document) > room:
            # Longer than the window itself, so no output budget can make it
            # fit. Clipping the tail costs the last rows of the table; sending
            # it whole costs the table. Both are losses, and this one is
            # visible in the log rather than a 400 that drops the article.
            logger.warning(
                "clipping a %d character document to %d to fit the context window",
                len(document), room,
            )
            document = document[:room]
        extra: Dict[str, Any] = {
            "chat_template_kwargs": {
                "template": self.NATIVE_TEMPLATE,
                "enable_thinking": False,
            }
        }
        if constrain:
            # vLLM 0.28 accepts `guided_json` in extra_body, raises nothing and
            # applies no constraint; `structured_outputs` is the one that binds.
            extra["structured_outputs"] = {"json": self.native_schema()}
        return {
            "model": model or self.default_model,
            "messages": [{"role": "user", "content": document}],
            "temperature": 0.0,
            "max_completion_tokens": max(
                floor, min(max_tokens, window - len(document) // 3 - 512)
            ),
            "extra_body": extra,
        }

    def parse_analyses_native(
        self,
        document: str,
        *,
        model: Optional[str] = None,
        #: The ceiling on the answer, not on the window. 4,096 severed 92 of
        #: the 871 benchmark tables -- every one of them stopped exactly here,
        #: with a median document of 982 tokens leaving ~14,900 free. The
        #: subtraction below is what protects the request; this only stopped
        #: it using what the window could already afford.
        #:
        #: It had been raised once before, from 2,048, for the same reason.
        max_tokens: int = 8192,
        context_window: Optional[int] = None,
    ) -> Tuple[ParseAnalysesOutput, Optional[str]]:
        """Parse a table with the fine-tuned extractor, and report its space.

        No function tool and no instructions: the schema goes in the template
        slot and the document in the message, which is the shape the model was
        trained on. Sending it the prompted path's four thousand tokens of
        rules would be an input it has never seen.

        Greedy, because every evaluation of this model was greedy and a
        sampled coordinate is a wrong coordinate.

        The output budget is what is left of the context after the document,
        not a fixed number. A long table is exactly the one worth reading, and
        asking for 8,192 tokens beside a 12,000 token prompt is refused with a
        400 rather than truncated -- so the request would fail on precisely
        the richest tables.

        The cap matters as much as the subtraction. Measured on the benchmark,
        every truncated answer stopped at the cap and none ran out of window:
        the median case held a 982-token document inside 16,384. A ceiling
        below what the window affords throws away the long answers the
        subtraction was written to protect.
        """
        request = self.fit_to_window(
            document, model=model, max_tokens=max_tokens, context_window=context_window
        )
        response = self.client.chat.completions.create(**request)
        return parse_payload(response.choices[0].message.content or "")

    #: Tokens kept free between the prompt and the answer's budget. The count is
    #: exact, so this covers only what the server adds after tokenizing.
    WINDOW_MARGIN = 16

    def fit_to_window(
        self,
        document: str,
        *,
        model: Optional[str] = None,
        max_tokens: int = 8192,
        context_window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """`native_request`, with the budget set from the prompt's real size.

        `native_request` estimates three characters per token, and dense
        tables run past that: on 2026-10-02, 74 of 49,368 articles were refused
        with a 400 because the prompt plus the budget it allowed overran the
        window -- one with the prompt alone filling it after the clip. The
        server's own tokenizer, run over the request with its chat template,
        gives the size exactly; the document is clipped by the ratio it
        measured until the floor fits, and the budget is what is left.

        A server without `/tokenize` keeps the estimate, as before.
        """
        window = context_window or self.CONTEXT_WINDOW
        floor = 256
        request = self.native_request(
            document, model=model, max_tokens=max_tokens, context_window=context_window
        )
        for _ in range(4):
            count = self._prompt_tokens(request)
            if count is None:
                return request
            spare = window - count - self.WINDOW_MARGIN
            if spare >= floor:
                request["max_completion_tokens"] = min(max_tokens, spare)
                return request
            sent = request["messages"][0]["content"]
            keep = int(len(sent) * (window - floor - self.WINDOW_MARGIN) / count * 0.97)
            logger.warning(
                "clipping a %d character document to %d: its %d prompt tokens leave "
                "no room for an answer", len(sent), keep, count,
            )
            request = self.native_request(
                sent[:keep], model=model, max_tokens=max_tokens, context_window=context_window
            )
        return request

    def _prompt_tokens(self, request: Dict[str, Any]) -> Optional[int]:
        """The prompt's size by the server's tokenizer, or None if it cannot say."""
        base = str(self.client.base_url).rstrip("/")
        if base.endswith("/v1"):
            base = base[: -len("/v1")]
        body = {
            "model": request["model"],
            "messages": request["messages"],
            "add_generation_prompt": True,
            "chat_template_kwargs": request["extra_body"]["chat_template_kwargs"],
        }
        try:
            response = httpx.post(f"{base}/tokenize", json=body, timeout=60)
            response.raise_for_status()
            return int(response.json()["count"])
        except Exception as exc:  # noqa: BLE001 - any failure falls back to the estimate
            logger.debug("no token count from %s/tokenize: %s", base, exc)
            return None

    def parse_analyses(
        self,
        prompt: str,
        *,
        model: Optional[str] = None,
    ) -> ParseAnalysesOutput:
        """Parse a neuroimaging table into structured analyses."""
        resolved_model = model or self.default_model
        function_schema = self._generate_function_schema(
            ParseAnalysesOutput,
            "parse_analyses",
        )
        # Some reasoning models reject function tools unless reasoning is
        # switched off: "Function tools with reasoning_effort are not supported
        # ... set reasoning_effort to 'none'". Only sent when configured, since
        # models that do not know the parameter reject it in turn.
        extra = {}
        effort = getattr(self.settings, "llm_reasoning_effort", None)
        if effort:
            extra["reasoning_effort"] = effort
        # Flex runs on spare capacity: about half price, but it queues, and
        # returns 429 resource_unavailable rather than throttling.
        tier = getattr(self.settings, "llm_service_tier", None)
        if tier:
            extra["service_tier"] = tier

        response = self.client.chat.completions.create(
            model=resolved_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a helpful assistant that parses neuroimaging "
                        "results tables into structured JSON for downstream analysis. "
                        "Respond using the parse_analyses function."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            functions=[function_schema],
            function_call={"name": "parse_analyses"},
            **extra,
        )
        function_call = response.choices[0].message.function_call
        if not function_call:
            raise ValueError("No function call returned from API")

        result_dict = json.loads(function_call.arguments)
        analyses = result_dict.get("analyses", [])
        if not isinstance(analyses, list):
            analyses = []

        cleaned_analyses: List[Dict[str, Any]] = []
        for analysis in analyses:
            if not isinstance(analysis, dict):
                logger.warning("Skipping non-dict analysis from LLM output: %r", analysis)
                continue

            valid_points: List[Dict[str, Any]] = []
            for point in analysis.get("points", []):
                if not isinstance(point, dict):
                    logger.debug("Skipping non-dict point from LLM output: %r", point)
                    continue

                coordinates = point.get("coordinates")
                if (
                    isinstance(coordinates, list)
                    and len(coordinates) == 3
                    and all(isinstance(coord, (int, float)) for coord in coordinates)
                ):
                    # A flag the schema no longer has (`is_deactivation`,
                    # `is_seed`) would fail the whole table, not one field.
                    valid_points.append({k: v for k, v in point.items() if k in _POINT_FIELDS})

            analysis["points"] = valid_points
            cleaned_analyses.append(analysis)

        result_dict["analyses"] = cleaned_analyses
        _coerce_point_values_schema(result_dict)
        try:
            return ParseAnalysesOutput(**result_dict)
        except Exception as exc:  # noqa: BLE001
            logger.warning("LLM output validation failed; skipping table: %s", exc)
            logger.debug("Result payload: %s", result_dict)
            return ParseAnalysesOutput(analyses=[])


def _coerce_point_values_schema(payload: Dict[str, Any]) -> None:
    analyses = payload.get("analyses")
    if not isinstance(analyses, list):
        return
    for analysis in analyses:
        if not isinstance(analysis, dict):
            continue
        points = analysis.get("points")
        if not isinstance(points, list):
            continue
        for point in points:
            if not isinstance(point, dict):
                continue
            values = point.get("values")
            if not isinstance(values, list):
                continue
            coerced: List[Dict[str, Any]] = []
            for value in values:
                if isinstance(value, dict):
                    normalized = _normalize_value_dict(value)
                    if normalized:
                        coerced.append(normalized)
                    continue
                if isinstance(value, (int, float)):
                    kind = normalize_statistic_kind("t")
                    coerced.append({"value": float(value), "kind": kind})
                    continue
                # attempt to parse numeric strings
                if isinstance(value, str):
                    try:
                        num = float(value)
                    except ValueError:
                        continue
                    kind = normalize_statistic_kind("t")
                    coerced.append({"value": num, "kind": kind})
                    continue
            if coerced:
                point["values"] = coerced


def _normalize_value_dict(value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    normalized_kind = normalize_statistic_kind(value.get("kind"))
    if normalized_kind is None:
        return None
    number = value.get("value")
    if not isinstance(number, (int, float)):
        return None
    return {"value": float(number), "kind": normalized_kind}


__all__ = ["CoordinateParsingClient"]
