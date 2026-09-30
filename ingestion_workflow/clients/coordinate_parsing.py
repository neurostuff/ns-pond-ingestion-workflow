"""LLM client for coordinate parsing tasks."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import ParseAnalysesOutput
from ingestion_workflow.models.statistics import normalize_statistic_kind
from ingestion_workflow.services.nuextract_payload import parse_payload


logger = logging.getLogger(__name__)


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
                            ["T", "Z", "F", "P", "R", "B"],
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

    def parse_analyses_native(
        self,
        document: str,
        *,
        model: Optional[str] = None,
        max_tokens: int = 4096,
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
        asking for 4,096 tokens beside a 12,000 token prompt is refused with a
        400 rather than truncated -- so the request would fail on precisely
        the richest tables.
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
        allowed = max(floor, min(max_tokens, window - len(document) // 3 - 512))
        response = self.client.chat.completions.create(
            model=model or self.default_model,
            messages=[{"role": "user", "content": document}],
            temperature=0.0,
            max_completion_tokens=allowed,
            extra_body={
                "chat_template_kwargs": {
                    "template": self.NATIVE_TEMPLATE,
                    "enable_thinking": False,
                }
            },
        )
        return parse_payload(response.choices[0].message.content or "")

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
                    valid_points.append(point)

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
