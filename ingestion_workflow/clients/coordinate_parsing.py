"""LLM client for coordinate parsing tasks."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import ParseAnalysesOutput
from ingestion_workflow.models.statistics import normalize_statistic_kind

logger = logging.getLogger(__name__)



def _is_flex_exhausted(error: BaseException) -> bool:
    """A 429 that means "no flex capacity", not "you are going too fast"."""
    text = str(error)
    return "429" in text and "flex" in text.lower()

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

    def _create(self, model: str, prompt: str, function_schema: dict, extra: dict):
        """Send the call, and do not lose it when flex has no capacity.

        Flex is scheduled on spare capacity and answers
        ``429 Flex does not have sufficient resources`` when there is none --
        a refusal, not a throttle, so retrying flex only spends the budget
        again. Dropping to the default tier costs more per token and finishes
        the work; observed at 22.7s on flex against 1.1s on the default when
        capacity was tight.
        """
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that parses neuroimaging "
                    "results tables into structured JSON for downstream analysis. "
                    "Respond using the parse_analyses function."
                ),
            },
            {"role": "user", "content": prompt},
        ]
        kwargs = dict(
            model=model,
            messages=messages,
            functions=[function_schema],
            function_call={"name": "parse_analyses"},
            **extra,
        )
        try:
            return self.client.chat.completions.create(**kwargs)
        except Exception as exc:
            fallback = getattr(self.settings, "llm_tier_fallback", True)
            if not (fallback and extra.get("service_tier") and _is_flex_exhausted(exc)):
                raise
            logger.warning("flex has no capacity; retrying on the default tier")
            kwargs.pop("service_tier", None)
            return self.client.chat.completions.create(**kwargs)

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

        response = self._create(resolved_model, prompt, function_schema, extra)
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
