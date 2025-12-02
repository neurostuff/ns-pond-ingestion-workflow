"""LLM client for coordinate parsing tasks."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import ParseAnalysesOutput


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
        )
        function_call = response.choices[0].message.function_call
        if not function_call:
            raise ValueError("No function call returned from API")

        result_dict = json.loads(function_call.arguments)
        for analysis in result_dict.get("analyses", []):
            valid_points: List[Dict[str, Any]] = []
            for point in analysis.get("points", []):
                coordinates = point.get("coordinates")
                if (
                    isinstance(coordinates, list)
                    and len(coordinates) == 3
                    and all(isinstance(coord, (int, float)) for coord in coordinates)
                ):
                    valid_points.append(point)
            analysis["points"] = valid_points
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
        points = analysis.get("points")
        if not isinstance(points, list):
            continue
        for point in points:
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
                    kind = "T" if isinstance(value, float) else "other"
                    coerced.append({"value": value, "kind": kind})
                    continue
                # attempt to parse numeric strings
                if isinstance(value, str):
                    try:
                        num = float(value)
                    except ValueError:
                        continue
                    kind = "T"
                    if num.is_integer():
                        kind = "other"
                        num = int(num)
                    coerced.append({"value": num, "kind": kind})
                    continue
            if coerced:
                point["values"] = coerced


def _normalize_value_dict(value: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    normalized_kind = _map_kind(value.get("kind"))
    if normalized_kind is None:
        return None
    result = dict(value)
    result["kind"] = normalized_kind
    return result


def _map_kind(kind: Any) -> Optional[str]:
    if kind is None:
        return None
    if isinstance(kind, str):
        normalized = kind.strip().lower()
        allowed = {
            "Z",
            "T",
            "F",
            "R",
            "P",
            "B",
            "other",
        }
        if normalized in allowed:
            return normalized
        if "z" in normalized:
            return "Z"
        if "t" in normalized or "stat" in normalized:
            return "T"
        if "f" in normalized:
            return "F"
        if "r" in normalized or "correlation" in normalized:
            return "R"
        if normalized.startswith("p"):
            return "P"
        if "beta" in normalized:
            return "B"
        return "other"
    return None


__all__ = ["CoordinateParsingClient"]
