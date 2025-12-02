"""LLM client for coordinate parsing tasks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openai import RateLimitError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion_workflow.clients.llm import GenericLLMClient
from ingestion_workflow.config import Settings
from ingestion_workflow.models import ParseAnalysesOutput
from ingestion_workflow.models.statistics import normalize_statistic_kind


logger = logging.getLogger(__name__)


@dataclass
class FunctionCallResult:
    name: str
    arguments: str


class CoordinateParsingClient(GenericLLMClient):
    """Client responsible for parsing coordinate tables via LLM."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None,
        use_flex_processing: Optional[bool] = None,
        retry_attempts: int = 5,
    ) -> None:
        super().__init__(
            settings,
            api_key=api_key,
            base_url=base_url,
            default_model=default_model,
        )
        config = settings or Settings()
        self.use_flex_processing = (
            use_flex_processing
            if use_flex_processing is not None
            else getattr(config, "openai_flex_processing", True)
        )
        self._retry_attempts = retry_attempts

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
        function_call = self._call_function(
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
            function_schema=function_schema,
            model=resolved_model,
        )
        if not function_call:
            raise ValueError("No function call returned from API")

        result_dict = json.loads(_strip_markdown_fence(function_call.arguments))
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

    def _call_function(
        self,
        *,
        messages: List[Dict[str, Any]],
        function_schema: Dict[str, Any],
        model: str,
    ) -> Optional[FunctionCallResult]:
        tool_name = function_schema["name"]
        tool_parameters = function_schema.get("parameters", {})
        tool_description = function_schema.get("description", "")
        tool_chat = {
            "type": "function",
            "function": {
                "name": tool_name,
                "description": tool_description,
                "parameters": tool_parameters,
            },
        }
        tool_flex = {
            "type": "function",
            "name": tool_name,
            "function": {
                "name": tool_name,
                "description": tool_description,
                "parameters": tool_parameters,
            },
        }

        @retry(
            retry=retry_if_exception_type(RateLimitError),
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=120),
            reraise=True,
        )
        def _send_request() -> Optional[FunctionCallResult]:
            if self.use_flex_processing:
                response = self.client.responses.create(
                    model=model,
                    input=messages,
                    tools=[tool_flex],
                    tool_choice={"type": "function", "name": tool_name},
                )
                return self._extract_flex_output(response)

            completion = self.client.chat.completions.create(
                model=model,
                messages=messages,
                tools=[tool_chat],
                tool_choice={"type": "function", "function": {"name": tool_name}},
            )
            return self._extract_chat_tool_call(completion)

        return _send_request()

    def _extract_flex_output(self, response: Any) -> Optional[FunctionCallResult]:
        text_parts: List[str] = []
        for output in getattr(response, "output", []):
            contents = getattr(output, "content", [])
            for content in contents:
                ctype = getattr(content, "type", None)
                if ctype in {"function_call", "tool_call"}:
                    func = getattr(content, "function", None) or getattr(content, "tool_call", None)
                    if func and getattr(func, "name", None) and getattr(func, "arguments", None):
                        return FunctionCallResult(name=func.name, arguments=func.arguments)
                if ctype == "tool_use":
                    tool_name = getattr(content, "name", None)
                    tool_input = getattr(content, "input", None)
                    if tool_name and tool_input is not None:
                        try:
                            arguments = json.dumps(tool_input)
                        except Exception:
                            arguments = str(tool_input)
                        return FunctionCallResult(name=tool_name, arguments=arguments)
                if ctype == "output_text":
                    text = getattr(content, "text", None)
                    if text:
                        text_parts.append(text)
        if text_parts:
            return FunctionCallResult(name="", arguments="".join(text_parts).strip())
        return None

    def _extract_chat_tool_call(self, completion: Any) -> Optional[FunctionCallResult]:
        choices = getattr(completion, "choices", [])
        if not choices:
            return None
        message = getattr(choices[0], "message", None)
        tool_calls = getattr(message, "tool_calls", None)
        if not tool_calls:
            return None
        call = tool_calls[0]
        func = getattr(call, "function", None)
        if func and getattr(func, "name", None) and getattr(func, "arguments", None):
            return FunctionCallResult(name=func.name, arguments=func.arguments)
        return None


def _coerce_point_values_schema(payload: Dict[str, Any]) -> None:
    analyses = payload.get("analyses")
    if not isinstance(analyses, list):
        return
    for analysis in analyses:
        if "analysis_name" in analysis and "name" not in analysis:
            analysis["name"] = analysis.pop("analysis_name")
        if "coordinates" in analysis and "points" not in analysis:
            coords = analysis.pop("coordinates")
            if isinstance(coords, dict) and {"X", "Y", "Z"} <= set(coords.keys()):
                analysis["points"] = [
                    {"coordinates": [coords.get("X"), coords.get("Y"), coords.get("Z")]}
                ]
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
                    kind = normalize_statistic_kind("t")
                    coerced.append({"value": float(value), "kind": kind})
                    continue
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


def _strip_markdown_fence(payload: str) -> str:
    text = payload.strip()
    if text.startswith("```"):
        # remove leading/trailing fenced blocks (``` or ```json)
        parts = text.split("```")
        if len(parts) >= 3:
            text = parts[1].lstrip("json").strip()
        else:
            text = text.strip("`")
    return text


__all__ = ["CoordinateParsingClient"]
