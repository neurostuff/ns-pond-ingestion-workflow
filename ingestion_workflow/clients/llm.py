"""Generic LLM client utilities."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional, Type

from openai import OpenAI
from pydantic import BaseModel, TypeAdapter

from ingestion_workflow.config import Settings


class GenericLLMClient:
    """Generic helper around the OpenAI client with Settings integration."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None,
    ) -> None:
        self.settings = settings
        self.api_key = self._resolve_api_key(api_key)
        self.base_url = base_url or (
            settings.llm_api_base if settings and settings.llm_api_base else None
        )
        if not self.api_key:
            raise ValueError(
                "LLM API key must be provided via arguments, "
                "settings.llm_api_key, or environment variables.",
            )
        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        if default_model:
            self.default_model = default_model
        elif settings and settings.llm_model:
            self.default_model = settings.llm_model
        else:
            self.default_model = "gpt-5-mini"

    def _resolve_api_key(self, override: Optional[str]) -> Optional[str]:
        if override:
            return override
        if self.settings and self.settings.llm_api_key:
            return self.settings.llm_api_key
        for env_var in ("LLM_API_KEY", "OPENAI_API_KEY"):
            candidate = os.getenv(env_var)
            if candidate:
                return candidate
        return None

    def _generate_function_schema(
        self,
        model_class: Type[Any],
        function_name: str,
    ) -> Dict[str, Any]:
        """Generate an OpenAI function-call schema from a Pydantic model or dataclass."""

        def convert_field(field_info: Dict[str, Any]) -> Dict[str, Any]:
            result: Dict[str, Any] = {}
            if "type" in field_info:
                result["type"] = field_info["type"]
            if "description" in field_info:
                result["description"] = field_info["description"]
            if "enum" in field_info:
                result["enum"] = field_info["enum"]
            if "anyOf" in field_info:
                for option in field_info["anyOf"]:
                    enum_values = option.get("enum")
                    if enum_values:
                        result["enum"] = enum_values
                        if option.get("type"):
                            result["type"] = option["type"]
                        break
            if "items" in field_info:
                result["items"] = convert_field(field_info["items"])
            if "minItems" in field_info:
                result["minItems"] = field_info["minItems"]
            if "maxItems" in field_info:
                result["maxItems"] = field_info["maxItems"]
            if field_info.get("type") == "number":
                if "minimum" in field_info:
                    result["minimum"] = field_info["minimum"]
                if "maximum" in field_info:
                    result["maximum"] = field_info["maximum"]
            if "$ref" in field_info:
                result["$ref"] = field_info["$ref"]
            return result

        if isinstance(model_class, type) and issubclass(model_class, BaseModel):
            full_schema = model_class.model_json_schema(
                ref_template="#/$defs/{model}",
            )
        else:
            full_schema = TypeAdapter(model_class).json_schema(ref_template="#/$defs/{model}")
        properties = {
            field_name: convert_field(field_info)
            for field_name, field_info in full_schema.get("properties", {}).items()
        }
        return {
            "name": function_name,
            "description": f"Process a task using the {function_name} function",
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": full_schema.get("required", []),
                "$defs": full_schema.get("$defs", {}),
            },
        }


__all__ = ["GenericLLMClient"]
