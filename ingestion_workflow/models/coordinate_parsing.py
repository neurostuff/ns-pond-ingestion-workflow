"""Pydantic models describing the LLM coordinate parsing response."""

from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel, Field, field_validator
from typing_extensions import Literal


class PointsValue(BaseModel):
    """Represents a value associated with a coordinate point."""

    value: Optional[Union[float, str]] = Field(
        None,
        description=(
            "Optional statistical value associated with the point (e.g., t-value, z-value)"
        ),
    )
    kind: Optional[
        Literal[
            "z-statistic",
            "t-statistic",
            "f-statistic",
            "correlation",
            "p-value",
            "beta",
            "other",
        ]
    ] = Field(
        None,
        description="Type of the value",
    )


class CoordinatePoint(BaseModel):
    """Represents a single coordinate point with its space information."""

    coordinates: List[float] = Field(
        description="List of three floats representing [x, y, z] coordinates"
    )
    space: Optional[str] = Field(
        None,
        description='Coordinate space, either "MNI", "TAL", or null if unknown',
    )
    values: Optional[List[PointsValue]] = Field(
        None,
        description=("Optional list of statistical values associated with the point"),
    )

    @field_validator("coordinates")
    @classmethod
    def validate_coordinates(cls, value: List[float]) -> List[float]:
        """Ensure the coordinate triple is valid."""
        if not isinstance(value, list):
            raise ValueError("Coordinates must be a list")
        if len(value) != 3:
            raise ValueError("Coordinates must contain exactly 3 values [x, y, z]")
        for index, coord in enumerate(value):
            if not isinstance(coord, (int, float)):
                raise ValueError(f"Coordinate at index {index} must be a number")
        return [float(coord) for coord in value]


class ParsedAnalysis(BaseModel):
    """Represents a single analysis with its metadata and points."""

    name: Optional[str] = None
    description: Optional[str] = None
    points: List[CoordinatePoint]


class ParseAnalysesOutput(BaseModel):
    """Output schema for the parse_analyses function."""

    analyses: List[ParsedAnalysis]


__all__ = [
    "CoordinatePoint",
    "ParsedAnalysis",
    "ParseAnalysesOutput",
    "PointsValue",
]
