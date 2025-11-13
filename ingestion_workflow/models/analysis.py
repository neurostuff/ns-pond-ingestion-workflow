"""Data models for parsed analyses and cache payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .ids import Identifier


class CoordinateSpace(str, Enum):
    """Coordinate space for stereotactic coordinates."""

    MNI = "MNI"
    TALAIRACH = "TAL"
    OTHER = "OTHER"


@dataclass
class Coordinate:
    """Single stereotactic coordinate point."""

    x: float
    y: float
    z: float
    space: CoordinateSpace = CoordinateSpace.MNI
    statistic_value: Optional[float] = None
    statistic_type: Optional[str] = None
    cluster_size: Optional[int] = None
    is_subpeak: bool = False
    is_deactivation: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "x": self.x,
            "y": self.y,
            "z": self.z,
            "space": self.space.value,
            "statistic_value": self.statistic_value,
            "statistic_type": self.statistic_type,
            "cluster_size": self.cluster_size,
            "is_subpeak": self.is_subpeak,
            "is_deactivation": self.is_deactivation,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Coordinate":
        space_value = payload.get("space", CoordinateSpace.MNI.value)
        return cls(
            x=float(payload["x"]),
            y=float(payload["y"]),
            z=float(payload["z"]),
            space=CoordinateSpace(space_value),
            statistic_value=payload.get("statistic_value"),
            statistic_type=payload.get("statistic_type"),
            cluster_size=payload.get("cluster_size"),
            is_subpeak=bool(payload.get("is_subpeak", False)),
            is_deactivation=bool(payload.get("is_deactivation", False)),
        )


@dataclass
class Condition:
    """Condition participating in a contrast."""

    name: str
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "description": self.description}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Condition":
        return cls(
            name=str(payload["name"]),
            description=payload.get("description"),
        )


@dataclass
class Contrast:
    """Statistical contrast definition."""

    name: str
    conditions: List[str] = field(default_factory=list)
    weights: List[float] = field(default_factory=list)
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "conditions": list(self.conditions),
            "weights": list(self.weights),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Contrast":
        return cls(
            name=str(payload["name"]),
            conditions=[str(value) for value in payload.get("conditions", [])],
            weights=[float(value) for value in payload.get("weights", [])],
            description=payload.get("description"),
        )


@dataclass
class Image:
    """Reference to a statistical brain image."""

    url: Optional[str] = None
    local_path: Optional[str] = None
    image_type: str = "statistic_map"
    space: Optional[CoordinateSpace] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "local_path": self.local_path,
            "image_type": self.image_type,
            "space": self.space.value if self.space else None,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Image":
        space = payload.get("space")
        return cls(
            url=payload.get("url"),
            local_path=payload.get("local_path"),
            image_type=str(payload.get("image_type", "statistic_map")),
            space=CoordinateSpace(space) if space else None,
        )


@dataclass
class Analysis:
    """Single analysis extracted from a coordinate table."""

    name: str
    description: Optional[str] = None
    coordinates: List[Coordinate] = field(default_factory=list)
    contrasts: List[Contrast] = field(default_factory=list)
    images: List[Image] = field(default_factory=list)
    table_id: Optional[str] = None
    table_number: Optional[int] = None
    table_caption: str = ""
    table_footer: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "coordinates": [coord.to_dict() for coord in self.coordinates],
            "contrasts": [contrast.to_dict() for contrast in self.contrasts],
            "images": [image.to_dict() for image in self.images],
            "table_id": self.table_id,
            "table_number": self.table_number,
            "table_caption": self.table_caption,
            "table_footer": self.table_footer,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "Analysis":
        return cls(
            name=str(payload["name"]),
            description=payload.get("description"),
            coordinates=[Coordinate.from_dict(item) for item in payload.get("coordinates", [])],
            contrasts=[Contrast.from_dict(item) for item in payload.get("contrasts", [])],
            images=[Image.from_dict(item) for item in payload.get("images", [])],
            table_id=payload.get("table_id"),
            table_number=payload.get("table_number"),
            table_caption=str(payload.get("table_caption", "")),
            table_footer=str(payload.get("table_footer", "")),
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass
class AnalysisCollection:
    """Collection of analyses for a single table or article."""

    slug: str
    analyses: List[Analysis] = field(default_factory=list)
    coordinate_space: CoordinateSpace = CoordinateSpace.MNI
    identifier: Optional[Identifier] = None

    def add_analysis(self, analysis: Analysis) -> None:
        self.analyses.append(analysis)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slug": self.slug,
            "coordinate_space": self.coordinate_space.value,
            "analyses": [analysis.to_dict() for analysis in self.analyses],
            "identifier": (self.identifier.__dict__.copy() if self.identifier else None),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AnalysisCollection":
        identifier_payload = payload.get("identifier")
        identifier = Identifier(**identifier_payload) if identifier_payload is not None else None
        slug = payload.get("slug") or ""
        return cls(
            slug=str(slug),
            analyses=[Analysis.from_dict(item) for item in payload.get("analyses", [])],
            coordinate_space=CoordinateSpace(
                payload.get("coordinate_space", CoordinateSpace.MNI.value)
            ),
            identifier=identifier,
        )


@dataclass
class CreateAnalysesResult:
    """Cache payload for a parsed table's analyses."""

    slug: str
    article_slug: str
    table_id: str
    sanitized_table_id: str
    analysis_collection: AnalysisCollection
    analysis_paths: List[Path] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slug": self.slug,
            "article_slug": self.article_slug,
            "table_id": self.table_id,
            "sanitized_table_id": self.sanitized_table_id,
            "analysis_collection": self.analysis_collection.to_dict(),
            "analysis_paths": [str(path) for path in self.analysis_paths],
            "metadata": dict(self.metadata),
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CreateAnalysesResult":
        paths = [Path(str(item)) for item in payload.get("analysis_paths", [])]
        slug = payload.get("slug") or ""
        article_slug = payload.get("article_slug") or ""
        return cls(
            slug=str(slug),
            article_slug=str(article_slug),
            table_id=str(payload.get("table_id", "")),
            sanitized_table_id=str(payload.get("sanitized_table_id", "")),
            analysis_collection=AnalysisCollection.from_dict(payload["analysis_collection"]),
            analysis_paths=paths,
            metadata=dict(payload.get("metadata", {})),
            error_message=payload.get("error_message"),
        )


__all__ = [
    "Analysis",
    "AnalysisCollection",
    "Condition",
    "Contrast",
    "Coordinate",
    "CoordinateSpace",
    "CreateAnalysesResult",
    "Image",
]
