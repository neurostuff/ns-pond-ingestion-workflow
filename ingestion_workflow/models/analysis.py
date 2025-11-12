"""Data models for NIMADS-formatted analyses and coordinates."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


class CoordinateSpace(str, Enum):
    """Coordinate space for stereotactic coordinates."""

    MNI = "MNI"
    TALAIRACH = "TAL"
    OTHER = "OTHER"


@dataclass
class Coordinate:
    """
    A single stereotactic coordinate point.

    Represents a single activation or deactivation peak in 3D space.
    """

    # X coordinate
    x: float

    # Y coordinate
    y: float

    # Z coordinate
    z: float

    # Coordinate space (MNI, Talairach, etc.)
    space: CoordinateSpace = CoordinateSpace.MNI

    # Statistical value at this coordinate (t, z, F, etc.)
    statistic_value: Optional[float] = None

    # Type of statistic (t, z, F, etc.)
    statistic_type: Optional[str] = None

    # Size of cluster this coordinate belongs to (in mm³)
    cluster_size: Optional[int] = None

    # Whether this is a subpeak within a cluster
    is_subpeak: bool = False

    # Whether this represents a deactivation
    is_deactivation: bool = False


@dataclass
class Condition:
    """
    A condition in a contrast.

    In NIMADS format, a contrast is defined by conditions and their weights.
    """

    # Name of the condition
    name: str

    # Detailed description of the condition
    description: Optional[str] = None


@dataclass
class Contrast:
    """
    A statistical contrast in an analysis.

    Represents a comparison between conditions, e.g., "task > baseline".
    """

    # Name of the contrast
    name: str

    # List of condition names involved in this contrast
    conditions: List[str] = field(default_factory=list)

    # Weights for each condition in the contrast
    weights: List[float] = field(default_factory=list)

    # Description of what this contrast tests
    description: Optional[str] = None


@dataclass
class Image:
    """
    Reference to a statistical brain image.

    In NIMADS, images can be referenced by URL or stored locally.
    """

    # URL to the image file
    url: Optional[str] = None

    # Local file path to the image
    local_path: Optional[str] = None

    # Type of image (statistic_map, contrast_map, etc.)
    image_type: str = "statistic_map"

    # Coordinate space of the image
    space: Optional[CoordinateSpace] = None


@dataclass
class Analysis:
    """
    A single analysis extracted from a table.

    This represents one row or group of rows from a coordinate table,
    typically corresponding to a single contrast or comparison.
    """

    # Name of the analysis
    name: str

    # Description of the analysis
    description: Optional[str] = None

    # List of coordinate points
    coordinates: List[Coordinate] = field(default_factory=list)

    # Contrasts tested in this analysis
    contrasts: List[Contrast] = field(default_factory=list)

    # Associated statistical images
    images: List[Image] = field(default_factory=list)

    # ID of the source table
    table_id: Optional[str] = None

    # Table number in the article
    table_number: Optional[int] = None

    # Caption of the source table
    table_caption: str = ""

    # Footer notes from the source table
    table_footer: str = ""

    # Additional metadata about the analysis
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_neurostore_format(self) -> Dict[str, Any]:
        """
        Convert analysis to Neurostore API format.

        Returns
        -------
        dict
            Analysis formatted for Neurostore upload
        """
        raise NotImplementedError()


@dataclass
class AnalysisCollection:
    """
    Collection of analyses for a single article.

    This typically corresponds to the contents of an analyses.json file
    in the NIMADS format.
    """

    # Hash ID of the source article
    hash_id: str

    # List of analyses
    analyses: List[Analysis] = field(default_factory=list)

    # Primary coordinate space used in these analyses
    coordinate_space: CoordinateSpace = CoordinateSpace.MNI

    def add_analysis(self, analysis: Analysis) -> None:
        """
        Add an analysis to the collection.

        Parameters
        ----------
        analysis : Analysis
            Analysis to add
        """
        raise NotImplementedError()


@dataclass
class CreateAnalysesResult:
    """Pipeline payload for the create-analyses workflow."""

    hash_id: str
    analysis_paths: List[Path] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hash_id": self.hash_id,
            "analysis_paths": [str(path) for path in self.analysis_paths],
            "metadata": dict(self.metadata),
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "CreateAnalysesResult":
        return cls(
            hash_id=str(payload["hash_id"]),
            analysis_paths=[
                Path(str(path))
                for path in payload.get("analysis_paths", [])
            ],
            metadata=dict(payload.get("metadata", {})),
            error_message=payload.get("error_message") or None,
        )

    def to_neurostore_format(self) -> Dict[str, Any]:
        """
        Convert entire collection to Neurostore API format.

        Returns
        -------
        dict
            Collection formatted for Neurostore upload
        """
        raise NotImplementedError()

    @classmethod
    def from_llm_response(
        cls,
        hash_id: str,
        llm_response: Dict[str, Any],
    ) -> AnalysisCollection:
        """
        Parse an LLM response into an AnalysisCollection.

        Parameters
        ----------
        hash_id : str
            Hash ID of the source article
        llm_response : dict
            Structured response from the LLM

        Returns
        -------
        AnalysisCollection
            Parsed analysis collection
        """
        raise NotImplementedError()
