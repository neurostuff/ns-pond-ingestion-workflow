"""The stage1 parse ns-pond writes must be what pondie reads."""

from __future__ import annotations

import json

from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    Coordinate,
    CoordinateSpace,
    Identifier,
)
from ingestion_workflow.workflow.sync import _write_stage1


def _collection() -> AnalysisCollection:
    collection = AnalysisCollection(
        slug="study::t1",
        coordinate_space=CoordinateSpace.MNI,
        identifier=Identifier(pmid="12345"),
    )
    collection.add_analysis(
        Analysis(
            name="patients > controls",
            description="group contrast",
            coordinates=[
                Coordinate(
                    x=-37.0,
                    y=-57.0,
                    z=44.0,
                    space=CoordinateSpace.MNI,
                    statistic_value=4.63,
                    statistic_type="T",
                    cluster_size=1225,
                    cluster_measure="voxels",
                ),
                Coordinate(x=45.0, y=-79.0, z=-18.0, space=CoordinateSpace.MNI),
            ],
            table_id="tbl0005",
            table_number=3,
            table_caption="Peak coordinates",
            table_footer="MNI space",
        )
    )
    return collection


def test_stage1_written_in_pondies_shape(tmp_path):
    path = tmp_path / "stage1" / "analyses.json"
    _write_stage1(path, {"tbl0005": _collection()}, overwrite=True)

    document = json.loads(path.read_text(encoding="utf-8"))
    assert list(document) == ["analyses"]

    analysis = document["analyses"][0]
    assert analysis["name"] == "patients > controls"
    assert analysis["table_id"] == "tbl0005"
    assert analysis["table_caption"] == "Peak coordinates"

    first, second = analysis["points"]
    # Nested under "coordinates": the shape pondie's ParsedAnalysis reads.
    assert first["coordinates"] == [-37.0, -57.0, 44.0]
    assert first["space"] == "MNI"
    assert first["values"] == [{"value": 4.63, "kind": "T"}]
    assert first["cluster_size"] == 1225
    # A point with no statistic carries no values key rather than a null one.
    assert "values" not in second


def test_stage1_points_parse_back_out_as_coordinates(tmp_path):
    """Round-trip through the same accessor logic pondie applies."""
    path = tmp_path / "stage1" / "analyses.json"
    _write_stage1(path, {"tbl0005": _collection()}, overwrite=True)
    document = json.loads(path.read_text(encoding="utf-8"))

    recovered = []
    for analysis in document["analyses"]:
        for point in analysis.get("points") or analysis.get("coordinates") or []:
            coords = point.get("coordinates")
            if isinstance(coords, dict):
                coords = [coords.get("x"), coords.get("y"), coords.get("z")]
            if isinstance(coords, (list, tuple)) and len(coords) == 3:
                recovered.append(tuple(float(value) for value in coords))

    assert recovered == [(-37.0, -57.0, 44.0), (45.0, -79.0, -18.0)]


def test_stage1_respects_overwrite_false(tmp_path):
    path = tmp_path / "stage1" / "analyses.json"
    path.parent.mkdir(parents=True)
    path.write_text("untouched", encoding="utf-8")

    _write_stage1(path, {"tbl0005": _collection()}, overwrite=False)

    assert path.read_text(encoding="utf-8") == "untouched"


def _bundle(source_value: str, pmid: str | None):
    from ingestion_workflow.models import (
        ArticleExtractionBundle,
        ArticleMetadata,
        DownloadSource,
        ExtractedContent,
    )

    return ArticleExtractionBundle(
        article_data=ExtractedContent(
            slug="s",
            source=DownloadSource(source_value),
            identifier=Identifier(pmid=pmid) if pmid else Identifier(doi="10.1/x"),
        ),
        article_metadata=ArticleMetadata(title="t"),
    )


def test_corpus_manifest_matches_pondies_three_column_form(tmp_path):
    from ingestion_workflow.workflow.sync import _write_corpus_manifest

    path = tmp_path / "pmids.tsv"
    _write_corpus_manifest(
        path,
        [("study-1", _bundle("pubget", "12345")), ("study-2", _bundle("elsevier", "67890"))],
    )

    rows = [line.split("\t") for line in path.read_text().strip().split("\n")]
    assert rows == [
        ["12345", "study-1", "pubget"],
        ["67890", "study-2", "elsevier"],
    ]


def test_corpus_manifest_keeps_study_id_column_when_pmid_is_missing(tmp_path):
    """pondie parses column 2; a DOI-only study must still yield a usable row."""
    from ingestion_workflow.workflow.sync import _write_corpus_manifest

    path = tmp_path / "pmids.tsv"
    _write_corpus_manifest(path, [("study-3", _bundle("pubget", None))])

    pmid, study_id, source = path.read_text().rstrip("\n").split("\t")
    assert pmid == ""
    assert study_id == "study-3"
    assert source == "pubget"
