"""An ns-pond record must survive a write/read round trip.

The layout is a contract with pondie, and before this there was no reader at
all, so nothing could check that what sync writes is what anyone can get back.
"""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.models import (
    Analysis,
    AnalysisCollection,
    ArticleExtractionBundle,
    Coordinate,
    CoordinateSpace,
    DownloadSource,
    ExtractedContent,
    ExtractedTable,
    Identifier,
)
from ingestion_workflow.models.metadata import ArticleMetadata, Author
from ingestion_workflow.services import nspond
from ingestion_workflow.services.nspond_schema import (
    encode_csv,
    encode_jsonl,
    encode_pretty_json,
    encode_stage1_json,
    read_record,
)

BASE = "22tHjbNRU8t2"


@pytest.fixture()
def written(tmp_path):
    identifier = Identifier(
        pmid="22848644", pmcid="PMC3407125", doi="10.1371/journal.pone.0041873"
    )
    # coordinates.csv is built from the table's coordinates, not the analysis'
    # -- the two carry the same points by different routes.
    table = ExtractedTable(
        table_id="tbl1",
        raw_content_path=tmp_path / "tbl1.html",
        coordinates=[
            Coordinate(x=-42.0, y=18.0, z=6.0),
            Coordinate(x=22.0, y=-54.0, z=-24.0),
        ],
    )
    content = ExtractedContent(
        slug=identifier.slug, source=DownloadSource.ACE, identifier=identifier, tables=[table]
    )
    metadata = ArticleMetadata(
        title="Audiovisual integration in older adults",
        authors=[Author(name="Jones, S"), Author(name="Noppeney, U")],
        journal="PLoS ONE",
        publication_year=2012,
        abstract="Naïve café rôle — non-ASCII on purpose.",
    )
    collection = AnalysisCollection(
        slug="tbl1",
        identifier=identifier,
        coordinate_space=CoordinateSpace.MNI,
        analyses=[
            Analysis(
                name="contrast 1",
                table_id="tbl1",
                coordinates=[
                    Coordinate(x=-42.0, y=18.0, z=6.0),
                    Coordinate(x=22.0, y=-54.0, z=-24.0),
                ],
            )
        ],
    )
    root = tmp_path / "pond"
    nspond.write_article(
        root, BASE, ArticleExtractionBundle(content, metadata), {"tbl1": collection}, []
    )
    return root


def test_the_record_reads_back(written):
    record = read_record(written, BASE)
    assert record.base_study_id == BASE
    assert record.identifiers["pmid"] == "22848644"
    assert set(record.processed) == {"ace"}


def test_metadata_survives(written):
    processed = read_record(written, BASE).processed["ace"]
    assert processed.metadata["title"] == "Audiovisual integration in older adults"
    assert processed.metadata["authors"] == "Jones, S; Noppeney, U"
    assert processed.metadata["publication_year"] == 2012
    assert "Naïve café rôle" in processed.metadata["abstract"]


def test_analyses_and_tables_survive(written):
    processed = read_record(written, BASE).processed["ace"]
    assert len(processed.tables) == 1
    assert processed.tables[0]["table_id"] == "tbl1"
    assert len(processed.analyses) == 1
    assert processed.analyses[0]["coordinate_space"] == "MNI"


def test_coordinates_survive_as_rows(written):
    processed = read_record(written, BASE).processed["ace"]
    assert [(r["x"], r["y"], r["z"]) for r in processed.coordinates] == [
        ("-42.0", "18.0", "6.0"),
        ("22.0", "-54.0", "-24.0"),
    ]


def test_stage1_keeps_the_shape_pondie_reads(written):
    """pondie reads each point's xyz from a nested `coordinates` key."""
    stage1 = read_record(written, BASE).stage1
    point = stage1["analyses"][0]["points"][0]
    assert point["coordinates"] == [-42.0, 18.0, 6.0]
    assert point["space"] == "MNI"


def test_a_stored_placeholder_is_not_synced(tmp_path):
    """pondie lists every zero-point entry as a null contrast, and analyses
    stored before the analyses stage dropped the placeholder still hold one."""
    identifier = Identifier(pmid="1")
    content = ExtractedContent(slug=identifier.slug, source=DownloadSource.PUBGET,
                               identifier=identifier)
    tables = AnalysisCollection(slug="s", identifier=identifier, analyses=[
        Analysis(name="UNKNOWN", table_id="t1"),
        Analysis(name="A > B", table_id="t1"),
        Analysis(name="UNKNOWN", table_id="t2",
                 coordinates=[Coordinate(x=1.0, y=2.0, z=3.0)])])
    root = tmp_path / "pond"
    nspond.write_article(root, BASE, ArticleExtractionBundle(content, ArticleMetadata(title="T")),
                         {"t1": tables}, [])
    record = read_record(root, BASE)
    assert [(a["name"], len(a["points"])) for a in record.stage1["analyses"]] == [
        ("A > B", 0), ("UNKNOWN", 1)]
    assert [a["name"] for a in record.processed["pubget"].analyses] == ["A > B", "UNKNOWN"]


def test_stage1_points_carry_sign_and_subpeak_but_no_retired_flag(written):
    """A point's sign is read from its statistic; `unsigned` marks one placed
    in the positive half with no statistic to place it by."""
    for analysis in read_record(written, BASE).stage1["analyses"]:
        for point in analysis["points"]:
            assert point["sign"] in {"positive", "negative", "unsigned"}
            assert isinstance(point["is_subpeak"], bool)
            assert not {"is_deactivation", "deactivation", "is_seed"} & set(point)


def test_a_prose_analysis_keeps_its_role_in_stage1(tmp_path):
    """A table analysis keeps the shape pondie reads; a prose one says what it is."""
    identifier = Identifier(pmid="1")
    content = ExtractedContent(slug=identifier.slug, source=DownloadSource.PUBGET, identifier=identifier)
    prose = AnalysisCollection(slug="s", identifier=identifier, coordinate_space=CoordinateSpace.MNI, analyses=[
        Analysis(name="amygdala seed", table_id="prose", metadata={"source": "prose", "role": "seed"},
                 coordinates=[Coordinate(x=-22.0, y=-4.0, z=-18.0)]),
        Analysis(name="faces > houses", table_id="prose", metadata={"source": "prose", "role": "result"},
                 coordinates=[Coordinate(x=40.0, y=-50.0, z=-20.0)])])
    root = tmp_path / "pond"
    nspond.write_article(root, BASE, ArticleExtractionBundle(content, ArticleMetadata(title="T")), {"prose": prose}, [])
    got = [(a["source"], a["role"], a["table_id"]) for a in read_record(root, BASE).stage1["analyses"]]
    assert got == [("prose", "seed", "prose"), ("prose", "result", "prose")]


def test_a_table_analysis_has_no_prose_keys_in_stage1(written):
    assert "role" not in read_record(written, BASE).stage1["analyses"][0]


def test_a_missing_text_file_is_not_an_error(written):
    """Not every article has full text, and its absence is ordinary."""
    assert read_record(written, BASE).processed["ace"].text is None


# -- the encoders are the contract, so pin their bytes ------------------------


def test_pretty_json_has_no_trailing_newline_and_escapes_non_ascii():
    assert encode_pretty_json({"x": "é"}) == json.dumps({"x": "é"}, indent=2).encode()
    assert not encode_pretty_json({"x": 1}).endswith(b"\n")


def test_stage1_json_keeps_indent_one_and_raw_non_ascii():
    out = encode_stage1_json({"x": "é"})
    assert out.endswith(b"\n")
    assert "é".encode() in out
    assert out.startswith(b'{\n "x"')


def test_jsonl_is_one_object_per_line():
    assert encode_jsonl([{"a": 1}, {"b": 2}]) == b'{"a": 1}\n{"b": 2}\n'


def test_an_empty_jsonl_is_an_empty_file():
    assert encode_jsonl([]) == b""


def test_csv_headers_are_the_ones_given():
    out = encode_csv([{"b": 2, "a": 1}], ["a", "b"]).decode()
    assert out.splitlines()[0] == "a,b"
    assert out.splitlines()[1] == "1,2"
