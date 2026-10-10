"""The parsed paper and the coordinate parse, against study_schema's contract."""

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
from ingestion_workflow.services import nspond, paper_parse
from ingestion_workflow.services.paper_parse import ParseInputs, statistic_kind
from study_schema import keys
from study_schema.jsonschema import load

BASE = "22tHjbNRU8t2"

TABLE_1 = """<table>
<tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>t</th><th>k</th></tr>
<tr><td>Faces &gt; Houses</td><td></td><td></td><td></td><td></td><td></td></tr>
<tr><td>Fusiform gyrus</td><td>&#8722;42</td><td>&#8722;55</td><td>&#8722;18</td>
<td>6.1</td><td>120</td></tr>
<tr><td>Amygdala</td><td>22</td><td>&#8722;4</td><td>&#8722;20</td><td>&#8722;4.2</td><td>40</td></tr>
<tr><td>Houses &gt; Faces</td><td colspan="5">n.s.</td></tr>
</table>"""

TABLE_2 = "<table><tr><th>Group</th><th>n</th></tr><tr><td>Patients</td><td>20</td></tr></table>"
TABLE_3 = (
    "<table><tr><th>x</th><th>y</th><th>z</th></tr><tr><td>1</td><td>2</td><td>3</td></tr></table>"
)

PASSAGE = "A seed was placed in the left amygdala (-22, -4, -20), as in our earlier work."
TEXT = f"Title\n\nMethods\n\n{PASSAGE}\n\nResults\n\nTable 1 ...\n"


def _collection(analyses):
    return AnalysisCollection(slug="t", coordinate_space=CoordinateSpace.MNI, analyses=analyses)


@pytest.fixture()
def written(tmp_path):
    identifier = Identifier(
        pmid="22848644", pmcid="PMC3407125", doi="10.1371/journal.pone.0041873"
    )
    tables = []
    for name, markup in (("tbl1", TABLE_1), ("tbl2", TABLE_2), ("tbl3", TABLE_3)):
        (tmp_path / f"{name}.html").write_text(markup, encoding="utf-8")
        tables.append(
            ExtractedTable(
                table_id=name,
                raw_content_path=tmp_path / f"{name}.html",
                caption=f"{name} caption",
                table_number=int(name[-1]),
            )
        )
    text = tmp_path / "text.txt"
    text.write_text(TEXT, encoding="utf-8")
    content = ExtractedContent(
        slug=identifier.slug,
        source=DownloadSource.PUBGET,
        identifier=identifier,
        tables=tables,
        full_text_path=text,
    )
    metadata = ArticleMetadata(
        title="Faces and houses",
        authors=[Author(name="Jones, S")],
        journal="PLoS ONE",
        publication_year=2012,
        source="pubmed",
        raw_metadata={"pubmed": {"MedlineCitation": {"Article": {"Language": "eng"}}}},
    )
    per_table = {
        "tbl1": _collection(
            [
                # The analyses stage's sign split: the positive half, then "<name> (negative)".
                Analysis(
                    name="Faces > Houses",
                    table_id="tbl1",
                    coordinates=[
                        Coordinate(
                            x=-42,
                            y=-55,
                            z=-18,
                            statistic_value=6.1,
                            statistic_type="T",
                            cluster_size=120,
                            cluster_measure="voxels",
                        )
                    ],
                ),
                Analysis(
                    name="Faces > Houses (negative)",
                    table_id="tbl1",
                    coordinates=[
                        Coordinate(
                            x=22,
                            y=-4,
                            z=-20,
                            statistic_value=-4.2,
                            statistic_type="T",
                            cluster_size=40,
                            cluster_measure="voxels",
                        )
                    ],
                ),
                Analysis(name="Houses > Faces", table_id="tbl1"),
            ]
        ),
        "prose": _collection(
            [
                Analysis(
                    name="amygdala seed",
                    table_id="prose",
                    metadata={"source": "prose", "role": "seed", "passages": [0]},
                    coordinates=[Coordinate(x=-22, y=-4, z=-20)],
                ),
            ]
        ),
    }
    root = tmp_path / "pond"
    target = nspond.write_article(
        root, BASE, ArticleExtractionBundle(content, metadata), per_table, []
    )
    inputs = ParseInputs(
        article_id="art-1",
        base_study_id=BASE,
        triage={
            "is_meta_analysis": False,
            "publication_types": ["Journal Article"],
            "tables": [
                {"table_id": "tbl1", "passes": True, "score": 0.97, "route": "read"},
                {"table_id": "tbl2", "passes": False, "score": 0.02, "route": "gate"},
                {"table_id": "tbl3", "passes": True, "score": 0.8, "route": "gate"},
            ],
        },
        excluded={"tbl3": {"reason": "not_coordinates", "note": "a design matrix"}},
        readings={"tbl1": "coordinates"},
        prose={"passages": [{"text": PASSAGE}]},
        passages_kept=1,
        restated=2,
        fingerprints={"extract": "fp-extract", "space": "fp-space"},
    )
    summary = paper_parse.write(
        target, ArticleExtractionBundle(content, metadata), per_table, inputs
    )
    paper = json.loads((target / "parse" / "parsed_paper.json").read_text())
    parse = json.loads((target / "parse" / "coordinate_parse.json").read_text())
    return paper, parse, summary


def _validate(instance, name):
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(instance, load(name))


def test_both_files_validate_against_the_json_schema(written):
    paper, parse, summary = written
    _validate(paper, "parsed-paper")
    _validate(parse, "coordinate-parse")
    assert "parse_problems" not in summary


def test_the_parsed_paper_points_at_the_synced_text(written):
    paper, parse, _ = written
    assert paper["text_path"] == "processed/pubget/text.txt"
    assert paper["text_length"] == len(TEXT)
    assert parse["text_sha256"] == paper["text_sha256"]


def test_bibliography_and_meta_analysis_come_from_metadata_and_triage(written):
    paper, _, _ = written
    bib = paper["bibliography"]
    assert bib["title"] == "Faces and houses"
    assert bib["authors"] == ["Jones, S"]
    assert bib["language"] == ["eng"]
    assert bib["publication_types"] == ["Journal Article"]
    assert paper["is_meta_analysis"] is False
    assert paper["header"]["identifiers"]["doi"] == "10.1371/journal.pone.0041873"
    assert paper["header"]["identifiers"]["neurostore_base_study_id"] == BASE


def test_tables_carry_body_rows_and_triage(written):
    paper, _, _ = written
    tbl1 = next(t for t in paper["tables"] if t["table_id"] == "tbl1")
    assert tbl1["column_headings"] == ["Region", "x", "y", "z", "t", "k"]
    assert [r["row"] for r in tbl1["rows"]] == [0, 1, 2, 3]
    assert tbl1["rows"][1]["cells"][0] == "Fusiform gyrus"
    tbl3 = next(t for t in paper["tables"] if t["table_id"] == "tbl3")
    assert tbl3["triage"]["excluded_by_hand"] is True


def test_keys_are_minted_from_the_cells_the_points_sit_in(written):
    _, parse, _ = written
    by_name = {}
    for a in parse["analyses"]:
        by_name.setdefault(a["name"], []).append(a)
    positive, negative = by_name["Faces > Houses"]
    assert positive["cells"] == [{"row": 1, "column_group": 0}]
    assert positive["key"] == keys.table_key("tbl1", [(1, 0)])
    assert positive["points"][0]["row"] == 1
    assert negative["key"] == keys.table_key("tbl1", [(2, 0)])
    # A contrast with no coordinates is keyed by the row that names it.
    null = by_name["Houses > Faces"][0]
    assert null["points"] == [] and null["cells"] == [{"row": 3, "column_group": 0}]


def test_the_sign_split_is_declared_not_left_in_the_name(written):
    _, parse, _ = written
    halves = [a for a in parse["analyses"] if a["name"] == "Faces > Houses"]
    assert [a["split"]["direction"] for a in halves] == ["positive", "negative"]
    assert {a["split"]["group"] for a in halves} == {halves[0]["key"]}
    assert [a["split"]["primary"] for a in halves] == [True, False]
    assert not any(a["name"].endswith("(negative)") for a in parse["analyses"])


def test_points_carry_no_sign_and_use_the_shared_statistic_kinds(written):
    _, parse, _ = written
    point = parse["analyses"][0]["points"][0]
    assert "sign" not in point
    assert point["values"] == [{"kind": "t", "value": 6.1}]
    assert point["cluster_measure"] == "voxels"
    assert [statistic_kind(k) for k in ("T", "Z", "F", "B", "P", None, "?")] == [
        "t",
        "z",
        "f",
        "beta",
        "p",
        "other",
        "other",
    ]


def test_a_prose_analysis_is_keyed_by_where_its_points_are_printed(written):
    _, parse, _ = written
    seed = next(a for a in parse["analyses"] if a["origin"] == "text")
    start = TEXT.index("-22, -4, -20")
    span = {"start_char": start, "end_char": start + len("-22, -4, -20")}
    assert seed["text_spans"] == [span]
    assert seed["points"][0]["text_span"] == span
    assert seed["key"] == keys.span_key("text", [(span["start_char"], span["end_char"])])
    assert (seed["role"], seed["anchor_kind"]) == ("anchor", "seed")


def test_two_analyses_of_one_passage_get_two_keys(tmp_path):
    passage = (
        "Faces activated the FFA (40, -52, -18; t = 5.1) "
        "and houses the PPA (x = 28, y = \u221246, z = \u22128)."
    )
    text = f"Results\n\n{passage}\n"
    collection = _collection(
        [
            Analysis(
                name=name,
                table_id="prose",
                metadata={"source": "prose", "role": "result", "passages": [0]},
                coordinates=[Coordinate(x=x, y=y, z=z)],
            )
            for name, (x, y, z) in (("faces", (40, -52, -18)), ("houses", (28, -46, -8)))
        ]
    )
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": passage}]})
    built = [
        paper_parse._prose_analysis(a, collection, text, inputs, [])[0]
        for a in collection.analyses
    ]
    assert [text[a.text_spans[0].start_char : a.text_spans[0].end_char] for a in built] == [
        "40, -52, -18",
        "28, y = \u221246, z = \u22128",
    ]
    assert built[0].key != built[1].key


def test_every_table_gets_its_reading(written):
    _, parse, _ = written
    readings = {t["table_id"]: t["reading"] for t in parse["tables"]}
    assert readings == {
        "tbl1": "coordinates",
        "tbl2": "rejected_by_triage",
        "tbl3": "excluded_by_hand",
    }
    assert parse["text_sweep"] == {
        "passages_found": 1,
        "passages_read": 1,
        "truncated": False,
        "points_dropped_as_table_restatements": 2,
    }


def test_the_parse_id_is_the_analyses_fingerprint(written, tmp_path):
    _, parse, _ = written
    assert len(parse["parse_id"]) == 64
    assert parse["header"]["inputs"][0] == {
        "artifact_kind": "parsed_paper",
        "fingerprint": parse["text_sha256"],
    }


def test_a_passage_is_placed_in_the_text_despite_its_whitespace():
    from ingestion_workflow.services.paper_parse import _locate

    text = "ab  The seed\nwas  here. cd"
    start, end = _locate("The seed was here.", text)
    assert text[start:end] == "The seed\nwas  here."


def test_a_passage_split_by_an_inlined_table_is_placed_by_its_head_and_tail():
    from ingestion_workflow.services.paper_parse import _locate

    head = "A seed was placed in the left amygdala at the peak,"
    tail = "as in the earlier study of the same group of patients."
    text = f"x {head} | 1 | 2 | {tail} y"
    start, end = _locate(f"{head} {tail}", text)
    assert text[start:end] == f"{head} | 1 | 2 | {tail}"
    assert _locate("Nothing like this is in the text at all, anywhere in it.", text) is None


def test_side_by_side_contrasts_differ_by_column_group(tmp_path):
    from ingestion_workflow.services.paper_parse import _grid, _table_analyses

    markup = """<table>
    <tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>x</th><th>y</th><th>z</th></tr>
    <tr><td>Insula</td><td>36</td><td>20</td><td>4</td><td>-34</td><td>18</td><td>2</td></tr>
    </table>"""
    (tmp_path / "t.html").write_text(markup, encoding="utf-8")
    collection = _collection(
        [
            Analysis(name="A > B", coordinates=[Coordinate(x=36, y=20, z=4)]),
            Analysis(name="C > D", coordinates=[Coordinate(x=-34, y=18, z=2)]),
        ]
    )
    built = _table_analyses(
        "tbl1", collection.analyses, collection, _grid(tmp_path / "t.html"), []
    )
    assert [[(c.row, c.column_group) for c in a.cells] for a in built] == [[(0, 0)], [(0, 1)]]
    assert built[0].key == keys.table_key("tbl1", [(0, 0)]) != built[1].key
