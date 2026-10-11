"""The parsed paper and the coordinate parse, against study_schema's contract."""

from __future__ import annotations

import json
from types import SimpleNamespace

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
from study_schema.models import paper_parse as pp

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


def _decided(role="result", *, origin="table", anchor_kind=None, prior=False, evidence=(),
             source=None):
    """`metadata.set_role` as the roles stage writes it (docs/set-roles-artifact.md)."""
    return {
        "role": role,
        "anchor_kind": anchor_kind,
        "from_prior_study": prior,
        "prior_study_evidence": list(evidence),
        "role_confidence": 0.9,
        "role_source": source or f"set-role-{origin}@1",
        "role_origin": origin,
    }


def _roles(text=""):
    return paper_parse._Roles(paper_parse._Text(text), [])


def _collection(analyses):
    """A collection as the roles stage passed it: each set without a decision here gets one."""
    for a in analyses:
        if "set_role" not in (a.metadata or {}):
            origin = "text" if (a.metadata or {}).get("source") == "prose" else "table"
            a.metadata = {**(a.metadata or {}), "set_role": _decided(origin=origin)}
        a.metadata.setdefault("held", False)
    return AnalysisCollection(slug="t", coordinate_space=CoordinateSpace.MNI, analyses=analyses)


@pytest.fixture()
def written(tmp_path):
    return _written(tmp_path)


def _written(tmp_path):
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
                # The analyses stage's sign split: the original half, then "<name> (inverse)".
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
                    name="Faces > Houses (inverse)",
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
                    # The prose model's own word says "result": the roles stage's decision stands.
                    metadata={
                        "source": "prose",
                        "role": "result",
                        "passages": [0],
                        "set_role": _decided("anchor", origin="text", anchor_kind="seed"),
                    },
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
    # study_schema's layout writes unset fields as null; the assertions ignore them.
    paper = _compact(json.loads((target / "parse" / "parsed_paper.json").read_text()))
    parse = _compact(json.loads((target / "parse" / "coordinate_parse.json").read_text()))
    return paper, parse, {**summary, "dir": target / "parse"}


def _compact(value):
    if isinstance(value, dict):
        return {k: _compact(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_compact(v) for v in value]
    return value


def _validate(instance, name):
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(instance, load(name))


def test_both_files_validate_against_the_json_schema(written):
    _, _, summary = written
    # As written, nulls and all.
    _validate(json.loads((summary["dir"] / "parsed_paper.json").read_text()), "parsed-paper")
    _validate(
        json.loads((summary["dir"] / "coordinate_parse.json").read_text()), "coordinate-parse"
    )
    assert "parse_omitted" not in summary


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
    assert positive["key"] == keys.table_key("tbl1", [(1, 0)], "Faces > Houses")
    assert positive["points"][0]["row"] == 1
    assert negative["key"] == keys.table_key("tbl1", [(2, 0)], "Faces > Houses")
    # A contrast with no coordinates is keyed by the row that names it.
    null = by_name["Houses > Faces"][0]
    assert null["points"] == [] and null["cells"] == [{"row": 3, "column_group": 0}]


def test_the_sign_split_is_declared_not_left_in_the_name(written):
    _, parse, _ = written
    halves = [a for a in parse["analyses"] if a["name"] == "Faces > Houses"]
    assert [a["split"]["half"] for a in halves] == ["original", "inverse"]
    assert [a["split"].get("original_analysis") for a in halves] == [None, halves[0]["key"]]
    assert not any(a["name"].endswith(("(inverse)", "(negative)")) for a in parse["analyses"])


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
    assert seed["key"] == keys.span_key("text", [(span["start_char"], span["end_char"])], seed["name"])
    assert (seed["role"], seed["anchor_kind"]) == ("anchor", "seed")


def test_every_role_is_the_roles_stage_s_decision(written):
    _, parse, _ = written
    roles = [(a["name"], a["role"], a["role_source"]) for a in parse["analyses"]]
    assert roles == [
        ("Faces > Houses", "result", "set-role-table@1"),
        ("Faces > Houses", "result", "set-role-table@1"),
        ("Houses > Faces", "result", "set-role-table@1"),
        ("amygdala seed", "anchor", "set-role-text@1"),
    ]
    assert {a["role_confidence"] for a in parse["analyses"]} == {0.9}


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
                metadata={"source": "prose", "passages": [0]},
                coordinates=[Coordinate(x=x, y=y, z=z)],
            )
            for name, (x, y, z) in (("faces", (40, -52, -18)), ("houses", (28, -46, -8)))
        ]
    )
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": passage}]})
    built = [
        paper_parse._prose_analysis(
            a, collection, paper_parse._Text(text), inputs, _roles(text)
        )
        for a in collection.analyses
    ]
    assert [text[a.text_spans[0].start_char : a.text_spans[0].end_char] for a in built] == [
        "40, -52, -18",
        "28, y = \u221246, z = \u22128",
    ]
    assert built[0].key != built[1].key


def _one_prose(metadata, passage="The peak is marked in Figure 2 (40, -52, -18)."):
    text = paper_parse._Text(f"Results\n\n{passage}\n")
    collection = AnalysisCollection(
        slug="t",
        coordinate_space=CoordinateSpace.MNI,
        analyses=[
            Analysis(
                name="marked peak",
                table_id="prose",
                metadata={"source": "prose", "passages": [0], "held": False, **metadata},
                coordinates=[Coordinate(x=40, y=-52, z=-18)],
            )
        ],
    )
    roles = paper_parse._Roles(text, [])
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": passage}]})
    return paper_parse._prose_analysis(collection.analyses[0], collection, text, inputs, roles), roles


def test_a_set_without_a_role_from_the_roles_stage_is_not_parsed():
    """No default: neither the prose model's role word nor `result` stands in."""
    for metadata in ({"role": "result"}, {"set_role": {**_decided(), "role_source": None}}):
        with pytest.raises(paper_parse.MissingRole, match="no role from the roles stage"):
            _one_prose(metadata)


def test_the_prose_model_s_role_word_is_not_read():
    built, _ = _one_prose({"role": "figure", "set_role": _decided("result", origin="text")})
    assert (built.role, built.anchor_kind, built.role_source) == ("result", None, "set-role-text@1")


def test_numbers_that_are_not_coordinates_are_omitted_with_the_deciding_model():
    built, roles = _one_prose({"set_role": _decided(None, origin="text")})
    assert built is None
    assert [(o.name, o.table_id, o.reason) for o in roles.omitted] == [
        ("marked peak", None, "not brain coordinates, as set-role-text@1 decided")
    ]


def test_prior_study_evidence_is_placed_in_the_parsed_paper_s_text():
    passage = "Peaks matched Smith et al. (40, -52, -18)."
    cited = "Peaks matched Smith et al."
    elsewhere = {"text": cited, "start_char": 0, "end_char": len(cited)}  # offsets of another text
    missing = {"text": "A sentence this text does not hold at all, anywhere."}
    built, roles = _one_prose(
        {"set_role": _decided("reference", origin="text", prior=True, evidence=[elsewhere, missing])},
        passage,
    )
    at = len("Results\n\n")
    assert (built.role, built.from_prior_study) == ("reference", True)
    assert [(s.start_char, s.end_char) for s in built.prior_study_evidence] == [(at, at + len(cited))]
    assert [o.kept for o in roles.omitted] == [True]
    assert "a citing sentence is not in the parsed paper's text" in roles.omitted[0].reason


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
    from ingestion_workflow.services.paper_parse import _locate, _Text

    text = "ab  The seed\nwas  here. cd"
    ((start, end),) = _locate("The seed was here.", _Text(text))
    assert text[start:end] == "The seed\nwas  here"


def test_a_passage_is_placed_despite_its_minus_signs_and_punctuation():
    from ingestion_workflow.services.paper_parse import _locate, _Text

    text = "x Talaraich coordinates, x: -49.5, y: -14.3 (302.47mm2) y"
    ((start, end),) = _locate(
        "Talaraich coordinates, x : \u221249.5, y : \u221214.3 (302.47 mm \u00b2)", _Text(text)
    )
    assert text[start:end] == "Talaraich coordinates, x: -49.5, y: -14.3 (302.47mm2"


def test_a_passage_split_by_an_inlined_table_is_placed_sentence_by_sentence():
    from ingestion_workflow.services.paper_parse import _locate, _Text

    head = "A seed was placed in the left amygdala at the peak."
    legend = "Figure 2. Colour bar displays t values for the comparison."
    tail = "It was placed as in the earlier study of the same group of patients."
    text = f"x {head} | 1 | 2 | {tail} y"
    pieces = _locate(f"{legend} {head} {tail}", _Text(text))
    assert [text[a:b] for a, b in pieces] == [head[:-1], tail[:-1]]
    assert _locate("Nothing like this is in the text at all, anywhere in it.", _Text(text)) == []


def test_a_point_outside_its_located_passage_is_found_where_it_is_printed_once():
    text = "Results\n\nThe peak lay in the left insula (x = \u221234, y = 18, z = 2).\n"
    collection = _collection(
        [
            Analysis(
                name="insula",
                table_id="prose",
                metadata={"source": "prose", "passages": [0]},
                coordinates=[Coordinate(x=-34, y=18, z=2)],
            )
        ]
    )
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": "Not in the text."}]})
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, paper_parse._Text(text), inputs, _roles(text)
    )
    (span,) = built.text_spans
    assert text[span.start_char : span.end_char] == "\u221234, y = 18, z = 2"


def test_a_prose_point_is_placed_in_its_sentence_not_at_a_table_row_printing_it():
    # From a sampled paper: the sentence glues the minus to the value before it,
    # which the parse once missed, placing the point at a table row instead.
    sentence = "The peak lay in the cuneus (\u2212 9 23\u201315) after training."
    text = f"Table 1\n\nCuneus\t\u22129\t23\t\u221215\n\nResults\n\n{sentence}\n"
    collection = _collection(
        [
            Analysis(
                name="cuneus",
                table_id="prose",
                metadata={"source": "prose", "passages": [0]},
                coordinates=[Coordinate(x=-9, y=23, z=-15)],
            )
        ]
    )
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": sentence}]})
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, paper_parse._Text(text), inputs, _roles(text)
    )
    (point,) = built.points
    span = point.text_span
    assert text[span.start_char : span.end_char] == "\u2212 9 23\u201315"


def test_an_analysis_not_in_the_text_is_omitted_with_its_reason():
    collection = _collection(
        [
            Analysis(
                name="lost",
                table_id="prose",
                metadata={"source": "prose", "passages": [0]},
                coordinates=[Coordinate(x=1, y=2, z=3)],
            )
        ]
    )
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": "Elsewhere."}]})
    roles = _roles("Results only.")
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, roles.text, inputs, roles
    )
    assert built is None
    assert [(o.name, o.table_id, o.reason) for o in roles.omitted] == [
        ("lost", None, "neither its passages nor its points are in the parsed paper's text")
    ]


def _parse(tmp_path, markup, per_table, text="Results\n", **inputs):
    """coordinate_parse over one table `tbl1` read from `markup`."""
    from ingestion_workflow.services.paper_parse import _grid

    (tmp_path / "t.html").write_text(markup, encoding="utf-8")
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[SimpleNamespace(table_id="tbl1", triage=None)],
    )
    return paper_parse.coordinate_parse(
        paper,
        text,
        {"tbl1": _grid(tmp_path / "t.html")},
        per_table,
        ParseInputs(article_id="a", **inputs),
    )


SHARED = """<table>
<tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
<tr><td>Fear &gt; Neutral</td><td></td><td></td><td></td></tr>
<tr><td>Amygdala</td><td>22</td><td>-4</td><td>-20</td></tr>
<tr><td>Insula</td><td>36</td><td>20</td><td>4</td></tr>
<tr><td>Surprise &gt; Neutral</td><td></td><td></td><td></td></tr>
<tr><td>Precuneus</td><td>-10</td><td>-39</td><td>44</td></tr>
<tr><td>Insula</td><td>36</td><td>20</td><td>4</td></tr>
<tr><td>Fear intercept</td><td></td><td></td><td></td></tr>
<tr><td>Insula</td><td>36</td><td>20</td><td>4</td></tr>
</table>"""


def test_a_coordinate_printed_in_two_rows_goes_to_its_own_analysis(tmp_path):
    collection = _collection(
        [
            Analysis(
                name="Fear > Neutral",
                coordinates=[Coordinate(x=22, y=-4, z=-20), Coordinate(x=36, y=20, z=4)],
            ),
            Analysis(
                name="Surprise > Neutral",
                coordinates=[Coordinate(x=-10, y=-39, z=44), Coordinate(x=36, y=20, z=4)],
            ),
            # Its only point is printed in all three blocks; the row naming it decides.
            Analysis(name="Fear intercept", coordinates=[Coordinate(x=36, y=20, z=4)]),
        ]
    )
    parse, omitted = _parse(tmp_path, SHARED, {"tbl1": collection})
    assert [[(c.row, c.column_group) for c in a.cells] for a in parse.analyses] == [
        [(1, 0), (2, 0)],
        [(4, 0), (5, 0)],
        [(7, 0)],
    ]
    assert omitted == []


def test_two_names_on_the_same_cells_are_both_kept_and_only_a_true_duplicate_is_omitted(tmp_path):
    collection = _collection(
        [
            Analysis(name="A > B", coordinates=[Coordinate(x=22, y=-4, z=-20)]),
            Analysis(name="a > b", coordinates=[Coordinate(x=22, y=-4, z=-20)]),
            Analysis(name="C > D", coordinates=[Coordinate(x=22, y=-4, z=-20)]),
            Analysis(name="Unprinted", coordinates=[Coordinate(x=1, y=1, z=1)]),
        ]
    )
    parse, omitted = _parse(
        tmp_path, SHARED, {"tbl1": collection}, readings={"tbl1": "coordinates"}
    )
    assert [a.name for a in parse.analyses] == ["A > B", "C > D"]
    key = keys.table_key("tbl1", [(1, 0)], "A > B")
    assert [str(o) for o in omitted] == [
        "tbl1: 'Unprinted': no row of the table prints its points",
        f"tbl1: 'a > b': a repeat of {key} ('A > B'): the same cells, name and points",
    ]
    (reading,) = parse.tables
    assert reading.reading == "coordinates"
    assert "omitted 'a > b'" in reading.reason and "omitted 'Unprinted'" in reading.reason


def test_a_table_set_that_is_not_coordinates_is_omitted_and_holds_its_rows(tmp_path):
    """Its rows stay its own, so the next set's cells, and so its key, do not move."""
    collection = _collection(
        [
            Analysis(
                name="Fear > Neutral",
                metadata={"set_role": _decided(None)},
                coordinates=[Coordinate(x=22, y=-4, z=-20), Coordinate(x=36, y=20, z=4)],
            ),
            Analysis(
                name="Surprise > Neutral",
                coordinates=[Coordinate(x=-10, y=-39, z=44), Coordinate(x=36, y=20, z=4)],
            ),
        ]
    )
    parse, omitted = _parse(
        tmp_path, SHARED, {"tbl1": collection}, readings={"tbl1": "coordinates"}
    )
    (kept,) = parse.analyses
    assert kept.key == keys.table_key("tbl1", [(4, 0), (5, 0)], "Surprise > Neutral")
    assert [str(o) for o in omitted] == [
        "tbl1: 'Fear > Neutral': not brain coordinates, as set-role-table@1 decided"
    ]
    assert "omitted 'Fear > Neutral': not brain coordinates" in parse.tables[0].reason


def test_a_placeholder_analysis_with_no_points_is_recorded_as_omitted(tmp_path):
    collection = _collection(
        [
            Analysis(name="UNKNOWN", coordinates=[]),
            Analysis(name="A > B", coordinates=[Coordinate(x=22, y=-4, z=-20)]),
        ]
    )
    parse, omitted = _parse(tmp_path, SHARED, {"tbl1": collection})
    assert [a.name for a in parse.analyses] == ["A > B"]
    assert [str(o) for o in omitted] == ["tbl1: 'UNKNOWN': a placeholder analysis with no points"]


def test_inverse_is_kept_in_the_name_unless_a_split_is_declared(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    collection = _collection(
        [
            Analysis(name="Fear > Neutral", coordinates=[Coordinate(x=36, y=20, z=4)]),
            Analysis(name="Other", coordinates=[Coordinate(x=-10, y=-39, z=44)]),
            # Not right after its original: no split, so the name keeps its direction.
            Analysis(name="Fear > Neutral (inverse)", coordinates=point),
        ]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    assert parse.analyses[2].name == "Fear > Neutral (inverse)"
    assert all(a.split is None for a in parse.analyses)


def test_the_legacy_negative_spelling_is_declared_and_stripped_too(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    collection = _collection(
        [
            Analysis(name="Fear > Neutral", coordinates=[Coordinate(x=36, y=20, z=4)]),
            Analysis(name="Fear > Neutral (negative)", coordinates=point),
        ]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    assert [a.name for a in parse.analyses] == ["Fear > Neutral", "Fear > Neutral"]
    assert [a.split.half for a in parse.analyses] == ["original", "inverse"]
    assert parse.analyses[1].split.original_analysis == parse.analyses[0].key


def _prose_parse(passage, names_and_points):
    """Entries are (name, points, passages[, set_role]); a point is (x, y, z) or a Coordinate."""
    text = f"Results\n\n{passage}\n"
    collection = _collection(
        [
            Analysis(
                name=name,
                table_id="prose",
                metadata={"source": "prose", "passages": passages,
                          **({"set_role": extra[0]} if extra else {})},
                coordinates=[p if isinstance(p, Coordinate) else Coordinate(x=p[0], y=p[1], z=p[2])
                             for p in points],
            )
            for name, points, passages, *extra in names_and_points
        ]
    )
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[],
    )
    return paper_parse.coordinate_parse(
        paper,
        text,
        {},
        {"prose": collection},
        ParseInputs(article_id="a", prose={"passages": [{"text": passage}]}),
    )


def test_one_sentence_naming_four_analyses_on_two_point_sets_keeps_all_four():
    passage = (
        "Relative to silence, PO and PC both activated the right insula (12, -2, 8), "
        "and NO and NC both activated the left putamen (14, -6, -4)."
    )
    parse, omitted = _prose_parse(
        passage,
        [
            ("PO > Sil", [(12, -2, 8)], [0]),
            ("PC > Sil", [(12, -2, 8)], [0]),
            ("NO > Sil", [(14, -6, -4)], [0]),
            ("NC > Sil", [(14, -6, -4)], [0]),
        ],
    )
    assert [a.name for a in parse.analyses] == ["PO > Sil", "PC > Sil", "NO > Sil", "NC > Sil"]
    assert len({a.key for a in parse.analyses}) == 4
    assert omitted == [] and parse.omitted_analyses is None


def test_a_prose_duplicate_is_omitted_and_recorded_with_its_reason():
    passage = "The poor reader group had a peak in the left fusiform (-42, -55, -18)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("poor reader", [(-42, -55, -18)], [0]),
            ("Poor  Reader", [(-42, -55, -18)], [0]),
            ("ASD group", [(-42, -55, -18)], [0]),
        ],
    )
    first, second = parse.analyses
    assert (first.name, second.name) == ("poor reader", "ASD group")
    assert [o.name for o in parse.omitted_analyses] == ["Poor  Reader"]
    reason = parse.omitted_analyses[0].reason
    assert reason == f"a repeat of {first.key} ('poor reader'): the same spans, name and points"
    assert parse.omitted_analyses[0].text_spans == first.text_spans
    assert [str(o) for o in omitted] == [f"text: 'Poor  Reader': {reason}"]


def test_a_prose_analysis_whose_passage_is_missing_is_recorded_as_omitted():
    parse, _ = _prose_parse(
        "A sentence with a peak (1, 2, 3).",
        [("kept", [(1, 2, 3)], [0]), ("lost", [(40, 41, 42)], [7])],
    )
    assert [a.name for a in parse.analyses] == ["kept"]
    assert [o.name for o in parse.omitted_analyses] == ["lost"] and parse.omitted_analyses[
        0
    ].text_spans is None


def test_an_inlined_table_has_text_spans_for_it_and_its_rows():
    from ingestion_workflow.services.paper_parse import _inlined, _Table, _Text

    grid = _Table(
        ["Region", "x", "y", "z"],
        [
            [("Amygdala", 0), ("22", 1), ("-4", 2), ("-20", 3)],
            [("Insula", 0), ("36", 1), ("20", 2), ("4", 3)],
        ],
    )
    text = (
        "The amygdala (22, -4, -20) was active.\n"
        "Region\tx\ty\tz\nAmygdala\t22\t\u22124\t\u221220\nInsula\t36\t20\t4\n"
    )
    spans = _inlined(grid, _Text(text))
    # Its first copy is mid-sentence; the row is the one starting a line.
    assert text[slice(*spans[0])] == "Amygdala\t22\t\u22124\t\u221220"
    assert text[slice(*spans[1])] == "Insula\t36\t20\t4"
    assert spans["table"] == (spans[0][0], spans[1][1])


def test_tables_sharing_a_header_row_each_find_it_in_their_own_block():
    from ingestion_workflow.services.paper_parse import _inlined_tables, _Table, _Text

    def grid(*rows):
        return _Table([], [[(c, i) for i, c in enumerate(r)] for r in rows])

    header = ("", "Simple condition", "Difficult condition")
    grids = {
        # Its last row is not printed with it, but the next table prints one like it.
        "tbl2": grid(
            header, ("G. front. med. r", "38", "4", "33"), ("ant. Cing.", "6", "22", "32")
        ),
        "tbl3": grid(header, ("G. front. med. r", "39", "1", "37")),
    }
    tables = [
        SimpleNamespace(table_id="tbl2", table_number=2, caption="Results of discrimination task"),
        SimpleNamespace(table_id="tbl3", table_number=3, caption="Results of labeling task"),
    ]
    text = (
        "Signal changes are given in Tables 2 and 3\n"
        "Table 2\nResults of discrimination task\n"
        "\tSimple condition\tDifficult condition\nG. front. med. r\t38\t4\t33\n\n"
        "Table 3\nResults of labeling task\n"
        "\tSimple condition\tDifficult condition\nG. front. med. r\t39\t1\t37\n"
        "ant. Cing.\t6\t22\t32\n"
    )
    two, three = _inlined_tables(tables, grids, _Text(text))
    block = text.index("Table 3")
    assert two["table"][1] < block and 2 not in two
    # tbl3's header row is the second copy of the line, not tbl2's.
    assert three[0][0] > block and three["table"] == (three[0][0], three[1][1])
    assert text[slice(*three[1])] == "G. front. med. r\t39\t1\t37"


ROWS = """<table>
<tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
<tr><td>Cuneus</td><td>2</td><td>-80</td><td>10</td></tr>
<tr><td>Insula</td><td>36</td><td>20</td><td>4</td></tr>
<tr><td>Caudate</td><td>12</td><td>14</td><td>8</td></tr>
<tr><td>Faces &gt; Houses</td><td></td><td></td><td></td></tr>
<tr><td>Precuneus</td><td>-10</td><td>-39</td><td>44</td></tr>
<tr><td>Insula</td><td>36</td><td>20</td><td>4</td></tr>
<tr><td>Putamen</td><td>-24</td><td>6</td><td>2</td></tr>
</table>"""


def test_a_repeated_point_goes_to_the_row_nearest_its_analysis_own(tmp_path):
    collection = _collection(
        [
            Analysis(
                name="Words > Rest",
                coordinates=[
                    Coordinate(x=-10, y=-39, z=44),
                    Coordinate(x=36, y=20, z=4),
                    Coordinate(x=-24, y=6, z=2),
                ],
            ),
        ]
    )
    parse, _ = _parse(tmp_path, ROWS, {"tbl1": collection})
    # No row names it; row 5 sits between its own rows 4 and 6, row 1 does not.
    assert [(c.row, c.column_group) for c in parse.analyses[0].cells] == [(4, 0), (5, 0), (6, 0)]


def test_a_repeated_point_avoids_a_row_another_analysis_took(tmp_path):
    collection = _collection(
        [
            # Placed first, by the row naming it: row 5.
            Analysis(name="Faces > Houses", coordinates=[Coordinate(x=36, y=20, z=4)]),
            Analysis(
                name="Words > Rest",
                coordinates=[
                    Coordinate(x=-10, y=-39, z=44),
                    Coordinate(x=36, y=20, z=4),
                    Coordinate(x=-24, y=6, z=2),
                ],
            ),
        ]
    )
    parse, omitted = _parse(tmp_path, ROWS, {"tbl1": collection})
    # Row 5 is nearer its own rows, but the first analysis holds it.
    assert [[c.row for c in a.cells] for a in parse.analyses] == [[5], [1, 4, 6]]
    assert omitted == []


def test_a_repeated_point_stays_in_its_analysis_column_block(tmp_path):
    markup = """<table>
    <tr><th>Region</th><th>x</th><th>y</th><th>z</th><th>x</th><th>y</th><th>z</th></tr>
    <tr><td>Cuneus</td><td>2</td><td>-80</td><td>10</td><td>1</td><td>-70</td><td>3</td></tr>
    <tr><td>Insula</td><td>14</td><td>-8</td><td>6</td><td>36</td><td>20</td><td>4</td></tr>
    <tr><td>Caudate</td><td>12</td><td>14</td><td>8</td><td>9</td><td>11</td><td>7</td></tr>
    <tr><td>Putamen</td><td>-24</td><td>6</td><td>2</td><td>-22</td><td>5</td><td>1</td></tr>
    <tr><td>Insula</td><td>36</td><td>20</td><td>4</td><td>30</td><td>18</td><td>-2</td></tr>
    </table>"""
    collection = _collection(
        [
            Analysis(
                name="Words > Rest",
                coordinates=[
                    Coordinate(x=2, y=-80, z=10),
                    Coordinate(x=14, y=-8, z=6),
                    Coordinate(x=36, y=20, z=4),
                ],
            ),
        ]
    )
    parse, _ = _parse(tmp_path, markup, {"tbl1": collection})
    # Row 1 is nearer, but in the other contrast's columns.
    assert [(c.row, c.column_group) for c in parse.analyses[0].cells] == [(0, 0), (1, 0), (4, 0)]


def test_write_refuses_two_files_that_disagree(tmp_path, monkeypatch):
    real = paper_parse.coordinate_parse

    def other_text(*args, **kwargs):
        parse, omitted = real(*args, **kwargs)
        return parse.model_copy(update={"text_sha256": "f" * 64}), omitted

    monkeypatch.setattr(paper_parse, "coordinate_parse", other_text)
    with pytest.raises(ValueError, match="different text"):
        _written(tmp_path)
    assert not list(tmp_path.rglob("coordinate_parse.json"))


def test_a_table_both_rejected_and_excluded_reads_as_excluded():
    paper = SimpleNamespace(
        tables=[SimpleNamespace(table_id="t", triage=pp.TableTriage(passes=False, route="gate"))]
    )
    inputs = ParseInputs(article_id="a", excluded={"t": {"reason": "not_coordinates"}})
    (reading,) = paper_parse._readings(paper, {}, inputs)
    assert (reading.reading, reading.reason) == ("excluded_by_hand", "not_coordinates")


def test_a_full_passage_list_is_truncated():
    from ingestion_workflow.pipeline.stages.passages import MAX_PASSAGES

    passages = {"passages": [{"text": "p"}] * MAX_PASSAGES}
    full = ParseInputs(article_id="a", prose=passages, passages_kept=MAX_PASSAGES)
    assert paper_parse._text_sweep(full).truncated is True
    short = ParseInputs(article_id="a", prose=passages, passages_kept=MAX_PASSAGES - 1)
    assert paper_parse._text_sweep(short).truncated is False


def test_a_point_names_its_space_only_when_it_differs_from_its_analysis():
    collection = _collection([])
    same = paper_parse._point(
        Coordinate(x=1, y=2, z=3, space=CoordinateSpace.MNI), collection, None
    )
    other = paper_parse._point(
        Coordinate(x=1, y=2, z=3, space=CoordinateSpace.TALAIRACH), collection, None
    )
    assert (same.space, other.space) == (None, "TAL")


def test_a_stated_other_space_is_written_as_other_and_validates():
    collection = AnalysisCollection(
        slug="t",
        coordinate_space=CoordinateSpace.OTHER,
        analyses=[
            Analysis(
                name="a",
                metadata={"source": "prose", "passages": [0], "set_role": _decided(origin="text"),
                          "held": False},
                coordinates=[Coordinate(x=40, y=-52, z=-18)],
            )
        ],
    )
    text = "Peak at 40, -52, -18."
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": text}]})
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, paper_parse._Text(text), inputs, _roles(text)
    )
    assert built.coordinate_space == "OTHER"
    pp.ParsedAnalysis.model_validate(built.model_dump())
    point = paper_parse._point(
        Coordinate(x=1, y=2, z=3, space=CoordinateSpace.OTHER), _collection([]), None
    )
    assert point.space == "OTHER"


def test_the_meta_analysis_basis_names_what_said_so():
    basis = paper_parse._meta_basis
    assert basis("A meta-analysis of fear", ["Journal Article"]) == "title"
    assert basis("Fear", ["Meta-Analysis"]) == "publication_type"
    assert basis("A meta-analysis of fear", ["Meta-Analysis"]) == "publication_type_and_title"


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
        "tbl1", collection.analyses, collection, _grid(tmp_path / "t.html"), _roles()
    )
    assert [[(c.row, c.column_group) for c in a.cells] for a in built] == [[(0, 0)], [(0, 1)]]
    assert built[0].key == keys.table_key("tbl1", [(0, 0)], "A > B") != built[1].key


def test_a_null_coordinate_space_leaves_the_parse_space_unset():
    # Built directly: the model's space is not nullable until #72 merges.
    collection = AnalysisCollection(
        slug="t", coordinate_space=CoordinateSpace.MNI, analyses=[]
    )
    object.__setattr__(collection, "coordinate_space", None)
    assert paper_parse._space_value(collection.coordinate_space) is None
    # With no table space, a point states only its own.
    bare = paper_parse._point(Coordinate(x=1, y=2, z=3), collection, None)
    named = paper_parse._point(
        Coordinate(x=1, y=2, z=3, space=CoordinateSpace.TALAIRACH), collection, None
    )
    assert (bare.space, named.space) == (None, "TAL")


def test_two_entries_with_one_key_and_other_points_are_one_analysis_holding_both():
    """Neither point is printed, so both fall back to the passage: one reported analysis."""
    passage = "Group differences in the left and right hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("patients > controls", [(-24, -20, -14)], [0]),
            ("Patients > controls", [(26, -18, -16), (-24, -20, -14)], [0]),
        ],
    )
    [merged] = parse.analyses
    assert [p.coordinates for p in merged.points] == [[-24, -20, -14], [26, -18, -16]]
    assert parse.omitted_analyses is None
    [note] = omitted
    assert note.kept and note.name == "Patients > controls"
    assert note.reason == (
        f"merged into {merged.key} ('patients > controls'): the same spans and name; "
        "its 1 other point(s) added"
    )


def test_one_point_printed_twice_for_two_same_named_analyses_keeps_both():
    passage = (
        "In Experiment 1, patients > controls peaked at (-24, -20, -14); in Experiment 2, "
        "patients > controls also peaked at (-24, -20, -14)."
    )
    parse, omitted = _prose_parse(
        passage,
        [("patients > controls", [(-24, -20, -14)], [0])] * 2,
    )
    first, second = parse.analyses
    at = len("Results\n\n")
    assert [a.points[0].text_span.start_char - at for a in (first, second)] == [
        passage.index("-24"), passage.rindex("-24")
    ]
    assert first.key != second.key and omitted == []


def test_the_same_point_with_other_statistic_values_is_one_point_and_the_note_says_so():
    passage = "Group differences in the left hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("patients > controls", [Coordinate(x=-24, y=-20, z=-14)], [0]),
            ("patients > controls",
             [Coordinate(x=-24, y=-20, z=-14, statistic_value=4.1, statistic_type="T")], [0]),
        ],
    )
    [merged] = parse.analyses
    assert [p.coordinates for p in merged.points] == [[-24, -20, -14]]
    [note] = omitted  # no new point: a repeat, with the conflicting values recorded
    assert not note.kept and "a repeat of" in note.reason
    assert "[-24.0, -20.0, -14.0] kept [], dropped [('t', 4.1)]" in note.reason


def test_a_merge_adding_a_point_of_another_statistic_kind_recomputes_the_analysis_statistic():
    passage = "Group differences in the left hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("patients > controls",
             [Coordinate(x=-24, y=-20, z=-14, statistic_value=4.1, statistic_type="T")], [0]),
            ("patients > controls",
             [Coordinate(x=26, y=-18, z=-16, statistic_value=3.3, statistic_type="Z")], [0]),
        ],
    )
    [merged] = parse.analyses
    assert len(merged.points) == 2 and merged.statistic is None  # T and Z: no single kind
    assert omitted[0].kept


def test_entries_of_one_key_with_different_roles_merge_into_a_held_analysis_with_both_roles():
    passage = "Group differences in the left and right hippocampus survived correction (Table S2)."
    result = _decided("result", origin="text", source="model-a@1")
    reference = _decided("reference", origin="text", prior=True, source="model-b@2")
    for order in ((result, reference), (reference, result)):
        parse, omitted = _prose_parse(
            passage,
            [
                ("p > c", [(-24, -20, -14)], [0], order[0]),
                ("p > c", [(26, -18, -16)], [0], order[1]),
            ],
        )
        [merged] = parse.analyses
        assert len(merged.points) == 2
        [note] = omitted
        conflict = note.role_conflict
        assert conflict["key"] == merged.key
        assert [(r["role"], r["role_source"]) for r in conflict["roles"]] == [
            (order[0]["role"], order[0]["role_source"]),
            (order[1]["role"], order[1]["role_source"]),
        ]
        assert "role conflict, held for review" in note.reason
    # The same roles are no conflict.
    _, omitted = _prose_parse(
        passage,
        [("p > c", [(-24, -20, -14)], [0], result), ("p > c", [(26, -18, -16)], [0], result)],
    )
    assert omitted[0].role_conflict is None


def test_a_point_repeated_for_differently_named_analyses_listed_out_of_print_order():
    passage = (
        "Patients > controls peaked at (-24, -20, -14). Activity there, (-24, -20, -14), "
        "also correlated with age."
    )
    at = len("Results\n\n")
    for names in (("patients > controls", "correlation with age"),
                  ("correlation with age", "patients > controls")):
        parse, _ = _prose_parse(passage, [(n, [(-24, -20, -14)], [0]) for n in names])
        got = {a.name: a.points[0].text_span.start_char - at for a in parse.analyses}
        assert got == {"patients > controls": passage.index("-24"),
                       "correlation with age": passage.index("-24")}


def test_a_kept_analysis_s_note_is_not_among_the_parse_s_omissions():
    passage = "The peak is marked in Figure 2 (40, -52, -18)."
    collection = _collection([
        Analysis(
            name="marked peak",
            table_id="prose",
            metadata={
                "source": "prose",
                "passages": [0],
                "set_role": _decided("reference", origin="text", prior=True,
                                     evidence=[{"text": "A sentence printed nowhere at all here."}]),
            },
            coordinates=[Coordinate(x=40, y=-52, z=-18)],
        )
    ])
    paper = SimpleNamespace(header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
                            text_sha256="0" * 64, tables=[])
    parse, omitted = paper_parse.coordinate_parse(
        paper, f"Results\n\n{passage}\n", {}, {"prose": collection},
        ParseInputs(article_id="a", prose={"passages": [{"text": passage}]}),
    )
    assert [a.name for a in parse.analyses] == ["marked peak"]
    assert [o.kept for o in omitted] == [True] and parse.omitted_analyses is None


def test_a_citing_sentence_printed_with_other_spaces_and_minus_signs_is_placed():
    printed = "As in Lee et al.,  the seed sat at (40, \u221252, \u221218)."
    cited = "As in Lee et al., the seed sat at (40, -52, -18)."
    built, roles = _one_prose(
        {"set_role": _decided("reference", origin="text", prior=True, evidence=[{"text": cited}])},
        printed,
    )
    at = len("Results\n\n")
    assert [(s.start_char, s.end_char) for s in built.prior_study_evidence] == [
        (at, at + printed.rindex("18") + 2)
    ]
    assert roles.omitted == []


def test_only_a_placeholder_name_is_marked_not_printed():
    passage = "The peak is marked in Figure 2 (40, -52, -18)."
    text = paper_parse._Text(f"Results\n\n{passage}\n")
    inputs = ParseInputs(article_id="a", prose={"passages": [{"text": passage}]})
    printed = []
    for name in ("marked peak", "UNKNOWN"):
        collection = _collection([
            Analysis(name=name, table_id="prose", metadata={"source": "prose", "passages": [0]},
                     coordinates=[Coordinate(x=40, y=-52, z=-18)]),
        ])
        built = paper_parse._prose_analysis(collection.analyses[0], collection, text, inputs, _roles())
        printed.append(built.name_is_printed)
    assert printed == [None, False]


@pytest.mark.parametrize(
    "metadata",
    [
        {"set_role": _decided()},  # no `held`
        {"set_role": {k: v for k, v in _decided().items() if k != "role_origin"}, "held": False},
        {"set_role": _decided("figure"), "held": False},
        {"set_role": {**_decided(), "from_prior_study": None}, "held": False},
    ],
)
def test_the_parse_checks_a_role_as_upload_and_sync_do(metadata):
    analysis = Analysis(name="x", table_id="t1", metadata=metadata, coordinates=[])
    with pytest.raises(paper_parse.MissingRole):
        _roles().of(analysis, "t1")
