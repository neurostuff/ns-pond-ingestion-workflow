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
from ingestion_workflow.models.statistics import point_values, schema_kind
from ingestion_workflow.services.paper_parse import ParseInputs
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


def _bare_table(table_id, caption=None, headings=None, footer=None, triage=None):
    """The model's parsed table, with only what the coordinate parse reads of it."""
    return pp.ParsedTable(
        table_id=table_id, caption=caption, footer=footer, column_headings=headings,
        triage=triage,
    )


def _roles(text=""):
    return paper_parse._Roles(paper_parse._Text(text), [])


def _prose(text, *passages):
    """A prose payload whose passages are spans of `text`, as the prose stage writes it."""
    spans = [(text.index(p), text.index(p) + len(p)) for p in passages]
    return {"text_sha256": paper_parse._Text(text).sha256,
            "passages": [{"span": list(s), "text": p} for s, p in zip(spans, passages)]}


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
                # The analyses stage's sign split, declared on both halves.
                Analysis(
                    name="Faces > Houses",
                    table_id="tbl1",
                    metadata={"split": {"half": "original", "index": 0}},
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
                    name="Faces > Houses",
                    table_id="tbl1",
                    metadata={"split": {"half": "inverse", "original_index": 0}},
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
    # As sync does: stage1 after the parse, with the parse's splits.
    target = nspond.write_article(
        root, BASE, ArticleExtractionBundle(content, metadata), per_table, [], stage1=False
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
        prose=_prose(TEXT, PASSAGE),
        passages_kept=1,
        restated=2,
        fingerprints={"extract": "fp-extract", "space": "fp-space"},
    )
    splits = {}
    summary = paper_parse.write(
        target, ArticleExtractionBundle(content, metadata), per_table, inputs, splits=splits
    )
    nspond.write_stage1(target, per_table, True, splits)
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


def test_cells_name_the_rows_the_points_sit_in(written):
    _, parse, _ = written
    by_name = {}
    for a in parse["analyses"]:
        by_name.setdefault(a["name"], []).append(a)
    original, inverse = by_name["Faces > Houses"]
    assert original["cells"] == [{"row": 1, "column_group": 0}]
    assert original["points"][0]["row"] == 1
    assert inverse["cells"] == [{"row": 2, "column_group": 0}]
    # A contrast with no coordinates cites the row that names it.
    null = by_name["Houses > Faces"][0]
    assert null["points"] == [] and null["cells"] == [{"row": 3, "column_group": 0}]


def test_every_key_is_the_one_its_name_space_and_points_give(written):
    _, parse, _ = written
    analyses = [pp.ParsedAnalysis(**a) for a in parse["analyses"]]
    by_origin = {}
    for a in analyses:
        by_origin.setdefault((a.origin, bool(a.points)), []).append(a)
    # A table analysis, one with no points, and a text analysis.
    assert set(by_origin) == {("table", True), ("table", False), ("text", True)}
    for a in analyses:
        assert a.key == keys.key_for(a)
        assert a.key == keys.analysis_key(a.name, "MNI", a.points)
    (null,) = by_origin[("table", False)]
    assert null.key == keys.analysis_key("Houses > Faces", "MNI", [])
    # The halves of the split share a name and differ by their points.
    original, inverse = by_origin[("table", True)]
    assert original.name == inverse.name and original.key != inverse.key


def test_the_sign_split_is_declared_not_left_in_the_name(written):
    _, parse, _ = written
    halves = [a for a in parse["analyses"] if a["name"] == "Faces > Houses"]
    assert [a["split"]["half"] for a in halves] == ["original", "inverse"]
    assert [a["split"].get("original_analysis") for a in halves] == [None, halves[0]["key"]]
    assert not any(a["name"].endswith("(inverse)") for a in parse["analyses"])
    # The stage composed the inverse half's name; the original's is printed.
    assert [a.get("name_is_printed") for a in halves] == [None, False]


def test_stage1_carries_each_halfs_sign_split_with_the_parses_key(written):
    _, parse, summary = written
    stage1 = json.loads((summary["dir"].parent / "stage1" / "analyses.json").read_text())
    declared = [a["split"] for a in parse["analyses"] if a.get("split")]
    assert [a["split"] for a in stage1["analyses"] if a.get("split")] == declared
    assert declared[1]["original_analysis"] == parse["analyses"][0]["key"]


def test_points_carry_no_sign_and_use_the_shared_statistic_kinds(written):
    _, parse, _ = written
    point = parse["analyses"][0]["points"][0]
    assert "sign" not in point
    assert point["values"] == [{"kind": "t", "value": 6.1}]
    assert point["cluster_measure"] == "voxels"
    assert [schema_kind(k) for k in ("T", "Z", "F", "B", "P", None, "?")] == [
        "t",
        "z",
        "f",
        "beta",
        "p",
        "other",
        "other",
    ]


def test_a_prose_analysis_s_spans_are_where_its_points_are_printed(written):
    _, parse, _ = written
    seed = next(a for a in parse["analyses"] if a["origin"] == "text")
    start = TEXT.index("-22, -4, -20")
    span = {"start_char": start, "end_char": start + len("-22, -4, -20")}
    assert seed["text_spans"] == [span]
    assert seed["points"][0]["text_span"] == span
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
    inputs = ParseInputs(article_id="a", prose=_prose(text, passage))
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
    inputs = ParseInputs(article_id="a", prose=_prose(text.text, passage))
    return paper_parse._prose_analysis(collection.analyses[0], collection, text, inputs, roles), roles


def test_a_set_without_a_role_from_the_roles_stage_is_not_parsed():
    """No default: neither the prose model's role word nor `result` stands in."""
    for metadata in ({"role": "result"}, {"set_role": {**_decided(), "role_source": None}}):
        with pytest.raises(paper_parse.MissingRole, match="no role from the roles stage"):
            _one_prose(metadata)


def test_the_prose_model_s_role_word_is_not_read():
    built, _ = _one_prose({"role": "figure", "set_role": _decided("result", origin="text")})
    assert (built.role, built.anchor_kind, built.role_source) == ("result", None, "set-role-text@1")


def test_a_point_printed_only_with_a_sign_reversed_keeps_no_span_and_says_so():
    """The extraction dropped the minus that a space parts from x; the text still prints it."""
    passage = "Activity in the left internal capsule (\u2212 40, -52, -18) increased."
    built, roles = _one_prose({"set_role": _decided(origin="text")}, passage)
    assert built.points[0].text_span is None
    (note,) = [o for o in roles.omitted if o.kept]
    printed = "\u2212 40, -52, -18"
    at = roles.text.text.index(printed)
    assert note.reason == (
        "kept; point (40, -52, -18) has no text_span: its passage prints it only with a "
        f"sign reversed, {printed!r} at {at}-{at + len(printed)}"
    )


def test_a_mirror_printed_outside_the_set_s_passages_is_not_noted():
    """The other hemisphere's peak elsewhere in the paper belongs to another set."""
    passage = "The right peak is listed in Table 2."
    text = paper_parse._Text(f"Results\n\nThe left peak (-40, -52, -18).\n\n{passage}\n")
    collection = _collection([
        Analysis(name="right", metadata={"source": "prose", "passages": [0]},
                 coordinates=[Coordinate(x=40, y=-52, z=-18)])
    ])
    roles = paper_parse._Roles(text, [])
    inputs = ParseInputs(article_id="a", prose=_prose(text.text, passage))
    built = paper_parse._prose_analysis(collection.analyses[0], collection, text, inputs, roles)
    assert built.points[0].text_span is None
    assert not [o for o in roles.omitted if o.kept]


def test_a_point_printed_as_stored_gets_no_sign_note():
    built, roles = _one_prose({"set_role": _decided(origin="text")})
    assert built.points[0].text_span is not None
    assert not [o for o in roles.omitted if o.kept]


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


def _insula(text, prose):
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
    roles = _roles(text)
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, paper_parse._Text(text),
        ParseInputs(article_id="a", prose=prose), roles,
    )
    return built, [o.reason for o in roles.omitted]


def test_a_repeated_passage_is_placed_at_its_own_copy_not_the_first():
    # htv7t5aoho5n: the abstract repeats the result sentence; the passage is the second.
    passage = "The peak lay in the left insula (x = \u221234, y = 18, z = 2)."
    text = f"Abstract\n\n{passage}\n\nResults\n\n{passage}\n"
    second = text.rindex(passage)
    prose = {"text_sha256": paper_parse._Text(text).sha256,
             "passages": [{"span": [second, second + len(passage)], "text": passage}]}
    built, _ = _insula(text, prose)
    (span,) = built.text_spans
    assert span.start_char > second
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
    inputs = ParseInputs(article_id="a", prose=_prose(text, sentence))
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
    roles = _roles("Results only.")
    inputs = ParseInputs(article_id="a", prose={"text_sha256": roles.text.sha256, "passages": []})
    built = paper_parse._prose_analysis(
        collection.analyses[0], collection, roles.text, inputs, roles
    )
    assert built is None
    assert [(o.name, o.table_id, o.reason) for o in roles.omitted] == [
        ("lost", None, "it names no passage of the parsed paper's text")
    ]


def test_a_point_printed_outside_its_passage_is_not_placed_there():
    # d47umdi7b5aj: a prose point printed once, in an inlined table far from its passage.
    passage = "The insula was active in both groups."
    text = f"Results\n\n{passage}\n\nTable 2\nInsula\t\u221234\t18\t2\n"
    built, _ = _insula(text, _prose(text, passage))
    (span,) = built.text_spans
    assert text[span.start_char : span.end_char] == passage
    assert built.points[0].text_span is None


def test_passages_indexing_another_text_place_nothing():
    text = "Results\n\nThe peak lay in the left insula (x = \u221234, y = 18, z = 2).\n"
    stale = _prose(text, "The peak")
    stale["text_sha256"] = "0" * 64
    assert _insula(text, stale) == (None, ["its passages index another text than the parsed paper's"])
    assert _insula(text, {"passages": [{"text": "The peak"}]})[0] is None  # a prose result from before spans


def _parse(tmp_path, markup, per_table, text="Results\n", caption=None, footer=None, **inputs):
    """coordinate_parse over one table `tbl1` read from `markup`."""
    from ingestion_workflow.services.paper_parse import _grid

    (tmp_path / "t.html").write_text(markup, encoding="utf-8")
    grid = _grid(tmp_path / "t.html")
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[_bare_table("tbl1", caption, grid.headings, footer)],
    )
    return paper_parse.coordinate_parse(
        paper,
        text,
        {"tbl1": grid},
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
    kept, other = parse.analyses
    assert [a.name for a in parse.analyses] == ["A > B", "C > D"]
    # One point, two names: the name keeps them two analyses.
    assert kept.key != other.key
    assert [str(o) for o in omitted] == [
        "tbl1: 'Unprinted': no row of the table prints its points",
        f"tbl1: 'a > b': it repeats a kept analysis (the same key): {kept.key}, 'A > B' "
        "read from table tbl1",
    ]
    (reading,) = parse.tables
    assert reading.reading == "coordinates"
    assert "omitted 'a > b'" in reading.reason and "omitted 'Unprinted'" in reading.reason


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


def test_the_split_is_read_from_the_declaration_never_the_name(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    inverse = {"split": {"half": "inverse", "original_index": 0}}
    collection = _collection(
        [
            Analysis(name="Fear > Neutral", coordinates=[Coordinate(x=36, y=20, z=4)],
                     metadata={"split": {"half": "original", "index": 0}}),
            Analysis(name="Other", coordinates=[Coordinate(x=-10, y=-39, z=44)]),
            # Declared, so paired by its original's index though not adjacent.
            Analysis(name="Fear > Neutral", coordinates=point, metadata=inverse),
            # Named like a half but declared as none: not a split, name kept.
            Analysis(name="Other (inverse)", coordinates=[Coordinate(x=36, y=20, z=4)]),
        ]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    fear, other, half, lookalike = parse.analyses
    assert half.name == "Fear > Neutral"
    assert (fear.split.half, half.split.half) == ("original", "inverse")
    assert fear.split.original_analysis is None and half.split.original_analysis == fear.key
    assert other.split is None and lookalike.split is None
    assert lookalike.name == "Other (inverse)"


def test_a_half_whose_original_is_not_in_the_table_is_declared_on_its_own(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    collection = _collection(
        [Analysis(name="Fear > Neutral", coordinates=point,
                  metadata={"split": {"half": "inverse", "original_index": 7}})]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    (half,) = parse.analyses
    assert half.split.half == "inverse" and half.split.original_analysis is None
    assert half.name == "Fear > Neutral"


def test_an_inverse_half_with_no_original_is_declared_on_its_own(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    collection = _collection(
        [Analysis(name="AD vs. NC", coordinates=point,
                  metadata={"split": {"half": "inverse", "original_index": None}})]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    (half,) = parse.analyses
    assert half.split.half == "inverse" and half.split.original_analysis is None
    assert half.name == "AD vs. NC"


def test_an_original_whose_inverse_is_not_in_the_parse_is_still_declared(tmp_path):
    collection = _collection(
        [Analysis(name="Fear > Neutral", coordinates=[Coordinate(x=36, y=20, z=4)],
                  metadata={"split": {"half": "original", "index": 0}})]
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    (original,) = parse.analyses
    assert original.split.half == "original" and original.split.original_analysis is None
    assert original.name == "Fear > Neutral"


def test_a_nan_statistic_gives_a_point_no_value():
    assert point_values(float("nan"), "T") == []


def test_halves_pair_by_the_stages_index_after_an_analysis_is_dropped(tmp_path):
    point = [Coordinate(x=22, y=-4, z=-20, statistic_value=-3.0, statistic_type="T")]
    stage = _collection(
        [
            Analysis(name="Left", coordinates=[Coordinate(x=-10, y=-39, z=44)]),
            Analysis(name="Fear > Neutral", coordinates=[Coordinate(x=36, y=20, z=4)],
                     metadata={"split": {"half": "original", "index": 1}}),
            Analysis(name="Fear > Neutral", coordinates=point,
                     metadata={"split": {"half": "inverse", "original_index": 1}}),
        ]
    )
    stage.analyses.pop(0)
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": stage})
    original, inverse = parse.analyses
    assert inverse.split.original_analysis == original.key
    assert (original.split.half, inverse.split.half) == ("original", "inverse")


def test_two_originals_of_one_name_each_pair_with_their_own_inverse(tmp_path):
    table = """<table>
<tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
<tr><td>A</td><td>1</td><td>2</td><td>3</td></tr>
<tr><td>B</td><td>4</td><td>5</td><td>6</td></tr>
<tr><td>C</td><td>7</td><td>8</td><td>9</td></tr>
<tr><td>D</td><td>10</td><td>11</td><td>12</td></tr>
</table>"""

    def half(coordinate, split, name="P > C"):
        return Analysis(
            name=name, coordinates=[Coordinate(**coordinate)], metadata={"split": split}
        )

    def inverse(i, **c):
        return half(c, {"half": "inverse", "original_index": i})

    collection = _collection(
        [
            half(dict(x=1, y=2, z=3), {"half": "original", "index": 0}),
            half(dict(x=4, y=5, z=6), {"half": "original", "index": 1}),
            inverse(0, x=7, y=8, z=9, statistic_value=-3.0, statistic_type="T"),
            inverse(1, x=10, y=11, z=12, statistic_value=-3.0, statistic_type="T"),
        ]
    )
    parse, omitted = _parse(tmp_path, table, {"tbl1": collection})
    o1, o2, i1, i2 = parse.analyses
    assert omitted == []
    assert i1.split.original_analysis == o1.key
    assert i2.split.original_analysis == o2.key


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
        ParseInputs(article_id="a", prose=_prose(text, passage)),
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
    assert reason == (
        f"it repeats a kept analysis (the same key): {first.key}, 'poor reader' read from the text"
    )
    assert parse.omitted_analyses[0].text_spans == first.text_spans
    assert [str(o) for o in omitted] == [f"text: 'Poor  Reader': {reason}"]


def test_the_restated_points_of_a_kept_text_analysis_are_among_the_parse_s_omissions():
    passage = "Craving correlated with the amygdala (-22, -4, -18) and the insula (36, 20, 2)."
    text = f"Results\n\n{passage}\n"
    collection = _collection([Analysis(
        name="negative correlation with craving", table_id="prose",
        metadata={"source": "prose", "passages": [0]}, coordinates=[Coordinate(x=36, y=20, z=2)])])
    restated = [{"passages": [0], "analysis": "negative correlation with craving", "restatement": False,
                 "points": [{"x": -22.0, "y": -4.0, "z": -18.0, "restates": [
                     {"table": "t1", "analysis": "patients > controls"},
                     {"table": "t2", "analysis": "craving"}]}]}]
    paper = SimpleNamespace(header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
                            text_sha256="0" * 64, tables=[])
    parse, _ = paper_parse.coordinate_parse(
        paper, text, {}, {"prose": collection},
        ParseInputs(article_id="a", prose=_prose(text, passage), restatements=restated))
    assert [a.name for a in parse.analyses] == ["negative correlation with craving"]
    (omission,) = parse.omitted_analyses
    assert omission.name == "negative correlation with craving"
    assert omission.reason == (
        "its other points are kept; these restate a table's result: "
        "table t1's 'patients > controls' at (-22, -4, -18); table t2's 'craving' at (-22, -4, -18)")
    (span,) = omission.text_spans
    assert text[span.start_char:span.end_char] == "-22, -4, -18"


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


def test_a_recorded_cut_says_whether_the_passages_were_truncated():
    from ingestion_workflow.pipeline.stages.passages import MAX_PASSAGES

    passages = {"passages": [{"text": "p"}] * MAX_PASSAGES}
    whole = ParseInputs(article_id="a", prose=passages, passages_kept=MAX_PASSAGES, passages_omitted=0)
    assert paper_parse._text_sweep(whole).truncated is False
    cut = paper_parse._text_sweep(
        ParseInputs(article_id="a", prose=passages, passages_kept=MAX_PASSAGES, passages_omitted=3))
    assert (cut.truncated, cut.passages_found) == (True, MAX_PASSAGES + 3)


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
    inputs = ParseInputs(article_id="a", prose=_prose(text, text))
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
    assert built[0].key == keys.key_for(built[0]) != built[1].key


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


def test_two_entries_of_one_name_on_one_passage_with_other_points_are_two_analyses():
    """Neither point is printed, so both fall back to the passage; their points differ."""
    passage = "Group differences in the left and right hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("patients > controls", [(-24, -20, -14)], [0]),
            ("Patients > controls", [(26, -18, -16), (-24, -20, -14)], [0]),
        ],
    )
    first, second = parse.analyses
    assert first.text_spans == second.text_spans and first.key != second.key
    assert [len(a.points) for a in (first, second)] == [1, 2]
    assert omitted == [] and parse.omitted_analyses is None


def test_one_point_printed_twice_for_two_same_named_analyses_is_kept_once_and_recorded():
    passage = (
        "In Experiment 1, patients > controls peaked at (-24, -20, -14); in Experiment 2, "
        "patients > controls also peaked at (-24, -20, -14)."
    )
    parse, omitted = _prose_parse(
        passage,
        [("patients > controls", [(-24, -20, -14)], [0])] * 2,
    )
    # One name, space and points: one key, so the second copy is the first's repeat.
    (kept,) = parse.analyses
    at = len("Results\n\n")
    assert kept.points[0].text_span.start_char - at == passage.index("-24")
    (repeat,) = parse.omitted_analyses
    assert repeat.reason.startswith(f"it repeats a kept analysis (the same key): {kept.key}")
    assert repeat.text_spans[0].start_char - at == passage.rindex("-24")
    assert [o.kept for o in omitted] == [False]


def test_the_same_point_with_other_statistic_values_is_two_analyses():
    passage = "Group differences in the left hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("patients > controls", [Coordinate(x=-24, y=-20, z=-14)], [0]),
            ("patients > controls",
             [Coordinate(x=-24, y=-20, z=-14, statistic_value=4.1, statistic_type="T")], [0]),
        ],
    )
    bare, valued = parse.analyses
    assert bare.key != valued.key
    assert (bare.points[0].values, valued.statistic) == (None, "t")
    assert omitted == []


def _copies_with_roles(first, second):
    """Two prose copies of one key, `first` and then `second` deciding their roles."""
    passage = "Group differences in the left and right hippocampus survived correction (Table S2)."
    return _prose_parse(
        passage,
        [("p > c", [(-24, -20, -14)], [0], first), ("p > c", [(-24, -20, -14)], [0], second)],
    )


RESULT = _decided("result", origin="text", source="model-a@1")
ANCHOR = _decided("anchor", origin="text", anchor_kind="seed", source="model-b@2")
REFERENCE = _decided("reference", origin="text", prior=True, source="model-c@3")


@pytest.mark.parametrize("order", [(RESULT, ANCHOR), (ANCHOR, RESULT)], ids=["first", "second"])
def test_copies_of_one_key_where_either_is_a_result_keep_one_result(order):
    parse, omitted = _copies_with_roles(*order)
    [kept] = parse.analyses
    assert (kept.role, kept.anchor_kind, kept.role_source) == ("result", None, "model-a@1")
    [note] = omitted
    assert not note.kept
    assert note.reason == (
        f"it repeats a kept analysis (the same key): {kept.key}, 'p > c' read from the text; "
        f"the copies' roles differ: {order[0]['role']!r} ({order[0]['role_source']}) and "
        f"{order[1]['role']!r} ({order[1]['role_source']}), kept 'result'"
    )


def test_copies_of_one_key_with_no_result_keep_the_first_copy_s_role():
    parse, omitted = _copies_with_roles(ANCHOR, REFERENCE)
    [kept] = parse.analyses
    assert (kept.role, kept.anchor_kind, kept.from_prior_study, kept.role_source) == (
        "anchor", "seed", False, "model-b@2",
    )
    assert omitted[0].reason.endswith(
        "the copies' roles differ: 'anchor' (model-b@2) and 'reference' (model-c@3), "
        "kept 'anchor'"
    )


def test_copies_of_one_role_with_another_anchor_kind_differ_and_keep_the_first():
    parse, omitted = _copies_with_roles(
        ANCHOR, _decided("anchor", origin="text", anchor_kind="roi", source="model-d@4")
    )
    assert parse.analyses[0].anchor_kind == "seed"
    assert omitted[0].reason.endswith(
        "the copies' roles differ: 'anchor' (model-b@2) and 'anchor' (model-d@4), kept 'anchor'"
    )


def test_copies_of_one_role_are_no_conflict():
    _, omitted = _copies_with_roles(RESULT, _decided("result", origin="text", source="x@9"))
    assert omitted[0].reason.endswith("'p > c' read from the text")


def _two_tables(tmp_path, markup, per_table):
    """coordinate_parse over the tables of `per_table`, each read from `markup`."""
    from ingestion_workflow.services.paper_parse import _grid

    (tmp_path / "t.html").write_text(markup, encoding="utf-8")
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[_bare_table(t) for t in per_table],
    )
    return paper_parse.coordinate_parse(
        paper,
        "Results\n",
        {t: _grid(tmp_path / "t.html") for t in per_table},
        per_table,
        ParseInputs(article_id="a", readings={t: "coordinates" for t in per_table}),
    )


def test_one_analysis_read_from_a_table_and_its_html_copy_is_kept_once(tmp_path):
    def read():
        return _collection(
            [
                Analysis(
                    name="Fear > Neutral",
                    coordinates=[Coordinate(x=22, y=-4, z=-20, statistic_value=5.8,
                                            statistic_type="T"),
                                 Coordinate(x=36, y=20, z=4)],
                ),
                Analysis(name="Surprise > Neutral", coordinates=[Coordinate(x=-10, y=-39, z=44)]),
            ]
        )

    parse, omitted = _two_tables(tmp_path, SHARED, {"1": read(), "html-table-1": read()})
    assert [(a.table_id, a.name) for a in parse.analyses] == [
        ("1", "Fear > Neutral"), ("1", "Surprise > Neutral"),
    ]
    fear, surprise = parse.analyses
    assert [str(o) for o in omitted] == [
        f"html-table-1: 'Fear > Neutral': it repeats a kept analysis (the same key): "
        f"{fear.key}, 'Fear > Neutral' read from table 1",
        f"html-table-1: 'Surprise > Neutral': it repeats a kept analysis (the same key): "
        f"{surprise.key}, 'Surprise > Neutral' read from table 1",
    ]
    readings = {r.table_id: r for r in parse.tables}
    assert readings["1"].reason is None
    assert readings["html-table-1"].reason.count("it repeats a kept analysis") == 2


def test_differently_named_analyses_with_identical_points_in_two_tables_get_two_keys(tmp_path):
    def read(name):
        return _collection(
            [Analysis(name=name, coordinates=[Coordinate(x=22, y=-4, z=-20),
                                              Coordinate(x=36, y=20, z=4)])]
        )

    parse, omitted = _two_tables(
        tmp_path, SHARED, {"1": read("PM > HC"), "2": read("PM > baseline")}
    )
    hc, baseline = parse.analyses
    assert [p.coordinates for p in hc.points] == [p.coordinates for p in baseline.points]
    assert hc.key != baseline.key
    assert omitted == []


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
        ParseInputs(article_id="a", prose=_prose(f"Results\n\n{passage}\n", passage)),
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
    inputs = ParseInputs(article_id="a", prose=_prose(text.text, passage))
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
        {"set_role": {**_decided(), "from_prior_study": "yes"}, "held": False},
    ],
)
def test_the_parse_checks_a_role_as_upload_and_sync_do(metadata):
    analysis = Analysis(name="x", table_id="t1", metadata=metadata, coordinates=[])
    with pytest.raises(paper_parse.MissingRole):
        _roles().of(analysis, "t1")


def test_a_table_in_a_stated_other_space_parses_and_validates(tmp_path):
    collection = AnalysisCollection(
        slug="t",
        coordinate_space=CoordinateSpace.OTHER,
        analyses=[Analysis(name="A > B", table_id="tbl1",
                           metadata={"set_role": _decided(), "held": False},
                           coordinates=[Coordinate(x=22, y=-4, z=-20)])],
    )
    parse, _ = _parse(tmp_path, SHARED, {"tbl1": collection})
    (analysis,) = parse.analyses
    assert analysis.coordinate_space == "OTHER"
    pp.CoordinateParse.model_validate(parse.model_dump())


NO_POINTS = """<table>
<tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>
<tr><td>Children &gt; Adults</td><td colspan="3">n.s.</td></tr>
</table>"""


def test_same_named_analyses_with_no_points_in_two_tables_are_one_analysis(tmp_path):
    """Two tables each reporting a contrast with no peaks: the second is the first's repeat."""
    def read():
        return _collection([Analysis(name="Children > Adults", coordinates=[])])

    parse, omitted = _two_tables(tmp_path, NO_POINTS, {"tbl0015": read(), "tbl0020": read()})
    [kept] = parse.analyses
    assert (kept.table_id, kept.name, kept.points) == ("tbl0015", "Children > Adults", [])
    assert [str(o) for o in omitted] == [
        f"tbl0020: 'Children > Adults': it repeats a kept analysis (the same key): "
        f"{kept.key}, 'Children > Adults' read from table tbl0015"
    ]
    readings = {r.table_id: r for r in parse.tables}
    assert readings["tbl0015"].reason is None
    assert readings["tbl0020"].reason == f"omitted 'Children > Adults': {omitted[0].reason}"


def _table_and_text(tmp_path, order):
    """`Fear > Neutral` read from table 1 and from the sentence restating it, in `order`."""
    from ingestion_workflow.services.paper_parse import _grid

    passage = "Fear > Neutral activated the amygdala (22, -4, -20) and the insula (36, 20, 4)."
    text = f"Results\n\n{passage}\n"
    (tmp_path / "t.html").write_text(SHARED, encoding="utf-8")

    def points():
        return [Coordinate(x=22, y=-4, z=-20), Coordinate(x=36, y=20, z=4)]

    read = {
        "1": _collection([Analysis(name="Fear > Neutral", coordinates=points())]),
        "prose": _collection([Analysis(
            name="Fear > Neutral", table_id="prose",
            metadata={"source": "prose", "passages": [0]}, coordinates=points(),
        )]),
    }
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[_bare_table("1")],
    )
    return paper_parse.coordinate_parse(
        paper,
        text,
        {"1": _grid(tmp_path / "t.html")},
        {t: read[t] for t in order},
        ParseInputs(article_id="a", readings={"1": "coordinates"}, prose=_prose(text, passage)),
    )


def test_a_table_analysis_and_the_sentence_restating_it_are_one_analysis(tmp_path):
    parse, omitted = _table_and_text(tmp_path, ["1", "prose"])
    [kept] = parse.analyses
    assert (kept.origin, kept.table_id) == ("table", "1")
    [repeat] = parse.omitted_analyses
    assert repeat.reason == (
        f"it repeats a kept analysis (the same key): {kept.key}, 'Fear > Neutral' "
        "read from table 1"
    )
    assert repeat.text_spans and [o.table_id for o in omitted] == [None]


def test_a_text_analysis_and_the_table_restating_it_are_one_analysis(tmp_path):
    parse, omitted = _table_and_text(tmp_path, ["prose", "1"])
    [kept] = parse.analyses
    assert (kept.origin, kept.table_id) == ("text", None)
    assert parse.omitted_analyses is None
    [repeat] = omitted
    assert str(repeat) == (
        f"1: 'Fear > Neutral': it repeats a kept analysis (the same key): {kept.key}, "
        "'Fear > Neutral' read from the text"
    )
    assert parse.tables[0].reason == f"omitted 'Fear > Neutral': {repeat.reason}"


def _point(x=22.0, space=None, kind="T", value=4.1, other=()):
    return Coordinate(x=x, y=-4, z=-20, space=space, statistic_value=value, statistic_type=kind,
                      other_statistics=list(other))


@pytest.mark.parametrize(
    "first, second",
    [
        pytest.param([_point(), _point(x=36)], [_point(), _point(x=38)], id="second-point"),
        pytest.param([_point()], [_point(space=CoordinateSpace.TALAIRACH)], id="own-space"),
        pytest.param([_point(kind="T")], [_point(kind="Z")], id="statistic-kind"),
        pytest.param(
            [_point()], [_point(other=[{"statistic_type": "p", "statistic_value": 0.01}])],
            id="p-value",
        ),
        pytest.param([_point(x=22.0)], [_point(x=22.5)], id="half-a-millimetre"),
        pytest.param([_point(x=22.01)], [_point(x=22.02)], id="hundredth-of-a-millimetre"),
    ],
)
def test_same_named_analyses_differing_in_one_point_detail_are_two_analyses(first, second):
    passage = "Group differences in the left hippocampus survived correction (Table S2)."
    parse, omitted = _prose_parse(passage, [("p > c", first, [0]), ("p > c", second, [0])])
    one, two = parse.analyses
    assert one.key != two.key
    assert (one.key, two.key) == (keys.key_for(one), keys.key_for(two))
    assert omitted == []


def test_points_in_another_order_and_another_spelling_of_the_name_are_one_analysis():
    passage = "Relative to silence, PO activated the insula and the putamen (Table S2)."
    parse, omitted = _prose_parse(
        passage,
        [
            ("PO - Sil", [_point(x=12), _point(x=14, kind="Z")], [0]),
            ("po\u2212\u00a0SIL", [_point(x=14, kind="Z"), _point(x=12)], [0]),
        ],
    )
    [kept] = parse.analyses
    [repeat] = omitted
    assert repeat.name == "po\u2212\u00a0SIL"
    assert repeat.reason.startswith(f"it repeats a kept analysis (the same key): {kept.key}")


def test_an_analysis_whose_points_mix_t_and_z_states_no_statistic():
    passage = "Group differences in the left hippocampus survived correction (Table S2)."
    parse, _ = _prose_parse(
        passage,
        [
            ("mixed", [_point(x=12, kind="T"), _point(x=14, kind="Z")], [0]),
            ("t only", [_point(x=12, kind="T"), _point(x=14, kind="T", value=0.01),
                        _point(x=16, kind="p", value=0.01)], [0]),
        ],
    )
    mixed, t_only = parse.analyses
    assert (mixed.statistic, t_only.statistic) == (None, "t")


def test_the_parse_is_stamped_with_the_version_that_keys_analyses_by_content(written):
    """Stored parses from before content keys carry location keys, so they must re-parse."""
    paper, parse, _ = written
    assert paper_parse.PAPER_PARSE_VERSION == 7
    assert {paper["header"]["producer"]["version"], parse["header"]["producer"]["version"]} == {
        "paper_parse.7"
    }


# PMC4366046 table 2 (PMID 25789862): seeds placed in rat, positions from bregma.
BREGMA_TABLE = """<table>
<tr><th>Network</th><th>Brain region</th><th>Coordinates in Bregma (in mm) M-L; D-V; A-P</th></tr>
<tr><td>DMN</td><td>Rostral Anterior Cingulate Cortex</td><td>0.5; 2.5; 1.7</td></tr>
<tr><td>Motor Network</td><td>Primary Motor Cortex</td><td>3.0; 1.5; 1.7</td></tr>
</table>"""
# The same paper's table 1: the human seeds, in Talairach.
TALAIRACH_TABLE = """<table>
<tr><th>Network</th><th>Brain region</th><th>Coordinates in Talairach (in mm) X; Y; Z</th></tr>
<tr><td>DMN</td><td>Posterior Cingulate Cortex</td><td>0; -47; 29</td></tr>
</table>"""
BREGMA_HEADINGS = ["Network", "Brain region", "Coordinates in Bregma (in mm) M-L; D-V; A-P"]
RAT_CAPTION = "Table 2. Seed placement in rat for; DMN, motor network."
HUMAN_CAPTION = "Table 1. Seed placement in humans for; DMN, motor network."


def _seeds():
    return _collection(
        [
            Analysis(name="DMN", coordinates=[Coordinate(x=0.5, y=2.5, z=1.7)]),
            Analysis(name="Motor Network", coordinates=[Coordinate(x=3.0, y=1.5, z=1.7)]),
        ]
    )


def test_a_bregma_table_is_left_out_with_its_reason(tmp_path):
    parse, omitted = _parse(tmp_path, BREGMA_TABLE, {"tbl1": _seeds()}, caption=RAT_CAPTION)
    assert parse.analyses == []
    assert [(o.name, o.table_id, o.reason) for o in omitted] == [
        ("DMN", "tbl1", paper_parse.RODENT_ATLAS_REASON),
        ("Motor Network", "tbl1", paper_parse.RODENT_ATLAS_REASON),
    ]
    [reading] = parse.tables
    assert (reading.reading, reading.reason) == ("no_coordinates", paper_parse.RODENT_ATLAS_REASON)


def test_a_bregma_table_overrides_the_analyses_stage_s_reading(tmp_path):
    parse, _ = _parse(
        tmp_path, BREGMA_TABLE, {"tbl1": _seeds()}, caption=RAT_CAPTION,
        readings={"tbl1": "coordinates"},
    )
    assert [t.reading for t in parse.tables] == ["no_coordinates"]


def test_a_talairach_table_is_still_read(tmp_path):
    parse, omitted = _parse(
        tmp_path, TALAIRACH_TABLE, {"tbl1": _collection(
            [Analysis(name="DMN", coordinates=[Coordinate(x=0, y=-47, z=29)])]
        )}, caption=HUMAN_CAPTION,
    )
    assert [a.name for a in parse.analyses] == ["DMN"]
    assert omitted == []


def _rodent(caption=None, headings=None, footer=None):
    return paper_parse.is_rodent_atlas_table(_bare_table("t", caption, headings, footer))


XYZ = ["Region", "x", "y", "z"]


@pytest.mark.parametrize(
    "caption, headings, footer",
    [
        ("Seed placement in rat", ["Coordinates in Bregma (mm)"], None),
        ("Injection sites in mice (Franklin and Paxinos)", XYZ, None),
        ("Rat brain regions, Paxinos & Watson", XYZ, None),
        ("Sites", XYZ, "Coordinates in the atlas of Paxinos and Watson"),
        ("Coordinates, Franklin & Paxinos", XYZ, None),
        ("Electrode sites in mice", ["Region", "AP", "ML", "DV"], None),
        ("Electrode sites in mice", ["Region", "A-P", "M/L", "D V"], None),
        ("Sites", XYZ, "Rats; coordinates relative to bregma"),
        ("Wistar rats: tracer sites", ["Region", "mm from bregma"], None),
        ("Sprague-Dawley recording sites", ["Region", "Interaural (mm)"], None),
        ("Sites in the C57BL/6 mouse brain", ["Region", "Bregma"], None),
        ("RAT TARGETS, PAXINOS", XYZ, None),
        ("Rat targets, paxinos atlas", XYZ, None),
        ("Mouse targets, Franklin atlas; mice n=4", XYZ, None),
        ("Rat targets", ["Region", "interaural"], None),
    ],
)
def test_what_marks_a_table_as_rodent_stereotaxic(caption, headings, footer):
    assert _rodent(caption, headings, footer)


@pytest.mark.parametrize(
    "caption, headings, footer",
    [
        ("Activation in humans, MNI coordinates", XYZ, None),
        ("Peaks in the Mai and Paxinos atlas", XYZ, None),
        ("Participants used a mouse to respond; Mai and Paxinos atlas", XYZ, None),
        ("Mouse-click task; DBS targets", ["Region", "AP", "ML", "DV"], None),
        ("TMS targets, distance from bregma", XYZ, None),
        ("Interaural time difference sites", XYZ, None),
        ("Rated confidence", XYZ, "ratio of 3 to 1; a pirate task; Paxinos"),
        ("Bregmatic suture sites", XYZ, "Rats"),
        ("Electrode sites in mice", ["Region", "AP", "ML"], None),
        ("Electrode sites in rat", ["Region", "MAP", "XML", "DVD"], None),
        ("Electrode sites in rat", ["Region", "MAP", "ML", "DV"], None),
        ("Electrode sites in rat", ["Region", "AP", "XML", "DV"], None),
        ("Electrode sites in rat", ["Region", "AP", "ML", "DVD"], None),
        ("Targets in humans", ["Region", "AP", "ML", "DV"], None),
        ("Peaks in the Paxinos atlas", XYZ, None),
        ("Seeds in Paxinos and Mai", XYZ, "rated"),
    ],
)
def test_what_does_not_mark_a_table_as_rodent_stereotaxic(caption, headings, footer):
    assert not _rodent(caption, headings, footer)


def test_a_rodent_word_only_in_a_body_row_does_not_leave_a_table_out(tmp_path):
    markup = TALAIRACH_TABLE.replace("Posterior Cingulate Cortex", "Rat bregma Paxinos")
    parse, omitted = _parse(
        tmp_path, markup, {"tbl1": _collection(
            [Analysis(name="DMN", coordinates=[Coordinate(x=0, y=-47, z=29)])]
        )}, caption=HUMAN_CAPTION,
    )
    assert [a.name for a in parse.analyses] == ["DMN"] and omitted == []


def test_a_footer_alone_leaves_a_table_out(tmp_path):
    parse, omitted = _parse(
        tmp_path, TALAIRACH_TABLE, {"tbl1": _collection(
            [Analysis(name="DMN", coordinates=[Coordinate(x=0, y=-47, z=29)])]
        )}, caption="Seeds", footer="Rats; positions from bregma",
    )
    assert parse.analyses == [] and omitted[0].reason == paper_parse.RODENT_ATLAS_REASON


def _paper_parse(tmp_path, tables, per_table, **inputs):
    """coordinate_parse over several tables: {table_id: (markup, parsed table)}."""
    from ingestion_workflow.services.paper_parse import _grid

    grids = {}
    for table_id, (markup, _) in tables.items():
        (tmp_path / f"{table_id}.html").write_text(markup, encoding="utf-8")
        grids[table_id] = _grid(tmp_path / f"{table_id}.html")
    paper = SimpleNamespace(
        header=SimpleNamespace(identifiers=pp.ArticleIdentifiers()),
        text_sha256="0" * 64,
        tables=[table for _, table in tables.values()],
    )
    return paper_parse.coordinate_parse(
        paper, "Results\n", grids, per_table, ParseInputs(article_id="a", **inputs)
    )


def _dmn():
    return _collection([Analysis(name="DMN", coordinates=[Coordinate(x=0, y=-47, z=29)])])


def test_a_paper_with_a_rodent_table_and_a_human_one_keeps_the_human_analyses(tmp_path):
    rat, human = _seeds(), _dmn()
    parse, omitted = _paper_parse(
        tmp_path,
        {
            "rat": (BREGMA_TABLE, _bare_table("rat", RAT_CAPTION, BREGMA_HEADINGS)),
            "human": (TALAIRACH_TABLE, _bare_table("human", HUMAN_CAPTION)),
        },
        {"rat": rat, "human": human},
    )
    assert [a.name for a in parse.analyses] == ["DMN"]
    assert [(o.table_id, o.name) for o in omitted] == [("rat", "DMN"), ("rat", "Motor Network")]
    assert len(parse.analyses) + len(omitted) == len(rat.analyses) + len(human.analyses)
    assert {t.table_id: t.reading for t in parse.tables} == {
        "rat": "no_coordinates",
        "human": "coordinates",
    }


def test_a_rodent_table_a_person_excluded_or_triage_rejected_keeps_that_reason(tmp_path):
    rejected = pp.TableTriage(passes=False, route="gate")
    parse, _ = _paper_parse(
        tmp_path,
        {
            "hand": (BREGMA_TABLE, _bare_table("hand", RAT_CAPTION, BREGMA_HEADINGS)),
            "gate": (BREGMA_TABLE, _bare_table("gate", RAT_CAPTION, BREGMA_HEADINGS, triage=rejected)),
        },
        {"hand": _seeds(), "gate": _seeds()},
        excluded={"hand": {"reason": "not_coordinates", "note": "a design matrix"}},
    )
    got = {t.table_id: (t.reading, t.reason) for t in parse.tables}
    assert got == {
        "hand": ("excluded_by_hand", "not_coordinates; a design matrix"),
        "gate": ("rejected_by_triage", "gate"),
    }
