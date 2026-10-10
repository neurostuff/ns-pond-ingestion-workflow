"""The labelling job and its three exports, on fixture units with a fake labeller."""

from __future__ import annotations

import json

from ingestion_workflow.services.set_roles import export, labeling
from ingestion_workflow.services.set_roles.label_schema import SCHEMA, check

TABLE_ROW = {
    "article_id": "123-10-1000-x",
    "source": "pubget",
    "table_id": "t2",
    "title": "Fear",
    "abstract": "We scanned.",
    "caption": "Table 2. Seeds and results",
    "footer": "Seeds from Lee et al. (2008).",
    "coordinate_space": "MNI",
    "n_analyses": 2,
    "origin": "curated",
    "kind": "coordinates",
    "table_serialised": (
        "#Region | #x | #y | #z | #t\nAmygdala seed | 20 | -4 | -18 |\nInsula | 38 | 12 | 2 | 5.1"
    ),
    "target_json": json.dumps(
        {
            "space": "MNI",
            "analyses": [
                {"name": "seed", "points": [[20.0, -4.0, -18.0, None, None, None]]},
                {
                    "name": "PPI",
                    "measure": "voxels",
                    "points": [[38.0, 12.0, 2.0, "T", 5.1, None]],
                },
            ],
        }
    ),
}
TABLE_UNIT = {
    "unit_id": "t:123-10-1000-x:t2",
    "origin": "table",
    "article_id": "123-10-1000-x",
    "table_label": "Table 2",
    "title": "Fear",
    "abstract": "We scanned.",
    "caption": TABLE_ROW["caption"],
    "footer": TABLE_ROW["footer"],
    "table_serialised": TABLE_ROW["table_serialised"],
    "citing": ["Table 2 lists the seed taken from Lee et al. (2008)."],
    "sets": [
        {"name": "seed", "points": [[20.0, -4.0, -18.0, None, None, None]]},
        {"name": "PPI", "points": [[38.0, 12.0, 2.0, "T", 5.1, None]]},
    ],
    "base_row": TABLE_ROW,
    "stratum": "prior",
}
PROSE_ROW = {
    "id": "silver-1",
    "article_id": "abc",
    "dataset": "silver_train",
    "space": "MNI",
    "text": "Peaks reported by Lee et al. (2008) were at (10, 16, 57). We found (58, 12, 12).",
    "points": [
        {
            "xyz": [10.0, 16.0, 57.0],
            "role": "result",
            "stat": None,
            "cluster": None,
            "analysis": "Lee",
        },
        {
            "xyz": [58.0, 12.0, 12.0],
            "role": "result",
            "stat": ["F", 6.1],
            "cluster": None,
            "analysis": "Age x Group",
        },
    ],
}
PROSE_UNIT = {
    "unit_id": "p:silver-1",
    "origin": "text",
    "article_id": "abc",
    "heading": "Results",
    "text": PROSE_ROW["text"],
    "before": "Methods were standard.",
    "after": "",
    "sets": [
        {"name": "Lee", "points": [PROSE_ROW["points"][0]]},
        {"name": "Age x Group", "points": [PROSE_ROW["points"][1]]},
    ],
    "base_row": PROSE_ROW,
    "stratum": "plain",
}
ANSWERS = {
    "t:123-10-1000-x:t2": {
        "sets": [
            {
                "set": 0,
                "role": "anchor",
                "anchor_kind": "seed",
                "from_prior_study": True,
                "evidence": [1, 2],
            },
            {
                "set": 1,
                "role": "result",
                "anchor_kind": None,
                "from_prior_study": False,
                "evidence": [],
            },
        ]
    },
    "p:silver-1": {
        "sets": [
            {
                "set": 0,
                "role": "reference",
                "anchor_kind": None,
                "from_prior_study": False,
                "evidence": [1],
            },
            {
                "set": 1,
                "role": "result",
                "anchor_kind": None,
                "from_prior_study": False,
                "evidence": [],
            },
        ]
    },
}


class FakeCaller:
    def __init__(self, answers, fail=()):
        self.answers, self.fail, self.calls = answers, set(fail), []

    def __call__(self, system, prompt, schema, unit_id):
        self.calls.append((system, prompt, schema, unit_id))
        if unit_id in self.fail:
            raise RuntimeError("codex exec: boom")
        return self.answers[unit_id], {"input_tokens": 1000, "output_tokens": 50, "calls": 1}


def _labels(tmp_path):
    caller = FakeCaller(ANSWERS)
    labeling.run([TABLE_UNIT, PROSE_UNIT], tmp_path, caller, model="gpt-6.1-sol", effort="low")
    return labeling.read_labels(tmp_path)


def test_tables_and_passages_get_their_own_instructions_and_numbered_sentences():
    system, prompt, sentences = labeling.render(TABLE_UNIT)
    assert system == labeling.TABLE_SYSTEM and "one table" in system
    assert sentences == [
        "Table 2. Seeds and results",
        "Seeds from Lee et al. (2008).",
        "Table 2 lists the seed taken from Lee et al. (2008).",
    ]
    assert (
        "[1] Seeds from Lee et al. (2008)." in prompt
        and "#0 seed -- 1 points; rows: Amygdala seed" in prompt
    )
    system, prompt, sentences = labeling.render(PROSE_UNIT)
    assert system == labeling.PROSE_SYSTEM and "Section heading: Results" in prompt
    assert (
        sentences[0] == "Methods were standard."
        and "#1 Age x Group -- (58.0, 12.0, 12.0)" in prompt
    )


def test_the_schema_is_strict_and_answers_are_checked():
    item = SCHEMA["properties"]["sets"]["items"]
    assert item["additionalProperties"] is False and set(item["required"]) == set(
        item["properties"]
    )
    assert check(ANSWERS["p:silver-1"], 2, 4) is None
    assert "expected 0..2" in check({"sets": ANSWERS["p:silver-1"]["sets"][:1]}, 3, 4)
    assert "evidence outside" in check(ANSWERS["t:123-10-1000-x:t2"], 2, 2)


def test_rows_record_label_provenance_and_their_origin_s_context_version(tmp_path):
    labels = _labels(tmp_path)
    seed = labels["t:123-10-1000-x:t2#0"]
    assert (seed["label"], seed["from_prior_study"], seed["evidence"]) == (
        "anchor:seed",
        True,
        [1, 2],
    )
    assert seed["evidence_text"][0] == "Seeds from Lee et al. (2008)."
    assert seed["table_context_version"] == 1 and "prose_context_version" not in seed
    lee = labels["p:silver-1#0"]
    assert (
        lee["prose_context_version"] == 1 and lee["from_prior_study"] is True
    )  # a reference is prior
    assert lee["model"] == "gpt-6.1-sol" and lee["label_version"] == 1


def test_the_job_resumes_skips_done_units_and_keeps_a_ledger(tmp_path):
    caller = FakeCaller(ANSWERS, fail={"p:silver-1"})
    counts = labeling.run(
        [TABLE_UNIT, PROSE_UNIT], tmp_path, caller, model="m", effort="low", batch_size=1
    )
    assert counts == {"units": 1, "sets": 2, "failed": 1}
    assert "boom" in (tmp_path / "errors.jsonl").read_text()
    caller = FakeCaller(ANSWERS)
    labeling.run([TABLE_UNIT, PROSE_UNIT], tmp_path, caller, model="m", effort="low", batch_size=1)
    assert [c[3] for c in caller.calls] == ["p:silver-1"]  # the done table is skipped
    assert len(list(tmp_path.glob("batch-*.jsonl"))) == 2
    assert labeling.ledger_totals(tmp_path)["m"]["calls"] == 3
    assert (
        labeling.ledger_totals(tmp_path)["m"]["input_tokens"] == 2000
    )  # a failed call reports no cost


def test_rare_strata_are_taken_in_turn():
    units = [{"unit_id": f"p{i}", "stratum": "plain"} for i in range(6)] + [
        {"unit_id": "r", "stratum": "prior"}
    ]
    order = [u["unit_id"] for u in labeling.stratified(units)]
    assert order.index("r") <= 1 and sorted(order) == sorted(u["unit_id"] for u in units)


def test_agreement_is_reported_per_origin(tmp_path):
    a = _labels(tmp_path / "a")
    b = {k: dict(v) for k, v in a.items()}
    b["p:silver-1#0"].update(label="anchor:roi", from_prior_study=False)
    out = labeling.agreement(a, b)
    assert out["table"]["label_agreement"] == 1.0
    assert out["text"]["label_agreement"] == 0.5 and out["text"]["disagreements"] == {
        "reference | anchor:roi": 1
    }


def test_encoder_rows_are_one_string_per_set_split_by_article(tmp_path):
    rows = list(export.encoder_rows([TABLE_UNIT, PROSE_UNIT], _labels(tmp_path)))
    assert [r["label"] for r in rows] == ["anchor:seed", "result", "reference", "result"]
    assert rows[0]["text"].startswith("[ORIGIN] table") and rows[2]["text"].startswith(
        "[ORIGIN] text"
    )
    assert rows[0]["table_context_version"] == 1 and rows[2]["prose_context_version"] == 1
    assert rows[0]["split"] == rows[1]["split"] == export.split_of("123-10-1000-x")


def test_synthetic_units_bring_their_own_labels():
    unit = {
        **PROSE_UNIT,
        "unit_id": "p:syn",
        "labels_from": "dataset",
        "sets": [{"name": "x", "points": [{"xyz": [1, 2, 3], "role": "prior_study"}]}],
    }
    [row] = export.encoder_rows([unit], {})
    assert (row["label"], row["from_prior_study"], row["label_source"]) == (
        "reference",
        True,
        "dataset",
    )


def test_nu_v21_rows_keep_their_format_and_gain_a_role_per_analysis(tmp_path):
    [row] = export.nu_v21_rows([TABLE_UNIT, PROSE_UNIT], _labels(tmp_path))
    assert set(TABLE_ROW) <= set(row) and row["table_serialised"] == TABLE_ROW["table_serialised"]
    target = json.loads(row["target_json"])
    assert [(a["name"], a["role"]) for a in target["analyses"]] == [
        ("seed", "seed"),
        ("PPI", "result"),
    ]
    assert target["analyses"][1]["measure"] == "voxels" and list(target["analyses"][0])[:2] == [
        "name",
        "role",
    ]
    assert "role" in json.loads(export.NU_V21_TEMPLATE)["analyses"][0]


def test_prose_rows_keep_their_format_and_take_each_set_s_role(tmp_path):
    [row] = export.prose_rows([TABLE_UNIT, PROSE_UNIT], _labels(tmp_path))
    assert [p["role"] for p in row["points"]] == ["prior_study", "result"]
    assert (
        row["text"] == PROSE_ROW["text"]
        and row["id"] == "silver-1"
        and row["prose_context_version"] == 1
    )


def test_a_unit_with_an_unlabelled_set_is_not_exported_to_the_extractors(tmp_path):
    labels = _labels(tmp_path)
    del labels["t:123-10-1000-x:t2#1"]
    assert list(export.nu_v21_rows([TABLE_UNIT], labels)) == []
