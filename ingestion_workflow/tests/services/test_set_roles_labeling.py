"""The labelling job and its three exports, on fixture units with a fake labeller."""

from __future__ import annotations

import json
import os
import sys
import textwrap

import pytest
from ingestion_workflow.services.set_roles import export, labeling
from ingestion_workflow.services.set_roles.label_schema import SCHEMA, check
from ingestion_workflow.services.set_roles.labels import COORDINATE_ROLES, SetRole

RESULT = {"role": "result", "anchor_kind": None, "from_prior_study": False}
SEED = {"role": "anchor", "anchor_kind": "seed", "from_prior_study": False}
REFERENCE = {"role": "reference", "anchor_kind": None, "from_prior_study": True}

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
        },
        separators=(",", ":"),  # train_v21.jsonl's target_json is compact
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
            "anchor_kind": None,
            "from_prior_study": False,
            "stat": None,
            "cluster": None,
            "analysis": "Lee",
        },
        {
            "xyz": [58.0, 12.0, 12.0],
            "role": "result",
            "anchor_kind": None,
            "from_prior_study": False,
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
                "coordinates": True,
                "role": "anchor",
                "anchor_kind": "seed",
                "from_prior_study": True,
                "evidence": [1, 2],
            },
            {
                "set": 1,
                "coordinates": True,
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
                "coordinates": True,
                "role": "reference",
                "anchor_kind": None,
                "from_prior_study": False,
                "evidence": [1],
            },
            {
                "set": 1,
                "coordinates": True,
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
    assert (seed["role"], seed["anchor_kind"], seed["from_prior_study"], seed["evidence"]) == (
        "anchor",
        "seed",
        True,
        [1, 2],
    )
    assert seed["evidence_text"][0] == "Seeds from Lee et al. (2008)."
    assert seed["table_context_version"] == 1 and "prose_context_version" not in seed
    lee = labels["p:silver-1#0"]
    assert (
        lee["prose_context_version"] == 2 and lee["from_prior_study"] is True
    )  # a reference is prior
    assert lee["model"] == "gpt-6.1-sol" and lee["label_version"] == 3


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
    b["p:silver-1#0"].update(role="anchor", anchor_kind="roi", from_prior_study=False)
    out = labeling.agreement(a, b)
    assert out["table"]["label_agreement"] == 1.0
    assert out["text"]["label_agreement"] == 0.5 and out["text"]["disagreements"] == {
        "reference | anchor roi": 1
    }


def test_encoder_rows_are_one_string_per_set_split_by_article(tmp_path):
    rows = list(export.encoder_rows([TABLE_UNIT, PROSE_UNIT], _labels(tmp_path)))
    assert [(r["role"], r["anchor_kind"]) for r in rows] == [
        ("anchor", "seed"),
        ("result", None),
        ("reference", None),
        ("result", None),
    ]
    assert all(r["coordinates"] for r in rows)
    assert rows[0]["text"].startswith("[ORIGIN] table") and rows[2]["text"].startswith(
        "[ORIGIN] text"
    )
    assert rows[0]["table_context_version"] == 1 and rows[2]["prose_context_version"] == 2
    assert rows[0]["split"] == rows[1]["split"] == export.split_of("123-10-1000-x")


def test_synthetic_units_bring_their_own_labels():
    unit = {
        **PROSE_UNIT,
        "unit_id": "p:syn",
        "labels_from": "dataset",
        "sets": [{"name": "x", "points": [{"xyz": [1, 2, 3], **REFERENCE}]}],
    }
    [row] = export.encoder_rows([unit], {})
    assert (row["role"], row["from_prior_study"], row["label_source"]) == (
        "reference",
        True,
        "dataset",
    )


def _held_out_unit(split="test", label_source="neurometabench+luna"):
    return {
        **PROSE_UNIT,
        "unit_id": "p:nmb-9",
        "dataset_split": split,
        "base_row": {**PROSE_ROW, "id": "nmb-9", "label_source": label_source},
    }


def test_hand_rows_are_the_prose_gold_set_in_test():
    hand = _held_out_unit("train", "hand")
    rows = list(export.encoder_rows([hand], {}))
    assert [(r["role"], r["label_source"], r["split"]) for r in rows] == [
        ("result", "hand", "test"),
        ("result", "hand", "test"),
    ]


def test_proposed_is_the_pipeline_s_proposal_never_the_label():
    synthetic = {
        **PROSE_UNIT,
        "unit_id": "p:syn",
        "labels_from": "dataset",
        "dataset": "synthetic",
        "sets": [{"name": "x", **SEED, "points": [{"xyz": [1, 2, 3], **SEED}]}],
    }
    wild = {**synthetic, "unit_id": "w:a:0", "dataset": "wild", "labels_from": None}
    [syn_ctx] = labeling.contexts(synthetic)
    [wild_ctx] = labeling.contexts(wild)
    assert labeling.serialize(synthetic, syn_ctx).startswith("[ORIGIN] text [PROPOSED] unknown ")
    assert wild_ctx.proposed == SetRole("anchor", "seed")  # the prose stage's own role
    [table_ctx, _] = labeling.contexts(TABLE_UNIT)
    assert table_ctx.proposed == SetRole("result")


def test_synthetic_sets_are_train_only():
    units = [
        {**PROSE_UNIT, "unit_id": f"p:syn-{i}", "article_id": f"syn-{i}", "labels_from": "dataset"}
        for i in range(30)
    ]
    for u in units:
        u["sets"] = [{"name": "x", "points": [{"xyz": [1, 2, 3], **RESULT}]}]
    assert {r["split"] for r in export.encoder_rows(units, {})} == {"train"}


def test_the_split_is_by_database_id_and_repeated_tables_share_one(tmp_path):
    slug = {**TABLE_UNIT, "unit_id": "t:123-10-1000-x:t3", "table_id": "t3"}
    dbid = {**TABLE_UNIT, "unit_id": "t:abcdefabcdef:t9", "article_id": "AbcdefABCDEF"}
    copy = {**TABLE_UNIT, "unit_id": "t:zzzzzzzzzzzz:t1", "article_id": "zzzzzzzzzzzz"}
    other = {
        **TABLE_UNIT,
        "unit_id": "t:yyyyyyyyyyyy:t1",
        "article_id": "yyyyyyyyyyyy",
        "table_serialised": "#Region | #x\nA | 1",
    }
    dbid["table_serialised"] = other["table_serialised"] + " | 2"
    units = [slug, dbid, copy, other]
    split = export.splits(units, {}, {"123-10-1000-x": "abcdefabcdef"})
    # slug -> abcdefabcdef (dbid, case folded); copy repeats slug's table text
    assert split[slug["unit_id"]] == split[dbid["unit_id"]] == split[copy["unit_id"]]
    assert labeling.duplicates(units) == {copy["unit_id"]: slug["unit_id"]}


def test_an_article_with_a_gold_label_is_test(tmp_path):
    caller = FakeCaller(ANSWERS)
    labeling.run([TABLE_UNIT], tmp_path, caller, model="gpt-6-astra", effort="medium")
    gold = labeling.read_labels(tmp_path)
    sibling = {**TABLE_UNIT, "unit_id": "t:123-10-1000-x:t3", "table_serialised": "x"}
    split = export.splits([TABLE_UNIT, sibling], gold)
    assert split == {TABLE_UNIT["unit_id"]: "test", sibling["unit_id"]: "test"}


def test_repeated_tables_take_their_first_copy_s_labels(tmp_path):
    copy = {**TABLE_UNIT, "unit_id": "t:zzzzzzzzzzzz:t1", "article_id": "zzzzzzzzzzzz"}
    assert [r["set_id"] for r in export.encoder_rows([TABLE_UNIT, copy], _labels(tmp_path))] == [
        "t:123-10-1000-x:t2#0",
        "t:123-10-1000-x:t2#1",
    ]


@pytest.mark.parametrize("cut", [10, 60])
def test_resume_tolerates_a_truncated_last_line(tmp_path, cut):
    labeling.run([TABLE_UNIT, PROSE_UNIT], tmp_path, FakeCaller(ANSWERS), model="m", effort="low")
    [batch] = tmp_path.glob("batch-*.jsonl")
    lines = batch.read_text().splitlines(keepends=True)
    # killed while writing the prose unit's second row
    batch.write_text("".join(lines[:3]) + lines[3][:cut])
    assert labeling.done_units(tmp_path) == {TABLE_UNIT["unit_id"]}
    assert set(labeling.read_labels(tmp_path)) == {
        "t:123-10-1000-x:t2#0",
        "t:123-10-1000-x:t2#1",
    }
    caller = FakeCaller(ANSWERS)
    labeling.run([TABLE_UNIT, PROSE_UNIT], tmp_path, caller, model="m", effort="low")
    assert [c[3] for c in caller.calls] == ["p:silver-1"]
    assert len(labeling.read_labels(tmp_path)) == 4


def test_plain_tables_are_sampled_but_hinted_ones_kept():
    plain = [
        {
            **TABLE_UNIT,
            "unit_id": f"t:p{i}",
            "stratum": "plain",
            "caption": "Results",
            "footer": "",
            "citing": [],
        }
        for i in range(10)
    ]
    plain[3]["footer"] = "Labels from the AAL atlas."
    kept = labeling.thin_plain([TABLE_UNIT, *plain], keep=2, seed=0)
    ids = [u["unit_id"] for u in kept]
    assert ids[0] == TABLE_UNIT["unit_id"] and "t:p3" in ids and len(ids) == 1 + 1 + 2
    assert ids == [u["unit_id"] for u in labeling.thin_plain([TABLE_UNIT, *plain], 2, seed=0)]


FAKE_CODEX = """\
#!{python}
import json, os, sys
if sys.argv[1:3] == ["features", "list"]:
    sys.exit(0)
sys.stdin.read()
state = os.path.join(os.environ["FAKE_CODEX_DIR"], "calls")
n = int(open(state).read()) if os.path.exists(state) else 0
open(state, "w").write(str(n + 1))
if n == 0:
    print(json.dumps({{"type": "turn.failed", "error": {{"message":
        "You've hit your usage limit. Try again at 11:36 PM."}}}}))
    sys.exit(1)
print(json.dumps({{"type": "thread.started", "thread_id": "t1"}}))
print(json.dumps({{"type": "item.completed", "item": {{"type": "agent_message",
    "text": {answer}}}}}))
print(json.dumps({{"type": "turn.completed", "usage": {{"input_tokens": 7, "output_tokens": 3}}}}))
"""


def test_a_spent_usage_limit_is_waited_out_then_the_unit_is_labelled(tmp_path, monkeypatch):
    llm = pytest.importorskip("pondie.extraction.llm")
    if not hasattr(llm, "CodexUsageLimit"):
        pytest.skip("pondie predates #10")
    codex = tmp_path / "codex"
    answer = json.dumps(ANSWERS["t:123-10-1000-x:t2"])
    codex.write_text(
        textwrap.dedent(FAKE_CODEX).format(python=sys.executable, answer=repr(answer))
    )
    codex.chmod(0o755)
    monkeypatch.setenv("FAKE_CODEX_DIR", str(tmp_path))
    waits = []
    monkeypatch.setattr(llm.time, "sleep", waits.append)
    caller = labeling.codex_caller("gpt-6.1-sol", "low", binary=str(codex))
    out = tmp_path / "labels"
    counts = labeling.run([TABLE_UNIT], out, caller, model="gpt-6.1-sol", effort="low")
    assert counts == {"units": 1, "sets": 2}
    assert len(waits) == 1 and waits[0] >= 60  # slept to the reset, once
    assert (tmp_path / "calls").read_text() == "2"
    assert labeling.ledger_totals(out)["gpt-6.1-sol"]["input_tokens"] == 7
    assert not os.path.exists(out / "errors.jsonl")


def _role_values(value, found):
    """Every `role` and `anchor_kind` anywhere in a row, target_json included."""
    if isinstance(value, dict):
        for k, v in value.items():
            if k in ("role", "anchor_kind"):
                found.append((k, v))
            _role_values(json.loads(v) if k == "target_json" else v, found)
    elif isinstance(value, list):
        for v in value:
            _role_values(v, found)
    return found


def test_every_exported_role_is_study_schema_s(tmp_path):
    labels = _labels(tmp_path)
    labels["t:123-10-1000-x:t2#1"].update(role=None, anchor_kind=None, from_prior_study=False)
    rows = list(export.encoder_rows([TABLE_UNIT, PROSE_UNIT], labels))
    found = _role_values(rows, [])
    assert {k for k, _ in found} == {"role", "anchor_kind"}
    for key, value in found:
        allowed = (
            COORDINATE_ROLES if key == "role" else ("roi", "seed", "stimulation_target", "node")
        )
        assert value in (*allowed, None), f"{key} {value!r} is not study_schema's"


def test_a_legacy_role_in_an_exported_row_is_refused(tmp_path):
    with pytest.raises(ValueError, match="not a CoordinateRole"):
        list(
            export.encoder_rows(
                [TABLE_UNIT],
                {
                    "t:123-10-1000-x:t2#0": {
                        "role": "anchor:roi",
                        "anchor_kind": None,
                        "from_prior_study": False,
                    }
                },
            )
        )


def test_an_answer_that_is_not_coordinates_has_no_role():
    from ingestion_workflow.services.set_roles.label_schema import set_role

    answer = {
        "set": 0,
        "coordinates": False,
        "role": None,
        "anchor_kind": None,
        "from_prior_study": False,
        "evidence": [],
    }
    assert check({"sets": [answer]}, 1, 0) is None
    assert set_role(answer) == SetRole(None)
    assert "coordinates without a role" in check({"sets": [{**answer, "coordinates": True}]}, 1, 0)
    assert SCHEMA["properties"]["sets"]["items"]["properties"]["role"]["enum"] == [
        *COORDINATE_ROLES,
        None,
    ]
