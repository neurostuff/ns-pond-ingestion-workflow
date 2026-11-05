"""The normalization's per-article rules on real records, against the Astra build's rows for them.

`data/astra_records.json` holds three open-access records of records_all2 (PMC3775485,
PMC3169847, PMC2532854), each paper's bibliography in the shape of ns-pond's `references`
artifact, and the rows out_all2 made of them. Their tasks are all named by the lexicon or
gated, so route B (the 86 MB fixed pickle) decides none of them.
"""

import copy
import json
import zlib
from pathlib import Path

import pytest
from ingestion_workflow.normalization import citations, contrasts, records
from ingestion_workflow.normalization.taskcat.identity import CardBuilder

DATA = json.loads((Path(__file__).parent / "data" / "astra_records.json").read_text())
STUDIES = sorted(DATA)
#: route B's own answer is carried on every task row; without its pickle it is absent
WITHOUT_ROUTE_B = ("route_b", "agree")


@pytest.fixture(scope="module")
def cb():
    return CardBuilder(route_b=None)


def strip(rows, keys):
    return [{k: v for k, v in r.items() if k not in keys} for r in rows]


def plain(x):
    return json.loads(json.dumps(x))


@pytest.mark.parametrize("study", STUDIES)
def test_pass_one_is_the_builds(study, cb):
    rows, _ = records.read(DATA[study]["record"], study, cb, records.schemas())
    build = DATA[study]["build"]
    assert strip(plain(rows["tasks"]), WITHOUT_ROUTE_B) == strip(
        build["tc_tasks"], WITHOUT_ROUTE_B
    )
    assert plain(rows["contrasts"]) == build["tc_contrasts"]


@pytest.mark.parametrize("study", STUDIES)
def test_pass_two_under_the_builds_labels_is_the_builds(study, cb):
    final = {(x["study"], x["task"]): x for x in DATA[study]["build"]["task_labels"]}
    rows, _ = records.read(DATA[study]["record"], study, cb, records.schemas(), final)
    build = DATA[study]["build"]
    added = ("atoms_from_description", "atoms_from_stimulus")
    assert strip(plain(rows["conditions"]), added) == build["conditions"]
    assert plain(rows["contrasts"]) == build["contrasts"]
    assert plain(rows["analyses"]) == build["analyses"]
    assert build["contrasts"], "a fixture with no contrast tests nothing here"


def test_final_labels_change_what_pass_two_reads(cb):
    """The labels are an input: relabelling a task moves its contrasts' family and key."""
    study = STUDIES[0]
    final = {(x["study"], x["task"]): x for x in DATA[study]["build"]["task_labels"]}
    moved = {
        k: {**x, "canonical": "hedtsk_stroop_color_word", "family": "conflict_and_interference"}
        for k, x in final.items()
    }
    rows, _ = records.read(DATA[study]["record"], study, cb, records.schemas(), moved)
    assert rows["contrasts"]
    assert all(
        k["family"] == "conflict_and_interference"
        and k["key"].startswith("conflict_and_interference|")
        for k in rows["contrasts"]
        if k["task"] is not None
    )


@pytest.mark.parametrize("study", STUDIES)
def test_references_artifact_resolves_the_records_markers(study):
    """ns-pond's references payload, as a bibliography, resolves the tasks' markers."""
    bib = citations.bib_from_references(DATA[study]["references"])
    rows = citations.task_refs(DATA[study]["record"], study, "", bib)
    marked = [
        t for t in DATA[study]["record"]["tasks"] if (t.get("references") or {}).get("value")
    ]
    if not marked:
        pytest.skip("no task cites anything")
    assert any(t["refs"] for t in rows)
    ids = {r["id"] for r in DATA[study]["references"]["references"]}
    assert all(x["rid"] in ids for t in rows for x in t["refs"])


def test_a_link_resolves_before_numbering():
    """A marker the source linked goes to the entry it links, not the entry at that position."""
    payload = {
        "references": [
            {"id": "r1", "position": 1, "authors": ["Smith"], "year": 2001, "title": "A"},
            {"id": "r2", "position": 2, "authors": ["Jones"], "year": 2005, "title": "B"},
        ],
        "citations": [{"text_span": {"text": "1"}, "references": ["r2"], "marker_in_text": True}],
    }
    bib = citations.bib_from_references(payload)
    from ingestion_workflow.normalization.taskcat.citations import resolve_with_bib

    assert [r["rid"] for r in resolve_with_bib("[1]", bib)] == ["r2"]
    assert [r["rid"] for r in resolve_with_bib("Jones et al., 2005", bib)] == ["r2"]
    assert citations.bib_from_references({"references": []}) is None


# ------------------------------------------------------------- contrast sets: fit, then assign
class WordEncoder:
    """A stand-in for MiniLM: a normalized bag of words, so similar wording is near."""

    revision = "test"

    def __call__(self, texts):
        import numpy as np

        rows = []
        for t in texts:
            rows.append(
                {
                    zlib.crc32(w.encode()) % 256: 1.0
                    for w in t.lower().replace(";", " ").replace(":", " ").split()
                }
            )
        out = np.zeros((len(texts), 256), np.float32)
        for i, v in enumerate(rows):
            for j, x in v.items():
                out[i, j] = x
        n = np.linalg.norm(out, axis=1, keepdims=True)
        n[n == 0] = 1
        return out / n

    def sides(self, docs):
        return self([contrasts.side_text(d["target"]) for d in docs]), self(
            [contrasts.side_text(d["baseline"]) for d in docs]
        )


def doc(
    study,
    analysis,
    target,
    baseline,
    family="working_memory",
    atoms=((), ()),
    canonical="hedtsk_n_back",
):
    def side(names, a):
        return [
            {
                "name": n,
                "level": None,
                "kind": "task_state",
                "atoms": list(a),
                "phases": [],
                "token": n,
                "description": "",
                "stimulus": "",
            }
            for n in names
        ]

    return {
        "id": f"{study}/{analysis}",
        "study": study,
        "analysis": analysis,
        "family": family,
        "canonical": canonical,
        "shape": "neither",
        "sign": 1,
        "task_name": "n-back task",
        "task_description": "letters",
        "target": side(target, atoms[0]),
        "baseline": side(baseline, atoms[1]),
    }


@pytest.fixture(scope="module")
def setfit(tmp_path_factory):
    pytest.importorskip("scipy")
    docs = [
        doc(f"p{i}", "a1", ["2-back"], ["0-back"], atoms=(["load:2"], ["load:0"]))
        for i in range(4)
    ]
    docs += [
        doc(f"q{i}", "a1", ["faces"], ["houses"], family="perceptual_judgment", canonical=None)
        for i in range(3)
    ]
    sets, members, arrays = contrasts.fit_sets(docs, WordEncoder(), log=lambda m: None)
    out = tmp_path_factory.mktemp("sets")
    contrasts.save_sets(out, sets, members, arrays)
    return contrasts.SetFit(out)


def test_fit_pools_each_family_apart(setfit):
    fams = sorted(s["family"] for s in setfit.sets.values())
    assert fams == ["perceptual_judgment", "working_memory"]
    assert all(s["n_papers"] >= 2 for s in setfit.sets.values())


def test_a_new_paper_joins_its_set_oriented(setfit):
    same = doc("new", "a1", ["2-back"], ["0-back"], atoms=(["load:2"], ["load:0"]))
    rev = doc("new", "a2", ["0-back"], ["2-back"], atoms=(["load:0"], ["load:2"]))
    got = setfit.assign([same, rev], WordEncoder())
    wm = next(s for s in setfit.sets.values() if s["family"] == "working_memory")["set"]
    assert got["new/a1"]["set"] == wm and got["new/a1"]["orient"] == 1
    assert got["new/a2"]["set"] == wm and got["new/a2"]["orient"] == -1
    assert got["new/a2"]["effective_sign"] == -1


def test_cannot_links_and_family_keep_a_contrast_out(setfit):
    clash = doc(
        "new", "a3", ["3-back"], ["0-back"], atoms=(["load:3"], ["load:0"])
    )  # load 3 contradicts load 2
    other = doc(
        "new", "a4", ["2-back"], ["0-back"], family="language", atoms=(["load:2"], ["load:0"])
    )
    unlike = doc("new", "a5", ["tones"], ["silence"])
    got = setfit.assign([clash, other, unlike], WordEncoder())
    assert got == {"new/a3": None, "new/a4": None, "new/a5": None}


def test_assignment_reads_the_saved_fit_only(setfit):
    """Assigning twice gives the same answer: assignment never folds a contrast into the fit."""
    d = doc("new", "a1", ["2-back"], ["0-back"], atoms=(["load:2"], ["load:0"]))
    before = copy.deepcopy(setfit.members)
    assert setfit.assign([d], WordEncoder()) == setfit.assign([d], WordEncoder())
    assert setfit.members == before
