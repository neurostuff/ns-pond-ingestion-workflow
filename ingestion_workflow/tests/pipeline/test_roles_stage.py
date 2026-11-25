"""`roles` gives every set the decision model's role, and nothing passes without one."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from ingestion_workflow.catalog import Artifact, Catalog, Outcome, Status
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.config import RoleModel, Settings
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.roles import (
    RolesStage,
    assign_roles,
    unassigned,
    uploaded_sets,
)
from ingestion_workflow.pipeline.stages.space import SpaceStage
from ingestion_workflow.pipeline.stages.sync import SyncStage
from ingestion_workflow.pipeline.stages.upload import UploadStage
from ingestion_workflow.services.set_roles import (
    COORDINATE_ROLES,
    Prediction,
    geometry,
)

TEXT = "Peaks from earlier work are listed in Table 2. Table 2 quotes Lee et al. (2008)."


#: An answer's kind, space and source when it does not give them.
DEFAULTS = (None, "template", "this_study")


class FakeClassifier:
    """Answers from the set's name: what a model trained on these names would say.

    An answer is `(role, p, kind=None, space="template", source="this_study")`.
    """

    def __init__(self, answers, source="fake@1"):
        self.answers = answers
        self.source = source
        self.seen = []

    def predict(self, texts):
        self.seen += texts
        out = []
        for text in texts:
            answer = next(v for k, v in self.answers.items() if f"[NAME] {k}" in text)
            role, p, kind, space, source = (*answer, *DEFAULTS[len(answer) - 2:])
            rest = (1 - p) / (len(COORDINATE_ROLES) - 1)
            out.append(
                Prediction(
                    role,
                    kind,
                    {n: (p if n == role else rest) for n in COORDINATE_ROLES},
                    space,
                    source,
                    1.0,
                )
            )
        return out


def _analysis(name, caption="", **meta):
    return {
        "name": name,
        "table_caption": caption,
        "table_footer": "",
        "description": None,
        "metadata": {"table_metadata": {"table_label": "Table 2"}, **meta},
        "coordinates": [{"x": -42.0, "y": 18.0, "z": 24.0, "space": "MNI"}],
    }


def _tables():
    return {
        "t2": {
            "slug": "a::t2",
            "coordinate_space": "MNI",
            "identifier": None,
            "analyses": [
                _analysis("patients > controls"),
                _analysis("Lee et al. (2008)", caption="Coordinates of prior studies"),
            ],
        }
    }


def _prose():
    return {
        "slug": "a",
        "coordinate_space": "MNI",
        "identifier": None,
        "analyses": [
            {
                "name": "amygdala seed",
                "coordinates": [{"x": 20.0, "y": -4.0, "z": -18.0, "space": "MNI"}],
                "metadata": {"source": "prose", "passages": [0]},
            }
        ],
    }


PASSAGES = [{"text": "The amygdala seed (20, -4, -18) was a 6 mm sphere.", "heading": "PPI"}]
TABLE_ANSWERS = {
    "patients > controls": ("result", 0.97),
    "Lee et al. (2008)": ("reference", 0.93, None, "template", "prior_study"),
    "amygdala seed": ("anchor", 0.99, "seed"),
}


def test_roles_is_required_between_resolve_and_space(tmp_path):
    order = list(STAGE_ORDER)
    assert order.index("resolve") < order.index("roles") < order.index("space")
    # On by default, with no model configured: it is the articles that fail, not the build.
    assert "roles" in [s.name for s in build([], Settings(data_root=tmp_path))]
    assert [s.name for s in build(["roles"], Settings(data_root=tmp_path))] == ["roles"]
    off = Settings(data_root=tmp_path)
    on = Settings(data_root=tmp_path, prose_model="nu-prose")
    assert RolesStage.upstream_for(off) == ("analyses", "tables")
    assert RolesStage.upstream_for(on) == ("resolve", "tables")
    assert SpaceStage.upstream_for(off) == SpaceStage.upstream_for(on) == ("roles", "tables")


def test_the_summary_counts_prose_sets_whose_points_are_not_in_their_passage():
    prose = FakeClassifier(TABLE_ANSWERS)
    elsewhere = [{**PASSAGES[0], "text": "The amygdala seed was a 6 mm sphere."}]
    for passages, missing in ((PASSAGES, 0), (elsewhere, 1)):
        _, summary = assign_roles({"prose": _prose()}, passages, TEXT, prose)
        assert summary["text_sets_without_local"] == missing
    assert "[LOCAL]" not in prose.seen[-1]


def test_every_set_is_read_through_its_origin_s_context_by_the_one_model():
    model = FakeClassifier(TABLE_ANSWERS)
    payload = {**_tables(), "prose": _prose()}
    out, summary = assign_roles(payload, PASSAGES, TEXT, model)
    # One call for the article's sets, table and prose alike.
    assert [t.split()[1] for t in model.seen] == ["table", "table", "text"]
    assert "[PASSAGE] The amygdala seed" in model.seen[2]
    # A held set stays in its place, so pondie's `t2#2` still names the second set.
    kept, held = out["t2"]["analyses"]
    assert (kept["metadata"]["held"], held["metadata"]["held"]) == (False, True)
    assert "held" not in out["t2"]
    [seed] = out["prose"]["analyses"]
    assert kept["metadata"]["set_role"] == {
        "role": "result",
        "anchor_kind": None,
        "from_prior_study": False,
        "space": "template",
        "coordinate_source": "this_study",
        "prior_study_evidence": [],
        "role_confidence": 0.97,
        "role_source": "fake@1",
        "role_origin": "table",
    }
    role = held["metadata"]["set_role"]
    assert (role["role"], role["from_prior_study"], role["coordinate_source"],
            role["role_source"]) == (
        "reference",
        True,
        "prior_study",
        "fake@1",
    )
    span = role["prior_study_evidence"][0]
    assert TEXT[span["start_char"] : span["end_char"]] == span["text"]
    assert span["text"] == "Table 2 quotes Lee et al. (2008)."
    assert held["metadata"]["table_metadata"] == {"table_label": "Table 2"}  # metadata kept
    seed_role = seed["metadata"]["set_role"]
    assert (seed_role["role"], seed_role["anchor_kind"], seed_role["role_origin"]) == (
        "anchor",
        "seed",
        "text",
    )
    assert seed_role["role_source"] == "fake@1"
    assert isinstance(summary.pop("role_values"), str)
    assert isinstance(summary.pop("role_records"), str)
    assert summary == {
        "tables": 2,
        "sets": 3,
        "sets_by_origin": {"table": 2, "text": 1},
        "text_sets_without_local": 0,
        "roles": {"result": 1, "reference": 1, "seed": 1},
        "held": 1,
        "not_brain": 0,
        "other_space": 0,
        "model": "fake@1",
    }
    assert unassigned(out) == []
    # Upload leaves the held set out.
    assert [a.name for a in AnalysisCollection.from_dict(uploaded_sets(out)["t2"]).analyses] == [
        "patients > controls"
    ]


def test_an_unsure_answer_is_still_the_model_s_and_says_how_unsure():
    table = FakeClassifier({**TABLE_ANSWERS, "Lee et al. (2008)": ("reference", 0.4)})
    out, _ = assign_roles(_tables(), [], TEXT, table)
    role = out["t2"]["analyses"][1]["metadata"]["set_role"]
    assert (role["role"], role["role_source"], role["role_confidence"]) == (
        "reference",
        "fake@1",
        0.4,
    )


def test_a_set_in_another_frame_keeps_its_role_and_is_held():
    """A rat seed is an anchor and a seed, in bregma millimetres: not uploaded."""
    rat = FakeClassifier({**TABLE_ANSWERS, "patients > controls": ("anchor", 0.9, "seed", "other")})
    out, summary = assign_roles(_tables(), [], TEXT, rat)
    seed = out["t2"]["analyses"][0]["metadata"]
    assert (seed["set_role"]["role"], seed["set_role"]["anchor_kind"], seed["set_role"]["space"],
            seed["held"]) == ("anchor", "seed", "other", True)
    assert (summary["held"], summary["other_space"]) == (2, 1)
    assert uploaded_sets(out)["t2"]["analyses"] == []


@pytest.mark.parametrize("source, prior", [
    ("this_study", False), ("prior_study", True), ("atlas_or_database", False)])
def test_from_prior_study_is_the_model_s_source(source, prior):
    model = FakeClassifier({**TABLE_ANSWERS,
                            "patients > controls": ("anchor", 0.9, "node", "template", source)})
    out, _ = assign_roles(_tables(), [], TEXT, model)
    role = out["t2"]["analyses"][0]["metadata"]["set_role"]
    assert (role["coordinate_source"], role["from_prior_study"]) == (source, prior)
    assert bool(role["prior_study_evidence"]) is prior


def test_without_a_model_no_set_gets_a_role_at_all(tmp_path):
    from ingestion_workflow.pipeline.stages.roles import _Model

    stage = RolesStage(Settings(data_root=tmp_path, prose_model="nu-prose"))
    payload = {**_tables(), "prose": _prose()}
    with pytest.raises(ModelUnavailable, match=r"no role model is configured \(role_model\)"):
        assign_roles(payload, PASSAGES, TEXT, _Model(stage))


def test_unassigned_names_every_set_without_a_decided_role():
    out, _ = assign_roles(_tables(), [], TEXT, FakeClassifier(TABLE_ANSWERS))
    assert unassigned(_tables()) == ["t2#0", "t2#1"]
    held = out["t2"]["analyses"][1]["metadata"]
    held["set_role"]["role_source"] = None
    assert unassigned(out) == ["t2#1"]  # a held set counts too
    held["set_role"] = {"role": "result"}  # incomplete
    assert unassigned(out) == ["t2#1"]


@pytest.mark.parametrize(
    "change",
    [
        {"role": "display"},  # not study_schema's
        {"role": "anchor", "anchor_kind": "bogus"},  # an anchor kind is study_schema's
        {"role": "bogus"},
    ],
)
def test_upload_and_sync_refuse_a_role_that_is_not_study_schema_s(change):
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.roles import refuse_unassigned

    out, _ = assign_roles(_tables(), [], TEXT, FakeClassifier(TABLE_ANSWERS))
    out["t2"]["analyses"][0]["metadata"]["set_role"].update(change)
    assert unassigned(out) == ["t2#0"]
    work = Work(ref=SimpleNamespace(id="a1"), source="", fingerprint="fp", upstream=None)
    for stage in ("upload", "sync"):
        assert refuse_unassigned(stage, work, out).status is Status.FAILED


def test_a_set_not_marked_held_or_not_is_unassigned():
    out, _ = assign_roles(_tables(), [], TEXT, FakeClassifier(TABLE_ANSWERS))
    del out["t2"]["analyses"][1]["metadata"]["held"]
    assert unassigned(out) == ["t2#1"]


@pytest.fixture
def model_dir(tmp_path):
    pytest.importorskip("torch")
    from ingestion_workflow.tests.decision_model import write_model

    return write_model(tmp_path / "model")


def test_the_model_is_part_of_each_origin_s_fingerprint(tmp_path, model_dir):
    from ingestion_workflow.tests.decision_model import write_model

    stage = RolesStage(Settings(data_root=tmp_path, prose_model="nu-prose",
                                role_model=RoleModel(path=str(model_dir))))
    assert stage.model_state()[0].startswith("role-test@v1+")
    fps = stage.models()
    # Each origin's part names its own context version as well as the model.
    assert set(fps) == {"table", "text"} and fps["table"] != fps["text"]
    write_model(model_dir, version="v2")
    assert all(stage.models()[o] != fps[o] for o in fps)
    stage.settings.role_model = None
    assert stage.model_state() == (None, "no role model is configured (role_model)")


@pytest.fixture
def env(tmp_path, evidence):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond")
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(pmid="7"), evidence)
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                                payload=_tables(), summary={"tables": 1})])
        yield settings, catalog, ref


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def test_without_a_model_the_article_is_blocked_and_everything_after_it(
    env, monkeypatch, model_dir
):
    settings, catalog, ref = env
    ctx = Context(settings, catalog)
    # More runs than the retries allow: nothing is attempted, so nothing is used up.
    for _ in range(ctx.max_attempts + 1):
        plan, outcomes = _run(RolesStage(settings), ctx, catalog, ref)
        assert (plan.blocked, plan.pending, outcomes) == (1, [], [])
        assert plan.reasons == {"no role model is configured (role_model)": 1}
        assert "no role model is configured" in plan.describe()
    assert catalog.attempt_counts([ref.id], "roles", "") in ({}, {ref.id: (0, None)})
    plan, outcomes = _run(SpaceStage(settings), ctx, catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    # A space artifact from before the roles stage existed is not enough for upload or sync.
    catalog.record([Outcome(article_id=ref.id, stage="space", source="", fingerprint="sp-0",
                            payload=_tables(), summary={"tables": 1}),
                    Outcome(article_id=ref.id, stage="upload", source="", fingerprint="up-0",
                            payload={}, summary={"tables": 1, "base_study_id": "b1"})])
    for stage in (UploadStage(settings), SyncStage(settings)):
        plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                          catalog.artifacts([ref.id], stage.requires))
        assert (stage.name, plan.blocked, plan.pending) == (stage.name, 1, [])
    # Configured: planned on the next run, with no --refresh.
    settings.role_model = RoleModel(path=str(model_dir))
    stage = RolesStage(settings)
    fake = FakeClassifier(TABLE_ANSWERS, "set-roles@1")
    monkeypatch.setattr(stage, "classifier", lambda: fake)
    plan, (done,) = _run(stage, ctx, catalog, ref)
    assert (plan.blocked, done.status) == (0, Status.OK)


def test_a_retrained_model_that_decides_the_same_roles_keeps_the_upload_basis_and_re_syncs(
    env, monkeypatch, model_dir
):
    """But it re-syncs: the parse writes each set's role_source, confidence and evidence. Upload
    sends what sync wrote, so it follows sync: the same roles leave space's upload basis as
    it was, and the parse with the new confidences goes up again."""
    settings, catalog, ref = env
    ctx = Context(settings, catalog)

    from ingestion_workflow.tests.decision_model import write_model

    def upload_fingerprint(version, answers):
        settings.role_model = RoleModel(path=str(write_model(model_dir, version=f"v{version}")))
        stage = RolesStage(settings)
        fake = FakeClassifier(answers, f"set-roles@{version}")
        monkeypatch.setattr(stage, "classifier", lambda: fake)
        _, (roled,) = _run(stage, ctx, catalog, ref)
        _, (spaced,) = _run(SpaceStage(settings), ctx, catalog, ref)
        spaced = catalog.artifacts([ref.id], "space")[ref.id][""]
        assert spaced.summary["role_records"] == roled.summary["role_records"]
        synced = Artifact(article_id=ref.id, stage="sync",
                          fingerprint=SyncStage(settings).fingerprint_for(spaced, "bs-1"))
        fingerprints.append((synced.fingerprint, UploadStage(settings).fingerprint_for(synced)))
        return roled.fingerprint, spaced.summary["upload_basis"]

    fingerprints = []
    roles_1, upload_1 = upload_fingerprint("1", TABLE_ANSWERS)
    roles_2, upload_2 = upload_fingerprint("2", {**TABLE_ANSWERS,
                                                 "patients > controls": ("result", 0.8)})
    assert roles_1 != roles_2 and upload_1 == upload_2  # same roles, other confidence
    _, upload_3 = upload_fingerprint("3", {**TABLE_ANSWERS,
                                           "Lee et al. (2008)": ("result", 0.9)})
    assert upload_3 != upload_2  # a set's role changed
    _, upload_4 = upload_fingerprint("4", TABLE_ANSWERS)
    assert upload_4 == upload_1  # the first model's roles again, from a model named otherwise
    (sync_1, sent_1), (sync_2, sent_2), _, (sync_4, sent_4) = fingerprints
    assert len({sync_1, sync_2, sync_4}) == 3
    assert len({sent_1, sent_2, sent_4}) == 3


def test_a_space_artifact_without_role_records_re_syncs_by_its_own_fingerprint(env):
    settings, _, ref = env
    sync = SyncStage(settings)
    older, newer = (Artifact(article_id=ref.id, stage="space", fingerprint=f) for f in "ab")
    assert sync.fingerprint_for(older, "bs-1") != sync.fingerprint_for(newer, "bs-1")
    same = {"role_records": "r"}
    assert sync.fingerprint_for(
        Artifact(article_id=ref.id, stage="space", fingerprint="a", summary=same), "bs-1"
    ) != sync.fingerprint_for(
        Artifact(article_id=ref.id, stage="space", fingerprint="a", summary={"role_records": "s"}),
        "bs-1")


def test_with_its_model_the_stage_writes_every_role(env, monkeypatch, model_dir):
    settings, catalog, ref = env
    settings.role_model = RoleModel(path=str(model_dir))
    stage = RolesStage(settings)
    fake = FakeClassifier(TABLE_ANSWERS, "set-roles@1")
    monkeypatch.setattr(stage, "classifier", lambda: fake)
    _, (done,) = _run(stage, Context(settings, catalog), catalog, ref)
    assert done.status is Status.OK and unassigned(done.payload) == []
    assert done.summary["model"] == "set-roles@1"


def test_upload_and_sync_refuse_a_payload_with_a_set_without_a_role(env):
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.roles import refuse_unassigned

    _, catalog, ref = env
    work = Work(ref=ref, source="", fingerprint="fp", upstream=None)
    refused = refuse_unassigned("upload", work, _tables())
    assert refused.status is Status.FAILED
    assert refused.error.startswith("sets without a role from the roles stage: t2#0, t2#1")
    out, _ = assign_roles(_tables(), [], TEXT, FakeClassifier(TABLE_ANSWERS))
    assert refuse_unassigned("upload", work, out) is None


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "path",
    [
        "pipeline/stages/roles.py",
        *sorted(
            str(p.relative_to(ROOT))
            for p in (ROOT / "services" / "set_roles").glob("*.py")
            if p.name not in ("labeling.py", "label_schema.py", "export.py")
        ),
    ],
)
def test_no_role_is_given_by_default(path):
    """No code that decides a pipeline set's role names `result` as a fallback or default."""
    source = (ROOT / path).read_text(encoding="utf-8")
    defaults = re.findall(
        r"""(?:=\s*|\bor\s+|get\([^)]*,\s*|default\s*=\s*)(?:SetRole\()?["']result["']"""
        r"""|\bRESULT\b|=\s*(?:SetRole\()?CoordinateRole\.result\b""",
        source,
    )
    assert defaults == [], f"{path} gives a default role: {defaults}"


def _voxel_indices():
    """PMC13593418 table 4: TBSS peaks printed as voxel indices under an "MNI" header."""
    analysis = _analysis("FA1")
    analysis["coordinates"] = [{"x": 82.0, "y": 99.0, "z": 35.0, "space": "MNI"}]
    return analysis


def test_a_set_that_cannot_be_brain_coordinates_has_no_role_and_is_not_asked():
    payload = _tables()
    payload["t2"]["analyses"].append(_voxel_indices())
    table = FakeClassifier(TABLE_ANSWERS)
    out, summary = assign_roles(payload, [], TEXT, table)
    refused = out["t2"]["analyses"][2]["metadata"]
    assert refused["held"] is True
    assert refused["set_role"]["role"] is None
    assert refused["set_role"]["role_source"] == geometry.SOURCE
    assert refused["set_role"]["role_reason"] == geometry.OUTSIDE
    assert not any("[NAME] FA1" in text for text in table.seen)
    assert (summary["sets"], summary["not_brain"]) == (3, 1)
    assert summary["sets_by_origin"] == {"table": 3}
    assert unassigned(out) == []
    # The model's sets keep the record they always had.
    assert "role_reason" not in out["t2"]["analyses"][0]["metadata"]["set_role"]


def test_a_near_origin_set_is_judged_by_its_table_not_alone():
    """A thalamic peak within 10 mm of the origin, in a table of brain peaks, goes to the model;
    the same set in a table where nothing reaches brain scale is refused."""
    def thalamus():
        analysis = _analysis("Left anterior thalamus")
        analysis["coordinates"] = [{"x": -8.0, "y": -7.0, "z": 9.0, "space": "MNI"}]
        return analysis

    answers = {**TABLE_ANSWERS, "Left anterior thalamus": ("result", 0.95)}
    payload = _tables()
    payload["t2"]["analyses"].append(thalamus())
    out, summary = assign_roles(payload, [], TEXT, FakeClassifier(answers))
    assert out["t2"]["analyses"][2]["metadata"]["set_role"]["role_source"] == "fake@1"
    assert summary["not_brain"] == 0

    alone = _tables()
    alone["t2"]["analyses"] = [thalamus()]
    out, summary = assign_roles(alone, [], TEXT, FakeClassifier(answers))
    assert out["t2"]["analyses"][0]["metadata"]["set_role"]["role_reason"] == geometry.NEAR_ORIGIN
    assert summary["not_brain"] == 1


def test_a_table_of_refused_sets_needs_no_model():
    payload = {"t2": {**_tables()["t2"], "analyses": [_voxel_indices()]}}
    out, summary = assign_roles(payload, [], TEXT, None)  # never asked
    assert out["t2"]["analyses"][0]["metadata"]["set_role"]["role"] is None
    assert summary["model"] is None
