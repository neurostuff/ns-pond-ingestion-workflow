"""`roles` gives every set its origin's classifier's role, and nothing passes without one."""

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
from ingestion_workflow.services.set_roles import COORDINATE_ROLES, Prediction, served_prompt

TEXT = "Peaks from earlier work are listed in Table 2. Table 2 quotes Lee et al. (2008)."


class FakeClassifier:
    """Answers from the set's name: what a model trained on these names would say."""

    def __init__(self, answers, source="fake-table@1"):
        self.answers = answers
        self.source = source
        self.seen = []

    def predict(self, texts):
        self.seen += texts
        out = []
        for text in texts:
            role, p, *kind = next(
                v for k, v in self.answers.items() if f"[NAME] {k}" in text
            )
            rest = (1 - p) / (len(COORDINATE_ROLES) - 1)
            out.append(
                Prediction(
                    role,
                    kind[0] if kind else None,
                    {n: (p if n == role else rest) for n in COORDINATE_ROLES},
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
        "coordinates": [{"x": 1.0, "y": 2.0, "z": 3.0, "space": "MNI"}],
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
    "Lee et al. (2008)": ("reference", 0.93),
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
    prose = FakeClassifier({"amygdala seed": ("anchor", 0.99, "seed")}, "fake-prose@2")
    elsewhere = [{**PASSAGES[0], "text": "The amygdala seed was a 6 mm sphere."}]
    for passages, missing in ((PASSAGES, 0), (elsewhere, 1)):
        _, summary = assign_roles({"prose": _prose()}, passages, TEXT, {"text": prose})
        assert summary["text_sets_without_local"] == missing
    assert "[LOCAL]" not in prose.seen[-1]


def test_every_set_gets_its_role_from_its_origin_s_model():
    table = FakeClassifier(TABLE_ANSWERS)
    prose = FakeClassifier({"amygdala seed": ("anchor", 0.99, "seed")}, "fake-prose@2")
    payload = {**_tables(), "prose": _prose()}
    out, summary = assign_roles(payload, PASSAGES, TEXT, {"table": table, "text": prose})
    assert all(t.startswith("[ORIGIN] table") for t in table.seen) and len(table.seen) == 2
    assert prose.seen and all(t.startswith("[ORIGIN] text") for t in prose.seen)
    assert "[PASSAGE] The amygdala seed" in prose.seen[0]
    # A held set stays in its place, so pondie's `t2#2` still names the second set.
    kept, held = out["t2"]["analyses"]
    assert (kept["metadata"]["held"], held["metadata"]["held"]) == (False, True)
    assert "held" not in out["t2"]
    [seed] = out["prose"]["analyses"]
    assert kept["metadata"]["set_role"] == {
        "role": "result",
        "anchor_kind": None,
        "from_prior_study": None,
        "prior_study_evidence": [],
        "role_confidence": 0.97,
        "role_source": "fake-table@1",
        "role_origin": "table",
    }
    role = held["metadata"]["set_role"]
    assert (role["role"], role["from_prior_study"], role["role_source"]) == (
        "reference",
        True,
        "fake-table@1",
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
    assert seed_role["role_source"] == "fake-prose@2"
    assert isinstance(summary.pop("role_values"), str)
    assert isinstance(summary.pop("role_records"), str)
    assert summary == {
        "tables": 2,
        "sets": 3,
        "sets_by_origin": {"table": 2, "text": 1},
        "text_sets_without_local": 0,
        "roles": {"result": 1, "reference": 1, "seed": 1},
        "held": 1,
        "sources": {"table": "fake-table@1", "text": "fake-prose@2"},
    }
    assert unassigned(out) == []
    # Upload leaves the held set out.
    assert [a.name for a in AnalysisCollection.from_dict(uploaded_sets(out)["t2"]).analyses] == [
        "patients > controls"
    ]


def test_an_unsure_answer_is_still_the_model_s_and_says_how_unsure():
    table = FakeClassifier({**TABLE_ANSWERS, "Lee et al. (2008)": ("reference", 0.4)})
    out, _ = assign_roles(_tables(), [], TEXT, {"table": table})
    role = out["t2"]["analyses"][1]["metadata"]["set_role"]
    assert (role["role"], role["role_source"], role["role_confidence"]) == (
        "reference",
        "fake-table@1",
        0.4,
    )


def test_a_set_whose_origin_has_no_model_gets_no_role_at_all():
    table = FakeClassifier(TABLE_ANSWERS)
    payload = {**_tables(), "prose": _prose()}
    with pytest.raises(ModelUnavailable, match="prose"):
        assign_roles(payload, PASSAGES, TEXT, {"table": table})
    assert table.seen == []  # nothing is decided for an article it cannot finish
    with pytest.raises(ModelUnavailable, match="table"):
        assign_roles(_tables(), [], TEXT, {"text": FakeClassifier({})})


def test_unassigned_names_every_set_without_a_decided_role():
    out, _ = assign_roles(_tables(), [], TEXT, {"table": FakeClassifier(TABLE_ANSWERS)})
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

    out, _ = assign_roles(_tables(), [], TEXT, {"table": FakeClassifier(TABLE_ANSWERS)})
    out["t2"]["analyses"][0]["metadata"]["set_role"].update(change)
    assert unassigned(out) == ["t2#0"]
    work = Work(ref=SimpleNamespace(id="a1"), source="", fingerprint="fp", upstream=None)
    for stage in ("upload", "sync"):
        assert refuse_unassigned(stage, work, out).status is Status.FAILED


def test_a_set_not_marked_held_or_not_is_unassigned():
    out, _ = assign_roles(_tables(), [], TEXT, {"table": FakeClassifier(TABLE_ANSWERS)})
    del out["t2"]["analyses"][1]["metadata"]["held"]
    assert unassigned(out) == ["t2#1"]


@pytest.fixture
def served(monkeypatch):
    """The model cards each role server lists, by endpoint; `/v1/models` as vLLM writes it."""
    cards = {"http://table/v1": [{"id": "role-table", "root": "/models/table"}],
             "http://prose/v1": [{"id": "role-text", "root": "/models/prose"}]}
    monkeypatch.setattr("ingestion_workflow.pipeline.stages.roles.served_models",
                        lambda endpoint: cards[endpoint])
    return cards


TABLE_MODEL = RoleModel(endpoint="http://table/v1", model="role-table")
PROSE_MODEL = RoleModel(endpoint="http://prose/v1", model="role-text")


def test_each_origin_s_model_is_checked_on_its_own(tmp_path, served, monkeypatch):
    stage = RolesStage(
        Settings(data_root=tmp_path, prose_model="nu-prose", role_model_table=TABLE_MODEL,
                 role_model_prose=PROSE_MODEL)
    )
    assert stage.model_state("table")[0].startswith("role-table@v5+")
    assert stage.model_state("text")[0].startswith("role-text@v5+")
    # A prompt whose models were trained on other prose contexts refuses the prose model only.
    stale = served_prompt.RolePrompt("v5", "x", context_versions={"table": 2, "text": 0})
    monkeypatch.setattr("ingestion_workflow.pipeline.stages.roles.PROMPT", stale)
    assert "context version 0" in stage.model_state("text")[1]
    assert stage.model_state("table")[1] is None
    monkeypatch.setattr("ingestion_workflow.pipeline.stages.roles.PROMPT", served_prompt.PROMPT)
    stage.settings.role_model_prose = None
    assert stage.model_state("text") == (
        None,
        "no prose role model is configured (role_model_prose)",
    )
    # Each model is its own part of the fingerprint.
    stage.settings.role_model_prose = PROSE_MODEL
    fps = {o: stage.model_fingerprint(o, stage.model_state(o)) for o in ("table", "text")}
    served["http://prose/v1"] = [{"id": "role-text", "root": "/models/prose2"}]
    assert stage.model_fingerprint("table", stage.model_state("table")) == fps["table"]
    assert stage.model_fingerprint("text", stage.model_state("text")) != fps["text"]


@pytest.fixture
def env(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond")
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(pmid="7"))
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                                payload=_tables(), summary={"tables": 1})])
        yield settings, catalog, ref


def _run(stage, ctx, catalog, ref):
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    outcomes = list(stage.execute(ctx, plan.pending))
    catalog.record(outcomes)
    return plan, outcomes


def test_without_a_table_model_the_article_is_blocked_and_everything_after_it(
    env, monkeypatch, served
):
    settings, catalog, ref = env
    ctx = Context(settings, catalog)
    # More runs than the retries allow: nothing is attempted, so nothing is used up.
    for _ in range(ctx.max_attempts + 1):
        plan, outcomes = _run(RolesStage(settings), ctx, catalog, ref)
        assert (plan.blocked, plan.pending, outcomes) == (1, [], [])
        assert plan.reasons == {"no table role model is configured (role_model_table)": 1}
        assert "no table role model is configured" in plan.describe()
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
    settings.role_model_table = TABLE_MODEL
    stage = RolesStage(settings)
    fake = FakeClassifier(TABLE_ANSWERS, "set-roles-table@1")
    monkeypatch.setattr(stage, "classifier", lambda origin: {"table": fake}[origin])
    plan, (done,) = _run(stage, ctx, catalog, ref)
    assert (plan.blocked, done.status) == (0, Status.OK)


def test_a_missing_prose_model_blocks_only_articles_with_prose_sets(env, served):
    settings, catalog, ref = env
    settings.prose_model = "nu-prose"
    settings.role_model_table = TABLE_MODEL
    other = catalog.register(Identifier(pmid="8"))
    catalog.record([
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint="r-1",
                payload=_tables(), summary={"tables": 1, "prose_analyses": 0, "basis": "an-1"}),
        Outcome(article_id=other.id, stage="resolve", source="", fingerprint="r-2",
                payload={"prose": _prose()}, summary={"tables": 0, "prose_analyses": 1}),
    ])
    stage = RolesStage(settings)
    ids = [ref.id, other.id]
    plan = stage.plan(Context(settings, catalog), [ref, other],
                      catalog.artifacts(ids, "roles"), catalog.artifacts(ids, "resolve"))
    assert [w.article_id for w in plan.pending] == [ref.id]
    assert plan.reasons == {"no prose role model is configured (role_model_prose)": 1}


def test_a_retrained_model_that_decides_the_same_roles_keeps_the_upload_basis_and_re_syncs(
    env, monkeypatch, served
):
    """But it re-syncs: the parse writes each set's role_source, confidence and evidence. Upload
    sends what sync wrote, so it follows sync: the same roles leave space's upload basis as
    it was, and the parse with the new confidences goes up again."""
    settings, catalog, ref = env
    ctx = Context(settings, catalog)

    def upload_fingerprint(version, answers):
        settings.role_model_table = TABLE_MODEL
        served["http://table/v1"] = [{"id": "role-table", "root": f"/models/table{version}"}]
        stage = RolesStage(settings)
        fake = FakeClassifier(answers, f"set-roles-table@{version}")
        monkeypatch.setattr(stage, "classifier", lambda origin: fake)
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


def test_with_its_model_the_stage_writes_every_role(env, monkeypatch, served):
    settings, catalog, ref = env
    settings.role_model_table = TABLE_MODEL
    stage = RolesStage(settings)
    fake = FakeClassifier(TABLE_ANSWERS, "set-roles-table@1")
    monkeypatch.setattr(stage, "classifier", lambda origin: {"table": fake}[origin])
    _, (done,) = _run(stage, Context(settings, catalog), catalog, ref)
    assert done.status is Status.OK and unassigned(done.payload) == []
    assert done.summary["sources"] == {"table": "set-roles-table@1"}


def test_upload_and_sync_refuse_a_payload_with_a_set_without_a_role(env):
    from ingestion_workflow.pipeline.plan import Work
    from ingestion_workflow.pipeline.stages.roles import refuse_unassigned

    _, catalog, ref = env
    work = Work(ref=ref, source="", fingerprint="fp", upstream=None)
    refused = refuse_unassigned("upload", work, _tables())
    assert refused.status is Status.FAILED
    assert refused.error.startswith("sets without a role from the roles stage: t2#0, t2#1")
    out, _ = assign_roles(_tables(), [], TEXT, {"table": FakeClassifier(TABLE_ANSWERS)})
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
