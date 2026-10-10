"""`roles` decides each set's role where its classifier is confident, and holds back the rest."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.pipeline.stages import STAGE_ORDER, build
from ingestion_workflow.pipeline.stages.roles import RolesStage, assign_roles
from ingestion_workflow.services.set_roles import ROLE_LABELS, Prediction
from ingestion_workflow.services.set_roles.model import CONTEXT_VERSIONS, META_FILE

TEXT = "Peaks from earlier work are listed in Table 2. Table 2 quotes Lee et al. (2008)."


class FakeClassifier:
    """Answers from the set's name: what a model trained on these names would say."""

    source = "fake@1"

    def __init__(self, answers):
        self.answers = answers
        self.seen = []

    def predict(self, texts):
        self.seen += texts
        out = []
        for text in texts:
            label, p, prior = next(v for k, v in self.answers.items() if f"[NAME] {k}" in text)
            rest = (1 - p) / (len(ROLE_LABELS) - 1)
            out.append(Prediction({n: (p if n == label else rest) for n in ROLE_LABELS}, prior))
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


def _payload():
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


def test_roles_runs_between_resolve_and_space():
    order = list(STAGE_ORDER)
    assert order.index("resolve") < order.index("roles") < order.index("space")


def test_roles_is_off_unless_a_model_is_configured(tmp_path):
    assert "roles" not in [s.name for s in build([], Settings(data_root=tmp_path))]
    with pytest.raises(ValueError, match="role_model"):
        build(["roles"], Settings(data_root=tmp_path))
    on = Settings(data_root=tmp_path, role_model=tmp_path / "model")
    assert "roles" in [s.name for s in build([], on)]
    assert RolesStage.upstream_for(on) == ("analyses", "tables")
    both = Settings(data_root=tmp_path, role_model=tmp_path / "model", prose_model="nu-prose")
    assert RolesStage.upstream_for(both) == ("resolve", "tables")


def test_a_confident_reference_is_held_back_with_its_citation():
    classifier = FakeClassifier(
        {
            "patients > controls": ("result", 0.97, 0.01),
            "Lee et al. (2008)": ("reference", 0.93, 0.9),
        }
    )
    out, summary = assign_roles(_payload(), [], TEXT, classifier, min_confidence=0.8)
    [kept] = out["t2"]["analyses"]
    [held] = out["t2"]["held"]
    assert kept["name"] == "patients > controls"
    assert kept["metadata"]["set_role"]["role_source"] == "fake@1"
    role = held["metadata"]["set_role"]
    assert (role["role"], role["from_prior_study"], role["proposal"]) == (
        "reference",
        True,
        "result",
    )
    span = role["prior_study_evidence"][0]
    assert (
        TEXT[span["start_char"] : span["end_char"]]
        == span["text"]
        == "Table 2 quotes Lee et al. (2008)."
    )
    assert held["metadata"]["table_metadata"] == {"table_label": "Table 2"}  # metadata kept
    assert summary == {
        "tables": 1,
        "sets": 2,
        "roles": {"result": 1, "reference": 1},
        "overridden": 1,
        "held": 1,
        "source": "fake@1",
    }
    # Upload reads the collection as before; `held` is not an analysis.
    assert [a.name for a in AnalysisCollection.from_dict(out["t2"]).analyses] == [
        "patients > controls"
    ]


def test_an_unsure_classifier_leaves_the_proposal():
    classifier = FakeClassifier(
        {
            "patients > controls": ("result", 0.97, 0.0),
            "Lee et al. (2008)": ("reference", 0.6, 0.4),
        }
    )
    out, summary = assign_roles(_payload(), [], TEXT, classifier, min_confidence=0.8)
    assert len(out["t2"]["analyses"]) == 2 and "held" not in out["t2"]
    role = out["t2"]["analyses"][1]["metadata"]["set_role"]
    assert (role["role"], role["role_source"], role["from_prior_study"]) == (
        "result",
        "proposal",
        False,
    )
    assert summary["overridden"] == 0


def test_a_prose_seed_keeps_its_prose_role_and_metadata():
    payload = {
        "prose": {
            "slug": "a",
            "coordinate_space": "MNI",
            "identifier": None,
            "analyses": [
                {
                    "name": "amygdala seed",
                    "coordinates": [{"x": 20.0, "y": -4.0, "z": -18.0, "space": "MNI"}],
                    "metadata": {"source": "prose", "role": "seed", "passages": [0]},
                }
            ],
        }
    }
    passages = [{"text": "The amygdala seed (20, -4, -18) was a 6 mm sphere.", "heading": "PPI"}]
    classifier = FakeClassifier({"amygdala seed": ("anchor:seed", 0.99, 0.02)})
    out, _ = assign_roles(payload, passages, None, classifier, min_confidence=0.8)
    [analysis] = out["prose"]["analyses"]
    assert analysis["metadata"]["role"] == "seed"  # the prose model's answer, as resolve wrote it
    assert analysis["metadata"]["set_role"]["anchor_kind"] == "seed"
    assert "[PASSAGE] The amygdala seed" in classifier.seen[0]


def test_the_stage_refuses_a_model_built_for_another_context(tmp_path):
    (tmp_path / META_FILE).write_text(
        json.dumps(
            {
                "name": "enc",
                "version": "1",
                "labels": list(ROLE_LABELS),
                "context_versions": {**CONTEXT_VERSIONS, "table": 0},
            }
        )
    )
    stage = RolesStage(Settings(data_root=tmp_path, role_model=tmp_path))
    with pytest.raises(ValueError, match="context versions"):
        stage.model_source()
    (tmp_path / META_FILE).write_text(
        json.dumps(
            {
                "name": "enc",
                "version": "1",
                "labels": list(ROLE_LABELS),
                "context_versions": CONTEXT_VERSIONS,
            }
        )
    )
    assert stage.model_source() == "enc@1"
