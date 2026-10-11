"""scripts/migrate_legacy_splits.py on fixture payloads in a real catalog."""

from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.ids import Identifier

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "migrate_legacy_splits.py"
_spec = importlib.util.spec_from_file_location("migrate_legacy_splits", SCRIPT)
migrate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate)


def _a(name, table_id="t1", value=4.0, x=1.0, **metadata):
    point = {"x": x, "y": 2.0, "z": 3.0, "statistic_value": value, "statistic_type": "T"}
    return {"name": name, "table_id": table_id, "coordinates": [point],
            "metadata": {"sanitized_table_id": table_id, **metadata}}


def _legacy(*analyses, slug="s::t1"):
    return {"slug": slug, "coordinate_space": "MNI", "analyses": list(analyses)}


@pytest.fixture
def catalog(tmp_path):
    root = tmp_path / "catalog"
    with Catalog.open(root) as cat:
        yield cat


def _store(catalog, payload, stages=("analyses", "roles", "space"), pmid="12"):
    ref = catalog.register(Identifier(pmid=pmid))
    catalog.record([
        Outcome(article_id=ref.id, stage=stage, source="", fingerprint=f"{stage}-fp",
                payload=payload, summary={"tables": len(payload)})
        for stage in stages
    ])
    return ref


def _payload(catalog, ref, stage="space"):
    return catalog.payload(catalog.artifact(ref.id, stage, ""))


def _halves(payload, table_id="t1"):
    return [(a["name"], a["metadata"].get("split")) for a in payload[table_id]["analyses"]]


def _run(catalog, tmp_path, apply=True, backup="backup"):
    out = io.StringIO()
    counts = migrate.run(catalog.root, apply=apply,
                         backup=tmp_path / backup if apply else None, out=out)
    return counts, out.getvalue()


def test_a_papers_own_negative_contrast_pairs_with_its_own_half(catalog, tmp_path):
    """"Load (negative)" is the paper's name; split, it was stored with a second suffix."""
    ref = _store(catalog, {"t1": _legacy(
        _a("Load"),
        _a("Load (negative)", x=2),
        _a("Load (negative) (negative)", value=-4.0, x=3),
    )})
    counts, _ = _run(catalog, tmp_path)
    for stage in ("analyses", "roles", "space"):
        payload = _payload(catalog, ref, stage)
        assert payload["t1"]["split_declared"] is True
        assert _halves(payload) == [
            ("Load", None),
            ("Load (negative)", {"half": "original", "index": 1}),
            ("Load (negative) (inverse)", {"half": "inverse", "original_index": 1}),
        ]
    assert (counts["payloads rewritten"], counts["splits paired"],
            counts["leftovers declared"]) == (3, 3, 0)


def test_pairs_are_found_from_the_end(catalog, tmp_path):
    ref = _store(catalog, {"t1": _legacy(
        _a("A"),
        _a("A (negative)", value=-4.0, x=2),
        _a("A (negative) (negative)", value=-4.0, x=3),
    )}, stages=("space",))
    _run(catalog, tmp_path)
    assert _halves(_payload(catalog, ref)) == [
        ("A", None),
        ("A (negative)", {"half": "original", "index": 1}),
        ("A (negative) (inverse)", {"half": "inverse", "original_index": 1}),
    ]


def test_both_suffix_spellings_pair(catalog, tmp_path):
    ref = _store(catalog, {"t1": _legacy(
        _a("A > B"), _a("A > B (inverse)", value=-4.0, x=3),
        _a("C"), _a("C (negative)", value=-4.0, x=4),
    )}, stages=("space",))
    _run(catalog, tmp_path)
    assert [s["half"] if s else None for _, s in _halves(_payload(catalog, ref))] == [
        "original", "inverse", "original", "inverse"]
    assert [n for n, _ in _halves(_payload(catalog, ref))] == [
        "A > B", "B > A", "C", "C (inverse)"]


def test_unpaired_leftovers_are_declared_inverse_halves_with_no_original(catalog, tmp_path):
    ref = _store(catalog, {
        # rsk26miv2pjw: the suffixed half first in its collection.
        "t1": _legacy(_a("Encoding (negative)", value=-4.0), _a("Retrieval")),
        # Its look-alike original sits in another table.
        "t2": _legacy(_a("X", table_id="t1"), _a("X (negative)", table_id="t2", value=-4.0),
                      slug="s::t2"),
    }, stages=("space",))
    counts, _ = _run(catalog, tmp_path)
    payload = _payload(catalog, ref)
    assert _halves(payload, "t1") == [
        ("Encoding (inverse)", {"half": "inverse", "original_index": None}), ("Retrieval", None)]
    assert _halves(payload, "t2") == [
        ("X", None), ("X (inverse)", {"half": "inverse", "original_index": None})]
    assert (counts["splits paired"], counts["leftovers declared"]) == (0, 2)


def test_a_migrated_split_is_the_split_the_stage_writes(catalog, tmp_path):
    """Name, values and declaration of each half match `split_by_sign` on the pooled analysis."""
    from ingestion_workflow.models import Coordinate
    from ingestion_workflow.services.create_analyses import split_by_sign

    def point(x, value, kind):
        return {"x": x, "y": 2.0, "z": 3.0, "statistic_value": value, "statistic_type": kind}

    original = [point(1.0, 5.0, "T")]
    inverse = [point(2.0, -3.0, "T"), point(3.0, -2.5, "Z"), point(4.0, -0.01, "P"),
               point(5.0, -1.5, None)]
    ref = _store(catalog, {"t1": _legacy(
        {**_a("Faces vs. Houses"), "coordinates": original},
        {**_a("Faces vs. Houses (negative)"), "coordinates": inverse},
    )}, stages=("space",))
    _run(catalog, tmp_path)
    migrated = AnalysisCollection.from_dict(_payload(catalog, ref)["t1"]).analyses

    stage = split_by_sign(
        "Faces vs. Houses", [Coordinate.from_dict(c) for c in original + inverse]
    )
    assert [(a.name, a.coordinates) for a in migrated] == [(n, c) for n, c, _ in stage]
    assert [a.metadata["split"]["half"] for a in migrated] == [s["half"] for _, _, s in stage]
    assert [c.statistic_value for c in migrated[1].coordinates] == [3.0, 2.5, -0.01, 1.5]


def test_a_suffixed_name_with_a_point_not_negative_is_the_papers_own(catalog, tmp_path):
    """The stage put only negative points in an inverse half."""
    ref = _store(catalog, {"t1": _legacy(
        _a("Deactivation (negative)", value=3.0),
        _a("Load"), _a("Load (negative)", x=2),
        _a("Rest (inverse)", value=None),
    )}, stages=("space",))
    counts, _ = _run(catalog, tmp_path)
    assert _halves(_payload(catalog, ref)) == [
        ("Deactivation (negative)", None), ("Load", None), ("Load (negative)", None),
        ("Rest (inverse)", None)]
    assert counts["suffixed names kept as printed (a point not negative)"] == 3
    assert counts["splits paired"] == counts["leftovers declared"] == 0


def test_a_second_run_changes_nothing(catalog, tmp_path):
    ref = _store(catalog, {"t1": _legacy(_a("Load"), _a("Load (negative)", value=-4.0))})
    _run(catalog, tmp_path)
    first = catalog.artifact(ref.id, "space", "")
    counts, _ = _run(catalog, tmp_path, backup="backup2")
    assert catalog.artifact(ref.id, "space", "").blob == first.blob
    assert counts["payloads rewritten"] == 0
    assert counts["payloads unchanged (declared or empty)"] == 3


def test_the_dry_run_writes_nothing(catalog, tmp_path):
    payload = {"t1": _legacy(_a("Load"), _a("Load (negative)", value=-4.0))}
    ref = _store(catalog, payload)
    before = {s: catalog.artifact(ref.id, s, "") for s in ("analyses", "roles", "space")}
    blobs = sorted(p.name for p in (catalog.root / "blobs").rglob("*"))
    counts, out = _run(catalog, tmp_path, apply=False)
    assert counts["splits paired"] == 3 and "dry run: nothing written" in out
    assert {s: catalog.artifact(ref.id, s, "") for s in before} == before
    assert sorted(p.name for p in (catalog.root / "blobs").rglob("*")) == blobs
    assert not (tmp_path / "backup").exists()


def test_apply_backs_up_first_repoints_and_leaves_old_blobs_and_fingerprints(catalog, tmp_path):
    payload = {"t1": _legacy(_a("Load"), _a("Load (negative)", value=-4.0))}
    ref = _store(catalog, payload)
    old = catalog.artifact(ref.id, "space", "")
    with pytest.raises(SystemExit, match="--backup"):
        migrate.run(catalog.root, apply=True, backup=None, out=io.StringIO())
    _run(catalog, tmp_path)
    new = catalog.artifact(ref.id, "space", "")
    assert new.blob != old.blob
    assert catalog.blobs.get(old.blob) == payload
    assert (new.status, new.fingerprint, new.summary, new.updated_at) == (
        old.status, old.fingerprint, old.summary, old.updated_at)
    with Catalog.open(tmp_path / "backup") as backup:
        assert backup.artifact(ref.id, "space", "").blob == old.blob
    collection = AnalysisCollection.from_dict(_payload(catalog, ref)["t1"])
    assert collection.split_declared


def test_declared_payloads_and_prose_sets_are_not_read_by_name(catalog, tmp_path):
    ref = _store(catalog, {
        "t1": {**_legacy(_a("Load"), _a("Load (negative)", value=-4.0)), "split_declared": True},
        "prose": _legacy(_a("Faces"), _a("Faces (negative)", table_id="prose", value=-4.0),
                         slug="s::prose"),
    }, stages=("resolve",))
    counts, _ = _run(catalog, tmp_path)
    payload = _payload(catalog, ref, "resolve")
    assert _halves(payload) == [("Load", None), ("Load (negative)", None)]
    assert payload["prose"]["split_declared"] is True
    assert [n for n, _ in _halves(payload, "prose")] == ["Faces", "Faces (negative)"]
    assert counts["splits paired"] == counts["leftovers declared"] == 0


def test_an_unreadable_payload_is_refused_with_its_reason(catalog, tmp_path):
    ref = _store(catalog, {"t1": {"slug": "s::t1"}}, stages=("space",))
    old = catalog.artifact(ref.id, "space", "").blob
    counts, out = _run(catalog, tmp_path)
    assert counts["payloads refused"] == 1
    assert "refused, table t1 has no analyses list: 1" in out
    assert catalog.artifact(ref.id, "space", "").blob == old
