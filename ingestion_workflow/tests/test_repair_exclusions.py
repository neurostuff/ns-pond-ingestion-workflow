"""The one-off hand-over of exclusions left on merged-away articles, on a real catalog."""

from __future__ import annotations

import importlib.util
import io
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.models.ids import Identifier

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "repair_exclusions.py"
spec = importlib.util.spec_from_file_location("repair_exclusions", SCRIPT)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


@pytest.fixture()
def stranded(tmp_path):
    """A merged catalog whose retired article still holds rows, as older code left it."""
    root = tmp_path / "cat"
    with Catalog.open(root) as cat:
        a = cat.register(Identifier(doi="10.1/r1"))
        b = cat.register(Identifier(pmcid="PMC51"))
        survivor = cat.register(Identifier(doi="10.1/r1", pmcid="PMC51")).id
        retired = b.id if survivor == a.id else a.id
        cat.exclude([(survivor, "t1", "kept reason", "")])
    with closing(sqlite3.connect(root / "catalog.sqlite")) as conn:
        conn.executemany(
            "INSERT INTO exclusions VALUES (?, ?, ?, '', '2026-01-01')",
            [(retired, "t1", "lost reason"), (retired, "t2", "a figure")],
        )
        conn.commit()
    return root, survivor, retired


def test_a_count_changes_nothing(stranded):
    root, survivor, retired = stranded
    out = io.StringIO()

    assert repair.run(root, out=out) == 2
    assert "2 exclusions on 1 retired articles" in out.getvalue()
    with Catalog.open(root) as cat:
        assert set(cat.exclusions()) == {survivor, retired}
    assert not (root / "catalog.pre-exclusions-repair.sqlite").exists()


def test_apply_backs_up_then_hands_the_rows_over(stranded):
    root, survivor, retired = stranded

    repair.run(root, apply=True, out=io.StringIO())

    with Catalog.open(root) as cat:
        marks = cat.exclusions()
    assert set(marks) == {survivor}
    assert marks[survivor]["t1"]["reason"] == "kept reason"
    assert set(marks[survivor]) == {"t1", "t2"}
    with closing(sqlite3.connect(root / "catalog.pre-exclusions-repair.sqlite")) as old:
        assert old.execute(
            "SELECT COUNT(*) FROM exclusions WHERE article_id=?", (retired,)
        ).fetchone()[0] == 2
    assert repair.run(root, out=io.StringIO()) == 0
