"""Legacy create_analyses rows are per (article, table); artifacts are per article.

Getting this wrong loses table parses silently and hands upload and sync a
payload shape they cannot read.
"""

from __future__ import annotations

import json
import sqlite3

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.migrate import migrate_caches
from ingestion_workflow.models import AnalysisCollection
from ingestion_workflow.models.ids import Identifier


def legacy_row(article_slug: str, table_id: str, pmid: str, coords: int):
    """The real shape: CreateAnalysesResult.to_dict(), one table of one article."""
    collection = {
        "slug": f"{article_slug}::{table_id}",
        "coordinate_space": "MNI",
        "identifier": {"pmid": pmid, "doi": None, "pmcid": None, "neurostore": None},
        "analyses": [
            {
                "name": f"analysis for {table_id}",
                "description": None,
                "coordinates": [
                    {"x": float(n), "y": 2.0, "z": 3.0, "space": "MNI",
                     "statistic_value": None, "statistic_type": None,
                     "cluster_size": None, "cluster_measure": None,
                     "is_subpeak": False, "is_deactivation": False, "is_seed": False}
                    for n in range(coords)
                ],
                "contrasts": [], "images": [], "table_id": table_id,
                "table_number": None, "table_caption": "", "table_footer": "",
                "metadata": {},
            }
        ],
    }
    return (
        f"{article_slug}::{table_id}",
        json.dumps(
            {
                "slug": f"{article_slug}::{table_id}",
                "article_slug": article_slug,
                "table_id": table_id,
                "sanitized_table_id": table_id,
                "analysis_collection": collection,
                "analysis_paths": [],
                "metadata": {},
                "error_message": None,
            }
        ),
        "2026-01-01", None, pmid, None, None,
    )


@pytest.fixture()
def legacy(tmp_path):
    path = tmp_path / "old" / "create_analyses" / "index.sqlite"
    path.parent.mkdir(parents=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE create_analyses (slug TEXT PRIMARY KEY, payload_json BLOB NOT NULL, "
        "cached_at TEXT NOT NULL, metadata_json BLOB, pmid TEXT, pmcid TEXT, doi TEXT)"
    )
    rows = [
        legacy_row("art-a", "tbl1", "1", 3),
        legacy_row("art-a", "tbl2", "1", 5),
        legacy_row("art-a", "tbl3", "1", 7),
        legacy_row("art-b", "tbl1", "2", 2),
    ]
    conn.executemany("INSERT INTO create_analyses VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return tmp_path / "old"


def analyses_payload(catalog, pmid):
    ref = catalog.resolve(Identifier(pmid=pmid))
    return catalog.payload(catalog.artifact(ref.id, "analyses", ""))


def test_every_table_survives(legacy, tmp_path):
    """Three legacy rows for one article must not overwrite each other."""
    with Catalog.open(tmp_path / "cat") as catalog:
        migrate_caches(legacy, catalog, stages=["analyses"])
        payload = analyses_payload(catalog, "1")

    assert sorted(payload) == ["tbl1", "tbl2", "tbl3"]


def test_the_payload_is_the_shape_consumers_read(legacy, tmp_path):
    """upload and sync do `{t: AnalysisCollection.from_dict(b) for t, b in
    payload.items()}`. The legacy row shape makes that raise."""
    with Catalog.open(tmp_path / "cat") as catalog:
        migrate_caches(legacy, catalog, stages=["analyses"])
        payload = analyses_payload(catalog, "1")

    collections = {t: AnalysisCollection.from_dict(b) for t, b in payload.items()}
    assert len(collections) == 3
    assert [len(c.analyses[0].coordinates) for c in collections.values()] == [3, 5, 7]


def test_articles_stay_separate(legacy, tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        migrate_caches(legacy, catalog, stages=["analyses"])
        assert sorted(analyses_payload(catalog, "1")) == ["tbl1", "tbl2", "tbl3"]
        assert sorted(analyses_payload(catalog, "2")) == ["tbl1"]


def test_grouping_survives_a_batch_boundary(legacy, tmp_path, monkeypatch):
    """An article's tables must not be split across two flushes, where the
    second would overwrite the first."""
    import ingestion_workflow.migrate as mig

    monkeypatch.setattr(mig, "BATCH", 1)
    with Catalog.open(tmp_path / "cat") as catalog:
        migrate_caches(legacy, catalog, stages=["analyses"])
        assert sorted(analyses_payload(catalog, "1")) == ["tbl1", "tbl2", "tbl3"]


def test_it_is_idempotent(legacy, tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        migrate_caches(legacy, catalog, stages=["analyses"])
        migrate_caches(legacy, catalog, stages=["analyses"])
        assert catalog.count_articles() == 2
        assert sorted(analyses_payload(catalog, "1")) == ["tbl1", "tbl2", "tbl3"]
