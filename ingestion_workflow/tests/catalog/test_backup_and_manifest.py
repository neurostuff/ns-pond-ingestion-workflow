"""One way to copy the catalog database, and one way to read a manifest."""

import json
import sqlite3
from contextlib import closing

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import register_manifest, resolve_manifest


def test_backup_holds_what_the_catalog_held_and_never_overwrites(tmp_path, evidence):
    with Catalog.open(tmp_path / "cat") as cat:
        cat.register(Identifier(pmid="1001"), evidence)
        copy = cat.backup(tmp_path / "out" / "before.sqlite")
        cat.register(Identifier(pmid="1002"), evidence)

        with closing(sqlite3.connect(copy)) as old:
            assert old.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 1
        before = copy.read_bytes()
        with pytest.raises(FileExistsError):
            cat.backup(copy)
        assert copy.read_bytes() == before


def test_a_manifest_resolved_without_registering_writes_nothing(tmp_path, evidence):
    path = tmp_path / "m.jsonl"
    path.write_text("\n".join(json.dumps({"pmid": p}) for p in ("1001", "1002", "1001")) + "\n")
    with Catalog.open(tmp_path / "cat") as cat:
        known = cat.register(Identifier(pmid="1001"), evidence)

        looked = resolve_manifest(cat, path)
        assert looked.entries == 3
        assert [ref.id for ref in looked.refs] == [known.id, known.id]
        assert [i.pmid for i in looked.unknown] == ["1002"]
        assert len(list(cat.all_article_ids())) == 1

        registered = register_manifest(cat, path, evidence)
        assert registered.entries == 3 and registered.unknown == []
        assert len(list(cat.all_article_ids())) == 2
