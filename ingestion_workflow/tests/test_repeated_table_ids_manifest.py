"""scripts/repeated_table_ids_manifest.py on a real catalog."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models.ids import Identifier

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "repeated_table_ids_manifest.py"
spec = importlib.util.spec_from_file_location("repeated_table_ids_manifest", SCRIPT)
manifest = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = manifest
spec.loader.exec_module(manifest)


def test_lists_repeated_ids_and_widens_their_exclusions(tmp_path, evidence):
    with Catalog.open(tmp_path / "k") as catalog:
        refs = [catalog.register(Identifier(doi=f"10.1/{n}"), evidence) for n in "abc"]
        for ref, ids in zip(refs, (["table-ii", "table-ii", "table-ii"], ["table-1", "table-2"],
                                   ["table-1", "table-1"])):
            catalog.record([Outcome(article_id=ref.id, stage="extract", source="pdf",
                                    fingerprint="x", summary={},
                                    payload={"tables": [{"table_id": i} for i in ids]})])
        catalog.exclude([(refs[0].id, "table-ii", "not coordinates", "rCBF"),
                         (refs[1].id, "table-1", "not coordinates", "")])
    out, tsv = tmp_path / "m.jsonl", tmp_path / "x.tsv"

    manifest.main([str(tmp_path / "k"), str(out), "--exclusions", str(tsv)])

    listed = [json.loads(line)["doi"] for line in out.read_text().splitlines()]
    assert sorted(listed) == ["10.1/a", "10.1/c"]
    assert tsv.read_text().splitlines() == [
        f"{refs[0].id}\ttable-ii-{n}\tnot coordinates\trCBF" for n in (2, 3)]
