"""An article's aliases are found through its own index, not by scanning every alias."""

from __future__ import annotations

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.models.ids import Identifier


def test_an_article_s_aliases_are_read_through_the_article_index(tmp_path, evidence):
    with Catalog.open(tmp_path / "k", evidence=evidence) as catalog:
        ref = catalog.register(Identifier(pmid="28668221", doi="10.1016/j.cortex.2017.05.018"))
        for n in range(200):
            catalog.register(Identifier(doi=f"10.1016/j.neuroimage.2003.07.{n:03d}"))
        asked = []
        catalog._conn.set_trace_callback(asked.append)
        assert catalog.aliases(ref.id, "doi") == ["10.1016/j.cortex.2017.05.018"]
        catalog._conn.set_trace_callback(None)
        query = next(q for q in asked if q.lstrip().upper().startswith("SELECT VALUE FROM ALIASES"))
        plan = " ".join(str(row[-1]) for row in catalog._conn.execute(f"EXPLAIN QUERY PLAN {query}"))
    assert "aliases_article_idx" in plan, plan
