"""The analyses stage sends the model exactly the tables triage passed.

The model service is a stub OpenAI-compatible server on localhost, so the
whole path runs: plan, payload read, table selection, the service, the client.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource, ExtractedContent, ExtractedTable
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.analyses import AnalysesStage


class ModelStub:
    """Answers every chat with no analyses, and keeps what it was asked."""

    def __init__(self):
        self.prompts = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                stub.prompts.append(body["messages"][-1]["content"])
                data = json.dumps({
                    "id": "c", "object": "chat.completion", "created": 0, "model": "m",
                    "choices": [{"index": 0, "finish_reason": "function_call", "message": {
                        "role": "assistant", "content": None,
                        "function_call": {"name": "parse_analyses",
                                          "arguments": '{"analyses": []}'}}}],
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tables_sent(self, table_ids):
        return [t for t in table_ids if any(f"MARK-{t}" in p for p in self.prompts)]


@pytest.fixture()
def env(tmp_path):
    gate = tmp_path / "gate.joblib"
    gate.write_bytes(b"a fitted gate")
    stub = ModelStub()
    settings = Settings(
        data_root=tmp_path / "d", cache_root=tmp_path / "c", catalog_root=tmp_path / "k",
        coordinate_gate_path=gate, llm_api_key="k", llm_api_base=stub.url,
    )
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog, stub
    stub.server.shutdown()


def _table(tmp_path, table_id, *, contains_coordinates=False):
    path = tmp_path / f"{table_id}.html"
    path.write_text(f"<table><tr><td>MARK-{table_id}</td></tr></table>", encoding="utf-8")
    return ExtractedTable(table_id=table_id, raw_content_path=path, caption="",
                          footer="", contains_coordinates=contains_coordinates)


def _run(env, tmp_path, tables, verdicts):
    settings, catalog, stub = env
    ref = catalog.register(Identifier(pmid="123"))
    content = ExtractedContent(slug="a", source=DownloadSource.PUBGET, tables=tables)
    catalog.record([
        Outcome(ref.id, "extract", "pubget", Status.OK, "ex-1", content.to_dict(),
                summary={"tables": len(tables)}),
        Outcome(ref.id, "triage", "", Status.OK, "tr-1",
                {"source": "pubget", "tables": verdicts},
                summary={"source": "pubget", "passed": 1,
                         "passed_tables": [v["table_id"] for v in verdicts if v["passes"]]}),
    ])
    stage = AnalysesStage(settings)
    ctx = Context(settings, catalog)
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], "analyses"),
                      catalog.artifacts([ref.id], "triage"))
    (work,) = plan.pending
    return list(stage.execute(ctx, [work]))


def test_the_model_is_sent_exactly_the_tables_triage_passed(env, tmp_path):
    # t1 holds coordinates but triage failed it; t2 triage passed; t3 neither.
    tables = [_table(tmp_path, "t1", contains_coordinates=True),
              _table(tmp_path, "t2"), _table(tmp_path, "t3")]
    verdicts = [{"table_id": "t1", "passes": False},
                {"table_id": "t2", "passes": True},
                {"table_id": "t3", "passes": False}]
    (outcome,) = _run(env, tmp_path, tables, verdicts)
    assert env[2].tables_sent(["t1", "t2", "t3"]) == ["t2"]
    assert outcome.status is Status.OK
    assert list(outcome.summary["readings"]) == ["t2"]


def test_a_table_triage_passed_that_the_extraction_lacks_is_skipped_not_sent(env, tmp_path):
    tables = [_table(tmp_path, "t1", contains_coordinates=True)]
    verdicts = [{"table_id": "t1", "passes": False}, {"table_id": "t9", "passes": True}]
    (outcome,) = _run(env, tmp_path, tables, verdicts)
    assert env[2].prompts == []
    assert outcome.status is Status.SKIPPED
    assert outcome.summary == {"tables": 0, "reason": "no coordinate tables"}
