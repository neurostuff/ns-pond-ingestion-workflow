import json

from ingestion_workflow.pipeline.provenance import ProvenanceLogger, ProvenanceEntry


def test_provenance_logger_records_batches(tmp_path):
    path = tmp_path / "prov.json"
    logger = ProvenanceLogger(path)
    entry = ProvenanceEntry(
        attempted=[{"hash_id": "abc123", "label": "PMC123"}],
        coordinate_successes=[{"hash_id": "abc123", "label": "PMC123"}],
        missing_coordinates=[],
        passed_to_next=[],
    )

    logger.record_batch("pubget", entry)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert "extractors" in payload
    assert "pubget" in payload["extractors"]
    batch = payload["extractors"]["pubget"][0]
    assert batch["attempted"][0]["label"] == "PMC123"
    assert batch["coordinate_successes"][0]["hash_id"] == "abc123"
