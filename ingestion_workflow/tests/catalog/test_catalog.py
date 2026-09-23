"""The catalog's job: stable identity, honest freshness, recorded failures."""

from __future__ import annotations

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier


@pytest.fixture()
def catalog(tmp_path):
    with Catalog.open(tmp_path / "cat") as cat:
        yield cat


def test_identity_survives_enrichment(catalog):
    first = catalog.register(Identifier(pmcid="PMC10634720"))
    second = catalog.register(Identifier(pmcid="PMC10634720", pmid="37961286"))
    third = catalog.register(Identifier(pmid="37961286", doi="10.1101/2023.10.21.563317"))

    assert first.id == second.id == third.id
    assert catalog.count_articles() == 1
    assert catalog.identifier(first.id).doi == "10.1101/2023.10.21.563317"


def test_separate_articles_stay_separate(catalog):
    a = catalog.register(Identifier(pmid="1"))
    b = catalog.register(Identifier(pmid="2"))
    assert a.id != b.id
    assert catalog.count_articles() == 2


def test_ids_that_turn_out_to_be_one_article_merge(catalog):
    a = catalog.register(Identifier(pmid="1"))
    b = catalog.register(Identifier(doi="10.1/x"))
    assert a.id != b.id

    merged = catalog.register(Identifier(pmid="1", doi="10.1/x"))
    assert merged.id in {a.id, b.id}
    assert catalog.count_articles() == 1
    assert catalog.resolve(Identifier(pmid="1")).id == merged.id
    assert catalog.resolve(Identifier(doi="10.1/x")).id == merged.id


def test_registering_is_idempotent(catalog):
    ids = [Identifier(pmid=str(n)) for n in range(50)]
    catalog.register_many(ids)
    catalog.register_many(ids)
    assert catalog.count_articles() == 50


def test_payloads_round_trip_through_the_blob_store(catalog):
    ref = catalog.register(Identifier(pmid="42"))
    catalog.record(
        [
            Outcome(
                article_id=ref.id,
                stage="extract",
                source="pubget",
                fingerprint="fp1",
                payload={"tables": [{"id": "t1"}]},
                summary={"tables": 1},
            )
        ]
    )
    artifact = catalog.artifact(ref.id, "extract", "pubget")
    assert artifact.status is Status.OK
    assert artifact.summary == {"tables": 1}
    assert catalog.payload(artifact) == {"tables": [{"id": "t1"}]}


def test_identical_payloads_share_one_blob(catalog):
    refs = catalog.register_many([Identifier(pmid=str(n)) for n in (1, 2)])
    catalog.record(
        [
            Outcome(article_id=ref.id, stage="extract", source="ace", payload={"same": True})
            for ref in refs
        ]
    )
    blobs = {catalog.artifact(ref.id, "extract", "ace").blob for ref in refs}
    assert len(blobs) == 1


def test_failures_are_recorded_and_counted(catalog):
    ref = catalog.register(Identifier(pmid="7"))
    for _ in range(3):
        catalog.record([Outcome.failure(ref.id, "download", "pubget", "timeout")])

    artifact = catalog.artifact(ref.id, "download", "pubget")
    assert artifact.status is Status.FAILED
    assert artifact.error == "timeout"

    counts = catalog.attempt_counts([ref.id], "download", "pubget")
    assert counts[ref.id][0] == 3


def test_status_counts_group_by_stage(catalog):
    refs = catalog.register_many([Identifier(pmid=str(n)) for n in range(4)])
    catalog.record([Outcome(article_id=refs[0].id, stage="download", source="ace")])
    catalog.record([Outcome.failure(refs[1].id, "download", "ace", "boom")])
    catalog.record([Outcome.failure(refs[2].id, "download", "ace", "gone", permanent=True)])

    counts = catalog.status_counts()["download"]
    assert counts == {"ok": 1, "failed": 1, "permanent": 1}


def test_blob_address_does_not_depend_on_compression(tmp_path):
    """The digest is taken before compression, so tuning the level is safe.

    If this breaks, every blob written at the old level is orphaned and the
    store doubles.
    """
    import hashlib
    import json

    from ingestion_workflow.catalog.blobs import BlobStore

    payload = {"tables": [{"id": f"t{i}", "coords": [1.0, 2.0, 3.0]} for i in range(8)]}
    store = BlobStore(tmp_path / "blobs")
    digest = store.put(payload)

    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert digest == hashlib.sha256(raw).hexdigest()
    assert store.get(digest) == payload


def test_blobs_written_at_any_level_are_readable(tmp_path):
    import gzip
    import hashlib
    import json

    from ingestion_workflow.catalog.blobs import BlobStore

    store = BlobStore(tmp_path / "blobs")
    payload = {"a": 1, "b": [1, 2, 3]}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()

    # Hand-write the blob the way an older version would have, at level 9.
    target = store.root / digest[:2] / f"{digest}.json.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(gzip.compress(raw, compresslevel=9))

    assert store.get(digest) == payload
    assert store.put(payload) == digest          # existing blob left alone
    assert target.read_bytes() == gzip.compress(raw, compresslevel=9)


def test_article_ids_are_frozen():
    """Article ids are the catalog's primary key.

    Hardcoded on purpose: if the hash, the digest size, the encoding or the id
    length is ever edited, this fails rather than silently re-keying every
    article in every existing catalog.
    """
    from ingestion_workflow.catalog.store import _article_id

    assert _article_id("pmcid:PMC10634720") == "lsfb2vznhrfz"
    assert _article_id("pmid:37961286") == "ckw3v7eecmlk"
    assert _article_id("doi:10.1016/j.neuroimage.2023.120") == "6rgtrdiydfza"


def test_article_ids_cannot_be_mistaken_for_neurostore_ids():
    """Neurostore's base_study_id is a mixed-case shortuuid-12. Both appear in
    `ingest show` output, so the catalog's own ids are deliberately a different
    shape: lowercase base32."""
    from ingestion_workflow.catalog.store import ID_LENGTH, _article_id

    for seed in ("pmcid:PMC1", "pmid:2", "doi:10.1/x"):
        article_id = _article_id(seed)
        assert len(article_id) == ID_LENGTH
        assert article_id == article_id.lower()
        assert set(article_id) <= set("abcdefghijklmnopqrstuvwxyz234567")


def test_a_random_id_would_also_be_correct(catalog, monkeypatch):
    """Determinism is for cross-catalog agreement, not for correctness.

    Idempotent registration and stability under enrichment come from the alias
    table. This pins that, so nobody later "fixes" a bug here that is not one.
    """
    import uuid

    from ingestion_workflow.catalog import store

    monkeypatch.setattr(store, "_article_id", lambda seed: uuid.uuid4().hex[:12])

    first = catalog.register_many([Identifier(pmid="5", pmcid="PMC5")])
    again = catalog.register_many([Identifier(pmid="5", pmcid="PMC5")])
    enriched = catalog.register_many([Identifier(pmid="5", pmcid="PMC5", doi="10.1/e")])

    assert [r.id for r in first] == [r.id for r in again] == [r.id for r in enriched]
    assert catalog.count_articles() == 1


def test_the_neurostore_id_becomes_an_alias(catalog):
    """base_study_id is assigned by Neurostore during upload, so it is not
    knowable at registration. Once learned it must be resolvable."""
    ref = catalog.register(Identifier(pmcid="PMC10634720"))
    assert ref.id != "5Qk2mNpXyJKH"

    catalog.add_aliases([(ref.id, "neurostore", "5Qk2mNpXyJKH")])

    found = catalog.resolve(Identifier(neurostore="5Qk2mNpXyJKH"))
    assert found is not None and found.id == ref.id
    assert catalog.identifier(ref.id).neurostore == "5Qk2mNpXyJKH"
    assert catalog.count_articles() == 1


def test_add_aliases_ignores_unknown_kinds_and_blanks(catalog):
    ref = catalog.register(Identifier(pmid="9"))
    catalog.add_aliases(
        [
            (ref.id, "not_a_kind", "x"),
            (ref.id, "neurostore", ""),
            ("", "neurostore", "y"),
        ]
    )
    assert catalog.identifier(ref.id).neurostore is None
