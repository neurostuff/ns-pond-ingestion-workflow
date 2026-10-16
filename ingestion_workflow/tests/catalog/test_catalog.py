"""The catalog's job: stable identity, honest freshness, recorded failures."""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import DUPLICATE_PUBMED_RECORDS, Identifier


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


def test_an_id_never_changes_once_assigned(catalog):
    """The invariant the whole catalog rests on: learning a stronger identifier
    attaches an alias, it does not re-key the article."""
    first = catalog.register(Identifier(doi="10.1/x"))
    catalog.record(
        [Outcome(article_id=first.id, stage="download", source="elsevier", payload={"a": 1})]
    )

    after_pmcid = catalog.register(Identifier(doi="10.1/x", pmcid="PMC1"))
    after_pmid = catalog.register(Identifier(doi="10.1/x", pmcid="PMC1", pmid="111"))

    assert first.id == after_pmcid.id == after_pmid.id
    assert catalog.count_articles() == 1
    assert catalog.artifact(first.id, "download", "elsevier") is not None

    known = catalog.identifier(first.id)
    assert (known.doi, known.pmcid, known.pmid) == ("10.1/x", "PMC1", "111")


def test_a_merge_keeps_the_older_article(catalog):
    """Merging picks the article that has had longer to accumulate artifacts,
    so the rewrite is the smaller one."""
    import time

    old = catalog.register(Identifier(doi="10.1/y"))
    catalog.record([Outcome(article_id=old.id, stage="download", source="elsevier")])
    time.sleep(1.1)  # created_at has second resolution
    new = catalog.register(Identifier(pmcid="PMC2"))
    assert old.id != new.id

    merged = catalog.register(Identifier(doi="10.1/y", pmcid="PMC2"))

    assert merged.id == old.id
    assert catalog.count_articles() == 1
    assert catalog.artifact(old.id, "download", "elsevier") is not None


def test_a_merge_carries_artifacts_over(catalog):
    a = catalog.register(Identifier(doi="10.1/m"))
    b = catalog.register(Identifier(pmcid="PMC7"))
    catalog.record([Outcome(article_id=a.id, stage="download", source="ace", payload={"n": 1})])
    catalog.record([Outcome(article_id=b.id, stage="extract", source="ace", payload={"n": 2})])

    merged = catalog.register(Identifier(doi="10.1/m", pmcid="PMC7"))

    stages = {art.stage for art in catalog.artifacts_for_article(merged.id)}
    assert stages == {"download", "extract"}
    assert catalog.stranded_artifacts() == 0


def test_a_clashing_artifact_is_not_stranded(catalog):
    """Both sides holding the same (stage, source) is the case that used to
    leave one attached to an article nothing can reach."""
    a = catalog.register(Identifier(doi="10.1/n"))
    b = catalog.register(Identifier(pmcid="PMC8"))
    catalog.record([Outcome(article_id=a.id, stage="download", source="ace", payload={"n": 1})])
    catalog.record([Outcome(article_id=b.id, stage="download", source="ace", payload={"n": 2})])

    merged = catalog.register(Identifier(doi="10.1/n", pmcid="PMC8"))

    assert catalog.stranded_artifacts() == 0
    kept = catalog.artifacts_for_article(merged.id)
    assert len(kept) == 1


def test_a_merge_keeps_the_successful_side_of_a_clash(catalog):
    a = catalog.register(Identifier(doi="10.1/o"))
    b = catalog.register(Identifier(pmcid="PMC9"))
    catalog.record([Outcome.failure(a.id, "download", "ace", "timeout")])
    catalog.record([Outcome(article_id=b.id, stage="download", source="ace", payload={"n": 2})])

    merged = catalog.register(Identifier(doi="10.1/o", pmcid="PMC9"))

    artifact = catalog.artifact(merged.id, "download", "ace")
    assert artifact.status is Status.OK
    assert catalog.payload(artifact) == {"n": 2}
    assert catalog.stranded_artifacts() == 0


def test_status_counts_articles_not_artifacts(catalog):
    """A stage that runs per source holds several artifacts for one article;
    the table has to stay comparable with the catalog total."""
    ref = catalog.register(Identifier(pmid="30"))
    catalog.record(
        [
            Outcome(article_id=ref.id, stage="download", source="ace"),
            Outcome(article_id=ref.id, stage="download", source="pubget"),
        ]
    )
    assert catalog.status_counts()["download"] == {"ok": 1}


def test_an_articles_state_is_the_best_its_sources_reached(catalog):
    ref = catalog.register(Identifier(pmid="31"))
    catalog.record(
        [
            Outcome.failure(ref.id, "download", "pubget", "no pmcid"),
            Outcome(article_id=ref.id, stage="download", source="ace"),
        ]
    )
    assert catalog.status_counts()["download"] == {"ok": 1}


def test_ready_counts_the_queue_at_each_stage(catalog):
    requirements = {"download": None, "extract": "download", "metadata": "extract"}
    refs = catalog.register_many([Identifier(pmid=str(n)) for n in range(40, 45)])
    catalog.record([Outcome(article_id=r.id, stage="download", source="ace") for r in refs[:3]])

    ready = catalog.ready_counts(requirements)
    assert ready["download"] == 2      # never attempted
    assert ready["extract"] == 3       # download ok, no extract entry
    assert ready["metadata"] == 0      # nothing has been extracted


def test_a_failed_upstream_is_not_ready(catalog):
    requirements = {"extract": "download"}
    ref = catalog.register(Identifier(pmid="50"))
    catalog.record([Outcome.failure(ref.id, "download", "ace", "boom")])
    assert catalog.ready_counts(requirements)["extract"] == 0


# -- the registration guard ----------------------------------------------------


def _pmids(catalog, article_id):
    return {v for k, v in catalog._conn.execute(
        "SELECT kind, value FROM aliases WHERE article_id=?", (article_id,)) if k == "pmid"}


def test_a_shared_pmcid_does_not_fuse_papers(tmp_path):
    """The pattern that built 244cm4x6up53: one cache stamped the same PMCID on
    hundreds of papers, each beside its own correct PMID and DOI."""
    with Catalog.open(tmp_path / "cat") as catalog:
        refs = catalog.register_many(
            [Identifier(pmid=str(1000 + i), doi=f"10.1/p{i}", pmcid="PMC7976178") for i in range(50)]
        )
        ids = {ref.id for ref in refs}
        assert len(ids) == 50
        assert all(len(_pmids(catalog, i)) == 1 for i in ids)
        holders = {row[0] for row in catalog._conn.execute(
            "SELECT article_id FROM aliases WHERE kind='pmcid'")}
        assert holders == {refs[0].id}
        assert catalog.conflicts >= 49   # each stamped record is refused, and says so


def test_a_known_doi_does_not_claim_a_different_paper(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        a = catalog.register(Identifier(pmid="1", doi="10.1/a"))
        b = catalog.register(Identifier(pmid="2", doi="10.1/a"))   # wrong DOI on paper 2
        assert a.id != b.id
        assert _pmids(catalog, a.id) == {"1"} and _pmids(catalog, b.id) == {"2"}
        assert catalog.resolve(Identifier(doi="10.1/a")).id == a.id


def test_two_halves_of_one_paper_still_merge(tmp_path):
    """The point of merging survives: a DOI-only and a PMID-only record of the same
    paper become one article once a record ties them together."""
    with Catalog.open(tmp_path / "cat") as catalog:
        a = catalog.register(Identifier(doi="10.1/a"))
        b = catalog.register(Identifier(pmid="1"))
        merged = catalog.register(Identifier(pmid="1", doi="10.1/a"))
        assert {a.id, b.id} >= {merged.id}
        assert catalog.resolve(Identifier(doi="10.1/a")).id == catalog.resolve(Identifier(pmid="1")).id
        assert catalog.conflicts == 0


def test_two_papers_are_not_merged_by_a_record_naming_both(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        a = catalog.register(Identifier(pmid="1", doi="10.1/a"))
        b = catalog.register(Identifier(pmid="2", doi="10.1/b"))
        got = catalog.register(Identifier(pmid="1", doi="10.1/b"))   # pairs 1 with 2's DOI
        assert got.id == a.id
        assert catalog.resolve(Identifier(pmid="2")).id == b.id
        assert catalog.resolve(Identifier(doi="10.1/b")).id == b.id


def test_a_learned_alias_does_not_move_from_its_article(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        a = catalog.register(Identifier(pmid="1"))
        b = catalog.register(Identifier(pmid="2"))
        catalog.add_aliases([(a.id, "neurostore", "BASE1")])
        catalog.add_aliases([(b.id, "neurostore", "BASE1")])
        assert catalog.resolve(Identifier(neurostore="BASE1")).id == a.id
        assert catalog.conflicts == 1


def test_an_already_fused_article_still_resolves(tmp_path):
    """Looking up an article that holds two PMIDs from before the guard must
    find it, not make an empty new article for every lookup."""
    with Catalog.open(tmp_path / "cat") as catalog:
        fused = catalog.register(Identifier(pmid="1", doi="10.1/a"))
        catalog._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES('pmid', '2', ?)", (fused.id,))
        catalog.add_aliases([(fused.id, "neurostore", "BASE1")])
        before = catalog._conn.execute("SELECT count(*) FROM articles").fetchone()[0]
        assert catalog.register(Identifier(neurostore="BASE1")).id == fused.id
        assert catalog.register(Identifier(pmid="2")).id == fused.id
        assert catalog._conn.execute("SELECT count(*) FROM articles").fetchone()[0] == before
        # a new paper's PMID beside the fused article's DOI is still refused
        assert catalog.register(Identifier(pmid="3", doi="10.1/a")).id != fused.id


SICI = "10.1002/(sici)1097-0193(1997)5:4<312::aid-hbm18>3.0.co;2-f"


def _as_version_one(root, *aliases):
    """A catalog written before doi aliases were bare: URL-form rows, schema version 1."""
    with Catalog.open(root) as cat:
        for article, kind, value in aliases:
            cat._conn.execute(
                "INSERT OR IGNORE INTO articles(id, created_at) VALUES(?, '2026-01-01')", (article,)
            )
            cat._conn.execute(
                "INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)", (kind, value, article)
            )
        cat._conn.execute("UPDATE meta SET value='1' WHERE key='schema_version'")


def _aliases(root, kind="doi"):
    with Catalog.open(root) as cat:
        return {
            (row["value"], row["article_id"])
            for row in cat._conn.execute("SELECT value, article_id FROM aliases WHERE kind=?", (kind,))
        }


def test_a_url_form_doi_alias_is_found_again_as_the_bare_doi(tmp_path):
    root = tmp_path / "cat"
    _as_version_one(root, ("a1", "pmid", "9234"), ("a1", "doi", "https://doi.org/" + SICI))

    with Catalog.open(root) as cat:
        ref = cat.register(Identifier(pmid="9234", doi="https://doi.org/" + SICI))
        assert ref.id == "a1"
        assert cat.count_articles() == 1
    assert _aliases(root) == {(SICI, "a1")}


def test_the_bare_doi_the_same_article_already_has_replaces_its_url_form(tmp_path):
    root = tmp_path / "cat"
    _as_version_one(root, ("a1", "doi", "https://doi.org/" + SICI), ("a1", "doi", SICI))
    assert _aliases(root) == {(SICI, "a1")}


def test_a_bare_doi_another_article_has_is_merged_when_it_can_be_the_same_paper(tmp_path):
    root = tmp_path / "cat"
    _as_version_one(root, ("a1", "doi", "https://doi.org/" + SICI), ("a2", "doi", SICI))
    assert _aliases(root) == {(SICI, "a1")}
    assert _articles(root) == {("a1", None), ("a2", "a1")}


def test_a_current_catalog_is_not_rewritten(tmp_path):
    root = tmp_path / "cat"
    with Catalog.open(root) as cat:
        cat._conn.execute("INSERT INTO articles(id, created_at) VALUES('a1', '2026-01-01')")
        cat._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES('doi', 'https://x.org/10.1/a', 'a1')")
    assert _aliases(root) == {("https://x.org/10.1/a", "a1")}


# -- doi case --------------------------------------------------------------------

UPPER = "10.1001/ARCHGENPSYCHIATRY.2012.135"


def test_a_doi_is_found_whatever_its_case(catalog):
    """A search service that lower-cases DOIs used to make a second article."""
    first = catalog.register(Identifier(pmid="1", doi=UPPER))
    assert catalog.resolve(Identifier(doi=UPPER.lower())).id == first.id
    assert catalog.register(Identifier(doi=UPPER.lower())).id == first.id
    assert catalog.count_articles() == 1


def test_a_doi_assigned_after_construction_is_stored_lower_case(catalog):
    """Enrichment sets `identifier.doi` as the provider spelled it."""
    identifier = Identifier(pmid="1")
    identifier.doi = UPPER
    assert identifier.doi == UPPER.lower()
    ref = catalog.register(identifier)
    catalog.add_aliases([(ref.id, "doi", "10.1/LEARNED")])
    assert _aliases(catalog.root) == {(UPPER.lower(), ref.id), ("10.1/learned", ref.id)}
    assert catalog.resolve(Identifier(doi="10.1/Learned")).id == ref.id


def test_only_basic_latin_letters_ignore_case(catalog):
    """DOI Handbook 4.3.4: A and a are one letter, Á and á are two."""
    upper = catalog.register(Identifier(doi="10.26321/Á.GUTIÉRREZ.ZARZA.02.2018.03"))
    lower = catalog.register(Identifier(doi="10.26321/á.gutiérrez.zarza.02.2018.03"))
    assert upper.id != lower.id
    assert upper.identifier.doi == "10.26321/Á.gutiÉrrez.zarza.02.2018.03"


def _as_version_two(root, *aliases, outcomes=()):
    """A catalog written while doi aliases kept their case: schema version 2."""
    with Catalog.open(root) as cat:
        for article, created, kind, value in aliases:
            cat._conn.execute(
                "INSERT OR IGNORE INTO articles(id, created_at) VALUES(?, ?)", (article, created)
            )
            cat._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                              (kind, value, article))
        cat.record(list(outcomes))
        cat._conn.execute("UPDATE meta SET value='2' WHERE key='schema_version'")


def _articles(root):
    with Catalog.open(root) as cat:
        rows = cat._conn.execute("SELECT id, merged_into FROM articles")
        return {(row["id"], row["merged_into"]) for row in rows}


def test_an_upper_case_doi_alias_is_found_again_lower_case(tmp_path):
    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "pmid", "1"), ("a1", "2026-01-01", "doi", UPPER))
    with Catalog.open(root) as cat:
        assert cat.register(Identifier(doi=UPPER.lower())).id == "a1"
        assert cat.count_articles() == 1
    assert _aliases(root) == {(UPPER.lower(), "a1")}


def test_the_other_spelling_of_an_articles_own_doi_is_dropped(tmp_path):
    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "doi", UPPER),
                    ("a1", "2026-01-01", "doi", UPPER.lower()))
    assert _aliases(root) == {(UPPER.lower(), "a1")}


def test_two_articles_of_one_doi_are_merged_with_their_artifacts(tmp_path, caplog):
    root = tmp_path / "cat"
    _as_version_two(
        root,
        ("new", "2026-02-01", "doi", "10.1/AB"), ("new", "2026-02-01", "pmid", "1"),
        ("old", "2026-01-01", "doi", "10.1/ab"),
        outcomes=[Outcome(article_id="new", stage="download", source="ace", payload={"n": 1}),
                  Outcome(article_id="old", stage="extract", source="ace", payload={"n": 2})],
    )

    with caplog.at_level("INFO", logger="ingestion_workflow.catalog.store"):
        with Catalog.open(root) as cat:
            assert cat.resolve(Identifier(doi="10.1/AB")).id == "old"
            assert cat.resolve(Identifier(pmid="1")).id == "old"
            assert {a.stage for a in cat.artifacts_for_article("old")} == {"download", "extract"}
            assert cat.payload(cat.artifact("old", "download", "ace")) == {"n": 1}
            assert cat.blobs.exists(cat.artifact("old", "download", "ace").blob)
            assert cat.stranded_artifacts() == 0
    assert _articles(root) == {("old", None), ("new", "old")}
    assert _aliases(root) == {("10.1/ab", "old")}
    assert "1 articles merged" in caplog.text


def test_two_papers_sharing_a_doi_are_left_and_logged(tmp_path, caplog):
    """The production case: 10.1093/CERCOR/11.9.888 on PMID 11552704 and
    10.1093/cercor/11.9.888 on PMID 11532893; one of them has the wrong DOI."""
    root = tmp_path / "cat"
    _as_version_two(
        root,
        ("a1", "2026-01-01", "pmid", "11552704"),
        ("a1", "2026-01-01", "doi", "10.1093/CERCOR/11.9.888"),
        ("a2", "2026-01-01", "pmid", "11532893"),
        ("a2", "2026-01-01", "doi", "10.1093/cercor/11.9.888"),
    )
    with caplog.at_level("INFO", logger="ingestion_workflow.catalog.store"):
        assert _articles(root) == {("a1", None), ("a2", None)}
    assert _aliases(root) == {("10.1093/CERCOR/11.9.888", "a1"), ("10.1093/cercor/11.9.888", "a2")}
    assert "another paper" in caplog.text and "1 left" in caplog.text


def test_canonicalising_doi_aliases_again_changes_nothing(tmp_path):
    root = tmp_path / "cat"
    _as_version_two(
        root,
        ("a1", "2026-01-01", "doi", UPPER), ("a1", "2026-01-01", "doi", UPPER.lower()),
        ("a2", "2026-01-01", "pmid", "1"), ("a2", "2026-01-01", "doi", "10.1/X"),
        ("a3", "2026-01-01", "pmid", "2"), ("a3", "2026-01-01", "doi", "10.1/x"),
        ("a4", "2026-01-01", "doi", "10.1/Y"), ("a5", "2026-01-02", "doi", "10.1/y"),
    )
    first = (_articles(root), _aliases(root))
    assert first[1] == {
        (UPPER.lower(), "a1"), ("10.1/X", "a2"), ("10.1/x", "a3"), ("10.1/y", "a4")}
    with Catalog.open(root) as cat:
        with cat._write() as conn:
            again = cat._canonical_doi_aliases(conn)
    assert again == {"rewritten": 0, "dropped": 0, "merged": 0, "left": 1}
    assert (_articles(root), _aliases(root)) == first


def test_an_article_merged_earlier_in_the_pass_is_followed(tmp_path, caplog):
    """The second spelling still names the article the first one merged away."""
    root = tmp_path / "cat"
    _as_version_two(
        root,
        ("old", "2026-01-01", "doi", "10.1/ab"),
        ("new", "2026-02-01", "doi", "10.1/AB"), ("new", "2026-02-01", "doi", "10.1/Ab"),
    )
    with caplog.at_level("INFO", logger="ingestion_workflow.catalog.store"):
        assert _aliases(root) == {("10.1/ab", "old")}
    assert "2 other spellings dropped, 1 articles merged" in caplog.text


def test_a_migration_backs_the_catalog_up_first(tmp_path, caplog):
    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "doi", UPPER))
    with caplog.at_level("INFO", logger="ingestion_workflow.catalog.store"):
        assert _aliases(root) == {(UPPER.lower(), "a1")}
    copy = root / "catalog.pre-v2.sqlite"
    assert str(copy) in caplog.text
    with sqlite3.connect(f"file:{copy}?mode=ro", uri=True) as conn:
        assert conn.execute("SELECT value FROM aliases").fetchall() == [(UPPER,)]
        version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        assert version == ("2",)


def _version_and_dois(root):
    with sqlite3.connect(f"file:{root / 'catalog.sqlite'}?mode=ro", uri=True) as conn:
        version = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
        return version, {r for r in conn.execute("SELECT value, article_id FROM aliases")}


def test_without_a_backup_nothing_is_migrated(tmp_path):
    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "doi", UPPER))
    (root / "catalog.pre-v2.sqlite").write_text("an earlier copy")
    with pytest.raises(FileExistsError, match="pre-v2"):
        Catalog.open(root)
    assert _version_and_dois(root) == ("2", {(UPPER, "a1")})


def test_a_failed_migration_leaves_the_catalog_as_it_was(tmp_path, monkeypatch):
    root = tmp_path / "cat"
    _as_version_two(
        root,
        ("a1", "2026-01-01", "doi", "10.1/AA"),          # rewritten before the failure
        ("a2", "2026-01-01", "doi", "10.1/BB"), ("a3", "2026-01-02", "doi", "10.1/bb"),
    )
    before = _version_and_dois(root)

    def crash(*args):
        raise OSError("disk full")

    monkeypatch.setattr(Catalog, "_merge", crash)
    with pytest.raises(OSError, match="disk full"):
        Catalog.open(root)
    assert _version_and_dois(root) == before


def test_a_catalog_opened_during_its_migration_waits_and_finds_it_current(tmp_path, monkeypatch):
    """A second process must not back up or migrate again, nor fail on the first's copy."""
    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "doi", UPPER))
    rewrite = Catalog._canonical_doi_aliases
    migrating = threading.Event()

    def slow(self, conn):
        migrating.set()
        time.sleep(0.5)
        return rewrite(self, conn)

    monkeypatch.setattr(Catalog, "_canonical_doi_aliases", slow)
    errors = []

    def open_it():
        try:
            Catalog.open(root).close()
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    first = threading.Thread(target=open_it)
    first.start()
    assert migrating.wait(5)
    second = threading.Thread(target=open_it)
    second.start()
    first.join()
    second.join()
    assert errors == []
    assert _version_and_dois(root) == ("3", {(UPPER.lower(), "a1")})
    assert sorted(p.name for p in root.glob("catalog.pre-*")) == ["catalog.pre-v2.sqlite"]


def test_the_rewrite_and_the_new_version_commit_together(tmp_path, monkeypatch):
    """A failure writing the version undoes the rewrite too."""
    from ingestion_workflow.catalog import store

    class Unwritable(int):
        def __str__(self):
            raise OSError("disk full")

    root = tmp_path / "cat"
    _as_version_two(root, ("a1", "2026-01-01", "doi", UPPER))
    monkeypatch.setattr(store, "SCHEMA_VERSION", Unwritable(3))
    with pytest.raises(OSError, match="disk full"):
        Catalog.open(root)
    assert _version_and_dois(root) == ("2", {(UPPER, "a1")})


def test_code_older_than_the_catalog_refuses_to_open_it(tmp_path, monkeypatch):
    """Older code would write its own version back and register the upper-case
    spelling as a second article."""
    from ingestion_workflow.catalog import store

    root = tmp_path / "cat"
    Catalog.open(root).close()
    monkeypatch.setattr(store, "SCHEMA_VERSION", 2)
    with pytest.raises(RuntimeError, match="schema version 3; this code reads version 2"):
        Catalog.open(root)
    assert _version_and_dois(root)[0] == "3"


def test_an_article_holding_two_pmids_presents_the_lower(catalog):
    """Two PubMed records of one paper, merged on purpose; every stage asks PubMed
    for the same one, whatever order the rows are read in."""
    ref = catalog.register(Identifier(pmid="22387514"))
    catalog._conn.execute(
        "INSERT INTO aliases(kind, value, article_id) VALUES('pmid', '12671262', ?)", (ref.id,))
    catalog._conn.execute(
        "INSERT INTO aliases(kind, value, article_id) VALUES('pmid', '9999999999', ?)", (ref.id,))
    for presented in (catalog.identifier(ref.id), catalog.identifiers([ref.id])[ref.id]):
        assert presented.pmid == "12671262"
        assert presented.other_ids == {DUPLICATE_PUBMED_RECORDS: "22387514,9999999999"}
        assert presented.pmids == ["12671262", "22387514", "9999999999"]
    assert catalog.identifier(catalog.register(Identifier(pmid="7")).id).other_ids is None



def test_a_search_follows_its_articles_through_a_merge(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        pmid_only = catalog.register(Identifier(pmid="1"))
        doi_only = catalog.register(Identifier(doi="10.1/m"))
        other = catalog.register(Identifier(doi="10.1/o"))
        search_id = catalog.record_search(
            "q", "2026", {"pubmed": "p", "sciencedirect": "s", "openalex": "o"},
            {
                "pubmed": [pmid_only.id, None],
                "sciencedirect": [doi_only.id, other.id],
                # one provider found both halves: they are one article after the merge
                "openalex": [pmid_only.id, doi_only.id],
            },
        )
        assert catalog.search_articles(search_id, only="sciencedirect") == [other.id]
        assert catalog.search_articles(search_id, only="pubmed") == []

        # enrichment later learns the two records are one article
        merged = catalog.register(Identifier(pmid="1", doi="10.1/m"))

        assert catalog.search_articles(search_id, only="sciencedirect") == [other.id]
        both = catalog.search_articles(search_id, found_by=["pubmed"])
        assert both == [merged.id]
        assert catalog.search_articles(search_id, found_by=["sciencedirect"]).count(merged.id) == 1
        assert catalog.search_articles(search_id, found_by=["openalex"]) == [merged.id]
        assert catalog.searches()[0]["providers"]["openalex"]["articles"] == 1
        (run,) = catalog.searches()
        assert run["providers"]["pubmed"]["without_id"] == 1
        assert run["providers"]["pubmed"]["articles"] == 1


def test_a_merge_that_leaves_the_hits_alone_strands_none(tmp_path):
    """A process without the search tables merges articles and never touches their hits."""
    with Catalog.open(tmp_path / "cat") as catalog:
        old = catalog.register(Identifier(pmid="1"))
        new = catalog.register(Identifier(doi="10.1/m"))
        search_id = catalog.record_search(
            "q", "2026", {"pubmed": "p", "sciencedirect": "s"},
            {"pubmed": [old.id], "sciencedirect": [new.id]},
        )
        with catalog._write() as conn:
            survivor = catalog._merge(conn, [old.id, new.id])
        retired = ({old.id, new.id} - {survivor}).pop()

        assert catalog.search_articles(search_id) == [survivor]
        assert catalog.search_articles(search_id, only="pubmed") == []
        assert catalog.search_articles(search_id, found_by=["sciencedirect"]) == [survivor]
        assert retired not in catalog.search_articles(search_id)
        assert catalog.identifiers([survivor])[survivor] == Identifier(pmid="1", doi="10.1/m")
        providers = catalog.searches()[0]["providers"]
        assert providers["pubmed"]["articles"] == providers["sciencedirect"]["articles"] == 1


def test_a_search_names_its_articles_oldest_first(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        refs = [catalog.register(Identifier(doi=f"10.1/{n}")) for n in "abc"]
        # ids sort differently from creation
        for ref, day in zip(refs, ("2026-01-03", "2026-01-01", "2026-01-02")):
            catalog._conn.execute("UPDATE articles SET created_at=? WHERE id=?", (day, ref.id))
        search_id = catalog.record_search(
            "q", "2026", {"pubmed": "p"}, {"pubmed": [ref.id for ref in refs]}
        )

        by_age = [refs[1].id, refs[2].id, refs[0].id]
        assert catalog.search_articles(search_id) == by_age
        assert catalog.search_articles(search_id, found_by=["pubmed"]) == by_age
        assert catalog.search_articles(search_id, only="pubmed") == by_age


def test_a_catalog_from_before_searches_gains_the_tables_and_stays_at_its_version(tmp_path):
    import sqlite3

    with Catalog.open(tmp_path / "cat") as catalog:
        catalog.register(Identifier(doi="10.1/z"))
    conn = sqlite3.connect(tmp_path / "cat" / "catalog.sqlite")
    conn.execute("DROP TABLE searches")
    conn.execute("DROP TABLE search_hits")
    conn.commit()
    conn.close()

    with Catalog.open(tmp_path / "cat") as catalog:
        assert catalog.searches() == []
        assert catalog.count_articles() == 1
    assert not list((tmp_path / "cat").glob("catalog.pre-*"))


def test_an_unknown_search_or_provider_is_refused(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        ref = catalog.register(Identifier(doi="10.1/k"))
        search_id = catalog.record_search("q", "2026", {"pubmed": "p"}, {"pubmed": [ref.id]})
        with pytest.raises(KeyError, match="no search 9"):
            catalog.search_articles(9)
        with pytest.raises(KeyError, match="did not search openalex"):
            catalog.search_articles(search_id, only="openalex")


def test_a_merge_carries_exclusions_over(catalog):
    a = catalog.register(Identifier(doi="10.1/x1"))
    b = catalog.register(Identifier(pmcid="PMC41"))
    catalog.exclude([(a.id, "t1", "no coordinates", ""), (b.id, "t2", "a figure", "see paper")])
    catalog.exclude([(b.id, Catalog.WHOLE_ARTICLE, "review", "")])

    merged = catalog.register(Identifier(doi="10.1/x1", pmcid="PMC41"))

    marks = catalog.exclusions([merged.id])
    assert set(marks[merged.id]) == {"t1", "t2", Catalog.WHOLE_ARTICLE}
    assert marks[merged.id]["t2"]["note"] == "see paper"
    assert set(catalog.exclusions()) == {merged.id}


def test_both_sides_excluding_one_table_leave_one_verdict(catalog):
    a = catalog.register(Identifier(doi="10.1/x2"))
    b = catalog.register(Identifier(pmcid="PMC42"))
    catalog.exclude([(a.id, "t1", "no coordinates", ""), (b.id, "t1", "a figure", "")])

    merged = catalog.register(Identifier(doi="10.1/x2", pmcid="PMC42"))

    assert list(catalog.exclusions()) == [merged.id]
    assert list(catalog.exclusions()[merged.id]) == ["t1"]


def test_an_exclusion_on_a_retired_id_lands_on_the_survivor(catalog):
    a = catalog.register(Identifier(doi="10.1/x3"))
    b = catalog.register(Identifier(pmcid="PMC43"))
    merged = catalog.register(Identifier(doi="10.1/x3", pmcid="PMC43"))
    retired = b.id if merged.id == a.id else a.id

    catalog.exclude([(retired, "t1", "a figure", "")])

    assert list(catalog.exclusions()) == [merged.id]
