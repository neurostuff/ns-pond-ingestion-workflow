"""Catalog schema. One sqlite database holds identity, artifacts and attempts."""

from __future__ import annotations

#: 2: doi aliases hold the bare DOI, not a resolver URL.
#: 3: doi aliases hold the canonical DOI: bare and lower case.
#: 4: ids are held by bucket: a preprint's DOI is `preprint_doi`, not `doi`; the other DOI and
#:    duplicate record kinds are `alternative_doi`, `alternative_pmid`, `alternative_pmcid`.
#: 5: a `runs` table, and `artifacts.run_id` naming the run an artifact was imported from.
SCHEMA_VERSION = 5

DDL = """
CREATE TABLE IF NOT EXISTS articles (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    merged_into TEXT
);

CREATE TABLE IF NOT EXISTS aliases (
    kind       TEXT NOT NULL,
    value      TEXT NOT NULL,
    article_id TEXT NOT NULL,
    PRIMARY KEY (kind, value)
);
CREATE INDEX IF NOT EXISTS aliases_article_idx ON aliases(article_id);

CREATE TABLE IF NOT EXISTS artifacts (
    article_id  TEXT NOT NULL,
    stage       TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL,
    fingerprint TEXT NOT NULL DEFAULT '',
    blob        TEXT,
    summary     TEXT NOT NULL DEFAULT '{}',
    error       TEXT,
    updated_at  TEXT NOT NULL,
    run_id      TEXT,
    PRIMARY KEY (article_id, stage, source)
);
CREATE INDEX IF NOT EXISTS artifacts_stage_status_idx ON artifacts(stage, status);
CREATE INDEX IF NOT EXISTS artifacts_article_idx ON artifacts(article_id);

CREATE TABLE IF NOT EXISTS attempts (
    article_id   TEXT NOT NULL,
    stage        TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT '',
    attempted_at TEXT NOT NULL,
    ok           INTEGER NOT NULL,
    error        TEXT
);
CREATE INDEX IF NOT EXISTS attempts_lookup_idx ON attempts(article_id, stage, source);

-- A person's verdict that a table holds no coordinates, whatever the extractor
-- made of it. `table_id` '*' covers the whole article. Upload leaves excluded
-- tables out, and retracts an article once nothing of it is left; sync does the
-- same to the corpus. Kept apart from `artifacts` because it is not produced by
-- a stage: rerunning the pipeline must never overwrite a human judgment.
CREATE TABLE IF NOT EXISTS exclusions (
    article_id TEXT NOT NULL,
    table_id   TEXT NOT NULL,
    reason     TEXT NOT NULL,
    note       TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY (article_id, table_id)
);

-- One row per `ingest add --search`: what was asked and what each provider was
-- sent. `providers` maps a provider to {query, records, without_id}.
CREATE TABLE IF NOT EXISTS searches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    query       TEXT NOT NULL,
    years       TEXT NOT NULL,
    searched_at TEXT NOT NULL,
    providers   TEXT NOT NULL
);

-- The catalog article each provider's hit resolved to, after registration and
-- enrichment. Read through `articles.merged_into`, never rewritten.
CREATE TABLE IF NOT EXISTS search_hits (
    search_id  INTEGER NOT NULL,
    provider   TEXT NOT NULL,
    article_id TEXT NOT NULL,
    PRIMARY KEY (search_id, provider, article_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS search_hits_article_idx ON search_hits(article_id);

-- An identifier registration did not add, and why: a second journal DOI that the
-- registries give another paper, or a PMID whose PubMed record is another article's.
-- Kept so a refusal is on record, not only in a log; refused again, it is refused_at
-- the latest time.
CREATE TABLE IF NOT EXISTS refusals (
    kind       TEXT NOT NULL,
    value      TEXT NOT NULL,
    article_id TEXT,
    reason     TEXT NOT NULL,
    refused_at TEXT NOT NULL,
    PRIMARY KEY (kind, value, reason)
) WITHOUT ROWID;

-- The ids a download's files name (an ACE `<pmid>.html`, a `pmcid_<n>/` directory,
-- the DOI in its XML), by the blob of its record, which fixes the files. An identity
-- repair finds by it every article whose download names an id the repair hands out.
-- Each blob read is in download_names_read, whether it names an id or not.
CREATE TABLE IF NOT EXISTS download_names (
    blob  TEXT NOT NULL,
    kind  TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (blob, kind, value)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS download_names_value_idx ON download_names(value, kind);
CREATE TABLE IF NOT EXISTS download_names_read (
    blob TEXT PRIMARY KEY
) WITHOUT ROWID;

-- One row per run of a stage made away from ns-pond (a cluster job, a collected set) or
-- a migration of its output. `artifacts.run_id` names the run an artifact came from; it
-- is null for the stages ns-pond runs itself. `config` is JSON.
CREATE TABLE IF NOT EXISTS runs (
    run_id         TEXT PRIMARY KEY,
    kind           TEXT NOT NULL,
    stage          TEXT NOT NULL,
    source         TEXT NOT NULL,
    pipeline       TEXT,
    version        TEXT,
    model          TEXT,
    adapter        TEXT,
    prompt_version TEXT,
    schema_version TEXT,
    code_commit    TEXT,
    machine        TEXT,
    config         TEXT NOT NULL DEFAULT '{}',
    bundle_id      TEXT,
    created_at     TEXT NOT NULL,
    imported_at    TEXT NOT NULL
);

-- What each data-source catalogue said, under its own ids; a pull of a catalogue replaces
-- all of its records.
CREATE TABLE IF NOT EXISTS data_source_records (
    provider  TEXT NOT NULL,
    record_id TEXT NOT NULL,
    record    TEXT NOT NULL,
    pulled_at TEXT NOT NULL,
    PRIMARY KEY (provider, record_id)
) WITHOUT ROWID;

-- The dictionary merged from those records (`data_sources.merge`), rebuilt by each pull.
-- The datasources stage searches papers for its aliases; list columns hold JSON arrays.
CREATE TABLE IF NOT EXISTS data_sources (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    kinds       TEXT NOT NULL,
    modalities  TEXT NOT NULL,
    url         TEXT NOT NULL,
    url_source  TEXT NOT NULL,
    rrids       TEXT NOT NULL,
    re3data_ids TEXT NOT NULL,
    part_of     TEXT NOT NULL,
    providers   TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS data_source_aliases (
    data_source_id TEXT NOT NULL,
    alias          TEXT NOT NULL,
    tier           TEXT NOT NULL,
    PRIMARY KEY (data_source_id, alias, tier)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

#: Run once the catalog is at SCHEMA_VERSION: it names a column a v4 catalog lacks until
#: the migration adds it.
CURRENT_DDL = """
CREATE INDEX IF NOT EXISTS artifacts_run_idx ON artifacts(run_id) WHERE run_id IS NOT NULL;
"""

PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA busy_timeout=30000",
    "PRAGMA temp_store=MEMORY",
)
