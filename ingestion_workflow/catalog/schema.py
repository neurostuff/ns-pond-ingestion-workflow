"""Catalog schema. One sqlite database holds identity, artifacts and attempts."""

from __future__ import annotations

#: 2: doi aliases hold the bare DOI, not a resolver URL.
#: 3: doi aliases hold the canonical DOI: bare and lower case.
SCHEMA_VERSION = 3

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
-- sent. `providers` maps a provider to {query, records, without_id}. A table
-- of its own, created on open, so existing catalogs gain it with no migration.
CREATE TABLE IF NOT EXISTS searches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    query       TEXT NOT NULL,
    years       TEXT NOT NULL,
    searched_at TEXT NOT NULL,
    providers   TEXT NOT NULL
);

-- The catalog article each provider's hit resolved to, after registration and
-- enrichment. A merge hands a retired article's rows to the survivor.
CREATE TABLE IF NOT EXISTS search_hits (
    search_id  INTEGER NOT NULL,
    provider   TEXT NOT NULL,
    article_id TEXT NOT NULL,
    PRIMARY KEY (search_id, provider, article_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS search_hits_article_idx ON search_hits(article_id);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

PRAGMAS = (
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA foreign_keys=ON",
    "PRAGMA busy_timeout=30000",
    "PRAGMA temp_store=MEMORY",
)
