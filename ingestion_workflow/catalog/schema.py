"""Catalog schema. One sqlite database holds identity, artifacts and attempts."""

from __future__ import annotations

SCHEMA_VERSION = 1

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
