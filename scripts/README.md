# scripts/

One-off tools that are not part of the `ingest` command.

| script | what it does |
|---|---|
| `generate_manifest.py` | build a JSONL identifiers manifest from a list of ids |
| `run_elsevier_tables.py` | fetch a few Elsevier articles and dump their tables, for debugging |

Removed in the catalog refactor, because `ingest` now covers them:

- `reextract_text.py` → `ingest refresh-text`, which carries the offsets stored
  against each text to its new version, or marks them stale.
- `run_playground_pipeline.py` → `ingest run --manifest … --stage …`
- `prune_create_analyses_cache.py` → `ingest run --refresh analyses`, which
  recomputes stale entries instead of deleting rows out from under the index.
