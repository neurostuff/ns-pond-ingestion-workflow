# scripts/

One-off tools that are not part of the `ingest` command.

| script | what it does |
|---|---|
| `generate_manifest.py` | build a JSONL identifiers manifest from a list of ids |
| `run_elsevier_tables.py` | fetch a few Elsevier articles and dump their tables, for debugging |
| `symbols_as_digits_manifest.py` | list the articles whose stored PDF text the pdf extractor now repairs or sets aside, for `--refresh extract:pdf` |
| `backfill_download_copies.py` | record the copy (route, version, access, url) of each download stored before the download stage recorded it; a dry run unless `--apply --backup <path>` |
| `rekey_metadata_reads.py` | give triage and prose, and what is made from them, the fingerprints of what they now read of the metadata, where the former formula matches; run once, before the metadata stage re-runs; a dry run unless `--apply --backup <path>` |
| `repair_doi_collisions.py` | settle the DOIs two catalog articles hold in different case, by `doi_collisions.tsv`; run once, before the schema version 3 migration |

Removed in the catalog refactor, because `ingest` now covers them:

- `reextract_text.py` → `ingest refresh-text`, which carries the offsets stored
  against each text to its new version, or marks them stale.
- `run_playground_pipeline.py` → `ingest run --manifest … --stage …`
- `prune_create_analyses_cache.py` → `ingest run --refresh analyses`, which
  recomputes stale entries instead of deleting rows out from under the index.
