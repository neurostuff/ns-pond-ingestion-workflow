# Migrating from the pre-catalog pipeline

## What changed for you

| before | after |
|---|---|
| `ingest run --config c.yaml -m ids.jsonl -s download -s extract` | `ingest run -c c.yaml -m ids.jsonl -s download -s extract` |
| `ingest search --query … --label vbm-1995` | `ingest add --query … ` (writes to the catalog, not a file) |
| `ingest download --manifest ids.jsonl` | `ingest run --stage download --manifest ids.jsonl` |
| `ingest extract --manifest ids.jsonl` | `ingest run --stage extract --manifest ids.jsonl` |
| `ingest create-analyses bundles.json` | `ingest run --stage analyses` |
| `ingest upload-analyses` | `ingest run --stage upload` |
| `ingest sync` (was a no-op) | `ingest run --stage sync` |
| `ingest index-legacy-downloads ace <dir>` | `ingest migrate <old-cache>` |
| — | `ingest status`, `ingest show <id>` |

`ingest run --manifest … --stage …` is unchanged in spirit, so the beast
`nohup` invocations keep working with the same shape.

## Settings that are gone

`force_redownload`, `force_reextract`, `ignore_cache_stages`,
`use_cached_inputs` and `cache_only_mode` were five overlapping ways to say
"ignore the cache". They are replaced by fingerprints plus one flag:

```bash
ingest run --refresh extract      # was: force_reextract / ignore_cache_stages
ingest run --refresh all          # everything
```

`stages:` in YAML is replaced by `--stage` on the command line, because which
stages to run is a property of the invocation, not of the configuration.

`neurostore_base_url`, `neurostore_token` and `neurostore_batch_size` are gone —
upload has always talked to Postgres directly and never read them.

New settings: `catalog_root` (default `./.catalog`), `max_attempts` (3) and
`retry_after_hours` (24).

## Running the migration

See [data-safety.md](data-safety.md) for the full sequence and the guarantees.
The short version:

```bash
ingest migrate /data/alejandro/projects/ns-pond/ingestion-cache-indices -c my_config.yaml --dry-run
ingest migrate /data/alejandro/projects/ns-pond/ingestion-cache-indices -c my_config.yaml
ingest status -c my_config.yaml
ingest run --dry-run -c my_config.yaml      # should be nearly all "fresh"
```

## What is not migrated, and why

- **`gather/*`** (1.66 GB, 2.6M rows) memoised "which identifiers belong to this
  article". The catalog's alias table *is* that memo, and it is populated as a
  side effect of migrating `download` and `extract`. Nothing is lost.
- **`.cache/metadata/semantic_scholar/*.json` and `.../pubmed/*.json`** were a
  second, per-provider metadata cache alongside the sqlite one. The sqlite index
  is migrated; these files are left in place and simply stop being read.

## Rolling back

Delete the catalog directory. It holds no unique data — every artifact points at
files that already existed, and the legacy indices were never modified.
