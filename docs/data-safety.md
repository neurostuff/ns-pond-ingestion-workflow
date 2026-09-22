# Not losing the corpus

The downloads on beast represent months of scraping under API rate limits and
cannot be regenerated cheaply. This is what the refactor does to protect them.

## Where the corpus is

```
beast:/data/alejandro/projects/ns-pond/
  data/                       39,349 article directories (the ns-pond output)
  data.tar.gz                 4.4 GB archive of the same
  ingestion-cache-indices/    14 legacy sqlite indices, 10.6 GB
  staging/manifests/          the JSONL manifests runs are driven by
  staging/logs/pipeline.log   8.1 GB
```

## Migration cannot write to the old caches

`ingest migrate` opens every legacy database through
[`_open_readonly`](../ingestion_workflow/migrate.py):

```python
conn = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
conn.execute("PRAGMA query_only = ON")
```

`immutable=1` takes no locks and creates no `-shm` or `-wal` sidecar, so it
works on read-only media and cannot modify the source file. When an
un-checkpointed WAL is present its contents would be invisible under
`immutable`, so that case falls back to `mode=ro` — which still cannot write the
database — and logs a warning.

This is verified rather than asserted:
`test_migration_never_writes_to_the_source` hashes the source before and after
and checks no sidecar appeared. The same check was run against a real 100 MB
copy of `download/ace`:

```
$ md5sum -c legacy_before.md5
.../legacy/download/ace/index.sqlite: OK
```

## Migration never moves the bytes

Legacy artifacts point at files already on disk — `source/ace/35864341.html`
and the rest. Migration copies the *rows*, and the paths inside them, into the
catalog. It does not relocate, rewrite or delete a single downloaded file. If
the catalog were thrown away entirely, every article would still be where it was.

## Migration is idempotent

Article ids are derived from identifiers:

```python
def _article_id(seed: str) -> str:
    return shortuuid.uuid(name=_UUID_NAMESPACE + seed)[:12]
```

so the same row always lands on the same article. Re-running a migration that
was interrupted adds nothing and duplicates nothing.

## Merges do not delete

When two ids turn out to name one article, `Catalog._merge` sets
`articles.merged_into` and repoints aliases. The superseded row stays. Nothing
on disk is unlinked.

## Recommended sequence on beast

```bash
# 1. See what would be imported, writing nothing at all.
ingest migrate /data/alejandro/projects/ns-pond/ingestion-cache-indices \
    --config my_config.yaml --dry-run

# 2. Import for real. The catalog is a NEW directory; the old one is untouched.
ingest migrate /data/alejandro/projects/ns-pond/ingestion-cache-indices \
    --config my_config.yaml

# 3. Check the catalog agrees with what you expect.
ingest status --config my_config.yaml
ingest show PMC10634720 --config my_config.yaml

# 4. Confirm the pipeline considers the corpus done, without doing anything.
ingest run --dry-run --config my_config.yaml
```

Step 4 is the real acceptance test: after a correct migration it should report
nearly everything `fresh` and almost nothing `pending`. If it wants to
re-download thousands of articles, **stop** — the migration mapped identifiers
wrongly, and the fix is to delete the new catalog directory and investigate.
Deleting the catalog is always safe; it holds no unique data.

Keep `ingestion-cache-indices/` until a full run has completed against the
catalog. Only then is it redundant.

## The one thing to be careful with

`ingest run --refresh <stage>` deliberately ignores cached results and recomputes.
On `download` against the whole catalog that means re-fetching everything. Pair
it with `--manifest` or `--limit`, and check with `--dry-run` first:

```bash
ingest run --stage download --refresh download --manifest small.jsonl --dry-run
```
