# What the pipeline does today

A review of `ingestion_workflow` as of `3e3a457`, against the live corpus on
beast (`/data/alejandro/projects/ns-pond`). Every number here was measured, not
estimated; the commands are in [benchmarks.md](benchmarks.md).

## The shape of it

Six stages run in a fixed order, driven by `workflow/orchastrator.py`:

| stage | input | output | where it caches |
|---|---|---|---|
| `gather` | PubMed queries, JSONL manifest | `Identifiers` | `.cache/gather/<provider>/index.sqlite` |
| `download` | `Identifiers` | `DownloadResult[]` | `.cache/download/<source>/index.sqlite` |
| `extract` | `DownloadResult[]` | `ArticleExtractionBundle[]` | `.cache/extract/<source>/index.sqlite` |
| `create_analyses` | bundles | `slug -> table -> AnalysisCollection` | `.cache/create_analyses/index.sqlite` |
| `upload` | analyses | `UploadOutcome[]` | `.cache/upload/index.sqlite` |
| `sync` | outcomes | `ns-pond/<base_study_id>/…` | — (writes files) |

Four download sources — `pubget`, `elsevier`, `ace`, `pdf` — are tried in
configured order per identifier, each falling through to the next on failure.
This heterogeneity is the point of the system and is preserved by the redesign.

## Live corpus, measured

```
download/pubget     463,584 rows   1.5 GB      extract/pubget    94,977 rows   482 MB
download/elsevier   102,624 rows   152 MB      extract/elsevier 102,021 rows   443 MB
download/ace         88,625 rows   100 MB      extract/ace       29,525 rows   152 MB
download/pdf            118 rows   148 KB      extract/pdf          —          732 KB
gather/s2           919,930 rows   729 MB      create_analyses   49,345 rows   373 MB
gather/pubmed       762,550 rows   624 MB      upload            27,411 rows    12 MB
gather/openalex           —        305 MB      metadata         205,027 rows   5.2 GB
                                               ───────────────────────────────────────
                                               14 sqlite files, 10.6 GB total
```

39,349 articles have made it all the way to `ns-pond/data/`. The indices are
**270 KB per article** — larger than most of the articles.

## Seven findings

### 1. The cache key is mutable, so identity is guessed at read time

`Identifier.slug` is `slugify(f"{pmid}-{doi}-{pmcid}")`. It is recomputed from
whatever identifiers are known *at that moment*. Enrichment discovers new ids,
so the same article gets a different key on a later run.

Real rows from `download/pubget`, all claiming `PMC10634720`:

```
pmc10634720||                                        (before enrichment)
37961286-10-1101-2023-10-21-563317-pmc10634720       (after enrichment)
40125571-10-1111-ejn-70081-pmc10634720               (a different article!)
```

The code compensates with `get_by_identifier`, which falls back to matching
`pmid`, then `doi`, then `pmcid`, taking `LIMIT 1`. The third row above shows
what that costs: two distinct articles are now mutually reachable through a
shared pmcid, and whichever sqlite returns first wins.

Because identity is unreliable, `expand_target_aliases` and `identifier_aliases`
in `workflow/common.py` exist to compare *sets of possible names* instead of
keys. That alias machinery then leaks into `upload.py` and `sync.py`.

### 2. "Hydrate the cache" is written four times, differently

| location | lookup used | filters by |
|---|---|---|
| `orchastrator._hydrate_downloads_from_cache` | `get_download_by_identifier` | slug |
| `upload._hydrate_downloads_from_cache` | `get_download_by_identifier` | alias set |
| `sync._hydrate_downloads_from_cache` | `get_download_by_identifier` | alias set + target filter |
| `cli.main._hydrate_downloads_from_cache` | `get_download` (exact slug only) | slug |

The CLI copy silently finds fewer downloads than the orchestrator copy for the
same manifest, because it never falls back to identifier columns.
`_hydrate_bundles_from_cache` is likewise duplicated between `orchastrator.py`
and `upload.py`, with the two copies differing in whether `existing_has` is
computed before or after the early `continue`.

### 3. Metadata is refetched for every article on every run

From `staging/logs/pipeline.log`, a run where 9,421 of 14,293 extractions were
already cached:

```
15:23:32 Enriching metadata for 14293 articles
15:23:32 Fetching metadata from Semantic Scholar for 14293 articles
15:23:47 Semantic Scholar returned metadata for 14119 articles
15:23:47 Fetching metadata from PubMed for 14293 articles      <- all of them
15:24:48 PubMed returned metadata for 14293 articles
15:24:48 Falling back to extractor metadata for 14293 articles  <- all of them
15:25:38 Successfully enriched metadata for 14293 articles
```

166 seconds, of which nearly all is redundant. Two causes:

- `run_extraction` calls `enrich_metadata` on *all* results, cache hits included.
- `MetadataService._needs_more_metadata` is `not _is_complete`, and
  `_is_complete` demands all nine fields — including `keywords`, `license` and
  `open_access`. Semantic Scholar essentially never returns all nine, so every
  article is "incomplete" forever. PubMed and the file fallback therefore run
  for 100% of articles on 100% of runs.

This is also why `metadata/index.sqlite` holds 205,027 rows and keeps growing.

### 4. Payloads are stored whole, uncompressed, in the index

`CacheIndex` puts `json.dumps(payload)` in a `payload_json` column. For
metadata that payload includes `raw_metadata` — the entire upstream API
response. Sampling five rows:

```
total bytes   raw_metadata bytes   share
32,555        28,928               89%
32,683        29,025               89%
24,450        21,162               87%
68,748        63,515               92%
49,167        44,577               91%
```

Average metadata payload is 23.7 KB; the largest is 1.3 MB. Nothing downstream
reads `raw_metadata`. **About 4.6 GB of the 5.2 GB metadata index is dead weight.**

### 5. Every lookup opens a new database

`load_index()` calls `sqlite3.connect()` and then `_initialize()` — `CREATE
TABLE IF NOT EXISTS`, `PRAGMA table_info`, three `CREATE INDEX IF NOT EXISTS`,
and a `commit()` — on *every call*. Connections are never closed.

`create_analyses` calls this once per table, `upload` once per slug:

```
get_cached_create_analyses_result   0.736 ms/lookup   (0.37 ms is the open+init)
get_cached_article_metadata         0.750 ms/lookup
```

At the current 49,345 tables that is ~36 s of pure schema re-initialisation per
run; at the 463,584 articles already downloaded it would be ~6 minutes, plus a
leaked file descriptor per lookup.

### 6. Whole-corpus loads on a manifest of fifty

`load_cached_analysis_collections(settings)` reads and deserialises the entire
`create_analyses` index into a dict, ignoring any selection. It is called
unconditionally by `run_upload`, and again by `sync._resolve_analyses`.

Measured, then extrapolated against the live index:

```
5,000 articles / 15,000 tables:   6.2 s,   174 MB resident  (35.6 KB/article)
49,345 rows (current corpus):    ~20 s,  ~1.4 GB
463,584 articles (downloaded):  ~190 s,  ~16 GB
```

Uploading fifty studies costs the same 1.4 GB as uploading all of them.

### 7. Failures are never recorded, so they retry forever

`cache_download_results` persists successes only. A `DownloadResult` with
`success=False` is dropped. On the next run the identifier is "missing" again
and every source is retried from scratch — for articles that are paywalled,
withdrawn, or simply have no PMC record.

`tmp/failed_pubget_downloads.txt` on beast is a hand-rolled workaround for
exactly this. The README lists it as a known issue.

## Smaller things worth fixing

- `run_with_executor` (`workflow/common.py`) builds `future_map` to track input
  order, then appends results in completion order and never uses the map.
  `reorder_results` exists, unused, right below it. Parallel stages return
  results in nondeterministic order.
- `configure_logging` sets the root logger to `DEBUG` and attaches a file
  handler with no level, so `filelock` and `urllib3` debug output is persisted.
  `staging/logs/pipeline.log` is **8.1 GB**. `Settings.verbose` exists and is
  read by nothing.
- `Settings` has 60 fields. `neurostore_base_url`, `neurostore_token` and
  `neurostore_batch_size` are dead — upload talks to Postgres directly, not the
  REST API. Five overlapping flags govern caching: `force_redownload`,
  `force_reextract`, `ignore_cache_stages`, `use_cached_inputs`,
  `cache_only_mode`.
- `services/cache.py` annotates `Dict[...]` without importing `Dict`. Only
  `from __future__ import annotations` keeps it from raising.
- `BaseExtractor.extract` is declared `(self, download_result)` but every
  implementation and caller passes `progress_hook` too.
- `ingest sync` is `def sync(): pass`. `services/file_sync.py` is a docstring.
- Four extractors, four unrelated storage conventions: `ace` under
  `<cache>/ace/html/<journal>/`, `pubget` under `<root>/<hash>/`, `elsevier`
  under `<cache>/elsevier/<sha256>/`, and `pdf` writing extractions to
  `data_root/extractions/pdf` — a different root from its downloads.
- Coordinates have two parallel models: `Coordinate` (flat x/y/z) and
  `CoordinatePoint` (nested list + `values`). `sync._write_stage1` hand-converts
  between them.
- Pinned git dependencies drift with no lockfile or CI. A fresh `pip install -e .`
  produced a tree where `pubget._text._insert_tables` and `pyarty.at` were both
  missing, so five test modules failed to import until the pins were reinstalled.

## What is already right, and is kept

- Source fallthrough — try pubget, then elsevier, then ace, then pdf — is the
  correct shape for heterogeneous access, and the per-source `_SUPPORTED_IDS`
  gate avoids pointless attempts.
- `create_analyses._stamp_matches` is the only place that asks *"was this
  produced by the prompt and model I am using now?"*. The redesign generalises
  that idea to every stage rather than discarding it.
- Extraction runs under `ProcessPoolExecutor` with a `spawn` context because
  CUDA cannot be re-initialised in a forked child. That comment is load-bearing.
- The `ns-pond` on-disk layout, and the `stage1/analyses.json` and `pmids.tsv`
  shapes that pondie consumes, are a downstream contract. They do not change.
