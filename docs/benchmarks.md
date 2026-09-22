# Measurements

Every number was produced on this machine (Python 3.13, local SSD) or read
directly off the live corpus on beast. Scripts are inline so they can be re-run.

## Live corpus, as found

```
$ ssh beast-proxy 'sqlite3 "file:$R/metadata/index.sqlite?immutable=1" \
    "select count(*), cast(avg(length(payload_json)) as int) from metadata_entries;"'
205027|23687
```

| index | rows | size |
|---|---:|---:|
| `metadata` | 205,027 | 5.2 GB |
| `download/pubget` | 463,584 | 1.5 GB |
| `gather/semantic_scholar` | 919,930 | 729 MB |
| `gather/pubmed` | 762,550 | 624 MB |
| `extract/pubget` | 94,977 | 482 MB |
| `extract/elsevier` | 102,021 | 443 MB |
| `create_analyses` | 49,345 | 373 MB |
| `gather/openalex` | — | 305 MB |
| `extract/ace` | 29,525 | 152 MB |
| `download/elsevier` | 102,624 | 152 MB |
| `download/ace` | 88,625 | 100 MB |
| `upload` | 27,411 | 12 MB |
| **total** | | **10.6 GB** |

`raw_metadata` is 87–92% of every metadata payload and nothing downstream reads it.

## Cache operations, before and after

Same workload: 40,000 articles with pmid + doi + pmcid, one download artifact each.

| operation | before | after | factor |
|---|---:|---:|---:|
| single artifact lookup | 736 µs | 18.2 µs | **40×** |
| cached-metadata lookup | 750 µs | 18.2 µs | **41×** |
| identifier → article | alias-set scan | 35.8 µs | — |
| plan 40,000 articles | n/a (no planner) | 0.43 s | — |
| write 40,000 artifacts | 2.5 s | 6.2 s | 0.4× |
| index open + schema init | 0.37 ms *per lookup* | once per process | — |

Writes are slower because each artifact now also writes a gzipped blob and an
attempt row. That is the trade: ~3.7 µs per article per run, against 718 µs
saved on every subsequent read, plus failures that stop being retried forever.

<details>
<summary>before — <code>bench_cache.py</code></summary>

```
write 2000 download entries          :    0.143s  (0.071 ms/entry)
partition_cached_downloads (2000)    :    0.123s  (0.062 ms/id)
get_cached_article_metadata x200     :    0.150s  (0.750 ms/lookup)
load_download_index x200 (open only) :    0.066s  (0.330 ms/open)
```
</details>

<details>
<summary>after — <code>bench_new.py</code></summary>

```
register 40,000 articles              :    2.72s ( 68.1 us/article)
record 40,000 download artifacts      :    6.17s (154.3 us/artifact)
plan all 40,000 in 500-article batches:    0.43s ( 10.7 us/article)
single artifact lookup x5,000         :    0.09s ( 18.2 us/lookup)
resolve identifier x5,000             :    0.18s ( 35.8 us/resolve)
```
</details>

## Memory

`load_cached_analysis_collections` read the entire create-analyses index into a
dict regardless of how many articles the run was about. The scheduler streams in
batches of 500.

| articles | before (resident) | after (resident) |
|---:|---:|---:|
| 5,000 | 174 MB | ~1 MB |
| 40,000 | ~1.4 GB | 1 MB |
| 463,584 | ~16 GB (extrapolated) | 1 MB |

```
RSS while streaming all 40,000 : 77 -> 78 MB (delta 1 MB)
```

The old figure is linear in corpus size; the new one is linear in batch size.

## Storage after migration

Real legacy indices, migrated locally. Sources verified byte-identical
afterwards with `md5sum -c`.

| source | legacy sqlite | catalog db | blobs | total | ratio |
|---|---:|---:|---:|---:|---:|
| `download/ace` + `download/pdf` + `upload` | 112.8 MB | 71.5 MB | 42.5 MB | 114.0 MB | 1.01× |
| `metadata` (5,000-row slice) | 169.6 MB | **3.5 MB** | 45.4 MB | 48.9 MB | **0.29×** |

The metadata slice is the one that matters, and it splits two ways:

- **total on disk** falls 3.4× — gzip on payloads the old index stored raw.
- **the queryable database** falls **48×**, from 169.6 MB to 3.5 MB, because
  `raw_metadata` moved out of the row. Extrapolated to the full index, the part
  that gets opened, indexed and scanned goes from 5.2 GB to roughly 110 MB.

Migration throughput: 88,743 download rows + 27,408 upload rows in 46.5 s.

## Metadata refetching

From `staging/logs/pipeline.log`, one real run — 9,421 of 14,293 extractions
were cache hits, and metadata was fetched for all 14,293 anyway:

```
15:23:32  Semantic Scholar for 14293 articles
15:23:47  ... returned 14119                     (15 s)
15:23:47  PubMed for 14293 articles              <- should have been 174
15:24:48  ... returned 14293                     (61 s)
15:24:48  fallback for 14293 articles            <- should have been 0
15:25:38  enriched 14293                         (50 s)
```

Cause: `_is_complete` required all nine metadata fields, including `keywords`,
`license` and `open_access`, which no provider returns together. Every article
was therefore "incomplete" forever.

After: `metadata` is its own stage with its own fingerprint, so cache hits are
not re-enriched at all, and `is_sufficient` stops the provider waterfall once
title + authors + three of four core fields are present. On that run the work
would have dropped from 14,293 articles to 4,872 — and on a rerun, to zero.

## What to optimise next

Ranked by measured cost against expected benefit:

1. **`extract` is the only CPU-bound stage** and already runs under a
   `ProcessPoolExecutor`. Docling PDF extraction is the long pole; beast has 4
   GPUs and `pdf_extract_workers` defaults to one per CUDA device. Worth
   profiling per-source throughput before touching anything else.
2. **`record()` writes one blob per artifact** (154 µs/artifact). Batching the
   gzip work across a thread pool would cut the write path, but at 6 s per
   40,000 articles it is not yet the bottleneck.
3. **`register_many` resolves aliases one identifier at a time** (68 µs each).
   A single `SELECT … WHERE (kind,value) IN (…)` per batch would help a first
   import of several hundred thousand ids; it is irrelevant on reruns.
4. **The blob store never garbage-collects.** Superseded blobs stay on disk.
   A `ingest gc` that deletes blobs no artifact references is worth adding once
   fingerprints start turning over in anger.
5. **Do not optimise `plan`.** At 10.7 µs/article, planning the entire 463,584
   article corpus costs 5 seconds.
