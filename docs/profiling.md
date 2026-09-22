# Profiling pass

Growth curves first, then profiles at the size the curves identified. Every
number below is measured on an 8-core dev box or read off the live corpus;
where a number is extrapolated it says so.

## Growth: nothing is superlinear

Three sizes, time and peak RSS together, cold and warm separately.

| operation | 100 | 1,000 | 10,000 | exponent |
|---|---:|---:|---:|---:|
| `register_many` cold | 72.3 µs | 66.9 µs | 62.1 µs | n^0.97 |
| `register_many` warm (all known) | 101.1 µs | 67.4 µs | 63.4 µs | n^0.96 |
| `record` cold | 786.6 µs | 626.2 µs | 644.9 µs | n^0.96 |
| `record` warm (blob already present) | 383.7 µs | 347.5 µs | 335.7 µs | n^0.97 |
| plan read, batched 500 | 15.1 µs | 14.6 µs | 12.3 µs | n^0.95 |

All linear, which is what the algorithms should be. Peak RSS grew by under
6 MB at every size. No escalation warranted on the curve alone — so the
question became *which constants actually matter*, and against what.

## The regime that matters

Per-article costs from the production log, next to the new code:

| | ms/article | measured on |
|---|---:|---|
| `Download[elsevier]` | **301–315** | 12,841 and 5,773 real articles |
| `Extract[elsevier]` | 3.8–34.3 | 84,782 and 1,050 real articles |
| `Download[pubget]` | 7.6–13.6 | 8,960 and 3,375 real articles |
| `Extract[pubget]` | 1.6–1.8 | 69,535 and 3,822 real articles |
| `Catalog.record` | 0.65 | this pass |

`record` is noise against elsevier downloads and a 38% tax on pubget
extraction. Both worth knowing before optimising anything.

## Finding 1 — the biggest cost is work that fails, and repeats

Reconstructed from the log by pairing each `processing N` with its `successes:`:

```
stage              attempted       ok   failed  seconds  ms/item   wasted on failures
Download[elsevier]    12,841    1,050   11,791    3,872    301.5              59.3 min
Download[elsevier]     5,773      697    5,076    1,820    315.3              26.7 min
Download[pubget]       8,960    3,305    5,655       68      7.6               0.7 min
Download[pubget]       3,375    1,228    2,147       46     13.6               0.5 min
...
total spent on work that failed                                                1.5 h
```

Since only successes were cached, every one of those failures was re-attempted
on the next run, forever. Recording failures — `permanent` for what can never
succeed, backoff and an attempt cap for the rest — is worth **134–182 minutes
across four runs on elsevier downloads alone**, depending on what share of the
failures are permanent:

```
 permanent share     run 1     run 2     run 3     run 4  saved by run 4
            50%        65        30        30         0          134 min
            70%        65        18        18         0          158 min
            90%        65         6         6         0          182 min
     (unchanged)       65        65        65        65
```

This is the single largest win available anywhere in the system, and it is a
property of the design rather than of any hot loop.

## Finding 2 — the extraction pool was slower than no pool at all

`BaseExtractor` spawned a `ProcessPoolExecutor` with `mp_context="spawn"` for
every extractor. Measured on 25 real pubget articles, 4 workers against serial:

```
  n=1      0.81x   (0.01s -> 0.01s)  POOL LOSES
  n=5      0.02x   (0.12s -> 4.96s)  POOL LOSES     <- 41x slower
  n=25     0.13x   (0.78s -> 5.94s)  POOL LOSES
  n=100    0.44x   (3.05s -> 6.93s)  POOL LOSES
  n=250    0.52x   (7.79s -> 15.08s) POOL LOSES
```

Parallelism made extraction **two to fifty times slower at every size tested**,
and threw `BrokenProcessPool` under load.

The mechanism, measured directly:

```
$ time python -c "import ingestion_workflow.extractors"
3.21s

  2622 ms   ingestion_workflow.extractors
  1394 ms     ingestion_workflow.extractors.elsevier_extractor
  1334 ms       pubget._coordinate_space
   942 ms         pubget._typing
   941 ms           nilearn.maskers
   577 ms             sklearn
```

`spawn` re-imports the package in every worker, and `pubget` pulls in nilearn
and sklearn. Each worker paid 3.2 seconds before doing any work.

The comment that chose `spawn` was correct but over-applied: CUDA cannot be
re-initialised in a forked child, and only the **PDF** extractor touches CUDA.
The three XML extractors paid a CUDA tax they never owed.

`mp_start_method` is now a per-extractor attribute — `fork` on the base class,
`spawn` on `PdfExtractor` — and the pool is skipped entirely below two items.
The same measurement afterwards:

```
  n=1      1.82x   n=5   1.22x   n=25  2.30x   n=100 2.80x   n=250 2.54x   pool wins
```

The pool now delivers real parallelism instead of a slowdown.

### Equivalence

`fork` must produce what `spawn` produced. Extraction output for 12 real
articles from each of pubget and elsevier, normalised for the temp root and the
`extracted_at` timestamp:

```
--- serial vs spawn4 ---  identical
--- serial vs fork4  ---  identical
```

(An earlier run of this check reported a mismatch. That was the harness: the
benchmark script had no `if __name__ == "__main__"` guard, so each spawn worker
re-executed its module top level and created its own temp root. The code was
never at fault.)

`test_pool_context.py` pins the start method per extractor and pins that a
single-item batch starts no pool at all.

### What it is worth

On the production path — the scheduler's 500-article batches — the pool startup
is better amortised, so the win is smaller than the microbenchmark suggests:

```
  spawn  1,000 articles in 2 batches of 500:  14.8s  (14.8 ms/article)
  fork   1,000 articles in 2 batches of 500:  10.8s  (10.8 ms/article)

  fork is 1.4x faster on the production path
  pool startup avoided per batch: 2.0s
  over 463,584 articles (927 batches): 0.5 h saved
```

Half an hour per full extract run, and up to 52x on the small batches that
`--limit`, retries and tail batches produce.

## Finding 3 — `ingest --help` took 3.3 seconds

Same root cause, different victim. `pipeline.stages.analyses` imported
`extractors.table_heuristics`, which ran `extractors/__init__.py`, which
imported all four extractors and therefore nilearn and sklearn — to print a
help message.

`extractors/__init__.py` and `clients/__init__.py` are now lazy (PEP 562
`__getattr__`), and `sanitize_table_id` moved out of `services.create_analyses`
— which drags in the OpenAI SDK — into `services/naming.py`. The two
`sanitize_table_id` implementations in this codebase are *not* interchangeable
(`table-1` versus `table-001`), so the one the ns-pond layout depends on was
moved rather than merged.

```
before          3.38s  3.20s  3.39s
after lazy      1.00s  0.98s  1.04s
after naming    0.61s  0.48s  0.46s
```

**3.3 s → 0.46 s, 7.2x.** What remains is 232 ms of pydantic, which every
command genuinely needs.

## Finding 4 — the write path is near its floor

`Catalog.record` at 10,000 artifacts with realistic 12 KB extract payloads:

| step | µs/artifact | share |
|---|---:|---:|
| `json.dumps(sort_keys=True, separators=…)` | 261.4 | 57% |
| `gzip.compress(level=6)` | 204.3 | 44% |
| `sha256` | 24.6 | 5% |

Only one change here is free. The blob's digest is taken **before**
compression, so the compression level does not move any blob's address:

```
gzip level=1   126.3 us   579 B
gzip level=3   121.4 us   517 B     <- chosen
gzip level=6   204.3 us   476 B
gzip level=9   220.9 us   476 B
```

Level 3 compresses 1.7x faster than level 6 for blobs 8.5% larger, digests
identical, and blobs already written at level 6 stay readable.
`test_blob_address_does_not_depend_on_compression` pins that.

Everything else in that table is load-bearing. `sort_keys=True` costs 62 µs
(31% of the encode) and could go, but it is what makes two equal payloads share
one blob — dropping it would orphan every existing blob and silently double the
store, which is not worth 62 µs.

## Scale check: the size the system actually runs at

The curve stopped at 10,000; the live corpus is 463,584 downloaded articles.
sqlite b-tree behaviour can change once the index stops fitting in cache, so
the whole thing was built:

```
build: register 61s (132 us/article), record 139s (301 us/artifact)
articles: 463,584

plan whole corpus (batched 500) :     7.2s  ( 15.5 us/article)  RSS +0 MB
single artifact lookup          :    23.6 us
resolve identifier              :    46.5 us
status_counts (full scan)       :     0.1s

catalog.sqlite 0.32 GB   blobs 0.10 GB   total 0.42 GB
vs the legacy download/pubget index for the same rows: 1.49 GB
```

RSS stayed between 55 and 68 MB throughout. Lookup drifts from 18.2 µs at
40,000 to 23.6 µs at 463,584 — b-tree depth, as expected, and not worth acting
on. Planning the entire corpus costs 7.2 seconds.

## What did not work

Kept so the next person does not repeat it.

- **Threading the blob writes.** `zlib` releases the GIL but `json.dumps` does
  not, and the encode is 57% of the work. 4 threads measured 1.12x, 8 threads
  0.96x — slower than serial level 3 alone.
- **Caching created blob directories.** `mkdir(exist_ok=True)` was called once
  per put instead of once per shard, which looked like 10,000 wasted syscalls.
  Measured 0.97x: the kernel's dentry cache already makes it free.
- **`blake2b` instead of `sha256`** for blob digests. 1.7x faster hashing, but
  hashing is 5% of the path and it would re-address every existing blob.
- **Dropping `sort_keys`.** Real 62 µs, but see above.

## What to look at next

1. `Download[elsevier]` at 302 ms/article is now the dominant cost of a full
   run by two orders of magnitude, and it is network-bound. The lever is
   concurrency and not re-attempting known failures — the latter is done.
2. `Extract[elsevier]` at 34.3 ms/article on its first pass versus 3.8 ms
   later suggests cold-cache disk rather than CPU. Worth confirming on beast
   before treating it as an extraction cost.
3. Beast has 4 GPUs and `pdf_extract_workers` defaults to one per CUDA device.
   Only 118 PDF downloads exist so far, so there is nothing to profile yet.
4. The blob store never garbage-collects. Superseded blobs accumulate as
   fingerprints turn over. An `ingest gc` that drops unreferenced blobs is the
   next real piece of work, not a speed optimisation.
