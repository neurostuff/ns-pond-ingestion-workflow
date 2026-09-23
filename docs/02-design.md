# Design

## One sentence

The pipeline maintains a **catalog of articles**; each article carries one
**artifact per (stage, source)**; an artifact is reused when its **fingerprint**
still matches, and recomputed otherwise.

Everything else follows from that. There is no second way to find out what is
cached, and no stage reaches into the cache on its own.

## The model

```
Article ──< Alias        (pmid / pmcid / doi / neurostore -> article)
   │
   └─────< Artifact      (stage, source) -> status, fingerprint, blob, summary
              │
              └──< Attempt   (append-only: when, ok, error)
```

### Article identity is assigned once and never recomputed

An article gets an opaque `article_id` the first time it is seen. Every
identifier ever associated with it becomes an **alias** pointing at that id.
Enrichment adds aliases; it never changes the id.

```python
cat.register(Identifier(pmcid="PMC10634720"))          # -> "a7Kq2mNp"
cat.register(Identifier(pmcid="PMC10634720",
                        pmid="37961286"))              # -> "a7Kq2mNp", + alias
```

Article ids come from `uuid5(NAMESPACE_URL, ARTICLE_NAME_PREFIX + seed)`.
`ARTICLE_NAME_PREFIX` is a UUID v5 *name* — hashed, never dereferenced, and it
does not resolve to anything. It is URI-shaped because RFC 4122's URL namespace
expects that. Both it and the namespace are frozen: editing either re-keys every
article in every catalog, which `test_article_ids_are_frozen` exists to catch.

This is the fix for [finding 1](01-current-behavior.md). Resolution is a single
indexed lookup on `aliases`, so `identifier_aliases`, `expand_target_aliases`
and the four `_hydrate_*_from_cache` copies all disappear.

When two ids that were thought distinct turn out to name one article, the
catalog records a **merge** (`articles.merged_into`) rather than deleting a row.
Nothing on disk is ever unlinked by a merge — see
[data-safety.md](data-safety.md).

### An artifact is what a stage produced for one article

```
(article_id, stage, source) -> status, fingerprint, blob_sha, summary, error
```

`status` is one of:

| status | meaning | retried? |
|---|---|---|
| `ok` | produced, reusable | only if fingerprint changed |
| `failed` | transient failure | yes, under backoff |
| `permanent` | 404, no PMC record, paywalled | never, without `--refresh` |
| `skipped` | source cannot handle this id | no |

Recording `failed` and `permanent` is the fix for
[finding 7](01-current-behavior.md): a paywalled article stops being retried on
every run, and `tmp/failed_pubget_downloads.txt` stops being necessary.

### Fingerprints replace five flags

Today: `force_redownload`, `force_reextract`, `ignore_cache_stages`,
`use_cached_inputs`, `cache_only_mode`. Their interactions are undocumented and
partly contradictory.

Instead, each stage declares what its output depends on:

```python
def fingerprint(self, ctx, article):
    return fp("extract", self.source, EXTRACTOR_VERSION,
              upstream=ctx.artifact(article, "download", self.source).fingerprint)
```

An artifact is **fresh** iff `status == "ok"`, its fingerprint equals the one
computed now, and its blob still exists on disk. Otherwise it is work. Change
the coordinate prompt and every analysis goes stale automatically — which is
what `create_analyses._stamp_matches` already does correctly for one stage, and
is here generalised to all of them.

The only override is `--refresh <stage>`, which ignores freshness for that stage.

### Blobs, not fat rows

The catalog row holds identity, status, fingerprint and a *small* summary (the
handful of fields that get queried or displayed). Anything large is gzipped into
a content-addressed store and referenced by hash:

```
<cache_root>/blobs/ab/abcd1234….json.gz
```

Identical payloads deduplicate for free. For metadata this is the fix for
[finding 4](01-current-behavior.md): `raw_metadata` goes to a blob, the row
keeps title/authors/abstract/journal/year/license/oa, and the 5.2 GB index
becomes a ~600 MB one.

### One connection, opened once

`Catalog` is a context manager holding a single WAL connection with the schema
created once at open. `load_index()`-per-lookup, and the file descriptor it
leaked each time, are gone ([finding 5](01-current-behavior.md)).

## Stages

A stage is four things and nothing more:

```python
class Stage(Protocol):
    name: str
    requires: str | None          # upstream stage, or None

    def sources(self, ctx) -> Sequence[str]
    def fingerprint(self, ctx, article, source) -> str
    def execute(self, ctx, batch: list[Work]) -> Iterator[Outcome]
```

The scheduler owns everything else: resolving the selection, asking the catalog
which articles are not fresh, batching them, calling `execute`, recording
outcomes and attempts. Stages never touch the catalog directly, so there is
exactly one place where "is this cached?" is decided.

The six stages keep their current responsibilities and their current
implementations. `download` and `extract` remain thin adapters over the four
existing extractors, which are unchanged — the heterogeneity of pubget /
elsevier / ace / pdf is the reason the system exists, not an accident to be
normalised away.

### Nothing loads the whole corpus

`plan` yields batches. `execute` consumes a batch. Results are recorded per
batch. A run over a fifty-article manifest touches fifty articles' worth of
memory, which is the fix for [finding 6](01-current-behavior.md).

## The command line

The old CLI had `run`, `search`, `download`, `extract`, `create-analyses`,
`upload-analyses`, `index-legacy-downloads`, and a `sync` that was `pass`. Some
took `--manifest`, some took a positional JSON file, and the `extract` command
hydrated its inputs differently from the orchestrator.

The new one has five verbs and one mental model:

```
ingest add      <ids | --query | --file>   register articles
ingest run      [--stage …] [--select …]   advance them
ingest status   [--stage …]                what state is everything in
ingest show     <id>                       everything known about one article
ingest migrate  <old-cache-root>           import pre-refactor caches
```

### `add` — the interface for new studies

```bash
ingest add 37961286 PMC10634720 10.1016/j.neuroimage.2023.120    # ids, any kind
ingest add --file pmids.txt                                      # one per line
ingest add --manifest staging/manifests/fmri-pet-2010.jsonl      # existing JSONL
ingest add --query '(fmri OR PET) AND ...' --start-year 2010     # PubMed search
```

`add` resolves and registers; it downloads nothing. It prints what was new and
what was already known, so adding a manifest twice is visibly a no-op.

### `run` — advance the catalog

```bash
ingest run                                   # every stage, everything pending
ingest run --stage download --stage extract  # just these
ingest run --select new                      # only never-attempted articles
ingest run --select failed                   # retry transient failures
ingest run --select stale                    # fingerprint changed
ingest run --manifest x.jsonl                # restrict to a set
ingest run --refresh create_analyses         # ignore freshness for one stage
ingest run --dry-run                         # print the plan, touch nothing
```

Default `--select pending` means *new + stale + retryable-failed*: "do the work
that needs doing". This is the piecemeal interface — any stage, any subset, any
time, with the cache deciding what is actually recomputed.

`--dry-run` prints the plan instead of executing it, so the answer to "what will
this run do?" never requires reading the code:

```
$ ingest run --manifest vbm-1995.jsonl --dry-run
selection: 4,812 articles from vbm-1995.jsonl

  download          1,204 pending      3,608 fresh
  extract             887 pending      3,201 fresh      724 blocked (no download)
  create_analyses     887 pending      2,918 fresh    1,007 skipped (no tables)
  upload            1,113 pending      3,699 fresh
  sync              1,113 pending      3,699 fresh
```

### `status` and `show` — answering questions without a REPL

```
$ ingest status
catalog: 463,584 articles

stage             ok      failed   permanent   pending
download     463,584       2,110      18,442         0
extract      226,523       1,004       5,118   231,053
…

$ ingest show PMC10634720
article a7Kq2mNp
  aliases   pmcid PMC10634720 · pmid 37961286 · doi 10.1101/2023.10.21.563317
  download  pubget    ok    2026-04-29  blob 3f2a…  2 files
  extract   pubget    ok    2026-04-29  blob 91bc…  6 tables, 4 with coordinates
  analyses  —         ok    2026-04-29  blob c40d…  4 collections, 61 coordinates
  upload    —         ok    2026-04-29  base_study 5Qk2mNpXy
```

## Configuration

`Settings` loses the dead fields (`neurostore_*`, `verbose` which nothing read)
and the five cache flags, and gains nothing. Logging gains a real level so that
an 8.1 GB `pipeline.log` of `filelock` debug output cannot happen again.

## What is deliberately not changed

- The four extractors, and their differing storage layouts. Normalising them is
  a separate change with its own migration risk; the catalog records wherever
  the bytes already are.
- The `ns-pond` output layout, `stage1/analyses.json` and `pmids.tsv`. These are
  a contract with pondie.
- The upload SQL and its Neurostore schema assumptions.
