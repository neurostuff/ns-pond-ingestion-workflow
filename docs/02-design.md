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

### Two ids, and they are not the same

| | assigned | shape | exists when |
|---|---|---|---|
| `article_id` | by the catalog, at registration | 12 lowercase base32, `lsfb2vznhrfz` | always |
| `base_study_id` | by **Neurostore**, during upload | mixed-case shortuuid-12, `5Qk2mNpXyJKH` | only after a successful upload |

`article_id` has to exist before anything is downloaded, because download,
extract, metadata and analyses all key on it. `base_study_id` cannot exist until
upload has run — where it is either created or *discovered* by matching an
existing base study on DOI or PMID — and most articles never get one. So they
are separate, and the catalog records the mapping: on a successful upload the
`base_study_id` is written as a `neurostore` alias, which makes
`ingest show <base_study_id>` work.

The two shapes are deliberately different so they cannot be confused where both
appear.

#### What goes into an `article_id`

One string: `"<kind>:<value>"`, from the strongest identifier the article had
when it was **first registered** — pmcid, else pmid, else doi, else neurostore.
Nothing else contributes.

| identifiers held at first registration | seed | id |
|---|---|---|
| pmcid, pmid, doi | `pmcid:PMC10634720` | `lsfb2vznhrfz` |
| pmid, doi | `pmid:37961286` | `ckw3v7eecmlk` |
| doi only | `doi:10.1016/j.neuroimage.2023.120` | `6rgtrdiydfza` |

**The id never changes once assigned.** A doi-seeded article that later learns
its pmcid keeps `6rgtrdiydfza` and gains an alias; its artifacts are untouched.
That is the invariant the rest of the catalog rests on.

The consequence is that the id is reproducible for a given first-observation,
not canonical: a catalog that happened to see the pmcid first would have named
the same article `lsfb2vznhrfz`. Two catalogs agree only when they saw the same
identifier first — true for a repeated migration over the same caches, which is
what the reproducibility is for, and false in general. Nothing depends on it
being canonical: `article_id` never leaves the catalog, `ns-pond` paths use the
Neurostore `base_study_id`, and a random id passes every test here except
cross-catalog agreement (`test_a_random_id_would_also_be_correct`).

When a merge does happen, the **older** article keeps its id, since it has had
longer to accumulate artifacts and is therefore the smaller rewrite.

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

#### Discovering work from Neurostore

The pipeline is otherwise one-way: `sync` writes `ns-pond`, and nothing reads
Neurostore looking for articles. `--from-neurostore` is the inbound path. It
reads the database the upload stage writes to, so the two cannot drift, and
takes group-level base studies that **are in a studyset** and have **no
`source='llm'` study**:

- *in a studyset*, because that is what marks a base study as one someone wants;
- *no llm study*, because that is exactly what this pipeline would add;
- *group level*, because meta-analyses are not papers to extract coordinates from.

It registers the `base_study_id` alongside the DOI, PMID and PMCID from the same
row. That matters: an article known only by its `base_study_id` is inert — no
download source can address it — so the bibliographic ids are what make it
actionable. Base studies with none of the three are skipped and counted.

Run against the staging database, the filter picks out:

```
base_studies, all levels          :    42,930
level = 'group'                   :    42,751
... and in a studyset             :    41,999
... and already has an llm study  :    27,126
... and has NO llm study  <-- work:    14,873
    of those, no doi/pmid/pmcid   :       218  (skipped)
    actionable                    :    14,655
```

The 27,126 with an llm study line up with the 27,411 rows in the legacy upload
cache, which is a useful check that the filter means what it says.

Discovery composes with migration rather than duplicating it. Migrating the
legacy caches first and then discovering 295 base studies:

```
$ ingest migrate <legacy>   ->  catalog articles: 0 -> 96,418
$ ingest add --from-neurostore --limit 300
  neurostore: 295 base studies with no llm study yet
  295 identifiers resolved to 65 new articles (230 already known)
```

230 of the 295 were already in the catalog from migrated downloads, so they
gained a `neurostore` alias on their existing article instead of becoming
duplicates, and they plan as `fresh` at download — the work already done is not
redone.

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

$ ingest show PMC10634720          # or 5Qk2mNpXyJKH, once uploaded
article lsfb2vznhrfz
  aliases   pmcid PMC10634720 · pmid 37961286 · doi 10.1101/2023.10.21.563317
            · neurostore 5Qk2mNpXyJKH
  download  pubget    ok    2026-04-29  blob 3f2a…  2 files
  extract   pubget    ok    2026-04-29  blob 91bc…  6 tables, 4 with coordinates
  analyses  —         ok    2026-04-29  blob c40d…  4 collections, 61 coordinates
  upload    —         ok    2026-04-29  base_study 5Qk2mNpXyJKH
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
