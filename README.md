# Ingestion Workflow

Finds neuroimaging papers, downloads them from whichever source will serve them,
pulls the coordinate tables out, turns those into analyses, and writes the
result to Neurostore and to `ns-pond`.

## The model

The pipeline keeps a **catalog of articles**. Each article carries one
**artifact per stage**, and an artifact is reused when its **fingerprint** still
matches — otherwise it is recomputed. That is the whole idea; the rest is detail.

```
             ┌──────────┐
 ingest add →│ catalog  │← ingest migrate
             └────┬─────┘
                  │  ingest run
                  │       tables
   download ─┬→ extract ───┐              ┌→ triage → analyses ─┐
      │      │             ├→ metadata ───┤                     ├→ [resolve] → space → upload → sync
      │      └→ [passages]─┘              └→ [prose] ───────────┘                                │
      │             prose                                                                    ns-pond/
   pubget, elsevier, ace, pdf: tried in order until one succeeds                             + Neurostore
```

Each track is an extraction and then a model call: `extract` keeps an
article's tables and `analyses` reads them; `passages` finds the coordinates
written in its Methods and Results and `prose` reads them. `metadata` sits
between, after both extractions and before both models. The bracketed stages
run only when `prose_model` is set; `resolve` merges what the two tracks found.

## Getting started

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[test]'

# Credentials, via .env or a YAML config file
cp .env.example .env
```

```bash
ingest add 37961286 PMC10634720 10.1016/j.neuroimage.2023.120   # ids of any kind
ingest add --query '(fmri OR PET) AND 2010:2025[dp]'            # or a PubMed search
ingest add --from-neurostore                                    # or work Neurostore is missing
ingest add --pdfs Originals/                                    # or PDFs you already have
ingest run --dry-run                                            # what would happen
ingest run                                                      # make it happen
ingest status                                                   # where everything stands
ingest show PMC10634720                                         # one article in detail
```

## Running it piecemeal

Every stage can run on its own, over any subset, at any time. The cache decides
what actually gets recomputed.

```bash
ingest run --stage download --stage extract        # just these stages
ingest run --manifest staging/manifests/vbm.jsonl  # just these articles
ingest run --select new                            # only never-attempted ones
ingest run --select failed                         # retry transient failures
ingest run --limit 50                              # a small bite
ingest run --refresh analyses                      # recompute despite the cache
ingest run --dry-run                               # print the plan, change nothing
```

`--dry-run` answers "what will this do?" without reading any code:

```
$ ingest run --manifest vbm-1995.jsonl --dry-run
selection: 4,812 articles from vbm-1995.jsonl

  plan
    download          1,204 pending      3,608 fresh
    extract             887 pending      3,201 fresh      724 blocked
    metadata            887 pending      3,925 fresh
    analyses            887 pending      2,918 fresh    1,007 skipped
    upload            1,113 pending      3,699 fresh
    sync              1,113 pending      3,699 fresh
```

`pending` is work. `fresh` is cached and still valid. `blocked` is waiting on an
upstream stage. `skipped` means the stage has nothing to do for that article
(no coordinate tables, or a source that cannot address it). `permanent` means it
will never succeed and is no longer retried.

## When an article turns out to have no text

What `prose` and the stages after it made came from an article's text. If every
source's extractor has since run on the article and found no text (an OK
extraction recording `has_text: false`), that work is no longer the article's,
so it is taken back: `passages`, `prose`, `resolve` and `space` record their
artifacts failed with `no extraction text`, whatever state they were in,
`upload` retracts the article's study version from neurostore (annotated
analyses stay), and `sync` moves its corpus directory to
`<ns_pond_root>-retracted/<base_study_id>-<time>/`. The run's summary counts
these per stage as `taken back`.

Nothing else takes work back. A failed extraction (an exception, a timeout,
downloaded files missing on disk, the extractor returning nothing), a text file
or payload blob that cannot be found, or any one source failing beside one that
found no text only blocks the article until it is fixed. A relative text path
recorded in the catalog is read from the directory holding the catalog, never
from the directory a run was started in.

A study any studyset holds is not retracted: upload records it failed with
`held for review: in N studysets`, keeps its `base_study_id`, leaves it in the
corpus, and counts it as `held for review`; the run's log lists their base
study ids, and the catalog keeps them (`stage = 'upload' AND error LIKE 'held for review%'`).
They are looked at again on every run, and retracted once no studyset holds them.
A retraction that fails (the tunnel drops, say) also keeps `base_study_id` and is
tried again on the next run; sync leaves the article in the corpus until
neurostore has confirmed it.

To undo one: once the article has text again, the next run makes everything
again from it and uploads a new study version; a moved corpus directory can be
moved back from `-retracted/`.

## Long runs

Runs are resumable by construction: everything a stage produces, including its
failures, is recorded before the next batch starts. Killing a run and restarting
it picks up where it left off.

```bash
nohup ingest run -c my_config.yaml -m staging/manifests/tbss-2006.jsonl \
      -s download -s extract &
```

## Choosing a deployment

```yaml
neurostore_env: staging     # or dev, or production
```

One line moves the ssh host, ssh user, container name, docker network and
forward port together, because Docker names containers per compose project and
those cannot be derived from the hostname. Every connection logs which
deployment it reached. `production` is inferred from the compose file and not
yet verified against the live host — see [Design](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design).

## Configuration

Precedence is CLI flags > YAML > environment > environment profile > defaults. See
[`configs/settings_reference.yaml`](configs/settings_reference.yaml) for every
option, and the [Design](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design) page for why the ones that govern caching
look the way they do.

## Documentation

Lives in the [wiki](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki).

**The system as it stands** — each change edits these:

| | |
|---|---|
| [Design](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design) | identity, fingerprints, stages, the command line, choosing a deployment |
| [Data safety](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Data-Safety) | how migration avoids losing the corpus |
| [Migration](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Migration) | command-by-command upgrade guide |

**[Changes](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Changes)** — records of changes worth explaining, frozen once merged:

| | |
|---|---|
| [#11 Catalog refactor](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/PR-11-Catalog-Refactor) | why the design is what it is, and the questions that shaped it |
| [What it replaced](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/PR-11-Before) | the pre-catalog pipeline, measured against the live corpus |
| [Benchmarks](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/PR-11-Benchmarks) | before/after numbers |
| [Profiling](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/PR-11-Profiling) | growth curves, where the time goes, and what did not work |

## Development

```bash
pytest ingestion_workflow/tests -q
ruff check ingestion_workflow
```

The pinned git dependencies (`pubget`, `ACE`, `elsevier-coordinate-extraction`,
`pyarty`) track branches, so they drift. If imports fail after a fresh install,
reinstall them with `--force-reinstall` before looking for a bug.
