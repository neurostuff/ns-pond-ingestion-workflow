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
   download → extract → metadata → analyses → upload → sync
      │          │                                        │
   pubget     4 sources tried in order                ns-pond/
   elsevier   until one succeeds                      + Neurostore
   ace
   pdf
```

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

The architecture, the measurements behind it, and the reasoning live in the
[wiki](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki).

| | |
|---|---|
| [Catalog refactor](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Catalog-Refactor) | why the design is what it is, and the questions that shaped it |
| [Current behavior](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Current-Behavior) | what the pipeline did before, measured against the live corpus |
| [Design](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design) | identity, fingerprints, stages, the command line, choosing a deployment |
| [Benchmarks](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Benchmarks) | before/after numbers |
| [Profiling](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Profiling) | growth curves, where the time goes, and what did not work |
| [Data safety](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Data-Safety) | how migration avoids losing the corpus |
| [Migration](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Migration) | command-by-command upgrade guide |

## Development

```bash
pytest ingestion_workflow/tests -q
ruff check ingestion_workflow
```

The pinned git dependencies (`pubget`, `ACE`, `elsevier-coordinate-extraction`,
`pyarty`) track branches, so they drift. If imports fail after a fresh install,
reinstall them with `--force-reinstall` before looking for a bug.
