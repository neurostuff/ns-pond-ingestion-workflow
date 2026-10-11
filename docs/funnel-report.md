# Funnel report

`ingest report funnel` writes one HTML page showing where every article in the
catalog stands at every stage, from download to sync, and why the ones that
stopped did. It only reads the catalog (opened read-only) and never opens a blob.

```bash
ingest report funnel --catalog /path/to/catalog --out funnel.html
# or, with the catalog from a config file
ingest report funnel -c configs/beast.yaml --out funnel.html
```

The page is self-contained: the counts are embedded as JSON and the code is
inline, so it opens offline from disk. On the beast catalog (679k articles,
3.1M artifacts) it takes about a minute and writes a file of a few MB.

## What a state means

Each article has exactly one state at each stage, so every stage row adds up to
the number of live articles; a row that does not is marked with a cross.

| state | meaning |
|---|---|
| passed | ok, and the next stage takes it as input |
| filtered | recorded, but nothing for the next stage (no coordinate tables, triage passed no table, ...) |
| failed / permanent | a failure the scheduler retries / does not retry, with its reason |
| taken_back | every extraction found no text, so what was made from it was withdrawn |
| held | upload kept the study because a studyset holds it |
| blocked | its input did not pass; the reason names the stage where it dropped out and why |
| pending | its input passed and nothing is recorded here yet |
| not_attempted | its input passed and the stage has never run on this catalog |
| stale | an artifact exists although its input did not pass: made from an input since lost, or under an older stage graph |
| unaccounted | an attempt is recorded with no outcome, or an unknown status; treat as a bug |

Error text is grouped into a few categories (not open access, HTTP 403,
source returned no article, code error, ...); the raw text is kept for the
drill-down. Unrecognised text shows as `other: ...` with numbers and URLs
masked.

## Using the page

- **Filters.** Downloaded-from source, the source whose text triage judged, and
  the date the article was registered select articles, so the funnel and the
  totals still reconcile. The state and reason filters narrow the tables and
  the drill-down.
- **Funnel.** The stages on the path to upload, with what passed on and what
  stayed behind at each one, split by state and reason.
- **Every stage.** Includes side stages (metadata, triage, analyses, notices,
  references). Click a number or a reason to list its articles.
- **Drill-down.** Up to `--samples` article ids per bucket (default 20), with a
  copy button and the command that prints every id in the bucket:

```bash
ingest report funnel-ids --catalog /path/to/catalog --stage upload --state blocked \
    --reason "open access" --source elsevier
```
