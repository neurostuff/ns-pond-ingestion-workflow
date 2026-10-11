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
3.1M artifacts) it takes about a minute and a half and writes a 1.4 MB file.
The command also prints the flow below as text.

To click through to each article's full trace, serve the page instead (it
binds to 127.0.0.1; from another machine, forward the port with `ssh -L`):

```bash
ingest report serve -c configs/beast.yaml --port 8765
ingest report trace 222lv3326a2c -c configs/beast.yaml   # one article, as JSON
```

## The flow

The main view runs top to bottom, one box per stage on the path to upload.
**In** is every article whose input passed. Each of them then passed on, was
left out, or is waiting; the box beside lists what was left out and what waits,
by state and reason, with what each means in plain words. Every box shows the
check `in = passed + left out + waiting`, and that its *in* equals what the
stage above passed. The numbers come from one count, which the CLI prints and
the page embeds; the page recounts only to apply its filters, and with no filter
it says whether its numbers match. A Sankey view of the same counts is one
click away.

Click a box or a reason to list its articles, with a search box, and the
`funnel-ids` command that lists the same set in full. Click an article for its
trace: on a file page, its state at every stage; when served, also every
record's path on disk, status, reason, summary, when it was written, its
fingerprint, its attempts, the records it was built from (and whether they were
rewritten after it), the stage's code version constants, and, with the
pipeline's config, whether each record is fresh, as that stage's own plan judges
it. The trace reads catalog rows only, never a payload.

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
| refused | its input passed, but the stage will not read it: the text came from a downloaded page that is not the article |
| blocked | its input did not pass; the reason names the stage where it dropped out and why. A failed or empty record left from an earlier run counts here, with its own status and reason added (`leftover skipped: ...`) |
| pending | its input passed and nothing is recorded here yet; `nothing to read (passages = 0)` marks an input with nothing in it, and `tables wait behind it` an article whose tables reach resolve only after this step |
| not_attempted | its input passed and the stage has never run on this catalog |
| stale | an ok artifact exists although its input did not pass: made from an input since lost, or under an older stage graph |
| unaccounted | an attempt is recorded with no outcome, or an unknown status; treat as a bug |

Error text is grouped into a few categories (not open access, HTTP 403,
source returned no article, code error, ...); the raw text is kept for the
drill-down. Unrecognised text shows as `other: ...` with numbers and URLs
masked. A download that served something other than the article is
`bad page: paywall`, `error_page`, `empty_shell` or `citation_stub`, at download
and at the extraction made from it.

The page reconciles when every stage accounts for every article, and when the
number of articles placed from a record at each stage equals the number the
catalog holds records for.

## Using the page

- **Filters.** These select articles, so the funnel and the totals still
  reconcile. *Downloaded from* keeps articles with an ok download from a ticked
  source; an article whose every download failed has source `none`, so this
  filter hides download failures. *Download tried at* keeps articles with any
  download recorded from a ticked source, failed or not: use it to see losses
  at download by source. *Judged text source* is the source whose text triage
  (or else passages) read, and *Registered* the day the article entered the
  catalog. The state and reason filters narrow the tables and the drill-down.
- **Funnel.** The stages on the path to upload, with what passed on and what
  stayed behind at each one, split by state and reason.
- **Every stage.** Includes side stages (metadata, triage, analyses, notices,
  references). Click a number or a reason to list its articles.
- **Drill-down.** Up to `--samples` article ids per bucket (default 20), with a
  copy button and the command that prints every id in the bucket, carrying the
  page's filters: `--source`, `--tried`, `--judged` and `--state` repeat for any
  of several; `--exact-reason` is a clicked reason and `--reason` the reason
  box's substring; `--from`/`--to` the registration days; `--prose/--no-prose`
  the page's stage graph. `--by-source` lists one source's own download or
  extraction rows, as the source table counts them:

```bash
ingest report funnel-ids --catalog /path/to/catalog --stage upload --state blocked \
    --reason "open access" --source elsevier
ingest report funnel-ids --catalog /path/to/catalog --stage download \
    --by-source elsevier --state failed --tried elsevier
```
