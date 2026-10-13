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

The main view runs top to bottom, one box per stage that gates articles, in the
order the pipeline runs them (the stage registry's default stages: download,
extract, passages, metadata, notices, triage, analyses, prose, resolve, roles,
space, sync, upload). **In** is every article whose input passed; for triage,
whose metadata was attempted. Each of them then passed, was left out, or is
waiting. The box beside lists what was left out and what waits, by state and
reason, with what each means in plain words. Records made under an older stage
graph sit apart, as "records outside the flow", and are clickable like the rest.
Every box shows the check `in = passed + left out + waiting`, and that its *in*
equals what its input passed (or attempted).

A reason joined from several sources (`HTTP 403 forbidden + not open access +
source returned no article`) is listed under its primary part, with the
combinations underneath; the counts add up. The primary part is the first, in
this order, of: a bad page; our own errors (code, database, parse, converter,
missing files); errors a retry may clear (rate limits, timeouts, connections,
5xx); the source refusing (403, other 4xx, 404, not a PDF); not open access;
the source having no article. `funnel-ids --reason-group` lists a group.

Every number comes from one count in Python (`page_model`): `ingest report
funnel` prints its flow and the page draws it as embedded. Only when an article
filter is set does the page recount in the browser, and a test holds that
recount equal to the Python count and to `funnel-ids` for the same filter. A
Sankey view of the same counts is one click away.

An extraction that records no text hash waits at passages for `ingest
refresh-text` (`waits for ingest refresh-text`); that command is not a stage, so
it shows as this reason rather than as a box.

Click a box, a count or a reason to list its articles, with a search box, and
the `funnel-ids` command that lists the same set in full. Click an article, or
type its id, for its trace: on a file page, its state at every stage; when
served, also every record's path on disk, status, reason, summary, when it was
written, its fingerprint, its attempts, the records it was built from (its
parent and the other inputs its fingerprint reads), whether those were
rewritten after it, the stage's current code version constants (a record's own
version is folded into its fingerprint), and, with the pipeline's config,
whether each record is fresh, as that stage's own plan judges it. The trace
reads catalog rows only, never a payload: where a plan needs one, passages'
freshness is worked out from the rows (the extraction it read and that text's
recorded hash), and any other says it is not known without the payload. A
stage that cannot plan says which setting it needs. A failed record shows its reason in
words (an error no category matches reads "the stage raised an error"); its raw
error, cut to 500 characters with anything token-like removed, sits under a
collapsed "technical detail". In `ingest report trace`'s JSON these are each
record's `reason` and `error_detail`.

## What a state means

Each article has exactly one state at each stage, so every stage row adds up to
the number of live articles; a row that does not is marked with a cross.

| state | meaning |
|---|---|
| passed | ok, and the next stage takes it as input |
| filtered | recorded, but nothing for the next stage (no coordinate tables, triage passed no table, ...) |
| failed / permanent | a failure the scheduler retries / does not retry, with its reason |
| taken_back | what was made from the article's text was withdrawn: every extraction found no text, or the tables and the prose were read again and hold no analysis |
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
`bad page: paywall`, `error_page`, `empty_shell`, `citation_stub` or `abstract_only`, at download
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
