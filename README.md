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

ACE's HTML cleaning (readabilipy) runs `node` from PATH and needs node >= 20.19.
If the node on PATH is older, or a shell profile is not sourced (nohup, ssh),
set `node_path` in the settings file or `INGEST_NODE_PATH` to the directory
holding a newer node. With ACE enabled, `ingest run` stops at the start and
names the node version it found when that node is too old.

When the text readability keeps holds under half of the running prose of a page
with two or more body section headings, ACE extraction builds it again two ways,
without readability (each paragraph once, without the page's menus) and without
its table placeholders, and keeps the way with the most prose. Such an extraction
says which way and why in `summary.text_from` (shown by `ingest show`) and in its
payload's `text_source`. An extraction made before keeps its text until
`ingest refresh-text --source ace` rebuilds it; that sets both, and the passages
stage reads a text again when it gained a coordinate.

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
so it is taken back: `passages`, `prose`, `resolve`, `roles` and `space` record
their artifacts failed with `no extraction text`, whatever state they were in
(a stage that never made one, such as `roles` before its model was configured,
records the failure too, so the stages after it see the take-back), `sync`
writes no parse and moves its corpus directory to
`<ns_pond_root>-retracted/<base_study_id>-<time>/`, and `upload` records the
reason once, with the base study and what the last upload sent, and sends
nothing again. Nothing is deleted from neurostore: it keeps the last upload. The
run's summary counts these per stage as `taken back`.

The same happens when the prose is read again and holds nothing, and the tables
are known to hold nothing too (`no analyses after re-read`). A table or prose
reading that failed, was cut short or has not run only holds the upload back
(`held back: upstream not fresh: ...`), with how many analyses the parse holds
against the last upload.

`roles` reads the prose sets' passages from the current `passages` artifact, so
it waits on any article whose prose was read from an earlier one: after
`passages` re-runs, run `prose` before `roles`.

Nothing else takes work back. A failed extraction (an exception, a timeout,
downloaded files missing on disk, the extractor returning nothing), a text file
or payload blob that cannot be found, or any one source failing beside one that
found no text only blocks the article until it is fixed. A relative text path
recorded in the catalog is read from the directory holding the catalog, never
from the directory a run was started in.

## When a downloaded page is not the article

A downloaded HTML page that is an empty shell, a paywall notice, an error page,
a citation stub (a PubMed record, a page with almost no prose) or only an
abstract (an abstract and its references: fewer than two body section headings,
fewer than 20 sentences of running prose, and under 400 words of body prose beside
the abstract, in any script) is a failed download: its
reason is in the artifact's `summary.bad_page` (`empty_shell`, `paywall`,
`error_page`, `citation_stub`, `abstract_only`), and the next source is tried.
A letter or a commentary without section headings is not abstract-only.
ACE moves such a page out of its cache to `<ace cache>-quarantine/`,
under the same relative path, and records both paths in `summary.moved_aside`;
nothing is deleted. The extraction made from that source's earlier download
fails with `download failed validation: <reason>`, triage and passages stop
reading its text, and prose stops reading passages made from it. An article no
source can serve stays failed and blocked; its text is not taken back.

`ingest revalidate-downloads` runs the same check on downloads already stored,
without fetching. It is a dry run by default and prints the bad pages by reason
and source and what will happen next to each article. `--apply --backup <path>`
copies the catalog database to a new `<path>` first, then records the failures;
stop other runs first. Running it again records nothing new. Then
`ingest run --stage download` plans each article's next source.

A retracted paper is marked, not removed: sync writes the notices stage's
corrections into the ParsedPaper's `bibliography.corrections`, upload always
sends the ParsedPaper with the parse, and neurostore sets the base study's
`is_retracted` from them. A paper that is itself a retraction notice is not
synced (`retraction notice`).

To undo a take-back: once the article has text again, the next run makes
everything again from it and uploads the new parse; a moved corpus directory can
be moved back from `-retracted/`.

## Wiley's PDFs

The `wiley` download source fetches an article's PDF from Wiley's Text and Data
Mining API (`https://api.wiley.com/onlinelibrary/tdm/v1/articles/<DOI>`). It is
last in the default `download_sources`, so Wiley is asked only for articles no
other source serves. It asks only about articles whose DOI has one of the
prefixes Crossref lists for Wiley (member 311), and it needs a TDM token from
Wiley in `WILEY_API_KEY` (or `wiley_api_key`). With `wiley` listed and no token,
the download stage stops for the rest of the run, with that reason, when it
first comes to ask Wiley: nothing is asked and no article is charged a try. A
401 from Wiley (the token refused) stops the stage the same way.

Its PDFs are kept as `<cache_root>/wiley/<article>/article.pdf`, the pdf
source's layout in a root of its own. Every downloaded PDF, whichever source
fetched it, is read the same way into the article's one `pdf` text
(`extractions/pdf/`, and `processed/pdf/` in the corpus). An article with both a
Wiley PDF and an open-access one gets its text from Wiley's, the version of
record. The extraction's catalog summary names the download it read
(`download_source: wiley`) when that is not the pdf source's.

Wiley's TDM terms allow 60 requests per 10 minutes, so the source asks one
article at a time, at most once every 10 seconds, across every batch and
extractor in the process. A 429 is waited out for as long as its Retry-After
asks, up to 60 seconds; a longer wait, or a fourth 429 in a row, leaves Wiley
for the rest of the run. A 404 is permanent for that article, and never counts
toward leaving Wiley for the run. A 403 (Wiley denying this address the
article) is retried on the usual schedule; twenty of them with no PDF served
leave Wiley for the run, as for any PDF host. An answer that is not a PDF fails
the article and is not kept.

## Landing pages

When OpenAlex lists an open-access copy by its landing page alone, with no PDF
link, the `pdf` source fetches the page once and follows the PDF it names in
`<meta name="citation_pdf_url">`, the tag publishers and repositories put there
for Google Scholar. Both requests go through the same host limits as any PDF:
a host that refuses is left for the run, and a 429 is waited out or deferred.
The attempt records the page as its `url` and the PDF as its `pdf_url`. A page
that names no PDF fails with `no_pdf_link`; a PDF on PubMed Central or Europe
PMC is left to those sources, as is a PMC article page listed as a landing page.

## Springer Nature's open-access JATS

The `springer` download source fetches an article's full-text JATS from
Springer Nature's Open Access API (`https://api.springernature.com/openaccess/jats`).
It serves only the articles Springer Nature publishes open access under a
licence (gold, and hybrid with one); an article that is free to read without a
licence, or open only through a repository copy, is not there. It comes after
`elsevier` in the default `download_sources`, so an article PMC or Europe PMC
already serves never spends the API's quota. It asks only about articles whose
DOI has one of the prefixes Crossref lists for Springer Nature (member 297),
and it needs an Open Access API key in `SPRINGER_API_KEY` (or
`springer_api_key`). With `springer` listed and no key, or a key the API
refuses (401), the download stage stops for the rest of the run, as for Wiley.

The JATS is kept as `<cache_root>/springer/<article>/article.xml`, with its
tables beside it as pubget lays out a PMC article, and is read by pubget's
code into the article's `springer` text.

A key may make 100 requests a minute and 500 a day. The source names 20 DOIs
in each request (the most a query takes), reads every page of the answer, and
asks at most once every 0.6 seconds. A DOI is asked with its look-alike dashes
made hyphens and without a backslash, and matched to the article returned
without regard to case. A 429 is waited out for as long as its Retry-After
asks, up to 60 seconds. A longer wait, a fourth 429 in a row, or a 403 is taken
as the day's quota spent: the source is left for the rest of the run, the
articles it had not answered stay pending for it, uncharged, while later
sources are tried, and the run report says `springer EXHAUSTED` with the API's
reason. A 5xx or a dropped connection is retried twice; then that request's
articles fail, to be retried on the usual schedule. An article the API does not
return (or a 404 saying it found no data) is not open access there, and is
permanent for this source; one returned without its body is retried. A query
the API refuses (400) is halved until the DOI it cannot name is alone; that one
is permanent. If every DOI of the query is refused even alone, the query is at
fault rather than the DOIs: the source is left for the run as for a spent
quota, and none of them is charged.

## Dead PDF URLs

The `pdf` source remembers, per article, each PDF URL that has turned out dead.
A URL is dead when the host answers it with a 4xx other than 408, 425 and 429
(a 404, 403 or 410, say), or when it answers 200 with something that is not a
PDF (a landing page or a paywall notice). A landing page is dead when it names
no PDF or the PDF it names is dead. A 5xx, a timeout, a connection error
and a 429 are not dead: they may answer differently later. The catalog keeps
only an article's latest PDF attempt, so each new attempt carries the earlier
dead URLs forward as `known_dead`, even if a provider no longer lists them.
Later runs record a dead URL and never fetch it; they fetch only URLs that are
new or that failed for a passing reason.

A `pdf` download that fails is always written `failed` (retried on the usual
schedule), never `permanent`: a provider may list a new copy later, and the dead
URLs are what keep the retries cheap. Other sources still write `permanent` for
errors that cannot resolve, such as a 404.

Records the `pdf` source wrote `permanent` before this are not migrated. Retry
them with `ingest run -s download --refresh download:pdf`. The refresh does not
clear the dead-URL memory: dead URLs stay unfetched, and only the other URLs are
tried again.

## Long runs

Runs are resumable by construction: everything a stage produces, including its
failures, is recorded before the next batch starts. Killing a run and restarting
it picks up where it left off.

```bash
nohup ingest run -c my_config.yaml -m staging/manifests/tbss-2006.jsonl \
      -s download -s extract &
```

## Choosing a deployment

`upload` posts each parse to neurostore's study_schema ingester
(`POST <neurostore_api_url>/api/study-schema-uploads/`) with the admin token from
the environment variable `NEUROSTORE_TOKEN` (only from there: it is not a setting
and is not read from `.env`):

```yaml
neurostore_api_url: https://staging.neurostore.xyz   # or https://neurostore.org
```

Unset, upload sends nothing and every article waits. An article neurostore has
no base study for is uploaded too: the ingester finds or creates its base study
by the doi, pmid or pmcid in the parse header, with the bibliography from the
ParsedPaper. Sync records an article it does not write as skipped, with the
reason: one with none of the three (`no neurostore base study id, and no doi, pmid
or pmcid to create one by`), a retraction notice (`retraction notice`), and two
catalog articles that reach sync under the same base study (`shares base study <id>
with <article>`), since each upload would replace the other's analyses. An article
reaches sync when sync would write it now or has a sync or upload record; one that
only holds neurostore's alias for the base study does not. Of two such articles,
when one was read from a preprint (its download is a submitted version) and the
other was not, the other is written and uploaded; otherwise neither is. A refused
parse (a 4xx
about the paper, such as `KEY_MISMATCH` or `DUPLICATE_KEY`) is that paper's
failure and is not retried. When neurostore does not answer a paper's retries
nor a health check, or refuses the token or the URL, the upload stops for the
rest of the run and its articles stay pending; nothing is recorded for them.
`dry_run: true` validates each parse and writes the request bodies under
`<data_root>/neurostore_dry_run/`, recording nothing in the catalog.

```yaml
neurostore_env: staging     # or dev, or production
```

`ingest add --from-neurostore` reads a deployment's database directly. That one
line moves the ssh host, ssh user, container name, docker network and forward
port together, because Docker names containers per compose project and those
cannot be derived from the hostname. `production` is inferred from the compose
file and not yet verified against the live host — see [Design](https://github.com/neurostuff/ns-pond-ingestion-workflow/wiki/Design).

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
