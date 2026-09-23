# Reading the LLM analyses, and the context they were parsed from

The `analyses` stage turns a coordinate table into structured analyses by sending
that table to an LLM. This describes where the results live, how to read them,
what the model was shown, and how that differs between sources.

## The short version

- Analyses live in the catalog as artifacts with `stage='analyses'`; the payload
  is in the blob store, keyed by `artifacts.blob`.
- **The prompt is not stored.** Only its version is, inside the fingerprint. The
  context has to be reconstructed from the extract artifact and the files on disk.
- The prompt template is identical for every source. **The raw table content
  inside it is not** — it is whatever bytes the extractor wrote, so pubget sends
  JATS XML, elsevier sends Elsevier XML, ACE sends HTML and pdf sends CSV.

## Where the analyses are

```
/data/alejandro/projects/ns-pond/catalog/catalog.sqlite   # rows
/data/alejandro/projects/ns-pond/catalog/blobs/           # payloads, gzipped JSON
```

One row per article, `source` empty (the stage does not fan out over sources):

```sql
SELECT article_id, status, summary, blob
FROM artifacts
WHERE stage = 'analyses';
```

`summary` is `{"tables": N, "coordinates": M}` — enough to filter on without
opening a blob. The payload is `{table_id: AnalysisCollection}`:

```jsonc
{
  "4": {                                  // table id as the extractor named it
    "slug": "<article-slug>::<table>",
    "coordinate_space": "MNI",
    "identifier": {...},
    "analyses": [
      {
        "name": "Faces > Houses",         // copied verbatim from the table
        "description": null,
        "table_id": "4", "table_number": 4,
        "table_caption": "...", "table_footer": "...",
        "contrasts": [], "images": [],
        "metadata": {"sanitized_table_id": "4", "table_metadata": {...}},
        "coordinates": [
          {"x": 46.0, "y": -4.0, "z": 8.0,
           "space": "MNI", "statistic_type": "Z", "statistic_value": 4.5,
           "cluster_size": null, "cluster_measure": null,
           "is_subpeak": false, "is_deactivation": false, "is_seed": false}
        ]
      }
    ]
  }
}
```

Reading one:

```python
import gzip, json, sqlite3
from pathlib import Path

BLOBS = Path("/data/alejandro/projects/ns-pond/catalog/blobs")
con = sqlite3.connect("file:/data/alejandro/projects/ns-pond/catalog/catalog.sqlite?mode=ro", uri=True)

row = con.execute(
    "SELECT blob FROM artifacts WHERE stage='analyses' AND article_id=?", (article_id,)
).fetchone()
digest = row[0]
payload = json.loads(gzip.decompress((BLOBS / digest[:2] / f"{digest}.json.gz").read_bytes()))
```

Always open the catalog read-only (`mode=ro`). Writers are running most of the
time and the file is the only copy.

## Not every analyses artifact came from an LLM

Of the artifacts present before the September 2026 run, **26,148 are migrated** —
imported from the pre-refactor `ns-pond` tree, where some other tool produced
them. They are marked by `summary` containing `migrated`:

```sql
SELECT summary LIKE '%migrated%' AS migrated, COUNT(*) FROM artifacts
WHERE stage='analyses' GROUP BY 1;
```

Migrated rows carry the same payload shape but were never sent to this pipeline's
LLM, and many have `"coordinates": []` with the real content in
`metadata.table_metadata`. **Do not treat them as LLM output**, and do not use
them to measure the model. Only rows without the `migrated` marker were parsed by
the prompt described below.

## What the model is shown

`CreateAnalysesService._build_prompt` sends one message per table: a long fixed
instruction block, then this context tail.

| field | source |
|---|---|
| `Article Title` | `article_metadata.title` — from the `metadata` stage |
| `Article Abstract` | `article_metadata.abstract`, `""` when absent |
| `Table ID` | the extractor's table id |
| `Table Number` | `table.table_number` |
| `Table Caption` | `table.caption` |
| `Table Footer` | `table.footer` |
| `Table Metadata` | `json.dumps(table.metadata, indent=2, sort_keys=True)` |
| `Raw Table Content` | `table.raw_content_path` read verbatim as UTF-8 |

Two things are worth noting about that list. The **full text is never sent** —
only the abstract, the caption and the footer, so anything the methods section
says about a coordinate space is invisible to the model. And `Table Metadata` is
dumped raw, which means it carries absolute filesystem paths into the prompt.

### Title and abstract are often absent

Title and abstract come from the `metadata` stage, which has run for far fewer
articles than `extract`. When there is no metadata artifact the prompt still goes
out, carrying the literal string `Article Title: None` and an empty abstract.

Measured over the first 505 articles of the September 2026 run, **55 (11%) had no
metadata artifact**. Across the whole catalog the gap is much wider: 203,189
articles have metadata against 672,119 registered. Running the `metadata` stage
over the analyses backlog before parsing is the cheapest available improvement to
the context, and it is worth doing before drawing conclusions about how well a
model performs here.

A table is only sent if it already has coordinates:

```python
if not table.contains_coordinates and not table.coordinates:
    continue
```

So the LLM's job is not finding coordinates — the extractor already did that. It
is grouping them into named analyses and attaching statistics.

## The context does differ by source

The template is identical. `Raw Table Content` is not: `_read_table_content`
reads `table.raw_content_path` byte for byte, and each extractor writes a
different format.

| source | file | format | median size |
|---|---|---|---|
| pubget | `<id>.xml` | JATS `<original-table>` | ~9 KB |
| elsevier | `<id>.xml` | Elsevier `ce:table` | ~14 KB |
| ace | `<id>.html` | HTML as scraped | ~6 KB |
| pdf | `<id>.csv` | CSV rendered from the parsed frame | ~0.8 KB |

Sampled 400 extracts per source in September 2026: every `raw_content_path`
existed, none were placeholders.

This matters when comparing sources. An elsevier table arrives as markup with
`rowspan`/`morerows` intact, so the prompt's instructions about spans apply. A
pdf table arrives as CSV, where that structure is already flattened and lost —
and the prompt opens by saying it parses "raw HTML/XML", which never describes
the pdf case. **Differences in analysis quality between sources are partly
differences in what the model was handed**, not only in the papers.

Two failure modes follow from reading a path rather than stored bytes. ACE writes
`<!-- ACE did not retain raw table HTML -->` when it has no HTML, which sends a
comment and no table. And if a file is deleted or rewritten after extraction, the
reconstruction below silently stops matching what the model saw.

## Reconstructing the context for an analysis

Join `analyses` to `extract` on `article_id`, pick the extract the stage would
have picked, then find the table by id.

```python
import gzip, json, sqlite3
from pathlib import Path

BLOBS = Path("/data/alejandro/projects/ns-pond/catalog/blobs")
CAT = "file:/data/alejandro/projects/ns-pond/catalog/catalog.sqlite?mode=ro"


def blob(digest):
    return json.loads(gzip.decompress((BLOBS / digest[:2] / f"{digest}.json.gz").read_bytes()))


def context_for(article_id):
    """The context the analyses stage would send, per table."""
    con = sqlite3.connect(CAT, uri=True)
    con.row_factory = sqlite3.Row

    # the stage takes the extract that found the most coordinate tables
    best = max(
        con.execute(
            "SELECT source, blob, summary FROM artifacts"
            " WHERE stage='extract' AND status='ok' AND blob IS NOT NULL AND article_id=?",
            (article_id,),
        ).fetchall(),
        key=lambda r: json.loads(r["summary"] or "{}").get("tables_with_coordinates", 0),
        default=None,
    )
    if best is None:
        return {}

    meta_row = con.execute(
        "SELECT blob FROM artifacts WHERE stage='metadata' AND article_id=?", (article_id,)
    ).fetchone()
    meta = blob(meta_row["blob"]) if meta_row and meta_row["blob"] else {}
    con.close()

    out = {}
    for table in blob(best["blob"]).get("tables") or []:
        if not (table.get("contains_coordinates") or table.get("coordinates")):
            continue  # the service skips these, so no prompt was ever built
        path = Path(table.get("raw_content_path") or "")
        out[table.get("table_id")] = {
            "source": best["source"],
            "article_title": meta.get("title"),
            "article_abstract": meta.get("abstract") or "",
            "table_number": table.get("table_number"),
            "table_caption": table.get("caption"),
            "table_footer": table.get("footer"),
            "table_metadata": table.get("metadata"),
            "raw_table_content": path.read_text("utf-8", errors="ignore") if path.exists() else None,
        }
    return out
```

`raw_table_content` of `None` means the file is gone and the context cannot be
recovered — the analyses payload is still valid, but it is no longer auditable.

To rebuild the exact prompt string rather than its parts, call the real builder,
which guarantees it stays correct as the prompt changes:

```python
from ingestion_workflow.services.create_analyses import CreateAnalysesService
prompt = CreateAnalysesService(settings)._build_prompt(bundle, table, table_text, table_key)
```

## Gaps

**Neither the prompt nor the model's reply is persisted.** `_run_one` stores only
the parsed collections; the fingerprint records `COORDINATE_PARSING_PROMPT_VERSION`
and `llm_model`, so you can tell *which* prompt and model ran but not what was
sent or returned. Reconstruction is therefore a faithful rebuild, not a record.
Anything needing a true audit trail — which table text produced a wrong
coordinate, whether a model regressed between versions — needs the stage to store
the prompt and the raw response alongside the payload.

The gateway records some of this independently: requests carry
`x-portkey-metadata` and responses an `x-portkey-trace-id`, so the provider-side
log is currently the only place a specific call can be recovered.
