# Searching for articles

`ingest add --search` runs one query against several literature services,
registers what they find in the catalog, fills in missing identifiers, and
reports how the services overlap.

```bash
ingest add --search 'fMRI OR "functional MRI" OR "functional magnetic resonance"' --year 2026
ingest add --search 'title: fMRI AND memory NOT rats' --year 2025:2026 --provider pubmed --provider openalex
```

| Option | Meaning |
|---|---|
| `--search QUERY` | the query, in the syntax below |
| `--year 2026`, `--year 2025:2026` | publication year, or an inclusive range (see below for what each provider dates by) |
| `--provider NAME` | repeatable: `pubmed`, `semantic_scholar`, `openalex`, `sciencedirect`. Default: all four |

## Query syntax

- **Terms**: `fMRI`, `resting-state`. Several words that belong together are a
  **phrase**: `"functional MRI"`.
- **Operators**, upper case: `AND`, `OR`, `NOT`. `a NOT b` means a and not b;
  `NOT` cannot begin a query or a group. `AND` binds tighter than `OR`, so
  `a OR b AND c` is `a OR (b AND c)`. Two terms side by side without an
  operator are refused: join them with `AND` or `OR`, or quote them as a phrase.
- **Parentheses** group: `fMRI AND (memory OR attention)`.
- **Field**: by default the query matches title and abstract. Begin it with
  `title:` or `abstract:` to match only there: `title: fMRI OR PET`. A field
  applies to the whole query.

The characters `[ ] * ? ~ | + : { } ^ \` are refused, as is a leading `-`:
they are operators in some providers' own syntax.

## What each provider is sent

The same query, `fMRI OR "functional MRI"` for 2026, becomes:

| Provider | Request |
|---|---|
| PubMed | `("fMRI"[tiab] OR "functional MRI"[tiab]) AND "journal article"[pt] AND 2026[dp]` |
| Semantic Scholar (`/graph/v1/paper/search/bulk`) | `query=fMRI \| "functional MRI"`, `year=2026` |
| OpenAlex (`/works`) | `filter=title_and_abstract.search:(fMRI OR "functional MRI"),type:article,publication_year:2026` |
| ScienceDirect (`/content/search/sciencedirect`) | `{"qs": "fMRI OR \"functional MRI\"", "date": "2026"}` |

What is searched:

| Provider | Default | `title:` | `abstract:` | Publication types | `--year` matches |
|---|---|---|---|---|---|
| PubMed | title and abstract `[tiab]` | `[ti]` | `[ab]` | journal articles | `[dp]`: the electronic or the print date |
| Semantic Scholar | title and abstract | refused | refused | all (preprints, conference papers, ...) | its `year` |
| OpenAlex | `title_and_abstract.search` | `title.search` | `abstract.search` | `type:article` | `publication_year` (the earliest date) |
| ScienceDirect | full text `qs` | `title` | refused | all of ScienceDirect | its `date` (the issue year) |

Because each provider dates a work its own way, an article published online
in December and in print in January can fall in different years at different
providers; near the ends of a `--year` range, "found by no other" counts are
inflated by such articles.

- Semantic Scholar's bulk search matches title and abstract together, so a
  `title:` or `abstract:` query is refused for it.
- ScienceDirect has no abstract field. A default query is searched as `qs`, the
  article's **full text** (references excluded), so it finds articles that only
  mention the terms in their methods or discussion. `title:` is searched as
  `title`; `abstract:` is refused. Its queries are limited to 250 characters.
  ScienceDirect's web search allows 8 Boolean connectors per field; the API
  counted the hits of a ninth and accepted ten (checked 2026-10-15), so no
  connector limit is enforced.
- OpenAlex separates filters with commas, so a comma in a term or phrase is
  refused for it.

When a provider cannot express part of a query, the search stops with an error
naming the provider before any provider is searched; nothing is dropped from a
query. Leave that provider out with `--provider`, or rephrase.

Each provider's results are paged to the end:

- PubMed returns at most 10,000 hits per search. A search finding more is run
  one `--year` year at a time, or without `--year` one year at a time from
  1781 (PubMed's earliest) to next year. If fewer hits are dated in that span
  than were found, it stops with an error rather than lose the rest.
- ScienceDirect serves at most 6,000 results of one query. A query finding more
  is run as its top-level `OR` clauses, and a clause still finding more as one
  query per year of `--year`. This plan is made from counts before any provider
  is searched, so a query that cannot be split (one year, no `OR` left, still
  over 6,000) is an error before any work is done. Each sub-query and the
  number it found is printed; the union of the parts, with an article found by
  two parts counted once, is the whole result.

## Credentials

| Provider | Setting |
|---|---|
| PubMed | `pubmed_email` (required), `pubmed_api_key` |
| Semantic Scholar | `semantic_scholar_api_key` (required) |
| OpenAlex | `openalex_email`, `openalex_api_key` |
| ScienceDirect | `elsevier_api_key` (required) |

## The report

After registering the hits and, unless `--no-enrich`, filling in their missing
DOI, PMID and PMCID, `add` prints, counted in catalog articles:

- per provider, the articles it found and how many no other provider found;
- for each pair of providers, how many articles both found;
- how many distinct articles there are, and how many the catalog already held
  (an article is already held when it is, or has been merged with, one the
  catalog held before this run);
- per provider, how many records had no DOI, PMID or PMCID. The catalog keys
  articles on those, so these records are not registered.

A PubMed search returns PMIDs only, and the other providers mostly DOIs, so the
overlap is counted after enrichment, when an article's identifiers have met.
