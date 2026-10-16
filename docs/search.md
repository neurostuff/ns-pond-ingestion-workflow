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
| `--year 2026`, `--year 2025:2026` | publication year, or an inclusive range (see below for what each provider dates by). **`--year` defaults to 1990 onward** (BOLD fMRI began in 1990), through the latest year with hits; give `--year 1980:1989` and so on for earlier work. The range used is printed |
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

The default years, 1990 onward, are sent as `1990:3000[dp]` (PubMed's own
"to the present"), `year=1990-`, `publication_year:>1989` and `"date": "1990-9999"`.

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
  The API honours at most 8 Boolean connectors (`AND`, `OR`, `NOT`) in one
  query and silently ignores the rest: measured 2026-10-15, adding a ninth OR
  term, or a ninth connector's AND or NOT, left the count unchanged. A longer
  query is split into halves of its `OR` clauses (below).
- OpenAlex separates filters with commas, so a comma in a term or phrase is
  refused for it.

When a provider cannot express part of a query, the search stops with an error
naming the provider before any provider is searched; nothing is dropped from a
query. Leave that provider out with `--provider`, or rephrase.

Each provider's results are paged to the end:

Before any provider pages its results, each provider that has to split a
search plans the split from counts. A search that cannot be split exactly is
refused at that point, with nothing searched or registered.

- PubMed returns at most 10,000 hits per search. A search finding more is run
  one year at a time over its years; for the default, from 1990 to the latest
  year PubMed dates one of its hits (a few records are dated years ahead). If fewer hits are
  dated in that span than were found, the search is refused rather than lose
  the rest. The year windows use ESearch's publication date, which counts the
  same records as `[dp]` (checked 2026-10-15: 1,542,428 for 2026 either way).
- OpenAlex pages by cursor, 100 works a page, and follows a cursor past the
  first page only for a request with an API key, so `openalex_api_key` is
  required.
- ScienceDirect serves at most 6,000 results of one query and honours at most
  8 connectors. Each query is split until every part fits:
  1. a query with more than 8 connectors into two halves of its `OR` clauses;
  2. a query finding more than 6,000 into two halves of its years (for the
     default, 1990 to the latest year it has hits). The halves' counts must add
     up to at least the whole, so an undated article would stop the search
     rather than be lost. They can add up to more: an issue dated across two
     years (e.g. 1984-1985) is in both, and is one record after deduplication
     (one query's 1844-2124 slot had 3 such). Halves are halved again down to
     single years; an empty half costs one count;
  3. a single year that is an `OR`, into two halves of its `OR` clauses;
  4. otherwise into `Q AND w` and `Q NOT w`, on a word `w` from a short fixed
     list (memory, disease, treatment, patients, ...). Words are counted in
     turn and the first that puts 35-65% of the hits on each side is taken;
     failing that, the one nearest to half. The two counts must add up to the
     query's, or the next word is tried. A word of the query itself, a word in
     every hit or in none, and a word that would take the query past 250
     characters are never used.

  Every part is split again until it fits; a part that no step can split is
  refused. The plan is printed as it is made, before any provider pages its
  results: a tree with each part's count and each split's counts. The parts
  of an `OR` split overlap; their results are deduplicated by DOI, else PII,
  else title and publication date, and their union is the whole result.

  Two other ways to split are not used. The API sorts by date only newest
  first, so paging a query in both date orders is not possible. Its only
  filter, `openAccess`, selects open-access articles when `true`, but `false`
  means no filter, not the closed ones (for one 2026 query: 6,790 unfiltered,
  3,191 with `true`, 6,790 with `false`), so it does not partition a query.

## Credentials

| Provider | Setting |
|---|---|
| PubMed | `pubmed_email` (required), `pubmed_api_key` |
| Semantic Scholar | `semantic_scholar_api_key` (required) |
| OpenAlex | `openalex_api_key` (required), `openalex_email` |
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
