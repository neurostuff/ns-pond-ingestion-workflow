# What the coordinate parse leaves out

`parse/coordinate_parse.json` (study_schema's `CoordinateParse`) records every table it did not
read as a `TableReading` with a `reason`, and every analysis it did not keep as an omission
with a `reason`. Nothing is dropped without one.

| Table reading | Reason |
|---|---|
| `excluded_by_hand` | the person's reason and note |
| `rejected_by_triage` | the triage route |
| `no_coordinates` | for a rodent atlas table: "its coordinates are rodent stereotaxic positions (bregma-relative or in a rodent atlas's axes), not brain coordinates in a template space" |

A table is a rodent atlas table when its caption, footer or column headings (never its rows)
either name a rodent atlas ("Paxinos and Watson", "Franklin and Paxinos"), or name an animal
(rat, mice, rodent, murine, "mouse brain", Sprague-Dawley, Wistar, Long-Evans, C57BL) together
with bregma, "interaural", Paxinos or Franklin, or all three of the AP, ML and DV axes. A bare
"mouse" is not an animal here, and human atlas references ("Mai and Paxinos") do not count.
Each analysis of such a table is omitted with the same reason; a table a person excluded or
triage rejected keeps that reading.

# What the parsed paper says about its copy and its identifiers

- `text_copy` is the copy the text was read from, as its download recorded it: `route`,
  `version`, `access`, and `url` and `preprint_server` where known. Sync fails for an article
  whose text's download is not ok, and stops when a download records no copy (run
  `scripts/backfill_download_copies.py` on such a catalog).
- The licences come from the metadata stage, which decides one for every copy an ok
  download holds, one for the paper, and one for each copy a provider lists that no
  download holds (`reported_copies`). Sync fails for an article whose metadata record holds
  no licence for the text's copy.
- A licence is `stated` by the first source in this order that states one:
  1. the copy's own record: PMC's cloud record (`pmc_cloud`), its JATS `<permissions>`
     (`jats`), Elsevier's `coredata` (`elsevier`) or the open licence the publisher's page
     ACE read marks (`publisher_page`), or, for a copy at a URL, the licence OpenAlex or
     Semantic Scholar lists for that URL;
  2. the own record of another held copy of the same version, other than a publisher's
     page, the download sources in the order they are tried;
  3. for the version of record, Crossref's `vor` deposit for the journal's DOI, then
     OpenAlex's location at the publisher, then a Crossref deposit that names no version;
     for an accepted manuscript, Crossref's `am` deposit; for a preprint, the preprint
     DOI's Crossref deposits, then OpenAlex's location at the preprint server;
  4. a held publisher's page, for every copy including its own: for the version of record,
     the licence it marks, else its copyright line; for an accepted manuscript,
     ScienceDirect's self-archiving licence (`sa-user-license`) once its start date has
     come. That licence is never the version of record's.

  A repository location and Semantic Scholar's open-access PDF describe only the copy at
  their URL, never the version of record. Within one record a licence in study_schema's
  list comes before an `other`; across sources, the earlier source decides. A JATS
  `license_ref` with `specific-use="textmining"` is read like any other: PMC tags the
  article's own licence that way. Crossref's `tdm` and `stm-asf` deposits grant mining or
  sharing, not a licence, and are not read. Terms that grant only text mining (PMC's
  `TDM`) are `all-rights-reserved`.
- A publisher's page is read outside its footer (a `footer` tag, or a class or id that
  contains `footer`, unless it holds the article's title, `article`, `main` or footnotes),
  figures, references, research data cards and ScienceDirect's supplementary files.
  Its licence marks are a link to a Creative Commons licence (an `href`, or an embedded
  JATS `xlink:href`), the links in a block classed `license`, the `license` of the record
  the page embeds (Taylor & Francis's JSON-LD), and the paragraphs and copyright lines that
  grant a Creative Commons licence in words, less their sentences about the article's data
  (a CC0 waiver for its data). The words are the article's own licence text: a link that
  names another open licence is not read beside them. When the words, or with none the
  links, name two different open licences, the page states none. Its copyright lines are
  its `dc.rights` and `prism.copyright` metadata and what an element classed `copyright`
  prints; one that names no licence is `all-rights-reserved`, stated. Only an open licence
  the page marks is its copy's own record. Its other marks and its copyright line credit a
  holder or name terms, often printed before a licence is granted, so they rank last, the
  copyright line before an `other` mark. A copy from ACE is `access: unrecorded` whatever
  its page states; the page of a preprint DOI is the preprint (`submitted`, at its server).
- When no source states one, the licence is `all-rights-reserved`, `assumed`.
- A licence is undecided, and the metadata artifact fails with the sources named, while a
  source before the deciding one (or, for an assumed licence, any source in the order) did
  not answer or could not be read. It fails, too, while OpenAlex or Semantic Scholar has
  not answered, since the copies they list are then unknown. A provider's outage spends
  none of the article's tries: it is asked again once the retry delay has passed.
- `bibliography.license` is the version of record's licence, or the preprint's when
  `publication_status` is `preprint`. `bibliography.oa_status` is OpenAlex's.
- `bibliography.provider` is the metadata record's provider: `pubmed`, `semantic_scholar` or
  `openalex`, or `elsevier` and `jats` for the record read from a held Elsevier or JATS file.
  `abstract_provider` is the provider whose abstract was kept. A record that names no
  provider is not parsed.
- `header.identifiers` holds every DOI the catalog has for the article. A DOI that
  `utils/preprints.py` names a preprint server for is in `preprint_dois` with its server; the
  other is `doi`. An article with two DOIs that are neither preprint's is not parsed.
- A PMID whose PubMed record is typed `Preprint` is `preprint_pmid`, and the PMCID that record
  lists is `preprint_pmcid`.
- `publication_status` is `preprint` when only a preprint's identifiers are known, and
  `published` otherwise.
- `header.neurostore_base_study_id` is the paper's neurostore base study, once sync knows it.
