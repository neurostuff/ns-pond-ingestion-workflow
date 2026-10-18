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
- No stage decides a licence yet, so `text_copy.license` and `bibliography.license` are both
  `all-rights-reserved`, `assumed`. Under it nothing may be passed on.
- `bibliography.provider` is the metadata record's provider: `pubmed`, `semantic_scholar` or
  `openalex`, or `elsevier` and `jats` for the record read from a held Elsevier or JATS file.
  An abstract is credited to the same provider (`abstract_provider`). A record that names no
  provider is not parsed.
- `header.identifiers` holds every DOI the catalog has for the article. A DOI that
  `utils/preprints.py` names a preprint server for is in `preprint_dois` with its server; the
  other is `doi`. An article with two DOIs that are neither preprint's is not parsed.
- A PMID whose PubMed record is typed `Preprint` is `preprint_pmid`, and the PMCID that record
  lists is `preprint_pmcid`.
- `publication_status` is `preprint` when only a preprint's identifiers are known, and
  `published` otherwise.
- `header.neurostore_base_study_id` is the paper's neurostore base study, once sync knows it.
