# The neurostore handoff, and paper links

Decided 2026-10-27. Read against ns-pond main 89ca223, neurostore master d514c2a6, the
neurostore-spec pin af474b0 and study_schema bc414b3, which both repositories pin. Nothing
here is built yet unless it says so.

## The handoff today

The whole handoff is one admin endpoint, `POST /api/study-schema-uploads/`. The upload stage
(`services/neurostore_ingest.py`) sends `{"coordinate_parse", "parsed_paper"}` as sync wrote
them, after checking them against study_schema's models, `layouts.check_paper` and its JSON
Schema. neurostore's ingester (`store/backend/neurostore/ingest/study_schema.py`) checks them
again against the same JSON Schema and writes the base study, the pipeline Study, its
analyses and points, the authors, the identifiers and the retraction state in one
transaction.

## Decisions on the handoff

1. **The text travels with the upload.** The body gets an optional `text`. The ingester
   upserts `Fulltext(base_study_id, sha256)` in the same transaction and refuses a text whose
   sha256 is not the parsed paper's `text_sha256`. Today only the review import writes
   `fulltexts` (`review/app/imports.py::_fulltext`), because the parsed paper carries
   `text_path`, not the text.
2. **neurostore owns a base study the pipeline uploads.** An upload sets
   `base_studies.user_id` to NULL and `public` to true, whoever held it before. The Studies
   users made under it keep their own `user_id` and `public`. The summary names the owner it
   took the base study from (`took_ownership_from`). After creation, the bibliography
   (title, abstract, journal, year) belongs to neurostore; the pipeline owns its Study and
   analyses, the identifiers it adds, the structured authors, the retraction state and
   `publication_status`.
3. **Licences are stored as categories only.** `base_studies.license` (the paper's licence,
   which governs the abstract) and `fulltexts.license` (the licence of the copy the text was
   read from) are `sa.Enum(LicenseId, native_enum=False)`: study_schema's ten values,
   `cc_by` to `other`. NULL means no pipeline upload has set it. Every upload overwrites
   both. Where a licence was stated, and whether it was assumed, stays in ns-pond. Which
   values allow redistribution comes from study_schema's `licensing.permits_redistribution`.
4. **An upload that follows a merge is an upload.** When the ingester follows
   `superseded_by` from the base study the header names (X) to the one that supersedes it
   (Y), its summary says `merged_from: [X]`. ns-pond records that as uploaded and learns Y as
   the alias. Today it records REFUSED (permanent) although the write committed. A summary of
   another base study without `merged_from` is still refused.
5. **Schema-version skew stops the run.** A 422 with code `SCHEMA_VERSION` becomes a
   `NeurostoreConfigError`, a StopRun that writes nothing. Today it is recorded as each
   paper's permanent refusal, and the upload fingerprint does not change when neurostore is
   fixed. The spec's 422 list gets `SCHEMA_VERSION`.
6. **A preprint server is any non-empty string in the spec.** Built:
   neurostuff/neurostore-spec#117. After it merges, neurostore repins its `openapi`
   submodule.
7. **No withdraw upload kind.** ns-pond takes analyses back only for `no extraction text`
   and `no analyses` (`pipeline/stage.py`, `TAKE_BACK_REASONS`). Neither changes the paper's
   status, and retractions already reach `is_retracted` through `bibliography.corrections`.
   neurostore keeps what it holds; the upload stage logs each take-back of an article it had
   uploaded, so a person can look at it.
8. **Any text source.** neurostore drops the CHECK on `fulltexts.source` and takes any
   non-empty source; an unknown one ranks after `pdf` in `FULLTEXT_SOURCE_PREFERENCE`.
   ns-pond sends its own name (`wiley` stays `wiley`).
9. **Stored, not served.** Everything new reaches Postgres through the upload. Nothing new
   is exposed for reading: no route and no response field for licences, links, link
   domains or `has_nifti`. The spec changes only for the upload itself: `text` in the body
   (`parsed_paper.links` rides in the existing open object), `merged_from` and
   `took_ownership_from` in the summary, and `SCHEMA_VERSION` among the 422 codes.

## Paper links

The goal: find papers that share neuroimaging data (NIfTI) on OSF or OpenNeuro and have no
NeuroVault link, so their authors can be asked to put the data on NeuroVault. NeuroVault is
an ordinary link domain; nothing matches papers against NeuroVault's collections.

### In ns-pond

Built on `feat/paper-links`, against study_schema#55.

- **`links`** requires `extract` and uses no network, like `references`. It reads every
  http(s) link from the source's markup (JATS `ext-link@xlink:href`, Elsevier
  `ce:inter-ref`, publisher HTML `a@href`), from a URL pattern over the text (PDFs, unmarked
  links), from the supplementary material JATS links to (ns-pond writes no
  `ParsedPaper.supplements`), and from the reference list, where a dataset DOI
  (`10.18112/openneuro.ds…`, `10.17605/OSF.IO/…`) counts as a link and another DOI does not.
  A publisher page's anchor counts when its text prints the link: an anchor of words
  ("PubMed", "Google Scholar") is the publisher's, not the paper's. The reference list is
  the one `references` reads (which runs by default, before `links`), so an occurrence from
  it names its entry by the id sync writes. It keeps where each link appeared (offsets, how
  it was found, the reference entry) and, in ns-pond only, its section and a context flag
  from the words around it (NIfTI, `.nii`, statistical maps, fMRI data).
- **`linkprobe`** requires `links` and uses the network for OSF and OpenNeuro links only.
  OSF's API lists a project's files (providers, folders, components, pages, at most 60
  requests), so a `.nii` or `.nii.gz` settles it. OpenNeuro also hosts EEG, MEG and iEEG
  datasets, so a dataset is not imaging by definition: its snapshot's summary names its
  modalities, and BIDS stores MRI and PET as NIfTI, so `mri` or `pet` among them is
  `has_nifti`. It records `has_nifti` with what it saw and when; a private or missing
  project or dataset leaves it unset, with the reason. No LLM.
- **Re-checks.** Decided 2026-10-29: an answer that could still change (no NIfTI seen, or
  the project private or gone) is asked again 90 days after it was checked
  (`RECHECK_AFTER`), because authors often share data after the paper is out. A NIfTI seen,
  and what a tagged OpenNeuro snapshot holds, are kept for good, and a re-check asks only
  the links that are due. Sync's fingerprint takes the probe's answers
  (`summary["answers"]`), not its blob, so an article is re-synced and re-uploaded only
  when an answer changed.
- **Order:** `references` → `links` → `linkprobe` → `sync`. Sync writes
  `links: [{url, has_nifti?, occurrences: [{found_by, start_char?, end_char?,
  reference_id?}]}]`, each link's occurrences in text order, and `references` (the
  references stage's list) into the parsed paper, and upload sends it. An occurrence's
  section is the `ParsedPaper.sections` span holding it (`paper_parse.links_in_section`). Sync does not wait for `links`: with no links
  artifact the parsed paper's `links` is absent ("not looked"); once `links` has read the
  text it is written, `[]` when the paper gives none.
- study_schema gets `ParsedPaper.links`, each `PaperLink` with its `occurrences`, and a
  `data_availability` `SectionKind`.

### In neurostore

- **`base_study_links`**: `id`, `base_study_id` (foreign key, `ON DELETE CASCADE`), `url`
  (normalised: lower-case host, no `www.`, no fragment, no trailing slash, a DOI as its
  `https://doi.org/` form; unique per base study), `host`, `registrable_domain` (by the
  public suffix list), `has_nifti` (nullable; NULL until probed), `source` (`pipeline` or
  `user`), timestamps.
- **Re-runs:** each upload replaces the base study's `source = 'pipeline'` rows.
- **Merges:** `merge_base_study` moves links to the base study it keeps, as it does other
  owned rows; the ADR that adds the table says so.
- **`link_domains(pattern, repository, kind)`**, with `kind` one of data_repository, code,
  preprint, publisher, doi_prefix, other. A link's category comes from a join, never from
  the link row, so recategorising needs no re-run. A migration seeds osf.io and
  `10.17605/osf.io`, openneuro.org and `10.18112/openneuro`, neurovault.org, github.com,
  gitlab.com, zenodo.org and `10.5281`, figshare.com and `10.6084`, and doi.org. A domain
  with no row has no category.
- **Privacy:** links can be read wherever their base study can, once there is an API; only
  the ingester writes pipeline rows.

The outreach list is then one query: base studies with a link whose domain is OSF or
OpenNeuro and `has_nifti` is true, and no link whose domain is NeuroVault.

## Open questions

- Should neurostore hide an abstract or a text whose licence does not allow
  redistribution? This decides when licences are exposed in the API (see the NOTE in the
  ingester's `_bibliography`).
- Provider metadata as a link source: Crossref `relation` (`is-supplemented-by`), DataCite.
- Should uploads send a `run_id`? The ingester stores one on each result and refuses a
  reused one with a different document; ns-pond sends none today.
- Excluding authors already on NeuroVault, not only papers: matching author names to
  NeuroVault owners is fuzzy.
- Read routes for links and licences, and the `GET /base-studies` filters
  (`link_repository`, `link_has_nifti`, `no_link_repository`), once wanted.
