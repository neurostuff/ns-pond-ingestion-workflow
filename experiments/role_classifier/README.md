# Set-role classifier

What each coordinate set is for -- a result of this study, a region it defined, a peak quoted
from another study, a bare slice position -- decided in the `roles` stage
(`ingestion_workflow/pipeline/stages/roles.py`), the only place a role is decided. The stage is
required: space, upload and sync read nothing it has not passed. There are two classifiers, a
table model (`role_model_table`) for nu-v21's table sets and a prose model (`role_model_prose`)
for nu-prose's sets, because different evidence matters for each. Neither extractor predicts a
role, and an article with a set whose origin has no usable model fails the stage; nothing falls
back to a default. The artifact the stage writes is described in `docs/set-roles-artifact.md`. The code
is in `ingestion_workflow/services/set_roles/`; this directory holds the job scripts.

```
services/set_roles/
  labels.py         study_schema's role fields, shared by table and prose sets
  common.py         point shape, cue counts, sentence and citation helpers
  table_context.py  a TABLE set's input: caption, footer, header, its rows, neighbours, citing sentences
  prose_context.py  a PROSE set's input: passage, heading, before/after, citation markers
  label_schema.py   the labeller's strict JSON answer
  labeling.py       per-origin prompts, the resumable codex job, agreement, ledger
  export.py         encoder rows
  classifier.py     a prediction as the recorded role, with its confidence and model
  model.py          the encoder and its heads, one model per origin (torch/transformers, lazily)
experiments/role_classifier/
  build_sets.py     labelling units from nu-v21's and the prose model's real training rows (beast)
  label_sets.py     label / compare / totals
  export_rows.py    the encoder rows
  train_encoder.py  fine-tune the encoder
```

## Labels

Each set's label is study_schema's three role fields, the ones `CoordinateParse` has:
`role` (a `CoordinateRole`), `anchor_kind` (an `AnchorKind`, for an anchor only) and
`from_prior_study`. The values are imported from `study_schema.models.paper_parse`, never
retyped (`labels.py`); every row the labeller and the encoder carry uses them.

| `role` | Meaning | Uploaded to neurostore |
|---|---|---|
| `result` | A finding of this study: peaks of a tested effect, including peaks drawn in a figure | yes |
| `anchor` | A location the study defined and used; `anchor_kind` `roi`, `seed`, `stimulation_target` or `node`. White-matter or CSF voxels whose timeseries are regressed out as nuisance are `roi` anchors (PMID 26589451) | yes, as a labelled set, not an analysis with statistics |
| `localization` | Where electrodes or sources were placed | no |
| `reference` | Coordinates quoted from another publication for comparison, including crosshairs at a prior study's coordinates | no |
| `other` | A real brain coordinate that is none of the above: bare slice positions with no finding, a simulated source position or lesion centre, a worked-example voxel of an atlas | no |

Numbers that are not brain coordinates at all (channel numbers, lattice points, a phantom's
positions, rodent stereotaxic coordinates) get no role, not `other`: the labeller answers `coordinates:
false` and `role: null`, the encoder's coordinates head learns them, and they are dropped from
the uploads.

`from_prior_study` is independent of the role: a seed taken from a meta-analysis is `role:
anchor`, `anchor_kind: seed`, `from_prior_study: true`. The model has a separate binary head
for it, and one for the anchor kind.

The current nu-prose model still answers with its own role words
(`prompts.prose_coordinates.ROLES`); nothing reads them. Resolve keeps every prose point, in
one analysis per name, and the prose role classifier decides each set's role.

Labels carry `label_version`: 1 for the rows answered under the earlier role vocabulary and
converted to study_schema's fields afterwards, 2 for answers that could say `other`, 3 once the
instructions gave the nuisance-regressor rule, 4 for the prose sets relabelled once `display`
was no longer a role (`labels/relabel-v4-no-display`). Training reads every label directory
given and takes each set's highest `label_version` (`labeling.latest_labels`); a set whose
latest label is still `display` is left out.

Decision rule (`classifier.decide`): the set's role is its origin's model's answer. Below a
coordinates probability of 0.5 the numbers are not coordinates (`role` null); otherwise the role
is the role head's top answer, however sure, and the record carries its confidence, so the
unsure ones can be reviewed. `from_prior_study` is set when the flag head reaches
`prior_threshold` (0.5), and always for a `reference`. Each decision is recorded with its
confidence, its model (`name@version`) and its origin.

## Inputs

Table and prose sets are read from different evidence, so each has its own builder and its
own version (`TABLE_CONTEXT_VERSION`, `PROSE_CONTEXT_VERSION`). Every label and training row
records the version of its origin, and a model records its origin and that origin's version;
the stage refuses a model for another origin or version. Both strings start with
`[ORIGIN] table|text`, then the short fields (`[POINTS]`, `[CUES]`, `[NAME]`), so truncation
cuts free text and never structure. No input carries a role: not the extractor's, not a
dataset's label.

```
table: ... [TABLE] [NEIGHBOURS] [CAPTION] [FOOTER] [HEADER] [ROWS] [CITED]...
text:  ... [HEADING] [CITATIONS] [PASSAGE] [BEFORE] [AFTER]
```

## Data

On beast, everything under `/data/james/agents/ing-x1/` (nothing large is in git):

```
PYTHONPATH=code python code/experiments/role_classifier/build_sets.py units --wild-passages 3000
```

`units/table_units.jsonl` is one unit per real nu-v21 training table (curated, real-positive,
hand-judged), its sets the row's target analyses; `units/prose_units.jsonl` one per prose
dataset passage (synthetic ones carry their generator's roles and are not sent to a model),
plus corpus passages for the encoder only. Each keeps its source row as `base_row`, which
the exporter reads.

Labelling runs where `codex login` is (the codex CLI is not installed on beast), through
pondie's `CodexCaller` with a pondie that includes #10 on PYTHONPATH:

```
python label_sets.py label units/table_units.jsonl labels/sol-table --model gpt-6.1-sol --pace 30 \
    --plain-sample 300
python label_sets.py label units/prose_units.jsonl labels/sol-prose --model gpt-6.1-sol --pace 30 \
    --skip-dataset-labelled
python label_sets.py compare labels/pilot-sol labels/pilot-astra
```

## Training the two models

Export writes one file per origin, from every label directory given (highest `label_version`
per set; between equal versions the first directory wins, so list a gold set first):

```
python export_rows.py out --units units/table_units.jsonl units/prose_units.jsonl \
    --labels labels/sol-table labels/sol-prose labels/relabel-v4-no-display \
    --id-map units/slug_dbids.json
```

The table model, from `labels/sol-table`'s sets (`out/encoder-table.jsonl`):

```
CUDA_VISIBLE_DEVICES=0 ~/venv-train/bin/python train_encoder.py table out/encoder-table.jsonl \
    models/set-roles-table-1
```

The prose model, from `labels/sol-prose` and its version-4 relabels (`out/encoder-text.jsonl`):

```
CUDA_VISIBLE_DEVICES=0 ~/venv-train/bin/python train_encoder.py text out/encoder-text.jsonl \
    models/set-roles-prose-1
```

Point `role_model_table` and `role_model_prose` at the two directories. A smoke run on CPU
(`--limit 12 --epochs 1 --max-length 64 --base sentence-transformers/all-MiniLM-L6-v2`) checks
either path in about a minute.

A prose dataset row that is evaluation data (val, test) or hand-labelled is never sent to a
labeller; the hand rows are the encoder's prose gold set.

The labeller skips held-out prose rows and tables whose text repeats another unit's (203; the
export copies the first copy's labels). `--plain-sample 300` labels 300 of the 5,946 `plain`
tables (all `result` in the pilot) plus every one whose caption, footer or citing sentences
name an atlas, mask, seed, ROI or prior study.

Splits (`export.splits`): by article under its database id (`units/slug_dbids.json` maps the
slug ids of 270 articles, 268 found in the corpus); articles sharing a table's text share a
split; an article with a gold label (astra or hand) or a held-out prose row is test; synthetic
sets are train only.
