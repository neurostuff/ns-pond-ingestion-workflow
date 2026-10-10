# Set-role classifier (X1) and role-bearing extractor data (X3)

What each coordinate set is for -- a result of this study, a region it defined, a peak quoted
from another study, a display position -- decided by a small fine-tuned encoder in the `roles`
stage (`ingestion_workflow/pipeline/stages/roles.py`, off unless `role_model` is set). The code
is in `ingestion_workflow/services/set_roles/`; this directory holds the job scripts.

```
services/set_roles/
  labels.py         study_schema's role fields, shared by table and prose sets
  common.py         point shape, cue counts, sentence and citation helpers
  table_context.py  a TABLE set's input: caption, footer, header, its rows, neighbours, citing sentences
  prose_context.py  a PROSE set's input: passage, heading, before/after, citation markers
  label_schema.py   the labeller's strict JSON answer
  labeling.py       per-origin prompts, the resumable codex job, agreement, ledger
  export.py         encoder rows, nu-v21 rows with role, prose rows with role
  classifier.py     when a prediction overrides the proposed role
  model.py          the encoder with two heads (torch/transformers, imported lazily)
experiments/role_classifier/
  build_sets.py     labelling units from nu-v21's and the prose model's real training rows (beast)
  label_sets.py     label / compare / totals
  export_rows.py    the three outputs
  train_encoder.py  fine-tune the encoder
```

## Labels

Each set's label is study_schema's three role fields, the ones `CoordinateParse` has:
`role` (a `CoordinateRole`), `anchor_kind` (an `AnchorKind`, for an anchor only) and
`from_prior_study`. The values are imported from `study_schema.models.paper_parse`, never
retyped (`labels.py`); every row the labeller, the encoder and the extractors' exports carry
uses them.

| `role` | Meaning | Uploaded to neurostore |
|---|---|---|
| `result` | A finding of this study: peaks of a tested effect | yes |
| `anchor` | A location the study defined and used; `anchor_kind` `roi`, `seed`, `stimulation_target` or `node` | yes, as a labelled set, not an analysis with statistics |
| `localization` | Where electrodes or sources were placed | no |
| `reference` | Coordinates quoted from another publication for comparison | no |
| `display` | A slice or crosshair position for a figure | no |
| `other` | A real brain coordinate that is none of the above: a simulated source position or lesion centre, a worked-example voxel of an atlas | no |

Numbers that are not brain coordinates at all (channel numbers, lattice points, a phantom's
positions, rodent stereotaxic coordinates) get no role, not `other`: the labeller answers `coordinates:
false` and `role: null`, the encoder's coordinates head learns them, and they are dropped from
the uploads and from the extractors' targets.

`from_prior_study` is independent of the role: a seed taken from a meta-analysis is `role:
anchor`, `anchor_kind: seed`, `from_prior_study: true`. The model has a separate binary head
for it, and one for the anchor kind.

The current nu-prose model answers in its own vocabulary (`prompts.prose_coordinates.ROLES`).
One legacy adapter, `prompts.prose_coordinates.study_schema_role`, reads it where its output
is read (resolve, and `build_sets.py` for the prose dataset): `roi`/`seed`/`target` -> anchor
of that kind, `prior_study` -> `reference` with `from_prior_study`, `figure` -> `display`,
`other` -> not coordinates (the model's `other` is anything else and does not separate real
brain coordinates that fit no role from non-coordinates, which are most of it; the classifier
finds the real ones). It goes when the retrained nu-prose, taught these fields by
`export.prose_rows`, ships.

Decision rule (`classifier.decide`): a table set is proposed `result` and a prose set its prose
model's role, as resolve recorded it. The classifier sets a set aside as not coordinates, or overrides the proposal only when its top probability is at least
`min_confidence` (the design sets 0.8), because a wrong override is costly -- a real result
relabelled `reference` is not uploaded. `from_prior_study` is set when the flag head reaches
`prior_threshold` (0.5), and always for a `reference`. Each decision is
recorded with its confidence, its source (`proposal` or the model's name and version) and the
proposal it overrode.

## Inputs

Table and prose sets are read from different evidence, so each has its own builder and its
own version (`TABLE_CONTEXT_VERSION`, `PROSE_CONTEXT_VERSION`). Every label and training row
records the version of its origin, and a model records both; the stage refuses a model whose
versions differ from the code's. Both strings start with `[ORIGIN] table|text`, then the
short fields (`[PROPOSED]`, `[POINTS]`, `[CUES]`, `[NAME]`), so truncation cuts free text and
never structure:

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
the exporters return in its own format with the role added.

Labelling runs where `codex login` is (the codex CLI is not installed on beast), through
pondie's `CodexCaller` with a pondie that includes #10 on PYTHONPATH:

```
python label_sets.py label units/table_units.jsonl labels/sol-table --model gpt-6.1-sol --pace 30 \
    --plain-sample 300
python label_sets.py label units/prose_units.jsonl labels/sol-prose --model gpt-6.1-sol --pace 30 \
    --skip-dataset-labelled
python label_sets.py compare labels/pilot-sol labels/pilot-astra
python export_rows.py out --units units/*.jsonl --labels labels/astra labels/sol-table labels/sol-prose \
    --id-map units/slug_dbids.json --prose-dataset /data/alejandro/jk-prose-coords/datasets/v3-20261006
CUDA_VISIBLE_DEVICES=0 ~/venv-train/bin/python train_encoder.py out/encoder.jsonl models/set-roles-1
```

`out/nu_v21_role.jsonl` trains nu-v21 with `out/nu_v21_template.json` as the template;
`out/prose/` replaces the v3 prose dataset directory that `build_ft.py` reads (`V3`), wide-context
files included. Its val and test rows and the 74 hand-labelled rows are never sent to a labeller
and pass through unchanged; the hand rows are the encoder's prose gold set.

The labeller skips held-out prose rows and tables whose text repeats another unit's (203; the
export copies the first copy's labels). `--plain-sample 300` labels 300 of the 5,946 `plain`
tables (all `result` in the pilot) plus every one whose caption, footer or citing sentences
name an atlas, mask, seed, ROI or prior study.

Splits (`export.splits`): by article under its database id (`units/slug_dbids.json` maps the
slug ids of 270 articles, 268 found in the corpus); articles sharing a table's text share a
split; an article with a gold label (astra or hand) or a held-out prose row is test; synthetic
sets are train only. `[PROPOSED]` is what the pipeline proposes: `result` for a table set, the
prose stage's role for a corpus passage, and `unknown` for a dataset row, whose roles are its
labels.
