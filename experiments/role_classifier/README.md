# Set-role classifier

What each coordinate set is for -- a result of this study, a region it defined, a peak quoted
from another study, a bare slice position -- decided in the `roles` stage
(`ingestion_workflow/pipeline/stages/roles.py`), the only place a role is decided. The stage is
required: space, upload and sync read nothing it has not passed. One decision model
(`role_model`) reads nu-v21's table sets and nu-prose's sets alike, each through its origin's
context builder. Neither extractor predicts a role, and while the model cannot be used every
article with a set is blocked at the stage; nothing falls back to a default. The artifact the stage writes is described in `docs/set-roles-artifact.md`. The code
is in `ingestion_workflow/services/set_roles/`; this directory holds the job scripts.

```
services/set_roles/
  labels.py         study_schema's role fields, shared by table and prose sets
  common.py         point shape, cue counts, sentence and citation helpers
  table_context.py  a TABLE set's input: caption, footer, header, its rows, neighbours, citing sentences
  prose_context.py  a PROSE set's input: its own sentence(s), cut to 400 chars around its coordinates (LOCAL), passage, heading, before/after, citation markers
  label_schema.py   the labeller's strict JSON answer
  labeling.py       per-origin prompts, the resumable codex job, agreement, ledger
  export.py         role-model training rows
  classifier.py     a prediction as the recorded role, with its confidence and model
  decision.py       the decision model: an encoder with five heads, loaded in the stage's process
  geometry.py       sets whose numbers cannot be template brain coordinates, refused before the model
experiments/role_classifier/
  build_sets.py       labelling units from nu-v21's and the prose model's real training rows (beast)
  label_sets.py       label / compare / totals
  export_rows.py      the NuExtract role models' training rows (retired)
  build_rows.py       the decision model's training rows, built with this code's context builders
  train_decision.py   fine-tunes the decision model (the run role-decision v1 was trained with)
  export_decision.py  a training run as the directory the stage loads
  check_decision.py   an exported model against its run's own eval predictions
```

## Labels

Each set's label is study_schema's three role fields, the ones `CoordinateParse` has:
`role` (a `CoordinateRole`), `anchor_kind` (an `AnchorKind`, for an anchor only) and
`from_prior_study`. The values are imported from `study_schema.models.paper_parse`, never
retyped (`labels.py`); every row the labeller and the role model carry uses them.

| `role` | Meaning | Uploaded to neurostore |
|---|---|---|
| `result` | A finding of this study: peaks of a tested effect, including peaks drawn in a figure | yes |
| `anchor` | A location the study defined and used; `anchor_kind` `roi`, `seed`, `stimulation_target` or `node`. White-matter or CSF voxels whose timeseries are regressed out as nuisance are `roi` anchors (PMID 26589451) | yes, as a labelled set, not an analysis with statistics |
| `localization` | Where electrodes or sources were placed | no |
| `reference` | Coordinates quoted from another publication for comparison, including crosshairs at a prior study's coordinates | no |
| `other` | A real brain coordinate that is none of the above: bare slice positions with no finding, a simulated source position or lesion centre, a worked-example voxel of an atlas | no |

Numbers that are not brain coordinates at all (channel numbers, lattice points, a phantom's
positions, rodent stereotaxic coordinates) get no role, not `other`: the labeller answers `coordinates:
false` and `role: null`, and so does the decision model.

`from_prior_study` is independent of the role: a seed taken from a meta-analysis is `role:
anchor`, `anchor_kind: seed`, `from_prior_study: true`. The decision model answers where the
coordinates come from (`coordinate_source`: `this_study`, `prior_study`, `atlas_or_database`),
and `from_prior_study` is `coordinate_source == prior_study` (always true for a `reference`).

Resolve keeps every prose point, in one analysis per name, and the prose role classifier
decides each set's role.[^no-prose-role]

Labels carry `label_version`: 1 for the rows answered under the earlier role vocabulary and
converted to study_schema's fields afterwards, 2 for answers that could say `other`, 3 once the
instructions gave the nuisance-regressor rule, 4 for the prose sets relabelled once `display`
was no longer a role (`labels/relabel-v4-no-display`). Training reads every label directory
given and takes each set's highest `label_version` (`labeling.latest_labels`); a set whose
latest label is still `display` is left out.

Decision rule (`classifier.decide`): the set's role is the model's answer, however sure, and the
record carries its probability, so the unsure ones can be reviewed. A set the model says is not
coordinates has no role. A set whose coordinates are not human template millimetres (`space:
other`: an animal brain, a head frame, voxel indices) keeps its role and is held. Each decision
is recorded with its confidence, its model (`name@version+digest`) and its origin.

## Inputs

Table and prose sets are read from different evidence, so each has its own builder and its
own version (`TABLE_CONTEXT_VERSION`, `PROSE_CONTEXT_VERSION`). Every label and training row
records the version of its origin, and the model's `decision.json` names the context versions it
was trained on; the stage refuses a model that names others. Both strings start with
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
plus corpus passages for the role model only. Each keeps its source row as `base_row`, which
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

## Training the decision model

The model in production is `role-decision` v1: `lytang/MiniCheck-RoBERTa-Large`, fully
fine-tuned for 3 epochs (frozen embeddings, 8-bit AdamW, max length 512) on 21,370 v6-labelled
sets (13,267 table and 5,860 prose training sets), on one 8 GB card. It was chosen over
MiniCheck-DeBERTa-v3-Large and ModernBERT-large at 1,024 tokens on the held-out roles evaluation
set: the best result recall (579/613 with the geometry check), the fewest sets that should be
uploaded held (42/955), and 170 of 186 prior-study and atlas anchors flagged. Rows, training and
export:

```
PYTHONPATH=<this checkout> python build_rows.py UNITS LABEL_DIR... --exclude-articles EVAL_ARTICLES --out rows.jsonl
python train_decision.py --rows rows.jsonl --eval eval_rows.jsonl --base lytang/MiniCheck-RoBERTa-Large \
    --out runs/roberta --epochs 3 --batch 8 --accum 2 --max-length 512 --optim adamw8bit \
    --freeze-embeddings --grad-ckpt
PYTHONPATH=<this checkout> python export_decision.py runs/roberta /data2/models/role-decision/v2/weights \
    --name role-decision --version v2 --table-context 2 --text-context 4
PYTHONPATH=<this checkout> python check_decision.py /data2/models/role-decision/v2/weights \
    eval_rows.jsonl runs/roberta/eval_predictions.jsonl
```

`check_decision.py` must report no disagreement before the stage is pointed at the new
directory (`role_model.path`).

## The retired NuExtract role models

Export writes one file per origin, from every label directory given (highest `label_version`
per set; between equal versions the first directory wins, so list a gold set first):

```
python export_rows.py out --units units/table_units.jsonl units/prose_units.jsonl \
    --labels labels/sol-table labels/sol-prose labels/relabel-v4-no-display \
    --id-map units/slug_dbids.json
```

These were NuExtract3 fine-tunes, one per origin, served on beast by vLLM; the decision model
replaced them. Their training rows are still what `export_rows.py` writes.

A prose dataset row that is evaluation data (val, test) or hand-labelled is never sent to a
labeller; the hand rows are the prose role model's gold set.

The labeller skips held-out prose rows and tables whose text repeats another unit's (203; the
export copies the first copy's labels). `--plain-sample 300` labels 300 of the 5,946 `plain`
tables (all `result` in the pilot) plus every one whose caption, footer or citing sentences
name an atlas, mask, seed, ROI or prior study.

Splits (`export.splits`): by article under its database id (`units/slug_dbids.json` maps the
slug ids of 270 articles, 268 found in the corpus); articles sharing a table's text share a
split; an article with a gold label (astra or hand) or a held-out prose row is test; synthetic
sets are train only.

## Synthetic sparse-role units

`synth/` builds labelling units for the sparse roles (`reference`, `localization`, `other`, the `anchor`
kinds) when real examples are scarce. Each unit is seeded from a real unit whose sets are all `result`,
rewritten into one target role with that role's cues, plus hard negatives that carry a sparse class's cue
but are another role. Units are read by `services/set_roles` (`table_context`, `prose_context`), and the
roles are study_schema's (`labels.COORDINATE_ROLES`, `ANCHOR_KINDS`); an MRS voxel is an `anchor` with
`anchor_kind` `roi`. Synthetic data is for training only: seeds, set names, captions and footers come only from units
`export.splits` puts in train, and a synthetic unit's `article_id` is `synth:<seed article>`. That is
the split the role models are evaluated on (the `split` field `export_rows` writes), so
`--id-map` is required, and must be the map the export uses: without it a slug-id article is split
under its slug and can seed a unit from the role model's val or test articles. Pass generation at least
the label dirs the export passes, gold ones (`pilot-astra`) included: an article with a gold label is
forced to test only when the gold dir is read, so a generation run without it can seed from an
article the export puts in test.

Set names, captions and footers do not follow the target. A class's names take the shapes of the real
labelled names of that class (a citation, "<region> seed", "P1", a contrast, ...) at the real rates; a
name is one of the class's own real names at the share they hold, else a real name of that shape from
any class that carries no other class's cue word (so "prior study" stays a reference name, as it is in
every real text set that has it). A table's caption and footer are those of a real labelled set of the
class, so a caption shared by several classes in real tables is shared by them here at the same rates,
except that no one real table supplies more than 5% (`role_units.SOURCE_CAP`) of a class's tables in a run:
past it a table takes the class's least-used real table, so a class of a few real tables draws them evenly;
a class with no real tables (table `other`) takes a real one that names no cue. `role_units.py` prints
the largest share one real table supplied, per field and class. A table has as many
coordinate rows as its seed, and a region's hemisphere word agrees with the sign of its x, in tables
and in new prose sentences. No two sets of a unit share a coordinate triple; a case the context
builders would misread (a real seed whose sets already share one) is redrawn from another seed and
recorded in `rejected.jsonl`.

```
# generate: units.jsonl (sets hold only name and points), truth.jsonl (the label by construction),
# rejected.jsonl (cases redrawn, with the reason)
python experiments/role_classifier/synth/role_units.py OUT_DIR --plan plan.json \
    --units TABLE_UNITS.jsonl PROSE_UNITS.jsonl --labels LABEL_DIR [LABEL_DIR ...] \
    --id-map units/slug_dbids.json [--seed 41]

# leak check: no label syntax in any text field or in what the labeller or the role model renders,
# no key a real unit does not have. Text is NFKC-normalised, stripped of zero-width characters
# and casefolded first, so "role\uff1a other", "Reference - prior study" and "role is localization"
# are caught as "role: other" is
python experiments/role_classifier/synth/leakcheck.py OUT_DIR

# name check: set names, captions and footers tell each class what real ones do, over 8+ runs
python experiments/role_classifier/synth/namecheck.py OUT_DIR_41 OUT_DIR_42 ... OUT_DIR_48 \
    --units TABLE_UNITS.jsonl PROSE_UNITS.jsonl --labels LABEL_DIR [LABEL_DIR ...] \
    --id-map units/slug_dbids.json

# rewrite pass: one codex call per unit, model gpt-6.1-sol (codex CLI; no OpenAI API endpoint).
# A rewrite that fails the leak check is refused; the whole output is leak-checked again.
python experiments/role_classifier/synth/rewrite.py OUT_DIR REWRITTEN_DIR [--workers 3] [--model gpt-6.1-sol]
```

`plan.json` is `{"table": {"reference": 20, "localization": 20, "anchor_roi_mrs": 5, "other": 20,
"stimulation_target": 10, "seed": 5, "node": 5, "hard_result": 10}, "text": {"reference": 20, ...,
"hard_anchor_prior": 5}}`. Each rng seed uses a seed unit once, so a larger set is several runs with
different `--seed` and `--id-prefix`.

The name check fits a bag-of-tokens logistic regression, class-weighted, on one field alone (a set's
name; a table's caption; its footer), in article-grouped 5-fold cross-validation, over the classes both
the generated runs and the real labelled train sets hold. Each article weighs as one per class, in
fitting and in scoring, however many sets it holds, so one paper's many tables do not make its caption
or footer the class's cue. A generated string copied from a real set is
grouped with that real article, so no model scores a string from an article it was fit on. Beside each
run it builds a null run: the same number of sets per class, each a copy of a random set of a random
real labelled article of the class (sets that share a caption in the run share one in the null). Per class it compares recall, and over
classes balanced accuracy, each the mean over the runs:

- generated sets tell their class no better than the null run's (no extra cue);
- a model fit on generated sets scores real sets no worse than one fit on the null run (no lost cue);
- a model fit on real sets scores generated sets near its real cross-validated score.

It exits 1 when a balanced accuracy is more than 0.10 (`namecheck.MAX_GAIN`) off, or a class's recall
more than 0.20 (`MAX_CLASS_SHIFT`) off. A class with fewer than 15 real sets or 15 real articles, or fewer than 8 distinct generated texts a run
(`MIN_CLASS_GEN`; a caption is one per table), is in the balanced accuracy but not held to a per-class rate; with fewer than 5 sets or 3 articles it is left out.
Both are printed (`unchecked`, `class_unchecked`), with each class's share of name shapes, the class each
real cue word leans to in generated names, and how far each number strays between runs. It needs 8 runs
(`MIN_RUNS`): the per-class numbers of 4 runs stray past the tolerances.

The rewrite keeps every number and citation verbatim and falls back to the original unit when a check
fails. `synth_prose.py` is the earlier prose-only generator (its surface forms are reused; it finds
coordinates with `services/prose_passages`). `ingestion_workflow/tests/services/test_role_synth.py`
runs small generations over several rng seeds, the leak check and the name check.

[^no-prose-role]: nu-prose-v2 returns no role.
