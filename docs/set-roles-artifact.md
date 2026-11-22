# The roles artifact

The `roles` stage decides what every coordinate set is for. It always runs, between `resolve`
(or `analyses`, when prose is off) and `space`, and `space`, `upload` and `sync` read nothing
it has not passed. This page is the shape it writes, for anything that reads a set's role.

## Where a set's role comes from

Every set's role is the answer of the decision model, and of nothing else. There is no proposal
from an extractor and no default role. The model is one encoder (RoBERTa-large, fine-tuned from
`lytang/MiniCheck-RoBERTa-Large`) with five heads, for table and prose sets alike, loaded in the
roles stage's own process (`services/set_roles/decision.py`). Each set is read through its
origin's context builder, whose `[ORIGIN]` line tells the model which it is:

| Origin | Which sets | Input |
|---|---|---|
| `table` | sets nu-v21 read from a table (`analyses`) | `services/set_roles/table_context.py` |
| `text` | sets nu-prose read from the text (`resolve`'s `prose` collection, `metadata.source == "prose"`) | `services/set_roles/prose_context.py` |

It is configured by its directory, as `experiments/role_classifier/export_decision.py` writes it:

```yaml
role_model:
  path: /data2/models/role-decision/v1/weights   # decision.json, encoder/, heads.safetensors, tokenizer/
  device: auto                                   # or cuda, cuda:1, cpu
  batch_size: 32                                 # sets per forward pass, shared by the articles under way
```

torch, transformers and safetensors are the `roles` extra (`pip install -e ".[roles]"`).

The heads answer, for each set: whether the numbers locate something in a brain or on a head;
whether they are human MNI/Talairach millimetres (`template`) or another frame (`other`: an
animal brain, a head or sensor frame, voxel indices, a surgical frame); the role
(`CoordinateRole`); an anchor's kind (`AnchorKind`); and where the coordinates come from
(`this_study`, `prior_study`, `atlas_or_database`). Each answer is its head's most probable
value, and the two binary heads decide at p >= 0.5, as the model was scored. `decision.json`
records the label orders and the context versions the training rows were built with; the stage
refuses a model whose labels are not study_schema's or whose context versions are not the ones
this code builds.

The source a set is recorded with is `<name>@<version>+<digest>`, the digest of the model
directory (its card, heads and encoder config by content, its weights by name, size and
modification time), so a model retrained into the same directory is a new source.

Before any model is asked, each set's points are checked (`services/set_roles/geometry.py`). A set
whose numbers cannot be template brain coordinates gets no role: most of its points outside the
brain box `prose_passages.plausible` checks, every value within 10 of zero, or every point the same
number three times. That catches voxel indices, latitude and longitude, positions from bregma or a
head frame, and counts read as x, y, z. Its `role` is null, `role_source` is `geometry@<version>`,
`role_reason` says which rule, and the model never sees it. A set with no readable point is the
model's.

## Artifact

`stage = "roles"`, `source = ""`.

**OK.** The payload has the upstream payload's shape, `{table_id: collection}`, where each
collection is an `AnalysisCollection` dict (`slug`, `identifier`, `coordinate_space`,
`analyses`). The stage changes two things:

1. Every analysis gains `metadata.set_role`:

   | Field | Type | Meaning |
   |---|---|---|
   | `role` | `CoordinateRole` value or null | `result`, `anchor`, `localization`, `reference`, `other`; null when the numbers are not brain coordinates (the model's answer, or the geometry check) |
   | `anchor_kind` | `AnchorKind` value or null | for an `anchor` only: `roi`, `seed`, `stimulation_target`, `node` |
   | `from_prior_study` | bool or null | the coordinates come from another publication: `coordinate_source == prior_study`, and always true for `reference`; null with a null role |
   | `space` | `template`, `other` or null | whether the coordinates are human MNI/Talairach millimetres; null with a null role |
   | `coordinate_source` | `this_study`, `prior_study`, `atlas_or_database` or null | where the coordinates come from; null with a null role |
   | `prior_study_evidence` | list of TextSpan | the citing sentences, `{start_char, end_char, text}` into the article text, or `{text}` when the sentence was not found there; empty unless `from_prior_study` |
   | `role_confidence` | float | the model's probability for what it decided: the role's (p(result) for a result), or, with a null role, that the numbers are not coordinates |
   | `role_source` | string | `geometry@<version>` for a set the geometry check refused; otherwise the model that decided, `<name>@<version>+<digest>` |
   | `role_origin` | `table` or `text` | which context builder read the set |
   | `role_reason` | string, only on a null role | the geometry rule that refused the set; absent on a model's decision |

   The values of `role` and `anchor_kind` are study_schema's enums
   (`study_schema.models.paper_parse`), and the field names are `CoordinateParse`'s.

2. Every analysis gains `metadata.held`: true for a set whose role is not `result` or `anchor`,
   or whose `space` is not `template`.
   A held set stays in the collection's `analyses`, in its place, so a set's position -- pondie's
   `table_id#ordinal` key -- never moves. `space` and stage1 keep held sets. The coordinate
   parse writes every set with a role, held or not, and neurostore stores each with its role;
   studyset releases export results only. A set whose `space` is `other` is written with
   `coordinate_space` `OTHER` (its key stays what was extracted). A set with a null role is left
   out of the parse (`paper_parse.NOT_BRAIN_REASON`, with its `role_source` and `role_reason`).

The summary is `{tables, sets, sets_by_origin, text_sets_without_local, tables_without_text, roles, held, not_brain, other_space, model, role_values, role_records, input}`:
`tables` counts collections that have an analysis (the selection gate reads it),
`text_sets_without_local` counts prose sets whose points the classifier could not find in their
passage, `tables_without_text` names each table whose sets were read without its header and rows
(no raw table file recorded, the file missing or unreadable), with the reason, `roles` counts
sets by role (an anchor by its kind; a null role is not counted), `not_brain` counts the sets the
geometry check refused, `other_space` the sets the model put in another frame, `model` names
the model that decided (null when the geometry check refused every set), `role_values` is a
digest of every set's decided `role`, `anchor_kind`, `from_prior_study`, `space`,
`coordinate_source` and
`held` (no confidence, no model), and `input` is a digest of what the stage read.

**Blocked, not failed.** When the model cannot be used (not configured, its directory without a
`decision.json`, its labels or context versions not this code's, or torch not installed), the
articles with sets are not attempted: the plan counts them as blocked, with the reason (for
example `no role model is configured (role_model)`), and logs the reason once. Nothing is
recorded for them, so no retry is used up, and they are planned on the first run with the model
configured, with no `--refresh`. Everything downstream of them stays blocked. A missing role
only ever blocks: it never takes anything back from neurostore or ns-pond. The fingerprint
includes the model, so configuring or retraining it makes the articles it reads stale.

**Stopped, not failed.** The stage stops, logging the reason, when the model cannot be loaded
(`the role model at ... cannot be loaded: ...`) or fails while running (`the role model at ...
failed: ...`, a CUDA error for instance), and on an interrupt. Nothing is recorded for the
articles under way or for the rest of the batch, no later batch of the stage is run, and the
report line reads `STOPPED: <reason>; N article(s) left pending`. The articles it answered
before the stop keep their roles. No retry is used up; the next run plans the rest again. A
model that cannot be used when the stage is planned is not a stop: the articles are blocked
with the reason, as above.

Up to 64 articles are read at once. Their sets share the model's batches (`batch_size`), so a
batch fills even when each article has one set. Each input is cut to the model's `max_length`
(512 tokens) as in training.

## Downstream

- `space` requires `roles`, and its payload is the roles payload with spaces filled in.
- `upload` and `sync` count an article as blocked unless its `roles` artifact is OK. Before
  writing, each refuses a payload with any set, held or not, that lacks `metadata.held` or a
  complete study_schema `set_role` (`sets without a role from the roles stage: ...`).
- `upload`'s fingerprint follows space's `upload_basis`: the roles stage's `input` and
  `role_values`, not the models. A retrained model re-uploads only the articles where a set's
  role changed; elsewhere neurostore keeps the earlier `role_confidence` and `role_source` until
  the article is uploaded again for another reason.
- ns-pond `stage1/analyses.json` (deprecated: read the coordinate parse) writes each analysis's role fields at its top level: `role`,
  `anchor_kind`, `from_prior_study`, `prior_study_evidence`, `role_confidence`, `role_source`,
  `role_origin`, and `held`, plus `source: "prose"` for a prose set. Held sets are written in
  their place, so the positional keys pondie derives do not move. An analysis without
  `set_role` is never written.

## Reading a role

Read `metadata.set_role` (or the analysis's fields in `parse/coordinate_parse.json`, never
stage1) for every set, table and prose
alike. Do not fall back to `result` when it is missing, and do not read `metadata.role` or a
point's `role`: resolve no longer writes them, and the prose model's own role word is not a
role.
