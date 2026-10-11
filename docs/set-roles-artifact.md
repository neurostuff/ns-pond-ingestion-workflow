# The roles artifact

The `roles` stage decides what every coordinate set is for. It always runs, between `resolve`
(or `analyses`, when prose is off) and `space`, and `space`, `upload` and `sync` read nothing
it has not passed. This page is the shape it writes, for anything that reads a set's role.

## Where a set's role comes from

Every set's role is the answer of a fine-tuned classifier of the set's origin, and of nothing
else. There is no proposal from an extractor and no default role.

| Origin | Which sets | Setting | Input |
|---|---|---|---|
| `table` | sets nu-v21 read from a table (`analyses`) | `role_model_table` | `services/set_roles/table_context.py` |
| `text` | sets nu-prose read from the text (`resolve`'s `prose` collection, `metadata.source == "prose"`) | `role_model_prose` | `services/set_roles/prose_context.py` |

Each model directory records the origin it reads and that origin's context version
(`set_roles.json`: `origin`, `context_version`). The stage refuses a model for another origin
or another context version.

## Artifact

`stage = "roles"`, `source = ""`.

**OK.** The payload has the upstream payload's shape, `{table_id: collection}`, where each
collection is an `AnalysisCollection` dict (`slug`, `identifier`, `coordinate_space`,
`analyses`). The stage changes two things:

1. Every analysis gains `metadata.set_role`:

   | Field | Type | Meaning |
   |---|---|---|
   | `role` | `CoordinateRole` value or null | `result`, `anchor`, `localization`, `reference`, `other`; null when the numbers are not brain coordinates |
   | `anchor_kind` | `AnchorKind` value or null | for an `anchor` only: `roi`, `seed`, `stimulation_target`, `node` |
   | `from_prior_study` | bool | the coordinates come from another publication (always true for `reference`) |
   | `prior_study_evidence` | list of TextSpan | the citing sentences, `{start_char, end_char, text}` into the article text, or `{text}` when the sentence was not found there; empty unless `from_prior_study` |
   | `role_confidence` | float | the classifier's probability for what it decided: the role's, or for a null role that the numbers are not coordinates |
   | `role_source` | string | the model that decided, `name@version` |
   | `role_origin` | `table` or `text` | which of the two models read the set |

   The values of `role` and `anchor_kind` are study_schema's enums
   (`study_schema.models.paper_parse`), and the field names are `CoordinateParse`'s.

2. Every analysis gains `metadata.held`: true for a set whose role is not uploaded (anything
   but `result` and `anchor`). A held set stays in the collection's `analyses`, in its place, so
   a set's position -- pondie's `table_id#ordinal` key -- never moves. `upload` leaves held sets
   out (`roles.uploaded_sets`); `space` and stage1 keep them.

The summary is `{tables, sets, sets_by_origin, roles, held, sources, role_values, input}`:
`tables` counts collections that have an analysis (the selection gate reads it), `roles` counts
sets by role (an anchor by its kind, a null role as `not_coordinates`), `sources` names each
origin's model, `role_values` is a digest of every set's decided `role`, `anchor_kind`,
`from_prior_study` and `held` (no confidence, no model), and `input` is a digest of what the
stage read.

**Blocked, not failed.** When a set's origin has no usable model (not configured, missing, for
another origin, or built on another context version), the articles with sets of that origin are
not attempted: the plan counts them as blocked, with the reason (for example
`no prose role model is configured (role_model_prose)`), and logs the reason once. Nothing is
recorded for them, so no retry is used up, and they are planned on the first run with the model
configured, with no `--refresh`. Everything downstream of them stays blocked. A missing role
only ever blocks: it never takes anything back from neurostore or ns-pond. The fingerprint
includes each origin's model, so configuring or retraining one makes the articles it reads stale.

## Downstream

- `space` requires `roles`, and its payload is the roles payload with spaces filled in.
- `upload` and `sync` count an article as blocked unless its `roles` artifact is OK. Before
  writing, each refuses a payload with any set, held or not, that lacks `metadata.held` or a
  complete study_schema `set_role` (`sets without a role from the roles stage: ...`).
- `upload`'s fingerprint follows space's `upload_basis`: the roles stage's `input` and
  `role_values`, not the models. A retrained model re-uploads only the articles where a set's
  role changed; elsewhere neurostore keeps the earlier `role_confidence` and `role_source` until
  the article is uploaded again for another reason.
- ns-pond `stage1/analyses.json` writes each analysis's role fields at its top level: `role`,
  `anchor_kind`, `from_prior_study`, `prior_study_evidence`, `role_confidence`, `role_source`,
  `role_origin`, and `held`, plus `source: "prose"` for a prose set. Held sets are written in
  their place, so the positional keys pondie derives do not move. An analysis without
  `set_role` is never written.

## Reading a role

Read `metadata.set_role` (or the top-level fields in stage1) for every set, table and prose
alike. Do not fall back to `result` when it is missing, and do not read `metadata.role` or a
point's `role`: resolve no longer writes them, and the prose model's own role word is not a
role.
