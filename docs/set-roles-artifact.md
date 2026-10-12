# The roles artifact

The `roles` stage decides what every coordinate set is for. It always runs, between `resolve`
(or `analyses`, when prose is off) and `space`, and `space`, `upload` and `sync` read nothing
it has not passed. This page is the shape it writes, for anything that reads a set's role.

## Where a set's role comes from

Every set's role is the answer of its origin's role model, and of nothing else. There is no
proposal from an extractor and no default role. The role models are NuExtract3 fine-tunes, one
per origin, served by vLLM.

| Origin | Which sets | Setting | Input |
|---|---|---|---|
| `table` | sets nu-v21 read from a table (`analyses`) | `role_model_table` | `services/set_roles/table_context.py` |
| `text` | sets nu-prose read from the text (`resolve`'s `prose` collection, `metadata.source == "prose"`) | `role_model_prose` | `services/set_roles/prose_context.py` |

Each is configured by its server and served model name:

```yaml
role_model_table:
  endpoint: http://localhost:8372/v1   # OpenAI-compatible server, up to /v1
  model: role-table                    # its served model name
```

Each set is one request, built as the models were trained and evaluated
(`services/set_roles/served_prompt.py`: the instruction, template and JSON schema, and the context
versions the training rows were built with). The answer is decoded under that JSON schema, so its
role is always one of study_schema's `CoordinateRole` values. The role is the model's answer, and
its probability is read from the logprobs of the answer's first role token, renormalised over the
tokens that begin exactly one role. The models give no "not coordinates" answer and no
prior-study flag: every set gets a role, and `from_prior_study` is true for a `reference` only.

The stage refuses the prompt when its context versions are not the ones this code builds, and a
server that does not list the model.

## Artifact

`stage = "roles"`, `source = ""`.

**OK.** The payload has the upstream payload's shape, `{table_id: collection}`, where each
collection is an `AnalysisCollection` dict (`slug`, `identifier`, `coordinate_space`,
`analyses`). The stage changes two things:

1. Every analysis gains `metadata.set_role`:

   | Field | Type | Meaning |
   |---|---|---|
   | `role` | `CoordinateRole` value | `result`, `anchor`, `localization`, `reference`, `other` |
   | `anchor_kind` | `AnchorKind` value or null | for an `anchor` only: `roi`, `seed`, `stimulation_target`, `node` |
   | `from_prior_study` | bool | the coordinates come from another publication (always true for `reference`) |
   | `prior_study_evidence` | list of TextSpan | the citing sentences, `{start_char, end_char, text}` into the article text, or `{text}` when the sentence was not found there; empty unless `from_prior_study` |
   | `role_confidence` | float | the model's probability for the role it answered, at the role's first token: p(result) for a result |
   | `role_source` | string | the model that decided, `<served model name>@<prompt version>+<checkpoint digest>`: the digest of the path `/v1/models` gives as the model's `root` (and, when that path is readable, its config, weights index and weight files) |
   | `role_origin` | `table` or `text` | which of the two models read the set |

   The values of `role` and `anchor_kind` are study_schema's enums
   (`study_schema.models.paper_parse`), and the field names are `CoordinateParse`'s.

2. Every analysis gains `metadata.held`: true for a set whose role is not uploaded (anything
   but `result` and `anchor`). A held set stays in the collection's `analyses`, in its place, so
   a set's position -- pondie's `table_id#ordinal` key -- never moves. `upload` leaves held sets
   out (`roles.uploaded_sets`); `space` and stage1 keep them.

The summary is `{tables, sets, sets_by_origin, text_sets_without_local, tables_without_text, roles, held, sources, role_values, input}`:
`tables` counts collections that have an analysis (the selection gate reads it),
`text_sets_without_local` counts prose sets whose points the classifier could not find in their
passage, `tables_without_text` names each table whose sets were read without its header and rows
(no raw table file recorded, the file missing or unreadable), with the reason, `roles` counts
sets by role (an anchor by its kind), `sources` names each
origin's model, `role_values` is a digest of every set's decided `role`, `anchor_kind`,
`from_prior_study` and `held` (no confidence, no model), and `input` is a digest of what the
stage read.

**Blocked, not failed.** When a set's origin has no usable model (not configured, its server
not listing it, or its prompt built on another context version), the articles with sets of that
origin are not attempted: the plan counts them as blocked, with the reason (for example
`no prose role model is configured (role_model_prose)`), and logs the reason once. Nothing is
recorded for them, so no retry is used up, and they are planned on the first run with the model
configured, with no `--refresh`. Everything downstream of them stays blocked. A missing role
only ever blocks: it never takes anything back from neurostore or ns-pond. The fingerprint
includes each origin's model, so configuring or retraining one makes the articles it reads stale.

**Stopped, not failed.** The stage stops, logging the reason, when a model's server is down or
is configured so it cannot answer the stage's requests:

- it still times out or answers 5xx after 3 tries, and no longer lists the model;
- it refuses a request (HTTP 4xx other than 429), for example a context longer than its
  `max_model_len` (`configuration error: ... answered HTTP 400 ...`);
- an answer is not the JSON schema's, because the server did not apply `structured_outputs`
  or cut the answer short (`configuration error: the answer ... is not the role schema's ...`);
- an answer has no role-token logprobs.

Nothing is recorded for the article it was reading or for the rest of the batch, and the plan
blocks the run's remaining articles of that origin with the reason (`the table role model
stopped this run: ...`). No retry is used up; the next run plans them again. A set's context is
at most 2,400 characters (`MAX_CHARS`), so a request with its answer is at most 10,006 tokens:
serve the models with a `max_model_len` of at least that.

**Failed.** Two answers fail the set's article, with its summary's `unanswered` naming each such
set (`table_id#index`) with its reason, and the stage goes on:

- the server, still listing the model, times out or answers 5xx for that input on every try
  (`server error on this input after 3 tries: ...`);
- the model answers `anchor` with a null `anchor_kind`, which the schema allows.

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
