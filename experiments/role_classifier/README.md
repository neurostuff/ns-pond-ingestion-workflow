# Set-role classifier (X1), draft

What each coordinate set is for -- a result of this study, a region it defined, a peak quoted
from another study, a display position -- decided by a small fine-tuned encoder that reads the
set's context. Designed as its own ingestion stage, `roles`, after `resolve` and before
`space`. This directory is the input builder and label scheme only: there is no training
code, no training data and no trained model yet, and nothing in the pipeline imports it.

```
set_roles/
  labels.py      the label scheme, and how a prediction becomes a recorded role
  context.py     the input: one tagged string per coordinate set
  classifier.py  when a prediction overrides the proposed role
  model.py       the encoder with two heads (torch/transformers, imported lazily)
```

## Labels

One label per set, from `ROLE_LABELS` in this order (it is the classifier head's output
order: append, never reorder). The label is study_schema's `CoordinateRole`, with the
`AnchorKind` folded in for anchors:

| Label | Meaning | Uploaded to neurostore |
|---|---|---|
| `result` | A finding of this study: peaks of a tested effect | yes |
| `anchor:roi` | A region of interest the study defined or used | yes, as a labelled set, not an analysis with statistics |
| `anchor:seed` | A seed for connectivity or PPI | yes, as above |
| `anchor:stimulation_target` | A TMS/tDCS/DBS target | yes, as above |
| `anchor:node` | A network node or parcel centre | yes, as above |
| `localization` | Where electrodes or sources were placed | no |
| `reference` | Coordinates quoted from another publication for comparison | no |
| `display` | A slice or crosshair position for a figure | no |
| `other` | None of the above | no |

A second, independent answer is `from_prior_study`: whether the coordinates come from
another publication. A seed taken from a meta-analysis is `anchor:seed` and
`from_prior_study: true`. The model has a separate binary head for it.

The prose model's roles map onto labels as `result` -> `result`, `roi` -> `anchor:roi`,
`seed` -> `anchor:seed`, `target` -> `anchor:stimulation_target`, `prior_study` ->
`reference`, `figure` -> `display`, `other` -> `other` (`label_from_prose`).

Decision rule (`classifier.decide`): a table set is proposed `result` and a prose set its prose
model's role. The classifier overrides the proposal only when its top probability is at least
`min_confidence` (the design sets 0.8), because a wrong override is costly -- a real result
relabelled `reference` is not uploaded. `from_prior_study` is set when the flag head reaches
`prior_threshold` (0.5), and always for a `reference`. Each decision is
recorded with its confidence, its source (`proposal` or the model's name and version) and the
label it overrode.

## Input format

`context.serialize(SetContext)` turns one set into one string. The short structured fields
come first, so that truncating to the encoder's length (`MAX_CHARS = 2400`, about 512
tokens) cuts free text and never structure. The same function builds training and inference
inputs. Its version is `CONTEXT_VERSION = 1`; bump it whenever the serialisation changes,
because a model trained on one version must not read another.

```
[ORIGIN] table|text
[PROPOSED] <label proposed before the classifier>
[POINTS] n=<count> stats=<kind:count,...> valued=<with a statistic>/<n> negative=<n>
         clusters=<n> subpeaks=<n> seeds=<n> mirrored=<n> integral=<n> spread=<mm>mm
[CUES] citations=<n> prior_words=<n> anchor_words=<n> display_words=<n>
[NAME] <analysis name, 200 chars>
[DESCRIPTION] <analysis description, 200 chars; omitted when empty>
-- table sets:
[TABLE] <"Table 2", 40>  [CAPTION] <500>  [FOOTER] <400>  [CITED] <a sentence citing the table, 300; up to 3>
-- text sets:
[HEADING] <120>  [PASSAGE] <900>  [BEFORE] <400>  [AFTER] <400>
```

It is one line; it is wrapped here only for reading. Empty fields are dropped. For example:

```
[ORIGIN] table [PROPOSED] result [POINTS] n=2 stats=none:2 valued=0/2 negative=0 clusters=0 subpeaks=0 seeds=0 mirrored=2 integral=2 spread=24mm [CUES] citations=0 prior_words=0 anchor_words=3 display_words=0 [NAME] ROIs for PPI [TABLE] Table 2 [CAPTION] Regions of interest used for the PPI analysis [CITED] As shown in Table 2, seeds were placed bilaterally.
```

`[POINTS]` describes the shape of the points, which is what separates a list of ROI centres
from a list of peaks: peaks carry statistics and cluster sizes, while ROI centres come in
fewer, have integer coordinates, and are often printed as left/right mirror pairs
(`mirrored`). `[CUES]` counts citation markers and words that signal a prior study, an anchor
or a display position, across all the text fields.

`context.contexts_for(payload, passages, article_text)` yields `(table_id, analysis_index,
SetContext)` for every analysis of the current analyses payload. That payload is a mapping
`table_id -> {"analyses": [...]}` whose analyses carry `name`, `description`,
`table_caption`, `table_footer`, `metadata.table_metadata.table_label`, and `coordinates`
(points with `x`, `y`, `z`, `statistic_type`, `statistic_value`, `cluster_size`,
`is_subpeak`, `is_seed`). Prose analyses are marked `metadata.source == "prose"` and name
their passages by index in `metadata.passages`. `article_text` supplies the sentences that
cite a table.

## Known gaps

- It reads the analyses payload ingestion writes today, not study_schema's `CoordinateParse`.
  Under CoordinateParse 0.5.0 the point-level `is_seed` flag is retired (a seed is a set's
  role), so the `seeds=` count and the field names will change when the input moves to the
  parse. That is a `CONTEXT_VERSION` bump.
- The citing sentences are found with a sentence regex and a table-label match. Once the
  parsed paper carries citations and their sentences, read them from there.
- No training data yet. The plan is LLM-labelled and curated sets plus synthetic hard
  cases, with the hard cases over-represented.
