# Medical-condition normalization gold set

`gold.tsv`: 280 `groups[].medical_condition` items from the production catalog (fit `fit-eec1437aa10b`, MONDO
2026-09-01), each graded for the MONDO and the ONVOC id `normalization/medical.py` wrote. 30 items per stratum
(common, rare, healthy_control, composite, subtype, abbreviation) and 100 drawn at random from every item, seed
20261127.

Grades, against the vocabulary release the run used:

| grade | meaning |
|---|---|
| E | the finest correct term the vocabulary has |
| A | correct, but broader or narrower than the best available term |
| P | a composite with some conditions placed and some lost |
| W | names a different condition |
| Um | unmapped, and a fitting term exists |
| Uc | unmapped, and none fits (risk states, procedures, vague strings) |
| X | absent sentinel, correct |
| Xm | a healthy group read as an unmapped condition |

`failure_class` names the cause of every grade other than E and X.

On beast, with the normalize venv (the `sensory-renorm` run's `env.sh`):

```bash
python -I survey.py 12          # groups.jsonl from the catalog, read-only
python sample.py                # items.json and the stratified sample
python score.py gold.tsv        # counts, precision and recall
```

To measure a linker change, run the `text` column through `probe.py heads.json` (triage, MONDO link and ONVOC bridge,
with each head's top candidates) under the old and new code, and re-grade only the rows whose ids changed.
Findings: the PR that added this directory.
