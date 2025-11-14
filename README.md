# Ingestion Workflow

This repository specifies the ingestion pipeline
for adding new studies to neurostore.

It is modular in design, the main file being
orchastrator.py which calls up services to:
1. find articles
2. download articles
3. extract tables from articles
4. create analyses from tables
5. upload studies and analyses to neurostore
6. syncronize neurostore base-study ids with ns-pond

## Design Principles
- re-using existing code from dependencies where possible
- being DRY and modular
- using batching parallel processing for cpu bound tasks
- using batching when calling external APIs whenever possible (only have function signatures for batch calls, not single calls)

## Running the Workflow

### CLI quickstart (`ingest`)
1. Install the project (editable mode keeps imports up to date): `python -m venv .venv && source .venv/bin/activate && pip install -e .[test]`.
2. Provide credentials via environment variables or a YAML config file. Common ones include `PUBMED_EMAIL`, `SEMANTIC_SCHOLAR_API_KEY`, `OPENALEX_EMAIL`, `NEUROSTORE_TOKEN`, and `LLM_API_KEY`. Any option in `ingestion_workflow.config.Settings` can live in the YAML file.
3. Use the Typer-powered CLI, exposed as the `ingest` command (see `ingest --help` for the full tree):
   - Full pipeline (respects configured stages):  
     `ingest run --config configs/pipeline.yaml`
   - Run only certain stages:  
     `ingest run --config configs/pipeline.yaml --stages gather download extract`
   - Seed identifiers:  
     `ingest search --config configs/pipeline.yaml --query "pain AND fmri" --start-year 2015`
   - Reuse a cached manifest for downloads/extraction:  
     `ingest download --manifest data/manifests/2024-06-ids.json`
   - Kick off extraction on cached downloads:  
     `ingest extract --manifest data/manifests/2024-06-ids.json`
   - Turn bundles into analyses artifacts (writes JSON or stdout):  
     `ingest create-analyses bundles/latest.json --output data/analyses/latest.json`

Helpful flags:
- `--use-cached-inputs/--no-use-cached-inputs` lets you control whether a skipped stage hydrates its inputs from cache.
- `--manifest` lets you bypass the gather stage entirely once you have a saved identifiers file.
- `--config` can point at any YAML file; relative paths resolve from the repo root.

Outputs land under the configured `data_root` (defaults to `./data`), with logs in `data/logs`, manifests in `data/manifests`, cached payloads in `.cache`, and optional exports mirrored under `data/export`.

### Working in a Python REPL
If you prefer to script or poke at intermediate artifacts interactively, you can drive the same workflow objects directly:

```python
>>> from pathlib import Path
>>> from ingestion_workflow.config import load_settings
>>> from ingestion_workflow.workflow.orchastrator import run_pipeline
>>> settings = load_settings(Path("configs/pipeline.yaml"))
>>> settings = settings.merge_overrides({"stages": ["gather", "download", "extract"]})
>>> state = run_pipeline(settings=settings)
>>> len(state.identifiers.identifiers)
42
```

Tips for REPL work:
- `load_settings()` already merges env vars, YAML, and ad-hoc overrides, so you can tweak behavior without editing files.
- `run_pipeline` returns a `PipelineState` dataclass (`identifiers`, `downloads`, `bundles`, `analyses`, plus per-stage metrics), making it easy to inspect what happened:  
  `state.stage_metrics["download"].cache_hits`
- Need just one stage? Import the helpers directly, e.g.:

```python
>>> from ingestion_workflow.workflow.download import run_downloads
>>> downloads = run_downloads(state.identifiers, settings=settings)
```

This keeps REPL explorations in sync with the exact logic the CLI uses while giving you flexibility to experiment or prototype new stages.

## Issues


- improve handling of inputs that have failed before and whether to retry them
- invalidating cache for other stages (not just download)
- implement upload stage
- implement ns-pond syncronization stage
