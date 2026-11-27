"""Check every offload spec's Hub pins against the Hub itself, read-only.

For each `configs/offload/*.json` (or the specs named): the base model at its revision, and the
adapter at its commit, with the adapter's sha256 read from the Hub's LFS metadata rather than
downloaded. A spec with merged `weights` names a directory on a cluster, which the Hub cannot
check; it is listed as unpinned. Also prints which repo the Hub serves each pin from, so a
repo that was transferred or renamed shows up as a redirect.

    HF_TOKEN=... python experiments/hf_models/check_pins.py [spec.json ...]

Exits 1 when any pin does not resolve or its sha256 differs.
"""

from __future__ import annotations

import sys
from pathlib import Path

from huggingface_hub import HfApi
from huggingface_hub.utils import HfHubHTTPError

from ingestion_workflow.services.offload.contract import load_spec
from ingestion_workflow.services.offload.remote import split_ref

ROOT = Path(__file__).resolve().parents[2]
ADAPTER = "adapter_model.safetensors"


def _resolve(api: HfApi, repo: str, revision: str):
    """(served repo id, private, license, {file: lfs sha256}), or the HTTP status."""
    try:
        info = api.model_info(repo, revision=revision, files_metadata=True)
    except HfHubHTTPError as error:
        return error.response.status_code
    card = info.card_data.to_dict() if info.card_data else {}
    files = {s.rfilename: (s.lfs.sha256 if s.lfs else None) for s in info.siblings or []}
    return info.id, info.private, card.get("license"), files


def check(api: HfApi, path: Path) -> list[str]:
    spec, problems = load_spec(path), []
    base = _resolve(api, spec.model, spec.model_revision)
    if isinstance(base, int):
        problems.append(f"base {spec.model}@{spec.model_revision}: HTTP {base}")
        print(f"| {path.name} | base | {spec.model}@{spec.model_revision[:12]} | HTTP {base} | | |")
    else:
        print(f"| {path.name} | base | {spec.model}@{spec.model_revision[:12]} | {base[0]} | "
              f"{'private' if base[1] else 'public'} | {base[2]} |")
    if spec.weights:
        print(f"| {path.name} | weights | `{spec.weights}` | not on the Hub | | |")
        return problems
    repo, commit = split_ref(spec.adapter)
    held = _resolve(api, repo, commit)
    if isinstance(held, int):
        problems.append(f"adapter {spec.adapter}: HTTP {held}")
        print(f"| {path.name} | adapter | {repo}@{commit[:12]} | HTTP {held} | | |")
        return problems
    served, private, licence, files = held
    found = files.get(ADAPTER)
    verdict = "sha256 ok" if found == spec.adapter_sha256 else f"sha256 {found}"
    if found != spec.adapter_sha256:
        problems.append(f"adapter {spec.adapter}: {verdict}, the spec names {spec.adapter_sha256}")
    moved = "" if served == repo else f" (redirected from {repo})"
    print(f"| {path.name} | adapter | {repo}@{commit[:12]} | {served}{moved}, {verdict} | "
          f"{'private' if private else 'public'} | {licence} |")
    return problems


def main(argv: list[str]) -> int:
    specs = [Path(a) for a in argv] or sorted((ROOT / "configs" / "offload").glob("*.json"))
    api = HfApi()
    print("| spec | pin | names | the Hub serves | visibility | license |")
    print("|---|---|---|---|---|---|")
    problems = [p for path in specs for p in check(api, path)]
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
