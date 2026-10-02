"""Serve the fine-tuned extractor, with the arguments derived rather than recalled.

Every launch of this server used to be typed out by hand, and each hand-typed
launch cost something: a context length that disagreed with the client's, a
`PATH` without the venv that holds `ninja` (the engine core then dies with a
bare "No such file or directory"), a `pkill -f "vllm serve"` that matched
nothing because vLLM renames its workers to `VLLM::Worker_TP0`, and a restart
ordered after a merge rather than before it, which handed PEFT four busy cards
and a `cudaErrorMemoryAllocation`.

None of those are hard. They are simply not things to remember, so they are
written down once, here, and computed from what the rest of the system already
declares:

* the served model name is `settings.llm_model`, the name the client asks for;
* the host and port are `settings.llm_api_base`, the address the client calls;
* `--max-model-len` is `CoordinateParsingClient.CONTEXT_WINDOW`, the window the
  client subtracts its output budget from -- a server narrower than that turns
  the budget arithmetic into 400s on exactly the longest tables;
* the parallelism is planned from the weights and the cards actually present.

`check` closes the loop from the other side: it asks a running server what it
is serving and refuses the run when that disagrees with the client.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
from ingestion_workflow.config import Settings

logger = logging.getLogger(__name__)

__all__ = [
    "ServerPlan",
    "plan",
    "plan_parallelism",
    "start",
    "stop",
    "check",
    "wait_until_ready",
    "ServerMismatch",
]

GIB = 1024 ** 3

#: What the paged attention pool needs beyond the weights for one card to be
#: worth using at all. The hybrid checkpoint keeps a key/value cache for only
#: its eight full-attention layers, so this is a floor, not an estimate.
KV_FLOOR = 1 * GIB

#: Below this much free per card after the weights, the engine must be told
#: to expect a small batch. vLLM reserves CUDA graph memory in proportion to
#: the batch it is promised and takes the KV cache from what is left over, so
#: on a tight card the defaults reserve everything and it dies at startup with
#: "No available memory for the cache blocks" -- measured here on 8 GiB cards
#: holding 3.8 GiB of sharded weights, where graphs alone wanted 2.43 GiB.
TIGHT_HEADROOM = 4 * GIB

#: vLLM is told to take this share of each card. The rest is the CUDA context,
#: the activations and the fragmentation that `expandable_segments` does not
#: recover.
GPU_UTILISATION = 0.92


class ServerMismatch(RuntimeError):
    """The running server is not the one the client is configured to talk to.

    Raised rather than logged. A benchmark that silently measured the model
    left over from the previous run is worse than one that does not start.
    """


@dataclass(frozen=True)
class ServerPlan:
    """A launch, fully resolved. Nothing below this is decided at run time."""

    weights: Path
    served_name: str
    host: str
    port: int
    max_model_len: int
    tensor_parallel: int
    data_parallel: int
    max_num_seqs: int
    max_num_batched_tokens: int
    gpus: Sequence[int]
    vllm_bin: Path

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}/v1"

    def command(self) -> List[str]:
        return [
            str(self.vllm_bin), "serve", str(self.weights),
            "--served-model-name", self.served_name,
            "--host", self.host,
            "--port", str(self.port),
            "--max-model-len", str(self.max_model_len),
            "--gpu-memory-utilization", str(GPU_UTILISATION),
            "--tensor-parallel-size", str(self.tensor_parallel),
            "--data-parallel-size", str(self.data_parallel),
            "--max-num-seqs", str(self.max_num_seqs),
            "--max-num-batched-tokens", str(self.max_num_batched_tokens),
            # The checkpoint carries a vision tower this workflow never sends
            # an image to; loading it costs weights these cards cannot spare.
            "--language-model-only",
            # A state-space layer carries its whole history in a recurrent
            # state, so there is no prefix to reuse the way there is for
            # attention. Leaving it on buys nothing and is not sound here.
            "--no-enable-prefix-caching",
        ]

    def environment(self) -> Dict[str, str]:
        env = dict(os.environ)
        # `ninja` ships in the vLLM venv and is not on the default PATH. The
        # engine core compiles at startup and dies with a bare ENOENT without
        # it, several screens above anything that mentions ninja.
        env["PATH"] = f"{self.vllm_bin.parent}:{env.get('PATH', '')}"
        env["CUDA_VISIBLE_DEVICES"] = ",".join(str(g) for g in self.gpus)
        env["VLLM_DISABLE_COMPILE_CACHE"] = "1"
        return env


def plan_parallelism(
    weight_bytes: int,
    gpu_bytes: int,
    gpu_count: int,
    *,
    kv_floor: int = KV_FLOOR,
    utilisation: float = GPU_UTILISATION,
) -> Tuple[int, int]:
    """Split the cards between sharding the weights and running copies.

    Tensor parallelism is a cost: every layer boundary becomes a collective.
    It is paid only when the weights do not otherwise fit, so this takes the
    *smallest* shard count that leaves each card room for its weights and a
    usable cache, and spends whatever is left on data parallelism.

    On four 8 GiB cards holding 7.6 GiB of weights that is 2 and 2, which is
    what was being typed by hand. The arithmetic is here so that it stays
    right on a machine with different cards.
    """
    if gpu_count < 1:
        raise ValueError("no GPUs to serve on")
    usable = int(gpu_bytes * utilisation) - kv_floor
    for shards in range(1, gpu_count + 1):
        if gpu_count % shards:
            continue
        if weight_bytes / shards <= usable:
            return shards, gpu_count // shards
    raise ServerMismatch(
        f"{weight_bytes / GIB:.1f} GiB of weights do not fit across "
        f"{gpu_count} x {gpu_bytes / GIB:.1f} GiB, even fully sharded"
    )


def plan_batching(headroom_bytes: int, window: int) -> Tuple[int, int]:
    """How large a batch to promise, given what the weights left on each card.

    Not a throughput knob. The promise is what vLLM reserves against, so a
    generous one on a tight card leaves nothing for the KV cache and the
    engine refuses to start -- which is how the comparison run was lost. The
    prefill is chunked, so a chunk of half the window still admits a document
    that fills it.
    """
    if headroom_bytes >= TIGHT_HEADROOM:
        return 256, window
    return 48, max(2048, window // 2)


def _weight_bytes(weights: Path) -> int:
    total = sum(f.stat().st_size for f in weights.glob("*.safetensors"))
    if not total:
        total = sum(f.stat().st_size for f in weights.glob("*.bin"))
    if not total:
        raise FileNotFoundError(f"no model weights under {weights}")
    return total


def _gpus() -> Tuple[List[int], int]:
    """The cards present, and the smallest one's memory.

    Sized by the smallest because the weights are sharded evenly: a mixed
    machine is bounded by its weakest card, not its average.
    """
    out = subprocess.run(
        ["nvidia-smi", "--query-gpu=index,memory.total", "--format=csv,noheader,nounits"],
        capture_output=True, text=True, check=True,
    ).stdout.strip().splitlines()
    cards = [line.split(",") for line in out if line.strip()]
    indexes = [int(index) for index, _ in cards]
    smallest = min(int(total) for _, total in cards) * 1024 * 1024
    return indexes, smallest


def plan(settings: Settings, *, weights: Optional[Path] = None) -> ServerPlan:
    """Resolve the launch from the settings the client already reads."""
    path = Path(weights or settings.extractor_weights or "").expanduser()
    if not path.is_dir():
        raise FileNotFoundError(
            "set extractor_weights to the merged model directory to serve "
            f"(got {path or 'nothing'})"
        )
    binary = settings.vllm_bin or shutil.which("vllm")
    if not binary:
        raise FileNotFoundError(
            "no vllm executable: set vllm_bin, or put one on PATH"
        )
    url = urlparse(settings.llm_api_base or "http://127.0.0.1:8000/v1")
    indexes, gpu_bytes = _gpus()
    if settings.extractor_gpus:
        wanted = {int(g) for g in settings.extractor_gpus.split(",")}
        indexes = [i for i in indexes if i in wanted]
    weight_bytes = _weight_bytes(path)
    tensor, data = plan_parallelism(weight_bytes, gpu_bytes, len(indexes))
    window = CoordinateParsingClient.CONTEXT_WINDOW
    headroom = int(gpu_bytes * GPU_UTILISATION) - weight_bytes // tensor
    seqs, batched = plan_batching(headroom, window)
    return ServerPlan(
        weights=path,
        served_name=settings.llm_model,
        host=url.hostname or "127.0.0.1",
        port=url.port or 8000,
        # The client subtracts its output budget from this number. A server
        # with a shorter window refuses the request outright rather than
        # truncating it, and does so on the longest tables -- the ones worth
        # reading.
        max_model_len=window,
        tensor_parallel=tensor,
        data_parallel=data,
        max_num_seqs=seqs,
        max_num_batched_tokens=batched,
        gpus=indexes,
        vllm_bin=Path(binary),
    )


def stop(*, grace: float = 10.0) -> None:
    """Stop any vLLM on this machine, workers included.

    The obvious `pkill -f "vllm serve"` finds the launcher and misses the
    engine cores and workers, which rename themselves to `VLLM::Worker_TP0`
    and keep the cards. Everything downstream then fails on memory that
    nothing appears to be using.

    Call this *before* merging an adapter, not after: PEFT needs the cards the
    old server is still holding.
    """
    patterns = ["VLLM::", "vllm serve"]
    for sig in (signal.SIGTERM, signal.SIGKILL):
        alive = False
        for pattern in patterns:
            found = subprocess.run(["pkill", f"-{sig.name[3:]}", "-f", pattern])
            alive = alive or found.returncode == 0
        if not alive:
            return
        time.sleep(grace if sig is signal.SIGTERM else 3.0)


def wait_until_ready(plan: ServerPlan, *, timeout: float = 900.0) -> None:
    """Block until the server answers, then confirm it is the right one."""
    import httpx

    deadline = time.monotonic() + timeout
    last: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{plan.base_url}/models", timeout=5.0)
            if response.status_code == 200:
                _assert_serves(plan, response.json())
                return
        except Exception as exc:  # noqa: BLE001 -- not up yet is the common case
            last = exc
        time.sleep(5.0)
    raise ServerMismatch(f"server never came up at {plan.base_url}: {last}")


def _assert_serves(plan: ServerPlan, payload: dict) -> None:
    models = {m.get("id"): m for m in payload.get("data", [])}
    if plan.served_name not in models:
        raise ServerMismatch(
            f"{plan.base_url} serves {sorted(models)}, and the client asks for "
            f"{plan.served_name!r}"
        )
    window = models[plan.served_name].get("max_model_len")
    if window is not None and window < plan.max_model_len:
        raise ServerMismatch(
            f"{plan.served_name} is served with a {window} token window and the "
            f"client computes its output budget against {plan.max_model_len}"
        )


def check(settings: Settings) -> str:
    """Confirm a live server matches the configured client, or raise.

    Cheap enough to call before a long run, and the one thing that would have
    caught a benchmark pointed at the previous checkpoint.
    """
    import httpx

    url = urlparse(settings.llm_api_base or "")
    base = f"http://{url.hostname or '127.0.0.1'}:{url.port or 8000}/v1"
    payload = httpx.get(f"{base}/models", timeout=5.0).json()
    models = {m.get("id"): m for m in payload.get("data", [])}
    if settings.llm_model not in models:
        raise ServerMismatch(
            f"{base} serves {sorted(models)}, and llm_model is "
            f"{settings.llm_model!r}"
        )
    window = models[settings.llm_model].get("max_model_len")
    if window is not None and window < CoordinateParsingClient.CONTEXT_WINDOW:
        raise ServerMismatch(
            f"{settings.llm_model} is served with a {window} token window and "
            f"the client computes against {CoordinateParsingClient.CONTEXT_WINDOW}"
        )
    return (
        f"{settings.llm_model} at {base}, window {window}, "
        f"weights {models[settings.llm_model].get('root')}"
    )


def start(
    settings: Settings,
    *,
    weights: Optional[Path] = None,
    log_file: Optional[Path] = None,
    wait: bool = True,
) -> ServerPlan:
    """Stop whatever is serving, launch the planned server, wait for it."""
    resolved = plan(settings, weights=weights)
    stop()
    destination = Path(log_file or f"/tmp/vllm-{resolved.served_name}.log")
    destination.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "serving %s as %s on %d card(s), tp=%d dp=%d, window %d -> %s",
        resolved.weights, resolved.served_name, len(resolved.gpus),
        resolved.tensor_parallel, resolved.data_parallel,
        resolved.max_model_len, destination,
    )
    with destination.open("wb") as handle:
        subprocess.Popen(
            resolved.command(),
            env=resolved.environment(),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    if wait:
        wait_until_ready(resolved)
    return resolved
