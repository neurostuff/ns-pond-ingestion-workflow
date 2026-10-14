"""The role models: NuExtract3 fine-tunes served by vLLM, one request per set.

The request is the one the models were trained and evaluated with
(`served_prompt.PROMPT`), greedy, with the answer decoded under the prompt's
JSON schema. The probability of each role is read from the first token of the
role value: the top 20 alternatives at that position, each counted for the one
role its text can begin, renormalised over the tokens that begin exactly one role.

`ModelUnavailable` stops the stage, and the classifier stays stopped
without asking again: the server is down, or it answers in a way only its
configuration explains (it refuses a request, or an answer is not the
schema's). A server that times out or answers 5xx is asked again; when it
still does not answer but lists the model, it is up and fails on this input
alone, which is that set's `Unanswered` reason. A set is never given a default role.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

import httpx

from ingestion_workflow.clients.model_server import ModelUnavailable
from .classifier import Prediction, Unanswered
from .served_prompt import PROMPT

#: Where the role value starts in the answer, whitespace removed.
_ROLE_KEY = '"role":"'
#: Alternatives read at the role token, as the models were scored.
TOP_LOGPROBS = 20
#: Room for the answer, which is about 15 tokens (`{"role": "anchor", "anchor_kind": "seed"}`).
MAX_COMPLETION_TOKENS = 64
#: Seconds to wait for one answer, tries per set, and sets asked at once.
TIMEOUT, ATTEMPTS, WORKERS = 120.0, 3, 16
CONFIGURATION = "configuration error:"


def _squeeze(text: str) -> str:
    return "".join(text.split())


def role_distribution(
    content: Optional[Sequence[Mapping[str, Any]]], roles: Sequence[str]
) -> Optional[Dict[str, float]]:
    """Each role's probability at the first token of the role value, or None if it is not found.

    `content` is the answer's `logprobs.content`. When one token both closes
    the key's quote and begins the value, its own alternatives are read.
    """
    toks = list(content or [])
    before = ""
    for k, tok in enumerate(toks):
        after = before + _squeeze(tok.get("token") or "")
        if _ROLE_KEY in after and _ROLE_KEY not in before:
            if after.index(_ROLE_KEY) + len(_ROLE_KEY) < len(after):
                alternatives, base = tok.get("top_logprobs") or [], before
            elif k + 1 < len(toks):
                alternatives, base = toks[k + 1].get("top_logprobs") or [], after
            else:
                return None
            mass = dict.fromkeys(roles, 0.0)
            for alt in alternatives:
                logprob = alt.get("logprob")
                p = math.exp(logprob) if logprob is not None and logprob > -1e30 else 0.0
                full = base + _squeeze(alt.get("token") or "")
                value = full[full.index(_ROLE_KEY) + len(_ROLE_KEY):] if _ROLE_KEY in full else ""
                value = value.split('"', 1)[0]
                hits = [r for r in roles if value and r.startswith(value)]
                if len(hits) == 1:
                    mass[hits[0]] += p
            total = sum(mass.values())
            if total <= 0:
                return None
            return {r: mass[r] / total for r in roles}
        before = after
    return None


def read_answer(choice: Mapping[str, Any]) -> Union[Prediction, Unanswered]:
    """The prediction in one completion choice.

    The answer is decoded under `PROMPT`'s JSON schema, so its role is always
    one of the vocabulary's, and its kind (an anchor's only) one of the schema's or
    null. An answer that is not -- cut short, not JSON, or a kind off the list -- means
    the server did not decode it as the stage asks: `ModelUnavailable`. The
    schema lets an anchor's kind be null, and such an anchor is recorded as it is.
    """
    raw = (choice.get("message") or {}).get("content") or ""
    try:
        answer = json.loads(raw)
    except ValueError:
        answer = None
    role = answer.get("role") if isinstance(answer, dict) else None
    kind = answer.get("anchor_kind") if role == "anchor" else None
    if role not in PROMPT.roles or kind not in (None, *PROMPT.anchor_kinds):
        raise ModelUnavailable(
            f"{CONFIGURATION} the answer {raw[:200]!r} (finish_reason "
            f"{choice.get('finish_reason')!r}) is not the role schema's: the server must "
            "apply structured_outputs and leave max_completion_tokens for the answer"
        )
    probabilities = role_distribution(
        (choice.get("logprobs") or {}).get("content"), PROMPT.roles
    )
    if probabilities is None:
        raise ModelUnavailable(
            f"{CONFIGURATION} no role-token logprobs to read the role's probability from: "
            "the served model does not write its roles as the role models do"
        )
    return Prediction(role=role, anchor_kind=kind, role_probabilities=probabilities)


def served_models(endpoint: str, timeout: float = 5.0) -> List[Dict[str, Any]]:
    """The model cards an OpenAI-compatible server lists at `<endpoint>/models`."""
    response = httpx.get(f"{endpoint.rstrip('/')}/models", timeout=timeout)
    response.raise_for_status()
    return [m for m in (response.json() or {}).get("data") or [] if isinstance(m, Mapping)]


#: Files of a model directory whose bytes identify the checkpoint; weights are
#: identified by name, size and modification time, which a re-merge changes.
_IDENTITY_FILES = ("config.json", "model.safetensors.index.json")


def model_identity(card: Mapping[str, Any]) -> str:
    """A digest of the checkpoint a served model card names.

    vLLM serves every checkpoint under whatever name it is given, so the name
    alone does not change when the model is retrained; the card's `root` is the
    path it loaded. When that path is readable here, its config, weights index
    and weight files are part of the digest, so a re-merge into the same path counts.
    """
    root = str(card.get("root") or card.get("id") or "")
    digest = hashlib.sha256(root.encode())
    path = Path(root)
    if root and path.is_dir():
        for name in _IDENTITY_FILES:
            if (path / name).is_file():
                digest.update(name.encode() + hashlib.sha256((path / name).read_bytes()).digest())
        for weights in sorted(path.glob("*.safetensors")):
            stat = weights.stat()
            digest.update(f"{weights.name}:{stat.st_size}:{stat.st_mtime_ns}".encode())
    return digest.hexdigest()


class ServedClassifier:
    """One origin's role model behind a vLLM endpoint (`<endpoint>/chat/completions`)."""

    def __init__(
        self,
        endpoint: str,
        model: str,
        *,
        source: str,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.url = f"{self.endpoint}/chat/completions"
        self.model = model
        #: `name@prompt version+checkpoint digest`, from the stage (`model_identity`).
        self.source = source
        self._client = httpx.Client()
        self._sleep = sleep or time.sleep
        self._stopped: Optional[str] = None
        self._lock = threading.Lock()

    def body(self, text: str) -> Dict[str, Any]:
        """The request for one serialised context."""
        return {
            "model": self.model,
            "temperature": 0.0,
            "max_completion_tokens": MAX_COMPLETION_TOKENS,
            "messages": [{"role": "user", "content": PROMPT.content(text)}],
            "chat_template_kwargs": {"template": PROMPT.template(), "enable_thinking": False},
            "structured_outputs": {"json": PROMPT.schema()},
            "logprobs": True,
            "top_logprobs": TOP_LOGPROBS,
        }

    def _healthy(self) -> bool:
        """Whether the server answers its model listing and lists this model."""
        try:
            return any(c.get("id") == self.model for c in served_models(self.endpoint))
        except Exception:  # noqa: BLE001 - any failure to answer is an outage
            return False

    def _stop(self, reason: str) -> ModelUnavailable:
        """The error that stops the stage, kept so no set is asked again in this run."""
        with self._lock:
            self._stopped = self._stopped or reason
        return ModelUnavailable(self._stopped)

    def predict(self, texts: Sequence[str]) -> List[Union[Prediction, Unanswered]]:
        with ThreadPoolExecutor(max_workers=min(WORKERS, max(1, len(texts)))) as pool:
            return list(pool.map(self._ask, texts))

    def _ask(self, text: str) -> Union[Prediction, Unanswered]:
        last = ""
        for attempt in range(ATTEMPTS):
            if self._stopped:
                raise ModelUnavailable(self._stopped)
            if attempt:
                self._sleep(2 ** (attempt - 1))
            try:
                response = self._client.post(self.url, json=self.body(text), timeout=TIMEOUT)
            except httpx.TransportError as exc:  # timeouts, refused and dropped connections
                last = f"{type(exc).__name__}: {exc}"
                continue
            if response.status_code >= 500 or response.status_code == 429:
                last = f"HTTP {response.status_code}: {response.text[:200]}"
                continue
            if response.status_code >= 400:
                # Every request has the one shape `body` builds, so a refusal -- a context
                # longer than the server's max_model_len, say -- is how the model is served.
                raise self._stop(
                    f"{CONFIGURATION} {self.url} answered HTTP {response.status_code} for "
                    f"{self.model}: {response.text[:300]}"
                )
            try:
                return read_answer(response.json()["choices"][0])
            except ModelUnavailable as exc:
                raise self._stop(str(exc)) from None
        if self._healthy():
            # Up, and failing on this input every time: this set's failure, not an outage,
            # so the articles after it are still read.
            return Unanswered(f"server error on this input after {ATTEMPTS} tries: {last}")
        raise self._stop(f"{self.model} at {self.url} did not answer in {ATTEMPTS} tries: {last}")
