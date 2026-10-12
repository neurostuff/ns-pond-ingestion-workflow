"""A served NuExtract3 role model: one request per set to an OpenAI-compatible vLLM endpoint.

The request is the one the model was trained and evaluated with
(`served_prompt`), greedy, with the answer held to the prompt's JSON schema.
The probability of each role is read from the first token of the role value:
the top 20 alternatives at that position, each counted for the one role its
text can begin, renormalised over the tokens that begin exactly one role.

A set's answer that cannot be read (not JSON, a role outside the vocabulary,
an anchor without its kind, no role-token alternatives) is `Unanswered`, with
the reason: never a default role. A server that times out or answers 5xx is
asked again. When it still does not answer, the server's model listing is
asked: if it lists the model, the server is up and fails on this input alone,
which is that set's `Unanswered` reason; otherwise `RoleModelUnavailable` is
raised, and the classifier stays `down` without asking again.
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
from study_schema.models.paper_parse import CoordinateRole

from .classifier import Prediction, Unanswered
from .served_prompt import RolePrompt

#: Where the role value starts in the answer, whitespace removed.
_ROLE_KEY = '"role":"'
#: Alternatives read at the role token, as the models were scored.
TOP_LOGPROBS = 20


class RoleModelUnavailable(RuntimeError):
    """The served model did not answer after every try: no set of the article gets a role."""


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


def read_answer(choice: Mapping[str, Any], prompt: RolePrompt) -> Union[Prediction, Unanswered]:
    """The prediction in one completion choice, or why it has none."""
    raw = (choice.get("message") or {}).get("content")
    try:
        answer = json.loads(raw or "")
    except (TypeError, ValueError):
        return Unanswered(f"the answer is not JSON: {str(raw)[:200]!r}")
    if not isinstance(answer, dict):
        return Unanswered(f"the answer is not an object: {str(raw)[:200]!r}")
    role, kind = answer.get("role"), answer.get("anchor_kind")
    if role not in prompt.roles:
        return Unanswered(f"role {role!r} is not one of {list(prompt.roles)}")
    if role == "anchor" and kind not in prompt.anchor_kinds:
        return Unanswered(f"an anchor with anchor_kind {kind!r}")
    probabilities = role_distribution(
        (choice.get("logprobs") or {}).get("content"), prompt.roles
    )
    if probabilities is None:
        return Unanswered("no role-token logprobs to read the role's probability from")
    return Prediction(
        coordinates_probability=None,
        role_probabilities=probabilities,
        kind_probabilities={kind: 1.0} if role == "anchor" else {},
        prior_probability=None,
        answer=role,
        answer_kind=kind if role == "anchor" else None,
    )


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
        prompt: RolePrompt,
        *,
        result_threshold: Optional[float] = None,
        timeout: float = 120.0,
        attempts: int = 3,
        workers: int = 16,
        source: Optional[str] = None,
        client: Optional[httpx.Client] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.url = f"{self.endpoint}/chat/completions"
        self.model = model
        self.prompt = prompt
        #: The stage passes the source with the checkpoint's identity (`model_identity`).
        self.source = source or f"{model}@{prompt.version}"
        # Recall of results over precision: a likely result is a result.
        self.role_thresholds = (
            {} if result_threshold is None else {CoordinateRole.result.value: result_threshold}
        )
        self._timeout = timeout
        self._attempts = max(1, attempts)
        self._workers = max(1, workers)
        self._client = client or httpx.Client(timeout=timeout)
        self._sleep = sleep or time.sleep
        self._down: Optional[str] = None
        self._lock = threading.Lock()

    def body(self, text: str) -> Dict[str, Any]:
        """The request for one serialised context."""
        return {
            "model": self.model,
            "temperature": 0.0,
            "max_completion_tokens": 64,
            "messages": [{"role": "user", "content": self.prompt.content(text)}],
            "chat_template_kwargs": {"template": self.prompt.template(), "enable_thinking": False},
            "structured_outputs": {"json": self.prompt.schema()},
            "logprobs": True,
            "top_logprobs": TOP_LOGPROBS,
        }

    def _healthy(self) -> bool:
        """Whether the server answers its model listing and lists this model."""
        try:
            return any(c.get("id") == self.model for c in served_models(self.endpoint))
        except Exception:  # noqa: BLE001 - any failure to answer is an outage
            return False

    @property
    def down(self) -> Optional[str]:
        """Why the server is taken to be down for the rest of this run, or None."""
        return self._down

    def predict(self, texts: Sequence[str]) -> List[Union[Prediction, Unanswered]]:
        with ThreadPoolExecutor(max_workers=min(self._workers, max(1, len(texts)))) as pool:
            return list(pool.map(self._ask, texts))

    def _ask(self, text: str) -> Union[Prediction, Unanswered]:
        last = ""
        for attempt in range(self._attempts):
            if self._down:
                raise RoleModelUnavailable(self._down)
            if attempt:
                self._sleep(2 ** (attempt - 1))
            try:
                response = self._client.post(self.url, json=self.body(text), timeout=self._timeout)
            except httpx.TransportError as exc:  # timeouts, refused and dropped connections
                last = f"{type(exc).__name__}: {exc}"
                continue
            if response.status_code >= 500 or response.status_code == 429:
                last = f"HTTP {response.status_code}: {response.text[:200]}"
                continue
            if response.status_code == 400:
                # The server refused this input (one too long for the model, say): this set only.
                return Unanswered(f"the server refused the request: {response.text[:200]}")
            if response.status_code >= 400:
                raise RoleModelUnavailable(
                    f"{self.url} answered HTTP {response.status_code} for {self.model}: "
                    f"{response.text[:200]}"
                )
            try:
                choice = (response.json().get("choices") or [None])[0]
            except ValueError:
                choice = None
            if not isinstance(choice, Mapping):
                return Unanswered(f"the response has no choice: {response.text[:200]!r}")
            return read_answer(choice, self.prompt)
        if self._healthy():
            # Up, and failing on this input every time: this set's failure, not an outage,
            # so the articles after it are still read.
            return Unanswered(
                f"server error on this input after {self._attempts} tries: {last}"
            )
        with self._lock:
            self._down = self._down or (
                f"{self.model} at {self.url} did not answer in {self._attempts} tries: {last}"
            )
        raise RoleModelUnavailable(self._down)
