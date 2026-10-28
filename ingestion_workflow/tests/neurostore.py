"""A neurostore that serves the study_schema ingester's route by its contract.

No neurostore takes this contract yet, so nothing can be recorded; this stands in for one,
on an httpx `MockTransport`. The contract is neurostore's own OpenAPI spec, the
`neurostore-spec` submodule beside this file, and study_schema's JSON Schema at the version
this repo pins.

Every upload is checked: the body against the route's request schema, its
`coordinate_parse` and `parsed_paper` against study_schema's, and its `text` against the
parsed paper's `text_sha256`. Every answer that is neurostore's own is checked against the
route's response schemas: a summary for a 2xx, an ErrorResponse otherwise. An answer from
something in front of neurostore (nginx's 502 page, a redirect, a maintenance page) is
`proxy`, and may not be JSON, so no JSON body escapes the check. A breach raises
ContractViolation and is kept in `violations`, which the `neurostore` fixture fails the
test on, so a caller that catches the error cannot hide it.

Uploads are answered by the answers a test scripts, in order, then by their summary. The
summary is built from the upload: the header's base study (or one it creates for a paper
whose header names none) and, per base study, how many of the parse's keys are new.
"""

from __future__ import annotations

import functools
import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Type
from urllib.parse import urlsplit

import httpx
import jsonschema
import yaml
from ingestion_workflow.services.neurostore_ingest import TOKEN_ENV, NeurostoreIngester, api_url
from openapi_schema_validator import OAS30Validator, oas30_format_checker
from referencing import Registry, Resource
from study_schema.jsonschema import load as load_study_schema

SPEC = Path(__file__).parent / "neurostore-spec" / "neurostore-openapi.yml"
#: The spec's path for the ingester, under the API's base path.
ROUTE = "/study-schema-uploads/"
HOST = "neurostore.test"
URL = f"http://{HOST}"
#: The test's admin token; a placeholder, never a real one.
TOKEN = "mock-token-5f2c9e"
#: What every ErrorResponse states as its time.
TIMESTAMP = "2026-10-27T00:00:00Z"
_URI = "urn:neurostore-spec"


class ContractViolation(AssertionError):
    """A request or an answer the contract does not allow."""


@functools.lru_cache(maxsize=None)
def spec() -> Dict[str, Any]:
    if not SPEC.exists():
        raise RuntimeError(
            f"{SPEC} is missing: run `git submodule update --init` to check out neurostore-spec"
        )
    return yaml.safe_load(SPEC.read_text("utf-8"))


def api_path() -> str:
    """The ingester's path on the server: the spec's base path, then its route."""
    return urlsplit(spec()["servers"][0]["url"]).path.rstrip("/") + ROUTE


def _operation() -> Dict[str, Any]:
    return spec()["paths"][ROUTE]["post"]


@functools.lru_cache(maxsize=None)
def _spec_validator(ref: str):
    registry = Registry().with_resource(_URI, Resource.opaque(spec()))
    return OAS30Validator({"$ref": _URI + ref}, registry=registry,
                          format_checker=oas30_format_checker)


def _schema_ref(content: Dict[str, Any]) -> str:
    return content["content"]["application/json"]["schema"]["$ref"]


def request_schema() -> str:
    """The route's request body schema, as a reference into the spec."""
    return _schema_ref(_operation()["requestBody"])


def response_schema(status: int) -> str:
    """The schema the route's answer with `status` follows: the summary for a 2xx, else
    ErrorResponse, which is every error's schema in the spec."""
    if 200 <= status < 300:
        return _schema_ref(_operation()["responses"][str(status)])
    return "#/components/schemas/ErrorResponse"


def spec_problems(ref: str, value: Any) -> List[str]:
    """Where `value` breaks the spec's schema `ref`; empty when it holds."""
    name = ref.rsplit("/", 1)[-1]
    return [f"{name}/{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
            for e in _spec_validator(ref).iter_errors(value)]


@functools.lru_cache(maxsize=None)
def _study_schema_validator(name: str):
    schema = load_study_schema(name)
    return jsonschema.validators.validator_for(schema)(schema)


def study_schema_problems(name: str, value: Any) -> List[str]:
    return [f"{name}/{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
            for e in _study_schema_validator(name).iter_errors(value)]


@dataclass(frozen=True)
class Answer:
    """One scripted answer. Build it with Neurostore's `reply`, `error`, `summary`,
    `proxy` or `drop`, which check it against the contract."""

    status: int = 200
    body: Any = None
    headers: Dict[str, str] = field(default_factory=dict)
    #: Not neurostore's: raw bytes from something in front of it.
    proxy: bool = False
    #: Fields laid over the upload's own summary, which is checked when it is sent.
    summary: Optional[Dict[str, Any]] = None
    #: The transport error the request fails with, instead of an answer.
    raises: Optional[Type[httpx.TransportError]] = None


@dataclass(frozen=True)
class Call:
    """A request as the server saw it, without its Authorization header."""

    method: str
    path: str
    content_type: Optional[str]
    content: bytes


class Neurostore:
    """One neurostore for one test; `transport` is what an ingester sends through.

    `closed` serves no request at all: any request is a violation.
    """

    def __init__(self, *, token: str = TOKEN, closed: bool = False) -> None:
        self.url = URL
        self.token = token
        self.closed = closed
        self.calls: List[Call] = []
        #: The bodies of the uploads that kept the contract, parsed.
        self.uploads: List[Dict[str, Any]] = []
        self.violations: List[str] = []
        self._script: List[Answer] = []
        #: The analysis keys each base study holds.
        self._keys: Dict[str, Set[str]] = {}
        self._errors = 0
        #: How the route answers a GET, the ingester's health check: neurostore serves the
        #: route and takes only POST there.
        self.health: Answer = self.error(405, "Method Not Allowed")
        self.transport = httpx.MockTransport(self._handle)

    # -- scripting -------------------------------------------------------------------------

    def script(self, *answers: Answer) -> "Neurostore":
        """Answer the next uploads with `answers`, in order."""
        self._script.extend(answers)
        return self

    def reply(self, status: int, body: Any, headers: Optional[Dict[str, str]] = None) -> Answer:
        """neurostore's own JSON answer; it must hold to the route's response schema."""
        self._check_answer(status, body)
        return Answer(status, body, dict(headers or {}))

    def error(self, status: int, detail: str, *, codes=(), field_name: Optional[str] = None,
              context: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None) -> Answer:
        """neurostore's ErrorResponse, with one field error per code."""
        self._errors += 1
        body: Dict[str, Any] = {
            "status": status, "title": HTTPStatus(status).phrase, "detail": detail,
            "type": "about:blank", "timestamp": TIMESTAMP, "request_id": f"req-{self._errors}",
        }
        if codes:
            body["errors"] = [
                {"field": field_name or "coordinate_parse", "code": code, "message": detail,
                 **({"context": context} if context is not None else {})}
                for code in codes]
        return self.reply(status, body, headers)

    def summary(self, **fields: Any) -> Answer:
        """A 200 whose summary is the upload's own with `fields` laid over it; checked
        against the summary schema when it is sent, since it is built from the upload."""
        return Answer(200, summary=fields)

    def proxy(self, status: int, body: bytes = b"",
              headers: Optional[Dict[str, str]] = None) -> Answer:
        """An answer from something in front of neurostore: never JSON, which would be
        neurostore's and fall under the contract."""
        if body:
            try:
                json.loads(body)
            except ValueError:
                pass
            else:
                self._violate([f"a proxy's {status} has a JSON body; a JSON answer is "
                               "neurostore's: script it with reply, error or summary"])
        return Answer(status, body, {"Content-Type": "text/html", **(headers or {})}, proxy=True)

    def drop(self, error: Type[httpx.TransportError] = httpx.RemoteProtocolError) -> Answer:
        """No answer: the connection drops (RemoteProtocolError) or times out
        (ReadTimeout, say)."""
        return Answer(raises=error)

    # -- serving ---------------------------------------------------------------------------

    def install(self, monkeypatch) -> None:
        """Serve every ingester `from_settings` builds for this server's URL, as the upload
        stage does; any other URL is left to the network."""
        made = NeurostoreIngester.from_settings.__func__

        def from_settings(cls, settings, **kwargs):
            if api_url(settings) == self.url:
                kwargs.setdefault("transport", self.transport)
            return made(cls, settings, **kwargs)

        monkeypatch.setattr(NeurostoreIngester, "from_settings", classmethod(from_settings))

    def check(self) -> None:
        """Raise when any request or answer broke the contract."""
        if self.violations:
            raise ContractViolation("; ".join(self.violations))

    def _violate(self, problems: List[str]) -> None:
        self.violations.extend(problems)
        raise ContractViolation("; ".join(problems))

    def _check_answer(self, status: int, body: Any) -> None:
        problems = spec_problems(response_schema(status), body)
        if problems:
            self._violate([f"the {status} answer breaks the contract: {p}" for p in problems])

    def _handle(self, request: httpx.Request) -> httpx.Response:
        content = request.read()
        self.calls.append(Call(request.method, request.url.path,
                               request.headers.get("Content-Type"), content))
        if self.closed:
            self._violate([f"{request.method} {request.url.path}: this test sends nothing"])
        if request.url.path != api_path():
            # neurostore's own answer to a path it does not route
            return self._send(self.error(404, "Not Found"), request)
        if request.method == "GET":
            return self._send(self.health, request)
        if request.method != "POST":
            return self._send(self.error(405, "Method Not Allowed"), request)
        body = self._check_upload(request, content)
        self.uploads.append(body)
        answer = self._script.pop(0) if self._script else self.summary()
        return self._send(answer, request, body)

    def _check_upload(self, request: httpx.Request, content: bytes) -> Dict[str, Any]:
        problems = []
        if request.headers.get("Authorization") != f"Bearer {self.token}":
            problems.append("the request does not carry the admin token as a Bearer token")
        if (request.headers.get("Content-Type") or "").split(";")[0] != "application/json":
            problems.append(f"Content-Type is {request.headers.get('Content-Type')!r}")
        try:
            body = json.loads(content)
        except ValueError as exc:
            self._violate([*problems, f"the body is not JSON: {exc}"])
        problems += spec_problems(request_schema(), body)
        if isinstance(body, dict):
            # The parts this client's upload always sends; the spec allows others alone.
            for name in ("coordinate_parse", "parsed_paper", "text"):
                if name not in body:
                    problems.append(f"the upload has no {name}")
            for name, schema in (("coordinate_parse", "coordinate-parse"),
                                 ("parsed_paper", "parsed-paper")):
                if name in body:
                    problems += study_schema_problems(schema, body[name])
            text, paper = body.get("text"), body.get("parsed_paper")
            if isinstance(text, str) and isinstance(paper, dict):
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                if digest != paper.get("text_sha256"):
                    problems.append(f"text hashes to {digest}, not parsed_paper's "
                                    f"text_sha256 {paper.get('text_sha256')}")
        if problems:
            self._violate(problems)
        return body

    def _send(self, answer: Answer, request: httpx.Request,
              upload: Optional[Dict[str, Any]] = None) -> httpx.Response:
        if answer.raises is not None:
            raise answer.raises("scripted: no answer", request=request)
        if answer.proxy:
            return httpx.Response(answer.status, content=answer.body, headers=answer.headers)
        body = answer.body
        if answer.summary is not None:
            body = {**self._summary_of(upload), **answer.summary}
            self._check_answer(answer.status, body)
            self._keys[body["base_study_id"]] = _keys(upload)
        return httpx.Response(answer.status, json=body, headers=answer.headers)

    def _summary_of(self, upload: Dict[str, Any]) -> Dict[str, Any]:
        parse = upload["coordinate_parse"]
        header = parse["header"]
        base = header.get("neurostore_base_study_id") or _created_base_study(header)
        keys, held = _keys(upload), self._keys.get(base, set())
        return {
            "kind": "revision" if parse.get("revision_of") else "parse",
            "base_study_id": base,
            "study_id": f"study-{base}",
            "parse_id": parse["parse_id"],
            "skeleton": {
                "created": len(keys - held),
                "updated": len(keys & held),
                "superseded": len(held - keys),
                "omitted_analyses": len(parse.get("omitted_analyses") or []),
                "orphaned_claims": [],
            },
        }


@contextmanager
def serve(monkeypatch, **kwargs: Any) -> Iterator[Neurostore]:
    """A Neurostore installed for `from_settings`, with the token in the environment;
    on leaving, ContractViolation if any request or answer broke the contract, even one
    the code under test caught."""
    monkeypatch.setenv(TOKEN_ENV, kwargs.get("token", TOKEN))
    server = Neurostore(**kwargs)
    server.install(monkeypatch)
    yield server
    server.check()


def _keys(upload: Dict[str, Any]) -> Set[str]:
    return {a["key"] for a in upload["coordinate_parse"].get("analyses") or []}


def _created_base_study(header: Dict[str, Any]) -> str:
    """The base study neurostore creates for a paper its header names none for: one per
    set of identifiers."""
    ids = json.dumps(header.get("identifiers"), sort_keys=True)
    return "new" + hashlib.sha256(ids.encode()).hexdigest()[:9]


__all__ = ["Answer", "Call", "ContractViolation", "HOST", "Neurostore", "TOKEN", "URL", "serve",
           "api_path", "spec"]
