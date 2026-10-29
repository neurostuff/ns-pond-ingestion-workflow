"""Upload one paper's coordinate parse through neurostore's study_schema ingester.

The ingester is `POST {api}/api/study-schema-uploads/`, admin only. Its body is
`{"coordinate_parse": <CoordinateParse>, "parsed_paper": <ParsedPaper>, "text": <str>}`;
its 200 is a summary of what the upload changed. The ParsedPaper is always sent: it gives
a base study the ingester creates its bibliography, and its `bibliography.corrections`
mark the paper retracted or not. `text` is the file the ParsedPaper's `text_path` names,
which neurostore stores as one of the base study's full texts. The upload is one
transaction keyed by the parse's analysis content keys, so sending the same parse again
changes nothing.

Each paper gets one artifact, `<study>/upload/neurostore_ingest.json`: the latest
upload's outcome at the top, the last successful summary per endpoint under
`last_uploaded`, and every upload's outcome, in order, under `history`. A dry run
validates and writes the exact request body to `neurostore_payload.json` and its own
record to `neurostore_dry_run.json`, under the required `dry_run_root/<study>/` (from_settings:
`<data_root>/neurostore_dry_run/<study>/`, outside the ns-pond tree); it never touches
the upload artifact.

How a response is treated:

    2xx with this parse's summary   uploaded; the summary is recorded
    2xx summary of another base     uploaded when its `merged_from` holds the base study
    study                           the header names (neurostore followed a merge), and
                                    the upload stage learns the other as an alias; else
                                    refused: the server filed the parse under a base
                                    study the header does not name, and would again
    408, 429, 5xx, a lost           retried with backoff; then, if the API is down,
    connection or timeout, a 2xx    NeurostoreUnavailable, and nothing is written; if
    that is not a summary of this   it is up, the paper is recorded as blocked
    parse
    401 twice, 403, a client the    NeurostoreAuthError; nothing is written
    tenant refuses
    3xx, 405, 415, a 404 that is    NeurostoreConfigError: the URL or the request is
    not about a base study, a 422   wrong for every paper alike; nothing is written
    SCHEMA_VERSION
    any other status                refused: recorded at once and never retried, since
                                    the same body gets the same answer (422
                                    KEY_MISMATCH, DUPLICATE_KEY or TEXT_MISMATCH among
                                    them)

The three errors are `StopRun`s: every later paper would fail the same way, so the
upload stage stops and leaves its papers pending. The API is up when the ingester's
route answers a GET with 405: it is served, and takes only POST.

The ingester trades the client id and secret in the environment variables
NEUROSTORE_CLIENT_ID and NEUROSTORE_CLIENT_SECRET with the deployment's Auth0 tenant for a
token (the client-credentials grant) before its first upload, and asks for another before
it expires or when neurostore refuses it with a 401. Neurostore takes the client as the
user `<client id>@clients`, which must hold its admin role. Neither the token nor the
secret is ever logged or written: request headers and bodies are never recorded, and both,
and any Authorization or Bearer value, are scrubbed from every recorded string and from
the HTTP libraries' log lines.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
import jsonschema
from pydantic import ValidationError
from study_schema import layouts
from study_schema.jsonschema import load as load_json_schema
from study_schema.models import paper_parse as pp

from ingestion_workflow.catalog import Status
from ingestion_workflow.catalog.models import utcnow
from ingestion_workflow.services.logging import get_logger
from ingestion_workflow.services.paper_parse import PARSE_DIR

logger = get_logger(__name__)

UPLOAD_PATH = "/api/study-schema-uploads/"
UPLOAD_DIR = "upload"
ARTIFACT_NAME = "neurostore_ingest.json"
DRY_RUN_NAME = "neurostore_dry_run.json"
#: Under `data_root`: where `from_settings` puts a dry run's files, one directory per study.
DRY_RUN_DIR = "neurostore_dry_run"
PAYLOAD_NAME = "neurostore_payload.json"
#: The environment variables the client credentials are read from; they are never settings.
CLIENT_ID_ENV = "NEUROSTORE_CLIENT_ID"
CLIENT_SECRET_ENV = "NEUROSTORE_CLIENT_SECRET"
#: Each deployment's Auth0 tenant and the audience its API checks tokens for, as neurostore's
#: ProductionConfig and StagingConfig set them. The audience names the Auth0 API, not a URL
#: requests go to: staging's frontend asks the same tenant for the same one.
AUTH0 = {
    "https://neurostore.org": ("https://neurosynth.us.auth0.com",
                               "https://neurostore.org/api/"),
    "https://staging.neurostore.xyz": ("https://neurosynth-staging.us.auth0.com",
                                       "https://staging.synth.neurostore.xyz/api/"),
}
#: Seconds before a token expires that another is asked for: one upload can take TIMEOUT.
REFRESH_MARGIN = 300.0
#: Bump when the artifact's fields change meaning.
ARTIFACT_VERSION = 3

#: Retries after a transient failure. With BACKOFF the waits are 2, 4, 8 and 16 s, so a
#: paper gives a restarting API about half a minute before the health check is asked.
RETRIES = 4
#: Seconds before the first retry; each later retry waits twice as long.
BACKOFF = 2.0
#: The 4xx answers that say nothing about the paper: a proxy timing the request out
#: (408) or a limiter turning it away (429). They are retried like a 5xx.
RETRIED = (408, 429)
#: Seconds one upload may take: neurostore's gunicorn kills a worker request at 120 s
#: (`--timeout 120`), so waiting longer only waits for an answer that never comes.
TIMEOUT = 120.0
#: Seconds the health check waits for its answer.
HEALTH_TIMEOUT = 30.0

#: How much of a refused or failed response body the artifact keeps.
BODY_EXCERPT = 2000
#: The longest wait between two attempts, whatever Retry-After asks for.
MAX_DELAY = 120.0
#: What the ingester's summary of a parse upload says it was.
SUMMARY_KINDS = ("parse", "revision")
_SECRET_WORDS = ("token", "authorization", "password", "secret", "api_key", "apikey", "cookie")
REDACTED = "[redacted]"
#: A bearer token is one run of visible ASCII; anything else is not a legal header
#: value, and httpx would refuse it on every paper, with the token in the error.
_TOKEN = re.compile(r"[\x21-\x7e]+")
_AUTH_VALUE = re.compile(
    r"(?i)(bearer\s+|authorization['\"]?\s*[:=]\s*['\"]?(?:bearer\s+)?)[^\s'\",}]+"
)
#: The loggers that can print a request: this module's and the HTTP stack's.
_HTTP_LOGGERS = (
    "httpx",
    "httpcore",
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
)


class Result(str, Enum):
    UPLOADED = "uploaded"
    #: Validated and written, not sent: nothing for the catalog to record.
    DRY_RUN = "dry_run"
    #: An answer the server will give again for the same body (400, 422, an unknown
    #: base study's 404, a summary filed under another base study, ...).
    REFUSED = "refused"
    #: 408, 429, 5xx, a timeout, a lost connection or a 2xx that is not this parse's
    #: summary, on every attempt, from an API that is up.
    BLOCKED = "blocked"
    #: Failed validation here, so never sent.
    INVALID = "invalid"
    #: sync wrote no parse for the paper.
    MISSING = "missing"

    @property
    def catalog_status(self) -> Optional[Status]:
        if self is Result.DRY_RUN:
            return None
        if self is Result.UPLOADED:
            return Status.OK
        if self in (Result.REFUSED, Result.INVALID, Result.MISSING):
            return Status.PERMANENT
        return Status.FAILED


class StopRun(RuntimeError):
    """Every later paper would fail the same way, so the run stops here."""


class NeurostoreAuthError(StopRun):
    """The client is missing or refused, or neurostore refuses its token."""


class NeurostoreConfigError(StopRun):
    """The URL or the request is wrong for every paper alike."""


class NeurostoreUnavailable(StopRun):
    """The API did not answer a paper's tries, nor its health check."""


class InvalidParse(ValueError):
    def __init__(self, problems: List[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def api_url(settings) -> str:
    """The API base URL, `neurostore_api_url`; NeurostoreConfigError when it is unset."""
    if not settings.neurostore_api_url:
        raise NeurostoreConfigError(
            "no neurostore_api_url: set it to the base URL of the neurostore the upload "
            "posts to (https://staging.neurostore.xyz, say), without /api"
        )
    return settings.neurostore_api_url.rstrip("/")


def read_client() -> Optional[Tuple[str, str]]:
    """The client id and secret from CLIENT_ID_ENV and CLIENT_SECRET_ENV, stripped (a
    pasted trailing newline or space is never part of either); None unless both are set."""
    pair = tuple((os.environ.get(name) or "").strip()
                 for name in (CLIENT_ID_ENV, CLIENT_SECRET_ENV))
    return pair if all(pair) else None


def auth0(settings) -> Optional[Tuple[str, str]]:
    """The Auth0 tenant and audience for the settings' neurostore: `neurostore_auth0_url`
    and `neurostore_auth0_audience` when set, else AUTH0's for its API URL."""
    known = AUTH0.get((settings.neurostore_api_url or "").rstrip("/"), (None, None))
    url = settings.neurostore_auth0_url or known[0]
    audience = settings.neurostore_auth0_audience or known[1]
    return (url.rstrip("/"), audience) if url and audience else None


def build_payload(study_dir: Path) -> Tuple[Dict[str, Any], List[str]]:
    """The request body for one paper, and the checks it passed.

    The documents are sent as sync wrote them, after they validate as study_schema's
    models and pass `layouts.check_paper` (the same checks sync made writing them,
    including each analysis key against its cells and the text against its hash), with
    the text the ParsedPaper's `text_path` names under `study_dir`, decoded from the
    bytes that hash so its UTF-8 is the hashed text. Raises FileNotFoundError when
    either file is missing and InvalidParse when a check fails.
    """
    parse_dir = Path(study_dir) / PARSE_DIR
    raw_parse = json.loads((parse_dir / "coordinate_parse.json").read_text("utf-8"))
    raw_paper = json.loads((parse_dir / "parsed_paper.json").read_text("utf-8"))
    problems: List[str] = []
    models = {}
    for name, model, raw in (
        ("coordinate_parse", pp.CoordinateParse, raw_parse),
        ("parsed_paper", pp.ParsedPaper, raw_paper),
    ):
        try:
            models[name] = model.model_validate(raw)
        except ValidationError as exc:
            problems += [
                f"{name}/{'/'.join(str(p) for p in err['loc'])}: {err['msg']}"
                for err in exc.errors()
            ]
    checks = ["model"]
    if not problems:
        problems += layouts.check_paper(layouts.PaperParse(**models), study_dir)
        checks.append("check_paper")
    problems += _json_schema_problems(raw_parse, raw_paper)
    checks.append("json_schema")
    if problems:
        raise InvalidParse(problems)
    text_path = models["parsed_paper"].text_path
    try:
        # Not read_text: it would turn a CRLF into LF, and the text would no longer hash.
        text = (Path(study_dir) / text_path).read_bytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidParse([f"{text_path} is not UTF-8: {exc}"]) from None
    return {"coordinate_parse": raw_parse, "parsed_paper": raw_paper, "text": text}, checks


def _json_schema_problems(raw_parse, raw_paper) -> List[str]:
    """The ingester's own check: study_schema's JSON Schema."""
    problems = []
    for name, raw in (("coordinate-parse", raw_parse), ("parsed-paper", raw_paper)):
        schema = load_json_schema(name)
        validator = jsonschema.validators.validator_for(schema)(schema)
        problems += [
            f"{name}/{'/'.join(str(p) for p in e.path)}: {e.message}"
            for e in validator.iter_errors(raw)
        ]
    return problems


def scrub(text: str, token: Optional[str]) -> str:
    """`text` without the token or any Authorization or Bearer value."""
    if token:
        text = text.replace(token, REDACTED)
    return _AUTH_VALUE.sub(lambda m: m.group(1) + REDACTED, text)


def redact(value: Any, token: Optional[str]) -> Any:
    """`value` with secret-named keys replaced and every string scrubbed."""
    if isinstance(value, dict):
        return {
            k: REDACTED if any(w in str(k).lower() for w in _SECRET_WORDS) else redact(v, token)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v, token) for v in value]
    if isinstance(value, str):
        return scrub(value, token)
    return value


class _Scrub(logging.Filter):
    """Scrubs every open ingester's token from a log record before any handler sees it.

    One filter for the module, installed at import, so ingesters add nothing to the
    loggers; each holds its token here only until it is closed.
    """

    def __init__(self) -> None:
        super().__init__()
        #: One entry per open ingester, so closing one keeps another's same token.
        self.tokens: List[str] = []

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        clean = scrub(message, None)
        # Longest first: one that begins another would leave the other's tail behind.
        for token in sorted(set(self.tokens), key=len, reverse=True):
            clean = scrub(clean, token)
        if clean != message:
            record.msg, record.args = clean, ()
        return True


_SCRUB = _Scrub()
for _name in (*_HTTP_LOGGERS, logger.name):
    logging.getLogger(_name).addFilter(_SCRUB)


class NeurostoreIngester:
    """Posts parses to one neurostore API with one admin token; one per run.

    A context manager: closing it closes its connections and drops its token from the
    log filter.
    """

    def __init__(
        self,
        base_url: str,
        *,
        dry_run: bool = False,
        dry_run_root: Optional[Path] = None,
        sleep: Optional[Callable[[float], None]] = None,
        transport: Optional[httpx.BaseTransport] = None,
        auth0: Optional[Tuple[str, str]] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.url = self.base_url + UPLOAD_PATH
        self.dry_run = dry_run
        if dry_run and dry_run_root is None:
            raise ValueError(
                "a dry run needs dry_run_root, a directory outside the ns-pond tree for its "
                "payloads (from_settings uses <data_root>/neurostore_dry_run)"
            )
        self.dry_run_root = Path(dry_run_root) if dry_run_root is not None else None
        self._sleep = sleep or time.sleep
        self._clock = clock or time.monotonic
        self._down: Optional[str] = None
        #: The tenant, audience, client id and secret a token is asked for with; the token
        #: and when it expires, none until the first upload asks for one.
        self._grant: Optional[Tuple[str, str, str, str]] = None
        self._token: Optional[str] = None
        self._expires = float("-inf")
        if not dry_run:
            client = read_client()
            if client is None:
                raise NeurostoreAuthError(
                    f"no neurostore client: set {CLIENT_ID_ENV} and {CLIENT_SECRET_ENV} to an "
                    f"Auth0 client of {base_url}'s tenant whose user holds neurostore's admin role"
                )
            if auth0 is None:
                raise NeurostoreConfigError(
                    f"no Auth0 tenant is known for {base_url}: set neurostore_auth0_url and "
                    "neurostore_auth0_audience to trade the client credentials for a token"
                )
            self._grant = (*auth0, *client)
            _SCRUB.tokens.append(client[1])
        self._client = None if dry_run else httpx.Client(timeout=TIMEOUT, transport=transport)

    @classmethod
    def from_settings(cls, settings, **kwargs) -> "NeurostoreIngester":
        kwargs.setdefault("dry_run", settings.dry_run)
        # A run's dry-run output belongs with the run's own data, not in the ns-pond tree.
        kwargs.setdefault("dry_run_root", Path(settings.data_root) / DRY_RUN_DIR)
        kwargs.setdefault("auth0", auth0(settings))
        return cls(api_url(settings), **kwargs)

    def __repr__(self) -> str:
        return f"NeurostoreIngester({self.url!r}, dry_run={self.dry_run})"

    @property
    def down(self) -> Optional[str]:
        """Why the API is taken to be down for the rest of this run, or None."""
        return self._down

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        for secret in (self._token, self._grant[3] if self._grant else None):
            if secret in _SCRUB.tokens:
                _SCRUB.tokens.remove(secret)

    def __enter__(self) -> "NeurostoreIngester":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def upload(self, study_dir: Path) -> Dict[str, Any]:
        """Upload one paper, write its artifact, and return it.

        `study_dir` is `<ns_pond_root>/<study_id>`, as sync wrote it. Raises a StopRun,
        writing nothing, when the token or the URL is wrong or the API is down.
        """
        study_dir = Path(study_dir)
        out = self.dry_run_root / study_dir.name if self.dry_run else study_dir / UPLOAD_DIR
        record: Dict[str, Any] = {
            "artifact_version": ARTIFACT_VERSION,
            "study_id": study_dir.name,
            "endpoint": self.url,
            "dry_run": self.dry_run,
            "at": utcnow(),
        }
        try:
            payload, checks = build_payload(study_dir)
        except FileNotFoundError as exc:
            return self._finish(out, record, Result.MISSING, error={"detail": str(exc)})
        except (InvalidParse, json.JSONDecodeError) as exc:
            error = {"detail": "invalid parse", "problems": getattr(exc, "problems", [str(exc)])}
            return self._finish(out, record, Result.INVALID, error=error)
        parse = payload["coordinate_parse"]
        record.update(
            article_id=parse["header"].get("article_id"),
            parse_id=parse.get("parse_id"),
            checks=checks,
        )
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        record["payload_sha256"] = hashlib.sha256(body).hexdigest()
        if self.dry_run:
            out.mkdir(parents=True, exist_ok=True)
            _write_atomic(out / PAYLOAD_NAME, body)
            record["payload_path"] = str(out / PAYLOAD_NAME)
            return self._finish(out, record, Result.DRY_RUN)
        if self._down:
            raise NeurostoreUnavailable(self._down)
        return self._send(out, record, body, parse)

    def _send(
        self, out: Path, record: Dict[str, Any], body: bytes, parse: Dict[str, Any]
    ) -> Dict[str, Any]:
        attempts: List[Dict[str, Any]] = []
        record["attempts"] = attempts
        error: Dict[str, Any] = {}
        refreshed = False
        attempt = 0
        while attempt < RETRIES + 1:
            attempt += 1
            retry_after = None
            unsendable = False
            record.pop("http_status", None)
            if self._clock() >= self._expires - REFRESH_MARGIN:
                self._ask_token()
            headers = {"Authorization": f"Bearer {self._token}",
                       "Content-Type": "application/json"}
            try:
                response = self._client.post(self.url, content=body, headers=headers)
            except (httpx.LocalProtocolError, httpx.UnsupportedProtocol) as exc:
                error = {"detail": self._describe(exc)}
                unsendable = True
            except httpx.TransportError as exc:  # timeouts, refused and dropped connections
                error = {"detail": self._describe(exc)}
                attempts.append({"attempt": attempt, "error": type(exc).__name__})
            else:
                status = response.status_code
                attempts.append({"attempt": attempt, "http_status": status})
                record["http_status"] = status
                if response.is_success:
                    summary, problem, foreign = self._summary(response, parse)
                    if problem is None:
                        return self._finish(out, record, Result.UPLOADED, summary=summary)
                    error = {**self._error(response), "detail": problem}
                    if foreign:
                        return self._finish(out, record, Result.REFUSED, error=error)
                elif status == 401 and not refreshed:
                    # Revoked or expired early: one new token, and the same try again.
                    refreshed = True
                    attempt -= 1
                    attempts[-1]["token_refused"] = True
                    self._ask_token()
                    continue
                else:
                    error = self._error(response)
                    stop = self._stop(response, status)
                    if stop is not None:
                        raise stop
                    if status not in RETRIED and status < 500:
                        return self._finish(out, record, Result.REFUSED, error=error)
                    retry_after = _seconds(response.headers.get("Retry-After"))
            if unsendable:
                # Raised here, outside the except block, so the httpx error (whose text
                # can hold the header) is not kept as this exception's __context__.
                raise NeurostoreConfigError(
                    f"the request to {self.url} cannot be sent: {error['detail']}"
                )
            if attempt <= RETRIES:
                delay = min(max(BACKOFF * 2 ** (attempt - 1), retry_after or 0), MAX_DELAY)
                self._sleep(delay)
        if not self._healthy():
            self._down = (
                f"{self.base_url} did not answer {RETRIES + 1} tries nor its health check "
                f"(last: {error.get('detail') or error.get('status')})"
            )
            raise NeurostoreUnavailable(self._down)
        # Up, and failing on this paper every time: its failure, not an outage, so the
        # papers after it are still sent.
        return self._finish(out, record, Result.BLOCKED, error=error)

    def _ask_token(self) -> None:
        """A token for the client, from its Auth0 tenant: NeurostoreAuthError when the
        tenant refuses the client, NeurostoreUnavailable when it does not answer."""
        url, audience, client_id, secret = self._grant
        try:
            response = self._client.post(f"{url}/oauth/token", timeout=HEALTH_TIMEOUT, json={
                "grant_type": "client_credentials", "client_id": client_id,
                "client_secret": secret, "audience": audience})
        except httpx.TransportError as exc:
            raise NeurostoreUnavailable(
                f"Auth0 at {url} did not answer: {type(exc).__name__}") from None
        if response.status_code in RETRIED or response.status_code >= 500:
            raise NeurostoreUnavailable(f"Auth0 at {url} answered {response.status_code}")
        try:
            answer = response.json() if response.is_success else {}
        except ValueError:
            answer = {}
        token = answer.get("access_token") if isinstance(answer, dict) else None
        if not isinstance(token, str) or not _TOKEN.fullmatch(token):
            try:
                why = response.json().get("error_description")
            except (ValueError, AttributeError):
                why = None
            raise NeurostoreAuthError(
                f"Auth0 at {url} gave the client in {CLIENT_ID_ENV} no token for {audience} "
                f"({response.status_code}{': ' + scrub(str(why), secret) if why else ''})")
        if self._token in _SCRUB.tokens:
            _SCRUB.tokens.remove(self._token)
        self._token = token
        _SCRUB.tokens.append(token)
        # No lifetime given: kept until neurostore refuses it.
        self._expires = self._clock() + float(answer.get("expires_in") or "inf")

    def _healthy(self) -> bool:
        """Whether the ingester's route is served: it answers a GET, which it does not
        take, with 405. A proxy with nothing behind it answers 502 or 503, and a server
        without the route 404."""
        try:
            return self._client.get(self.url, timeout=HEALTH_TIMEOUT).status_code == 405
        except Exception:  # noqa: BLE001 - any failure to answer is an outage
            return False

    def _stop(self, response: httpx.Response, status: int) -> Optional[StopRun]:
        """The error that stops the upload when this status means every paper would fail."""
        if status in (401, 403):
            return NeurostoreAuthError(
                f"neurostore refused the token Auth0 gave the client in {CLIENT_ID_ENV} with "
                f"{status} at {self.url}; its user, {self._grant[2]}@clients, must hold "
                f"neurostore's admin role, and its audience ({self._grant[1]}) must be the one "
                "this deployment checks"
            )
        if 300 <= status < 400:
            where = scrub(response.headers.get("Location", "?"), self._token)
            return NeurostoreConfigError(
                f"{self.url} redirects ({status}) to {where}; set neurostore_api_url to the "
                "base URL it names (without /api)"
            )
        # The ingester's own 404 is for an unknown base study ("Record <id> not found
        # in BaseStudy"); any other 404 means the route is not at this URL.
        if (status == 404 and "BaseStudy" not in response.text) or status in (405, 415):
            return NeurostoreConfigError(
                f"{self.url} answered {status}: the ingester is not at this URL or does not "
                "take this request; check neurostore_api_url (the base URL, without /api)"
            )
        if status == 422:
            skew = scrub("; ".join(_schema_version_skew(response)), self._token)
            if skew:
                return NeurostoreConfigError(
                    f"{self.url} takes study_schema documents at another schema version: "
                    f"{skew}. Every parse of this run would be refused alike; pin study_schema "
                    "to the version neurostore is pinned to, or deploy a neurostore pinned "
                    "to this one"
                )
        return None

    def _summary(self, response: httpx.Response, parse: Dict[str, Any]):
        """(the redacted summary, why it is not this parse's, whether it is this parse's
        filed under a base study the header does not name)."""
        try:
            summary = response.json()
        except ValueError:
            return None, "the 2xx body is not JSON, so not the ingester's summary", False
        if not isinstance(summary, dict) or summary.get("kind") not in SUMMARY_KINDS:
            return None, "the 2xx body is not the ingester's summary of a parse upload", False
        if summary.get("parse_id") != parse.get("parse_id"):
            return (None, f"the summary is of parse {summary.get('parse_id')!r}, not this one",
                    False)
        if not summary.get("base_study_id"):
            return None, "the summary names no base study", False
        expected = parse["header"].get("neurostore_base_study_id")
        if expected and summary["base_study_id"] != expected \
                and expected not in (summary.get("merged_from") or []):
            # Its id would be learned as this article's alias; the same body gets the same
            # answer, so it is the paper's refusal, not a failure to retry. A summary whose
            # merged_from names the expected base study followed a merge, and is this upload.
            return None, (f"the summary is of base study {summary['base_study_id']!r}, "
                          f"not {expected!r}"), True
        return redact(summary, self._token), None, False

    def _describe(self, exc: Exception) -> str:
        return f"{type(exc).__name__}: {scrub(str(exc), self._token)}"

    def _error(self, response: httpx.Response) -> Dict[str, Any]:
        error: Dict[str, Any] = {"status": response.status_code}
        try:
            parsed = response.json()
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            for field in ("title", "detail", "errors"):
                if field in parsed:
                    error[field] = parsed[field]
            codes = sorted({e["code"] for e in parsed.get("errors") or []
                            if isinstance(e, dict) and e.get("code")})
            if codes:
                error["codes"] = codes
        # Scrubbed whole, then cut: a cut through the token would leave its head behind.
        error["body_excerpt"] = scrub(response.text, self._token)[:BODY_EXCERPT]
        return redact(error, self._token)

    def _finish(
        self,
        out: Path,
        record: Dict[str, Any],
        result: Result,
        *,
        summary: Any = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        record["status"] = result.value
        if summary is not None:
            record["summary"] = summary
        if error is not None:
            record["error"] = redact(error, self._token)
        out.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            artifact = record
            _write_json(out / DRY_RUN_NAME, artifact)
        else:
            artifact = self._append(out / ARTIFACT_NAME, record, result)
        log = logger.info if result in (Result.UPLOADED, Result.DRY_RUN) else logger.warning
        log(
            "neurostore upload %s: %s%s",
            record["study_id"],
            result.value,
            f" ({record['http_status']})" if record.get("http_status") else "",
        )
        return artifact

    def _append(self, path: Path, record: Dict[str, Any], result: Result) -> Dict[str, Any]:
        """The artifact with `record` on top and added to the history; a success is
        also kept per endpoint, so a later failure cannot erase it."""
        previous: Dict[str, Any] = {}
        if path.exists():
            try:
                previous = json.loads(path.read_text("utf-8"))
            except ValueError:
                aside = path.with_name(path.name + ".unreadable")
                path.replace(aside)
                logger.warning("unreadable upload artifact moved to %s", aside)
        last_uploaded = dict(previous.get("last_uploaded") or {})
        if result is Result.UPLOADED:
            last_uploaded[record["endpoint"]] = {
                k: record[k] for k in ("at", "parse_id", "payload_sha256", "summary")
            }
        entry = {k: v for k, v in record.items() if k != "summary"}
        artifact = {
            **record,
            "last_uploaded": last_uploaded,
            "history": [*(previous.get("history") or []), entry],
        }
        _write_json(path, artifact)
        return artifact


def _schema_version_skew(response: httpx.Response) -> List[str]:
    """For each SCHEMA_VERSION error of a 422, which document states which version against
    the one neurostore is pinned to; empty when there is none."""
    try:
        parsed = response.json()
    except ValueError:
        return []
    errors = parsed.get("errors") if isinstance(parsed, dict) else None
    skew = []
    for error in errors if isinstance(errors, list) else []:
        if not isinstance(error, dict) or error.get("code") != "SCHEMA_VERSION":
            continue
        context = error.get("context")
        provided = context.get("provided") if isinstance(context, dict) else None
        provided = provided if isinstance(provided, dict) else {}
        skew.append(f"{error.get('field') or 'a document'} states "
                    f"{provided.get('schema_version', '?')}, neurostore is pinned to "
                    f"{provided.get('pinned', '?')}")
    return skew


def _seconds(value: Optional[str]) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _write_json(path: Path, value: Any) -> None:
    _write_atomic(path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False).encode())


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


__all__ = [
    "ARTIFACT_NAME",
    "AUTH0",
    "CLIENT_ID_ENV",
    "CLIENT_SECRET_ENV",
    "DRY_RUN_NAME",
    "PAYLOAD_NAME",
    "InvalidParse",
    "NeurostoreAuthError",
    "NeurostoreConfigError",
    "NeurostoreIngester",
    "NeurostoreUnavailable",
    "Result",
    "StopRun",
    "api_url",
    "auth0",
    "build_payload",
    "read_client",
    "redact",
    "scrub",
]
