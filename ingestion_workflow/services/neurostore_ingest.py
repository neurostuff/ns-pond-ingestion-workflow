"""Upload one paper's coordinate parse through neurostore's study_schema ingester.

The ingester is `POST {api}/api/study-schema-uploads/`, admin only. Its body is
`{"coordinate_parse": <CoordinateParse>}`, here with the paper's ParsedPaper beside it
(table captions and the bibliography of a base study it creates) and optionally a
`run_id`; its 200 is a summary of what the upload changed. The upload is one
transaction and is keyed by the parse's analysis keys, so sending the same parse
again changes nothing, and a re-parse that reads an analysis from other cells is
aliased onto the old one rather than duplicated.

Each paper gets one artifact, `<study>/upload/neurostore_ingest.json`: the latest
upload's outcome at the top, the last successful summary per endpoint under
`last_uploaded`, and every upload's outcome, in order, under `history`. A dry run
validates and writes the exact request body to `neurostore_payload.json` and its own
record to `neurostore_dry_run.json`, under the required `dry_run_root/<study>/` (from_settings:
`<data_root>/neurostore_dry_run/<study>/`, outside the ns-pond tree); it never touches
the upload artifact.

How a response is treated:

    2xx with this parse's summary   uploaded; the summary is recorded
    401, 403, a malformed token     NeurostoreAuthError stops the run
    3xx, 405, 415, a 404 that is    NeurostoreConfigError stops the run: the URL or
    not about a base study          the request is wrong for every paper alike
    429, 5xx, timeout, lost         retried with backoff, then recorded as blocked;
    connection, a 2xx that is not   `max_blocked` papers blocked in a row stop the
    this parse's summary            run (NeurostoreUnavailable)
    any other status                refused: recorded at once and never retried,
                                    since the same body gets the same answer

The admin token is read from the environment variable `neurostore_token_env` names
and is never logged or written: request headers are never recorded, and the token and
any Authorization or Bearer value are scrubbed from every recorded string and from the
HTTP libraries' log lines.
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
from dotenv import dotenv_values
from pydantic import ValidationError
from study_schema import layouts
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
#: Bump when the artifact's fields change meaning.
ARTIFACT_VERSION = 2

#: The API each `neurostore_env` uploads to when `neurostore_api_url` is unset. dev
#: has no entry because its public URL has not been confirmed; set the URL for it.
API_URLS = {
    "staging": "https://staging.neurostore.xyz",
    "production": "https://neurostore.org",
}

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
    #: A status the server will give again for the same body (400, 422, an unknown
    #: base study's 404, ...).
    REFUSED = "refused"
    #: 429, 5xx, a timeout, a lost connection or a 2xx that is not this parse's
    #: summary, on every attempt.
    BLOCKED = "blocked"
    UNAUTHORIZED = "unauthorized"
    #: The URL or the request is wrong for every paper (3xx, 405, 415, a route 404).
    MISCONFIGURED = "misconfigured"
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
    """The token is missing, malformed or refused."""


class NeurostoreConfigError(StopRun):
    """The URL or the request is wrong for every paper alike."""


class NeurostoreUnavailable(StopRun):
    """Too many papers in a row were blocked by transient failures."""


class InvalidParse(ValueError):
    def __init__(self, problems: List[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def api_url(settings) -> str:
    """The API base URL: `neurostore_api_url`, else the one `neurostore_env` names."""
    if settings.neurostore_api_url:
        return settings.neurostore_api_url.rstrip("/")
    env = getattr(settings.neurostore_env, "value", settings.neurostore_env)
    if env not in API_URLS:
        raise ValueError(f"no API URL is known for neurostore_env {env!r}; set neurostore_api_url")
    return API_URLS[env]


def read_token(name: str) -> Optional[str]:
    """The token from the environment, or from `.env` as Settings reads its own fields.

    Stripped, because a pasted trailing newline or space is never part of a token.
    """
    value = os.environ.get(name) or dotenv_values(".env").get(name) or ""
    return value.strip() or None


def build_payload(
    study_dir: Path, *, parsed_paper: bool = True, run_id: Optional[str] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """The request body for one paper, and the checks it passed.

    The documents are sent as sync wrote them, after they validate as study_schema's
    models and pass `layouts.check_paper` (the same checks sync made writing them,
    including each analysis key against its cells and the text against its hash).
    Raises FileNotFoundError when either file is missing and InvalidParse when a
    check fails.
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
    problems += _json_schema_problems(raw_parse, raw_paper, checks)
    if problems:
        raise InvalidParse(problems)
    payload: Dict[str, Any] = {"coordinate_parse": raw_parse}
    if parsed_paper:
        payload["parsed_paper"] = raw_paper
    if run_id:
        payload["run_id"] = run_id
    return payload, checks


def _json_schema_problems(raw_parse, raw_paper, checks: List[str]) -> List[str]:
    """The ingester's own check, study_schema's JSON Schema, when jsonschema is installed.

    It is not a dependency of this package; `checks` in the artifact says whether it ran.
    """
    try:
        import jsonschema
    except ImportError:
        return []
    from study_schema.jsonschema import load

    problems = []
    for name, raw in (("coordinate-parse", raw_parse), ("parsed-paper", raw_paper)):
        validator = jsonschema.validators.validator_for(load(name))(load(name))
        problems += [
            f"{name}/{'/'.join(str(p) for p in e.path)}: {e.message}"
            for e in validator.iter_errors(raw)
        ]
    checks.append("json_schema")
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
    """Scrubs every live ingester's token from a log record before any handler sees it.

    One filter for the module, installed at import, so it keeps working for an
    ingester that is never closed and for log lines written after it is.
    """

    def __init__(self) -> None:
        super().__init__()
        self.tokens: set = set()

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        clean = scrub(message, None)
        for token in self.tokens:
            clean = scrub(clean, token)
        if clean != message:
            record.msg, record.args = clean, ()
        return True


_SCRUB = _Scrub()
for _name in (*_HTTP_LOGGERS, logger.name):
    logging.getLogger(_name).addFilter(_SCRUB)


class NeurostoreIngester:
    """Posts parses to one neurostore API with one admin token; one per run."""

    def __init__(
        self,
        base_url: str,
        *,
        token_env: str = "NEUROSTORE_TOKEN",
        timeout: float = 120.0,
        retries: int = 4,
        backoff: float = 2.0,
        max_blocked: int = 20,
        send_parsed_paper: bool = True,
        dry_run: bool = False,
        dry_run_root: Optional[Path] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.url = base_url.rstrip("/") + UPLOAD_PATH
        self.token_env = token_env
        self.retries = retries
        self.backoff = backoff
        self.max_blocked = max_blocked
        self.send_parsed_paper = send_parsed_paper
        self.dry_run = dry_run
        if dry_run and dry_run_root is None:
            raise ValueError(
                "a dry run needs dry_run_root, a directory outside the ns-pond tree for its "
                "payloads (from_settings uses <data_root>/neurostore_dry_run)"
            )
        self.dry_run_root = Path(dry_run_root) if dry_run_root is not None else None
        self._sleep = sleep
        self._blocked_in_a_row = 0
        self._token = None if dry_run else read_token(token_env)
        if not dry_run and not self._token:
            raise NeurostoreAuthError(
                f"no neurostore token: set {token_env} to an admin token for {base_url}"
            )
        if self._token and not _TOKEN.fullmatch(self._token):
            raise NeurostoreAuthError(
                f"the token in {token_env} is not a valid header value: it holds whitespace "
                "or a non-ASCII character"
            )
        if self._token:
            _SCRUB.tokens.add(self._token)
        self._client = None if dry_run else httpx.Client(timeout=timeout)

    @classmethod
    def from_settings(cls, settings, **kwargs) -> "NeurostoreIngester":
        kwargs.setdefault("dry_run", settings.dry_run)
        # A run's dry-run output belongs with the run's own data, not in the ns-pond tree.
        kwargs.setdefault("dry_run_root", Path(settings.data_root) / DRY_RUN_DIR)
        return cls(
            api_url(settings),
            token_env=settings.neurostore_token_env,
            timeout=settings.neurostore_upload_timeout,
            retries=settings.neurostore_upload_retries,
            backoff=settings.neurostore_upload_backoff,
            max_blocked=settings.neurostore_upload_max_blocked,
            send_parsed_paper=settings.neurostore_upload_parsed_paper,
            **kwargs,
        )

    def __repr__(self) -> str:
        return f"NeurostoreIngester({self.url!r}, dry_run={self.dry_run})"

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def __enter__(self) -> "NeurostoreIngester":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def upload(
        self, study_dir: Path, *, out_dir: Optional[Path] = None, run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Upload one paper, write its artifact, and return it.

        `study_dir` is `<ns_pond_root>/<study_id>`, as sync wrote it. Raises a StopRun,
        after writing the artifact, when the token or the URL is wrong or too many
        papers in a row were blocked.
        """
        study_dir = Path(study_dir)
        if out_dir is not None:
            out = Path(out_dir)
        elif self.dry_run:
            out = self.dry_run_root / study_dir.name
        else:
            out = study_dir / UPLOAD_DIR
        record: Dict[str, Any] = {
            "artifact_version": ARTIFACT_VERSION,
            "study_id": study_dir.name,
            "endpoint": self.url,
            "dry_run": self.dry_run,
            "at": utcnow(),
        }
        try:
            payload, checks = build_payload(
                study_dir, parsed_paper=self.send_parsed_paper, run_id=run_id
            )
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
        artifact = self._send(out, record, body, parse)
        if artifact["status"] == Result.BLOCKED.value:
            self._blocked_in_a_row += 1
            if self.max_blocked and self._blocked_in_a_row >= self.max_blocked:
                raise NeurostoreUnavailable(
                    f"{self._blocked_in_a_row} papers in a row were blocked by {self.url} "
                    f"(last: {artifact['error'].get('detail')}); stopping the run until it "
                    "answers again."
                )
        else:
            self._blocked_in_a_row = 0
        return artifact

    def _send(
        self, out: Path, record: Dict[str, Any], body: bytes, parse: Dict[str, Any]
    ) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}
        attempts: List[Dict[str, Any]] = []
        record["attempts"] = attempts
        error: Dict[str, Any] = {}
        for attempt in range(1, self.retries + 2):
            retry_after = None
            unsendable = False
            record.pop("http_status", None)
            try:
                response = self._client.post(self.url, content=body, headers=headers)
            except httpx.TimeoutException:
                error = {"detail": "timeout"}
                attempts.append({"attempt": attempt, "error": "timeout"})
            except (httpx.LocalProtocolError, httpx.UnsupportedProtocol) as exc:
                error = {"detail": self._describe(exc)}
                attempts.append({"attempt": attempt, "error": type(exc).__name__})
                unsendable = True
            except httpx.TransportError as exc:
                error = {"detail": self._describe(exc)}
                attempts.append({"attempt": attempt, "error": type(exc).__name__})
            else:
                status = response.status_code
                attempts.append({"attempt": attempt, "http_status": status})
                record["http_status"] = status
                if response.is_success:
                    summary, problem = self._summary(response, parse)
                    if problem is None:
                        return self._finish(out, record, Result.UPLOADED, summary=summary)
                    error = {**self._error(response), "detail": problem}
                else:
                    error = self._error(response)
                    stop = self._stop(response, status)
                    if stop is not None:
                        result, exc_class, message = stop
                        self._finish(out, record, result, error=error)
                        raise exc_class(f"{message} Stopping the run.")
                    if status != 429 and status < 500:
                        return self._finish(out, record, Result.REFUSED, error=error)
                    retry_after = _seconds(response.headers.get("Retry-After"))
            if unsendable:
                # Raised here, outside the except block, so the httpx error (whose text
                # can hold the header) is not kept as this exception's __context__.
                self._finish(out, record, Result.MISCONFIGURED, error=error)
                raise NeurostoreConfigError(
                    f"the request to {self.url} cannot be sent: {error['detail']}. "
                    "Stopping the run."
                )
            if attempt <= self.retries:
                delay = min(max(self.backoff * 2 ** (attempt - 1), retry_after or 0), MAX_DELAY)
                self._sleep(delay)
        return self._finish(out, record, Result.BLOCKED, error=error)

    def _stop(self, response: httpx.Response, status: int):
        """(result, exception, message) when this status means every paper would fail."""
        if status in (401, 403):
            return Result.UNAUTHORIZED, NeurostoreAuthError, (
                f"neurostore refused the token in {self.token_env} with {status} at "
                f"{self.url}; it must be an admin token for this deployment."
            )
        if 300 <= status < 400:
            where = scrub(response.headers.get("Location", "?"), self._token)
            return Result.MISCONFIGURED, NeurostoreConfigError, (
                f"{self.url} redirects ({status}) to {where}; set neurostore_api_url to the "
                "base URL it names (https, without /api)."
            )
        # The ingester's own 404 is for an unknown base study ("Record <id> not found
        # in BaseStudy"); any other 404 means the route is not at this URL.
        if (status == 404 and "BaseStudy" not in response.text) or status in (405, 415):
            return Result.MISCONFIGURED, NeurostoreConfigError, (
                f"{self.url} answered {status}: the ingester is not at this URL or does not "
                "take this request; check neurostore_api_url (the base URL, without /api)."
            )
        return None

    def _summary(self, response: httpx.Response, parse: Dict[str, Any]):
        """The redacted summary, and why it is not this parse's summary, if it is not."""
        try:
            summary = response.json()
        except ValueError:
            return None, "the 2xx body is not JSON, so not the ingester's summary"
        if not isinstance(summary, dict) or summary.get("kind") not in SUMMARY_KINDS:
            return None, "the 2xx body is not the ingester's summary of a parse upload"
        if summary.get("parse_id") != parse.get("parse_id"):
            return None, f"the summary is of parse {summary.get('parse_id')!r}, not this one"
        expected = (parse["header"].get("identifiers") or {}).get("neurostore_base_study_id")
        if not summary.get("base_study_id") or (
            expected and summary["base_study_id"] != expected
        ):
            return None, (
                f"the summary is of base study {summary.get('base_study_id')!r}, "
                f"not {expected!r}"
            )
        return redact(summary, self._token), None

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
    "API_URLS",
    "ARTIFACT_NAME",
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
    "build_payload",
    "read_token",
    "redact",
    "scrub",
]
