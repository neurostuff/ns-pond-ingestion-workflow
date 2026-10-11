"""Upload one paper's coordinate parse through neurostore's study_schema ingester.

The ingester is `POST {api}/api/study-schema-uploads/`, admin only. Its body is
`{"coordinate_parse": <CoordinateParse>}`, here with the paper's ParsedPaper beside it
(table captions and the bibliography of a base study it creates) and optionally a
`run_id`; its 200 is a summary of what the upload changed. The upload is one
transaction and is keyed by the parse's analysis keys, so sending the same parse
again changes nothing, and a re-parse that reads an analysis from other cells is
aliased onto the old one rather than duplicated.

Each paper gets one artifact, `<study>/upload/neurostore_ingest.json`, saying what
happened: the server's summary, or the refusal or transient block with its status and
an excerpt of the body. A dry run validates and writes the exact request body to
`<study>/upload/neurostore_payload.json` instead of sending it.

How a response is treated:

    2xx                       uploaded; the summary is recorded
    401, 403                  the token is refused: NeurostoreAuthError stops the run
    429, 5xx, timeout,        retried with backoff, then recorded as blocked
    connection error
    any other status          refused: recorded at once and never retried, since the
                              same body gets the same answer

The admin token is read from the environment variable `neurostore_token_env` names
and is never logged or written; anything in a response that names or echoes it is
redacted before the artifact is written.
"""

from __future__ import annotations

import hashlib
import json
import os
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
PAYLOAD_NAME = "neurostore_payload.json"
#: Bump when the artifact's fields change meaning.
ARTIFACT_VERSION = 1

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
_SECRET_WORDS = ("token", "authorization", "password", "secret", "api_key", "apikey", "cookie")
REDACTED = "[redacted]"


class Result(str, Enum):
    UPLOADED = "uploaded"
    DRY_RUN = "dry_run"
    #: A status the server will give again for the same body (400, 404, 422, ...).
    REFUSED = "refused"
    #: 429, 5xx, a timeout or a lost connection on every attempt.
    BLOCKED = "blocked"
    UNAUTHORIZED = "unauthorized"
    #: Failed validation here, so never sent.
    INVALID = "invalid"
    #: sync wrote no parse for the paper.
    MISSING = "missing"

    @property
    def catalog_status(self) -> Status:
        if self in (Result.UPLOADED, Result.DRY_RUN):
            return Status.OK
        if self in (Result.REFUSED, Result.INVALID, Result.MISSING):
            return Status.PERMANENT
        return Status.FAILED


class NeurostoreAuthError(RuntimeError):
    """The token is missing or refused; every later paper would fail the same way."""


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
    """The token from the environment, or from `.env` as Settings reads its own fields."""
    return os.environ.get(name) or dotenv_values(".env").get(name) or None


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


def redact(value: Any, token: Optional[str]) -> Any:
    """`value` with secret-named keys and every copy of the token replaced."""
    if isinstance(value, dict):
        return {
            k: REDACTED if any(w in str(k).lower() for w in _SECRET_WORDS) else redact(v, token)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v, token) for v in value]
    if isinstance(value, str) and token and token in value:
        return value.replace(token, REDACTED)
    return value


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
        send_parsed_paper: bool = True,
        dry_run: bool = False,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.url = base_url.rstrip("/") + UPLOAD_PATH
        self.token_env = token_env
        self.retries = retries
        self.backoff = backoff
        self.send_parsed_paper = send_parsed_paper
        self.dry_run = dry_run
        self._sleep = sleep
        self._token = None if dry_run else read_token(token_env)
        if not dry_run and not self._token:
            raise NeurostoreAuthError(
                f"no neurostore token: set {token_env} to an admin token for {base_url}"
            )
        self._client = None if dry_run else httpx.Client(timeout=timeout)

    @classmethod
    def from_settings(cls, settings, **kwargs) -> "NeurostoreIngester":
        kwargs.setdefault("dry_run", settings.dry_run)
        return cls(
            api_url(settings),
            token_env=settings.neurostore_token_env,
            timeout=settings.neurostore_upload_timeout,
            retries=settings.neurostore_upload_retries,
            backoff=settings.neurostore_upload_backoff,
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

        `study_dir` is `<ns_pond_root>/<study_id>`, as sync wrote it. Raises
        NeurostoreAuthError, after writing the artifact, when the token is refused.
        """
        study_dir = Path(study_dir)
        out = Path(out_dir) if out_dir is not None else study_dir / UPLOAD_DIR
        artifact: Dict[str, Any] = {
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
            return self._finish(out, artifact, Result.MISSING, error={"detail": str(exc)})
        except (InvalidParse, json.JSONDecodeError) as exc:
            problems = getattr(exc, "problems", [str(exc)])
            error = {"detail": "invalid parse", "problems": problems}
            return self._finish(out, artifact, Result.INVALID, error=error)
        header = payload["coordinate_parse"]["header"]
        artifact.update(
            article_id=header.get("article_id"),
            parse_id=payload["coordinate_parse"].get("parse_id"),
            checks=checks,
        )
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        artifact["payload_sha256"] = hashlib.sha256(body).hexdigest()
        if self.dry_run:
            out.mkdir(parents=True, exist_ok=True)
            _write_atomic(out / PAYLOAD_NAME, body)
            artifact["payload_path"] = str(out / PAYLOAD_NAME)
            return self._finish(out, artifact, Result.DRY_RUN)
        return self._send(out, artifact, body)

    def _send(self, out: Path, artifact: Dict[str, Any], body: bytes) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}
        attempts: List[Dict[str, Any]] = []
        artifact["attempts"] = attempts
        error: Dict[str, Any] = {}
        for attempt in range(1, self.retries + 2):
            retry_after = None
            try:
                response = self._client.post(self.url, content=body, headers=headers)
            except httpx.TimeoutException:
                error = {"detail": "timeout"}
                attempts.append({"attempt": attempt, "error": "timeout"})
            except httpx.TransportError as exc:
                error = {"detail": type(exc).__name__}
                attempts.append({"attempt": attempt, "error": type(exc).__name__})
            else:
                status = response.status_code
                attempts.append({"attempt": attempt, "http_status": status})
                if response.is_success:
                    artifact["http_status"] = status
                    summary = self._json(response)
                    return self._finish(out, artifact, Result.UPLOADED, summary=summary)
                error = self._error(response)
                if status in (401, 403):
                    artifact["http_status"] = status
                    self._finish(out, artifact, Result.UNAUTHORIZED, error=error)
                    raise NeurostoreAuthError(
                        f"neurostore refused the token in {self.token_env} with {status} at "
                        f"{self.url}; it must be an admin token for this deployment. "
                        "Stopping the run."
                    )
                if status != 429 and status < 500:
                    artifact["http_status"] = status
                    return self._finish(out, artifact, Result.REFUSED, error=error)
                retry_after = _seconds(response.headers.get("Retry-After"))
            if attempt <= self.retries:
                delay = min(max(self.backoff * 2 ** (attempt - 1), retry_after or 0), MAX_DELAY)
                self._sleep(delay)
        artifact["http_status"] = error.get("status")
        return self._finish(out, artifact, Result.BLOCKED, error=error)

    def _json(self, response: httpx.Response) -> Any:
        try:
            return redact(response.json(), self._token)
        except ValueError:
            return {"body_excerpt": redact(response.text[:BODY_EXCERPT], self._token)}

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
        error["body_excerpt"] = response.text[:BODY_EXCERPT]
        return redact(error, self._token)

    def _finish(
        self,
        out: Path,
        artifact: Dict[str, Any],
        result: Result,
        *,
        summary: Any = None,
        error: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        artifact["status"] = result.value
        if summary is not None:
            artifact["summary"] = summary
        if error is not None:
            artifact["error"] = error
        out.mkdir(parents=True, exist_ok=True)
        _write_atomic(
            out / ARTIFACT_NAME,
            json.dumps(artifact, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8"),
        )
        log = logger.info if result in (Result.UPLOADED, Result.DRY_RUN) else logger.warning
        log(
            "neurostore upload %s: %s%s",
            artifact["study_id"],
            result.value,
            f" ({artifact['http_status']})" if artifact.get("http_status") else "",
        )
        return artifact


def _seconds(value: Optional[str]) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


__all__ = [
    "API_URLS",
    "ARTIFACT_NAME",
    "PAYLOAD_NAME",
    "InvalidParse",
    "NeurostoreAuthError",
    "NeurostoreIngester",
    "Result",
    "api_url",
    "build_payload",
    "read_token",
    "redact",
]
