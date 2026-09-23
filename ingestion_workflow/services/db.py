"""Database connectivity helpers for the upload stage."""

from __future__ import annotations

import socket
import subprocess
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Callable, Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sshtunnel import SSHTunnelForwarder

from ingestion_workflow.config import Settings
from ingestion_workflow.services.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SSHTunnel(AbstractContextManager):
    """Context manager for SSH tunneling to the remote Postgres service."""

    settings: Settings
    forwarder: Optional[SSHTunnelForwarder] = None

    def __enter__(self) -> "SSHTunnel":
        self.start()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.stop()

    def start(self) -> None:
        """Start the SSH tunnel if configured."""
        if not self.settings.upload_use_ssh:
            logger.debug("SSH tunneling disabled via settings.")
            return

        if self.forwarder is not None:
            logger.debug("SSH tunnel already running.")
            return

        # Name the environment, so a log or a console never leaves it ambiguous
        # which deployment a run wrote to.
        logger.info(
            "Connecting to the %s database: %s@%s -> %s:%s",
            getattr(self.settings.neurostore_env, "value", self.settings.neurostore_env),
            self.settings.upload_ssh_user,
            self.settings.upload_ssh_host,
            self.settings.upload_remote_bind_host,
            self.settings.upload_remote_bind_port,
            extra={"to_console": True},
        )
        remote_target = _resolve_remote_bind_host(self.settings)
        self.forwarder = SSHTunnelForwarder(
            (self.settings.upload_ssh_host, 22),
            ssh_username=self.settings.upload_ssh_user,
            ssh_private_key=str(self.settings.upload_ssh_key.expanduser()),
            remote_bind_address=(remote_target, self.settings.upload_remote_bind_port),
            local_bind_address=("localhost", self.settings.upload_local_forward_port),
        )
        self.forwarder.start()

    def stop(self) -> None:
        """Stop the SSH tunnel."""
        if self.forwarder is None:
            return
        try:
            self.forwarder.stop()
        finally:
            self.forwarder = None

    @property
    def local_port(self) -> int:
        """Return the local port in use for the tunnel."""
        if self.forwarder is not None and self.forwarder.is_active:
            return self.forwarder.local_bind_port
        return self.settings.upload_local_forward_port


class SessionFactory:
    """Create SQLAlchemy sessions for the upload pipeline."""

    def __init__(
        self,
        settings: Settings,
        engine: Optional[Engine] = None,
        *,
        tunnel: Optional[SSHTunnel] = None,
    ) -> None:
        self.settings = settings
        self._engine = engine
        self._tunnel = tunnel
        self._sessionmaker: Optional[sessionmaker] = None

    def bind_engine(self, engine: Engine) -> None:
        """Bind a pre-configured engine."""
        self._engine = engine
        self._sessionmaker = None

    def configure(self, engine_builder: Callable[[str], Engine] | None = None) -> None:
        """Build and bind an engine if one is not already configured."""
        if self._engine is not None:
            return
        url = self._resolved_db_url()
        if engine_builder is None:
            self._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={"connect_timeout": self.settings.upload_connect_timeout},
                future=True,
            )
        else:
            self._engine = engine_builder(url)
        self._sessionmaker = None

    def _resolved_db_url(self) -> str:
        host = self.settings.upload_db_host or "localhost"
        port = (
            self._tunnel.local_port
            if self.settings.upload_use_ssh and self._tunnel is not None
            else self.settings.upload_remote_bind_port
        )
        return (
            f"postgresql+psycopg://{self.settings.upload_db_user}"
            f":{self.settings.upload_db_password}@{host}:{port}/{self.settings.upload_db_name}"
        )

    def _ensure_sessionmaker(self) -> sessionmaker:
        if self._sessionmaker is None:
            if self._engine is None:
                raise RuntimeError("SessionFactory requires an engine to be configured.")
            self._sessionmaker = sessionmaker(bind=self._engine, future=True)
        return self._sessionmaker

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Yield a SQLAlchemy session."""
        maker = self._ensure_sessionmaker()
        session: Session = maker()
        try:
            yield session
        finally:
            session.close()

    def healthcheck(self) -> bool:
        """Simple connectivity check."""
        maker = self._ensure_sessionmaker()
        with maker() as session:
            try:
                session.execute(text("SELECT 1"))
                return True
            except Exception as exc:  # pragma: no cover - stubbed behavior
                logger.error("DB healthcheck failed: %s", exc)
                return False


__all__ = ["SSHTunnel", "SessionFactory"]


def _resolve_remote_bind_host(settings: Settings) -> str:
    """
    Resolve the remote bind host for the SSH tunnel.

    If the configured host does not resolve locally (common when the target is a
    container name only resolvable inside the remote Docker network), attempt to
    look up the container's IP via SSH + `docker inspect` on the remote host.
    """
    host = settings.upload_remote_bind_host
    if not host:
        return host

    try:
        socket.getaddrinfo(host, settings.upload_remote_bind_port)
        return host  # resolvable locally, no action needed
    except Exception:
        logger.debug("Local DNS lookup failed for %s; attempting remote docker inspect.", host)

    network_name = settings.upload_remote_container_network
    address = _container_ip(settings, host, network_name)
    if address:
        logger.info(
            "Resolved remote container %s on network %s to %s",
            host,
            network_name,
            address,
        )
        return address

    # The container need not be on the configured network -- a compose stack
    # names its own. Rather than fail the tunnel, take whichever address it
    # does have; a container on several networks is reachable on any of them.
    address = _container_ip(settings, host, network=None)
    if address:
        logger.info(
            "Remote container %s is not on %s; using its address %s",
            host,
            network_name,
            address,
        )
        return address

    logger.warning(
        "Could not resolve remote container %s to an address; using the name, "
        "which only works if the remote host can resolve it.",
        host,
    )
    return host


def _container_ip(settings: Settings, host: str, network: Optional[str]) -> Optional[str]:
    """Ask the remote docker for a container's IP, on one network or any."""
    if network:
        template = (
            '{{ (index .NetworkSettings.Networks "' + network + '").IPAddress }}'
        )
    else:
        template = "{{ range .NetworkSettings.Networks }}{{ .IPAddress }} {{ end }}"

    try:
        output = subprocess.check_output(
            [
                "ssh",
                f"{settings.upload_ssh_user}@{settings.upload_ssh_host}",
                f"docker inspect -f '{template}' {host}",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).strip()
    except Exception as exc:  # pragma: no cover - best-effort resolution
        logger.debug("docker inspect for %s failed: %s", host, exc)
        return None

    for candidate in output.split():
        if candidate and candidate != "<no value>":
            return candidate
    return None
