"""Database connectivity helpers for the upload stage."""

from __future__ import annotations

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

        logger.info(
            "Starting SSH tunnel %s -> %s:%s",
            self.settings.upload_ssh_host,
            self.settings.upload_remote_bind_host,
            self.settings.upload_remote_bind_port,
        )
        self.forwarder = SSHTunnelForwarder(
            (self.settings.upload_ssh_host, 22),
            ssh_username=self.settings.upload_ssh_user,
            ssh_private_key=str(self.settings.upload_ssh_key.expanduser()),
            remote_bind_address=(
                self.settings.upload_remote_bind_host,
                self.settings.upload_remote_bind_port,
            ),
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
