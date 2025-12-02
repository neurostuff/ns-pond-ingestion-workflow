"""SQLAlchemy ORM models for the upload pipeline (minimal mapping)."""

from __future__ import annotations

import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

import shortuuid


def _gen_id() -> str:
    """Match Neurostore's ID scheme (shortuuid length 12)."""
    return shortuuid.ShortUUID().random(length=12)


class Base(DeclarativeBase):
    pass


class BaseStudy(Base):
    __tablename__ = "base_studies"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    publication: Mapped[str | None] = mapped_column(String, nullable=True)
    doi: Mapped[str | None] = mapped_column(String, nullable=True)
    pmid: Mapped[str | None] = mapped_column(String, nullable=True)
    pmcid: Mapped[str | None] = mapped_column(String, nullable=True)
    authors: Mapped[str | None] = mapped_column(String, nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    public: Mapped[bool | None] = mapped_column(Boolean, default=True)
    level: Mapped[str | None] = mapped_column(String, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata_", JSON, nullable=True)
    is_oa: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    has_coordinates: Mapped[bool] = mapped_column(Boolean, default=False)
    has_images: Mapped[bool] = mapped_column(Boolean, default=False)
    ace_fulltext: Mapped[str | None] = mapped_column(Text, nullable=True)
    pubget_fulltext: Mapped[str | None] = mapped_column(Text, nullable=True)

    versions: Mapped[list["Study"]] = relationship(
        back_populates="base_study",
        cascade="all, delete",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("doi", "pmid", name="doi_pmid"),
        CheckConstraint("level in ('group','meta')", name="ck_base_studies_level"),
    )


class Study(Base):
    __tablename__ = "studies"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    publication: Mapped[str | None] = mapped_column(String, nullable=True)
    doi: Mapped[str | None] = mapped_column(String, nullable=True)
    pmid: Mapped[str | None] = mapped_column(String, nullable=True)
    pmcid: Mapped[str | None] = mapped_column(String, nullable=True)
    authors: Mapped[str | None] = mapped_column(String, nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    public: Mapped[bool | None] = mapped_column(Boolean, default=True)
    level: Mapped[str | None] = mapped_column(String, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata_", JSON, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    source_id: Mapped[str | None] = mapped_column(String, nullable=True)
    source_updated_at = mapped_column(DateTime(timezone=True), nullable=True)
    base_study_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("base_studies.id", ondelete="CASCADE"), index=True
    )

    tables: Mapped[list["Table"]] = relationship(
        back_populates="study",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    analyses: Mapped[list["Analysis"]] = relationship(
        back_populates="study",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    base_study: Mapped[BaseStudy | None] = relationship(back_populates="versions")

    __table_args__ = (
        CheckConstraint("level in ('group','meta')", name="ck_studies_level"),
    )


class Table(Base):
    __tablename__ = "tables"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    study_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("studies.id", ondelete="CASCADE"), index=True
    )
    t_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    footer: Mapped[str | None] = mapped_column(Text, nullable=True)
    caption: Mapped[str | None] = mapped_column(Text, nullable=True)

    study: Mapped[Study | None] = relationship(back_populates="tables")
    analyses: Mapped[list["Analysis"]] = relationship(
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint("study_id", "t_id", name="uq_tables_study_t_id"),
    )


class Analysis(Base):
    __tablename__ = "analyses"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    study_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("studies.id", ondelete="CASCADE"), index=True
    )
    table_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("tables.id", ondelete="SET NULL"), index=True
    )
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata_", JSON, nullable=True)
    order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    study: Mapped[Study | None] = relationship(back_populates="analyses")
    table: Mapped[Table | None] = relationship(back_populates="analyses")
    points: Mapped[list["Point"]] = relationship(
        back_populates="analysis",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Point(Base):
    __tablename__ = "points"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    x: Mapped[float | None] = mapped_column()
    y: Mapped[float | None] = mapped_column()
    z: Mapped[float | None] = mapped_column()
    space: Mapped[str | None] = mapped_column(String, nullable=True)
    kind: Mapped[str | None] = mapped_column(String, nullable=True)
    image: Mapped[str | None] = mapped_column(String, nullable=True)
    label_id: Mapped[float | None] = mapped_column()
    analysis_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("analyses.id", ondelete="CASCADE"), index=True
    )
    cluster_size: Mapped[float | None] = mapped_column()
    subpeak: Mapped[bool | None] = mapped_column(Boolean, default=False)
    deactivation: Mapped[bool | None] = mapped_column(Boolean, default=False)
    order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    analysis: Mapped[Analysis | None] = relationship(back_populates="points")
    values: Mapped[list["PointValue"]] = relationship(
        "PointValue",
        back_populates="point",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class PointValue(Base):
    __tablename__ = "point_values"

    id: Mapped[str] = mapped_column(Text, primary_key=True, default=_gen_id)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    point_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("points.id", ondelete="CASCADE"), index=True, nullable=True
    )
    kind: Mapped[str | None] = mapped_column(String, nullable=True)
    value: Mapped[float | None] = mapped_column(Float, nullable=True)

    point: Mapped[Point | None] = relationship(back_populates="values")
