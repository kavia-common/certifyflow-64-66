from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    Column,
    String,
    DateTime,
    Enum,
    ForeignKey,
    Text,
    Integer,
    JSON,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, Mapped, mapped_column

from .db import Base


class RunStatusEnum(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    partial = "partial"


class AttemptStatusEnum(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    canceled = "canceled"


class Run(Base):
    __tablename__ = "runs"
    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    correlation_key: Mapped[Optional[str]] = mapped_column(String(255), index=True, unique=True, nullable=True)
    branch: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    target_env: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)

    # List of certification types as JSON array of strings
    certification_types: Mapped[list] = mapped_column(JSON, default=list)

    attempts: Mapped[List["Attempt"]] = relationship("Attempt", back_populates="run", cascade="all, delete-orphan")
    assets: Mapped[List["Asset"]] = relationship("Asset", back_populates="run", cascade="all, delete-orphan")
    notification: Mapped[Optional["NotificationSetting"]] = relationship(
        "NotificationSetting", back_populates="run", uselist=False, cascade="all, delete-orphan"
    )


class Attempt(Base):
    __tablename__ = "attempts"
    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("runs.run_id", ondelete="CASCADE"), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    executor: Mapped[str] = mapped_column(String(32), default="local")
    correlation_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    run: Mapped["Run"] = relationship("Run", back_populates="attempts")
    assets: Mapped[List["Asset"]] = relationship("Asset", back_populates="attempt", cascade="all, delete-orphan")

    __table_args__ = (
        # Idempotency within a run for attempts via correlation_key (nullable)
        UniqueConstraint("run_id", "correlation_key", name="uq_attempt_ckey_per_run"),
    )


class Asset(Base):
    __tablename__ = "assets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("runs.run_id", ondelete="CASCADE"), index=True, nullable=True)
    attempt_id: Mapped[Optional[str]] = mapped_column(String(64), ForeignKey("attempts.attempt_id", ondelete="CASCADE"), index=True, nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    run: Mapped[Optional["Run"]] = relationship("Run", back_populates="assets")
    attempt: Mapped[Optional["Attempt"]] = relationship("Attempt", back_populates="assets")


class NotificationSetting(Base):
    __tablename__ = "notifications"
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("runs.run_id", ondelete="CASCADE"), primary_key=True)
    notification_url: Mapped[Optional[str]] = mapped_column(String(2083), nullable=True)
    notification_email: Mapped[Optional[str]] = mapped_column(String(320), nullable=True)

    run: Mapped["Run"] = relationship("Run", back_populates="notification")
