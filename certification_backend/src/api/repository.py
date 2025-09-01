from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Run, Attempt, Asset, NotificationSetting
from .schemas import (
    RunStatus,
    AttemptStatus,
    AssetRef,
    NotificationTarget,
)


def _to_asset_ref(a: Asset) -> AssetRef:
    return AssetRef(name=a.name, path=a.path, content_type=a.content_type, size_bytes=a.size_bytes)


def _to_attempt_status(a: Attempt) -> AttemptStatus:
    assets = [_to_asset_ref(x) for x in a.assets]
    return AttemptStatus(
        attempt_id=a.attempt_id,
        run_id=a.run_id,
        status=a.status,
        started_at=a.started_at,
        finished_at=a.finished_at,
        message=a.message,
        assets=assets,
        metrics=a.metrics or {},
        executor=a.executor or "local",
        correlation_key=a.correlation_key,
    )


def _aggregate_run_status_from_attempts(attempts: List[Attempt]) -> str:
    statuses = {a.status for a in attempts if a.status}
    if not statuses:
        return "queued"
    if "running" in statuses:
        return "running"
    if statuses == {"succeeded"}:
        return "succeeded"
    if statuses == {"failed"}:
        return "failed"
    if "failed" in statuses and "succeeded" in statuses:
        return "partial"
    # default partial if mixed
    return "partial"


def _to_run_status(r: Run) -> RunStatus:
    attempts = [_to_attempt_status(a) for a in sorted(r.attempts, key=lambda x: (x.started_at or r.created_at))]
    assets = [_to_asset_ref(a) for a in r.assets]
    return RunStatus(
        run_id=r.run_id,
        correlation_key=r.correlation_key,
        branch=r.branch,
        target_env=r.target_env,
        certification_types=list(r.certification_types or []),
        created_at=r.created_at,
        last_updated=r.last_updated,
        status=r.status,
        attempts=attempts,
        assets=assets,
    )


# PUBLIC_INTERFACE
def create_or_get_run(session: Session, run: Run, notification: Optional[NotificationTarget]) -> Run:
    """Create a run, or return existing one if correlation key enforces idempotency."""
    if run.correlation_key:
        existing = session.execute(select(Run).where(Run.correlation_key == run.correlation_key)).scalar_one_or_none()
        if existing:
            return existing

    session.add(run)
    session.flush()

    # Save notification if provided
    if notification and (notification.notification_url or notification.notification_email):
        ns = NotificationSetting(
            run_id=run.run_id,
            notification_url=str(notification.notification_url) if notification.notification_url else None,
            notification_email=str(notification.notification_email) if notification.notification_email else None,
        )
        session.merge(ns)
        session.flush()
    return run


# PUBLIC_INTERFACE
def list_runs(session: Session) -> List[RunStatus]:
    """List all runs with their attempts and assets eagerly loaded."""
    runs = session.execute(select(Run)).scalars().all()
    return [_to_run_status(r) for r in runs]


# PUBLIC_INTERFACE
def get_run(session: Session, run_id: str) -> Optional[RunStatus]:
    """Get run by run_id."""
    r = session.get(Run, run_id)
    return _to_run_status(r) if r else None


# PUBLIC_INTERFACE
def find_run_by_correlation_key(session: Session, ckey: Optional[str]) -> Optional[RunStatus]:
    """Find run by correlation key if provided."""
    if not ckey:
        return None
    r = session.execute(select(Run).where(Run.correlation_key == ckey)).scalar_one_or_none()
    return _to_run_status(r) if r else None


# PUBLIC_INTERFACE
def create_attempt(
    session: Session,
    run_id: str,
    types: List[str],
    correlation_key: Optional[str],
) -> AttemptStatus:
    """Create an attempt under a run with idempotency via correlation key."""
    # Enforce run exists
    run = session.get(Run, run_id)
    if not run:
        raise ValueError("Run not found")

    # Idempotency
    if correlation_key:
        existing = session.execute(
            select(Attempt).where(Attempt.run_id == run_id, Attempt.correlation_key == correlation_key)
        ).scalar_one_or_none()
        if existing:
            return _to_attempt_status(existing)

    attempt = Attempt(
        attempt_id=__import__("uuid").uuid4().hex,
        run_id=run_id,
        status="queued",
        started_at=None,
        finished_at=None,
        message=None,
        metrics={"requested_types": ",".join(types)},
        executor="local",
        correlation_key=correlation_key,
    )
    session.add(attempt)
    session.flush()

    # Update run aggregate timestamps and status
    run.last_updated = datetime.now(timezone.utc)
    run.status = _aggregate_run_status_from_attempts([attempt, *run.attempts])
    session.flush()

    return _to_attempt_status(attempt)


# PUBLIC_INTERFACE
def get_attempt(session: Session, run_id: str, attempt_id: str) -> Optional[AttemptStatus]:
    """Return attempt by run and attempt id."""
    a = session.get(Attempt, attempt_id)
    if not a or a.run_id != run_id:
        return None
    return _to_attempt_status(a)


# PUBLIC_INTERFACE
def update_attempt_status_and_assets(
    session: Session,
    attempt_id: str,
    status: str,
    message: Optional[str],
    started_at: Optional[datetime],
    finished_at: Optional[datetime],
    assets: List[AssetRef],
) -> AttemptStatus:
    """Update attempt status and attach assets, then update run aggregate."""
    a = session.get(Attempt, attempt_id)
    if not a:
        raise ValueError("Attempt not found")

    if started_at is not None:
        a.started_at = started_at
    if finished_at is not None:
        a.finished_at = finished_at
    a.status = status
    a.message = message

    # Replace assets for attempt
    # First clear existing attempt assets
    for existing in list(a.assets):
        session.delete(existing)
    session.flush()

    for asset in assets:
        session.add(
            Asset(
                attempt_id=a.attempt_id,
                run_id=a.run_id,
                name=asset.name,
                path=asset.path,
                content_type=asset.content_type,
                size_bytes=asset.size_bytes,
            )
        )

    # Update run aggregate
    r = session.get(Run, a.run_id)
    if r:
        r.last_updated = datetime.now(timezone.utc)
        r.status = _aggregate_run_status_from_attempts(r.attempts)

    session.flush()
    return _to_attempt_status(a)
