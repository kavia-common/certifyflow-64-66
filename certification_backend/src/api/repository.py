from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from .models import Run, Attempt, Asset, NotificationSetting
from .schemas import (
    RunStatus,
    AttemptStatus,
    AssetRef,
    NotificationTarget,
)

try:
    # Lazy import to avoid hard failures in contexts where storage isn't configured
    from .storage import S3StorageService, StorageConfig  # type: ignore
except Exception:  # pragma: no cover - optional
    S3StorageService = None  # type: ignore
    StorageConfig = None  # type: ignore


def _augment_with_signed_url(ref: AssetRef) -> AssetRef:
    """
    Enrich AssetRef with signed_url if storage_key can be derived and storage is configured.
    Convention: if path starts with "s3://bucket/key" or is stored as "s3:bucket/key" or "obj://bucket/key",
    we treat the portion after bucket as the storage key. Otherwise, if path resembles an object key we try to sign it.
    """
    if S3StorageService is None or StorageConfig is None:
        return ref

    # Determine storage key convention: prefer explicit storage_key on ref if already set
    storage_key = getattr(ref, "storage_key", None)
    bucket = None
    path = ref.path or ""

    # Parse common forms
    lowered = path.lower()
    if not storage_key:
        if lowered.startswith("s3://") or lowered.startswith("obj://"):
            try:
                # s3://bucket/key...
                _, rest = path.split("://", 1)
                bucket, storage_key = rest.split("/", 1)
            except Exception:
                storage_key = None
        elif lowered.startswith("s3:"):
            try:
                # s3:bucket/key...
                _, rest = path.split(":", 1)
                bucket, storage_key = rest.split("/", 1)
            except Exception:
                storage_key = None

    try:
        cfg = StorageConfig.from_env()  # may raise if not configured
    except Exception:
        # storage not configured; return as-is
        return ref

    # Only sign if bucket matches configured bucket (or bucket unspecified)
    if storage_key:
        if bucket and bucket != cfg.bucket:
            return ref
        # generate signed url
        try:
            svc = S3StorageService(cfg)
            url = svc.generate_presigned_url(storage_key)
            public_url = svc.object_url(storage_key)
            if url or public_url:
                # prefer signed if present, else public
                ref.signed_url = url or public_url  # type: ignore[attr-defined]
                ref.storage_key = storage_key  # type: ignore[attr-defined]
        except Exception:
            # best-effort
            pass

    return ref


def _to_asset_ref(a: Asset) -> AssetRef:
    ref = AssetRef(name=a.name, path=a.path, content_type=a.content_type, size_bytes=a.size_bytes)
    return _augment_with_signed_url(ref)


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
def list_runs(session: Session, page: Optional[int] = None, per_page: Optional[int] = None) -> Tuple[List[RunStatus], int]:
    """List runs with optional pagination. Returns (runs, total)."""
    stmt = select(Run).order_by(Run.created_at.desc())
    total = session.execute(select(func.count()).select_from(Run)).scalar_one()
    if page and per_page:
        offset = (page - 1) * per_page
        stmt = stmt.offset(offset).limit(per_page)
    runs = session.execute(stmt).scalars().all()
    return ([_to_run_status(r) for r in runs], total)


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
def list_attempts(session: Session, run_id: str, page: Optional[int] = None, per_page: Optional[int] = None) -> Tuple[List[AttemptStatus], int]:
    """List attempts for a run with optional pagination. Returns (attempts, total)."""
    total = session.execute(select(func.count()).select_from(Attempt).where(Attempt.run_id == run_id)).scalar_one()
    stmt = select(Attempt).where(Attempt.run_id == run_id).order_by(Attempt.started_at.nullsfirst(), Attempt.attempt_id)
    if page and per_page:
        offset = (page - 1) * per_page
        stmt = stmt.offset(offset).limit(per_page)
    items = session.execute(stmt).scalars().all()
    return ([_to_attempt_status(a) for a in items], total)


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
