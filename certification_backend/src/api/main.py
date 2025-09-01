import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Path as FPath, Request, status
from fastapi.middleware.cors import CORSMiddleware

# Switch to SQLAlchemy-backed repository
from .db import session_scope, Base, engine
from .models import Run as RunModel
from .repository import (
    create_or_get_run as repo_create_or_get_run,
    list_runs as repo_list_runs,
    get_run as repo_get_run,
    find_run_by_correlation_key as repo_find_run_by_correlation_key,
    create_attempt as repo_create_attempt,
    get_attempt as repo_get_attempt,
    update_attempt_status_and_assets as repo_update_attempt_status_and_assets,
)
from .schemas import (
    CreateRunRequest,
    CreateRunResponse,
    RunStatus,
    AttemptStatus,
    CreateAttemptRequest,
    ListRunsResponse,
    NotificationTarget,
    AssetRef,
)

# Storage (optional import; will raise at runtime if misconfigured when first used)
try:
    from .storage import S3StorageService, StorageConfig
except Exception:
    S3StorageService = None  # type: ignore
    StorageConfig = None  # type: ignore

# PUBLIC_INTERFACE
def get_app() -> FastAPI:
    """Factory to create FastAPI app with routes and settings."""
    app = FastAPI(
        title="Certification Orchestration API",
        version="0.1.0",
        description=(
            "Service to orchestrate certification runs (code quality, security, tests, e2e, performance, soak). "
            "Local runner supports pylint, bandit, pytest. Long-running suites dispatched via Airflow (stub). "
            "Asynchronous trigger with polling and optional notification callbacks."
        ),
        openapi_tags=[
            {"name": "health", "description": "Service health and metadata"},
            {"name": "runs", "description": "Create and retrieve certification runs"},
            {"name": "attempts", "description": "Create and retrieve attempts for a run"},
        ],
    )

    # Ensure tables exist (idempotent)
    Base.metadata.create_all(bind=engine)

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Services and helpers
    storage_root = Path(os.getenv("CERT_STORAGE_DIR", "data/storage")).resolve()
    storage_root.mkdir(parents=True, exist_ok=True)

    local_runner = LocalRunner(storage_root=storage_root)
    # Initialize Airflow client if configured; fall back to stub if missing config
    try:
        from .airflow_client import AirflowClient  # real client
        airflow = AirflowClient()
    except Exception:
        # If configuration not provided yet, keep the stub for local only executions
        airflow = AirflowClientStub()

    @app.get("/", tags=["health"], summary="Health Check")
    # PUBLIC_INTERFACE
    def health_check():
        """Basic health check endpoint returning service status."""
        return {"message": "Healthy", "time": datetime.now(timezone.utc).isoformat()}

    @app.post(
        "/runs",
        tags=["runs"],
        status_code=status.HTTP_202_ACCEPTED,
        summary="Create a certification run",
        description="Create a new run to group attempts. Idempotent via correlation_key.",
        response_model=CreateRunResponse,
    )
    # PUBLIC_INTERFACE
    async def create_run(
        request: Request,
        payload: CreateRunRequest = Body(...),
        background_tasks: BackgroundTasks = None,
    ) -> CreateRunResponse:
        """Creates a run and triggers an initial attempt for the requested certification types."""
        # Idempotency via correlation key
        with session_scope() as session:
            existing_run = repo_find_run_by_correlation_key(session, payload.correlation_key) if payload.correlation_key else None
            if existing_run:
                polling_url = str(request.url_for("get_run_status", run_id=existing_run.run_id))
                return CreateRunResponse(run=existing_run, polling_url=polling_url, message="Run already exists (idempotent).")

            now = datetime.now(timezone.utc)
            run = RunModel(
                run_id=__import__("uuid").uuid4().hex,
                correlation_key=payload.correlation_key,
                branch=payload.branch,
                target_env=payload.target_env,
                certification_types=list(payload.certification_types),
                created_at=now,
                last_updated=now,
                status="queued",
            )
            persisted = repo_create_or_get_run(session, run, payload.notification)

            # Create an initial attempt
            attempt = repo_create_attempt(
                session=session,
                run_id=persisted.run_id,
                types=payload.certification_types,
                correlation_key=payload.correlation_key,
            )

        # Set executor and orchestrate outside transaction
        attempt.executor = resolve_executor(payload.certification_types)

        if payload.async_execution:
            assert background_tasks is not None
            background_tasks.add_task(
                orchestrate_attempt_db,
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                payload.notification,
            )
        else:
            await orchestrate_attempt_db(
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                payload.notification,
            )

        with session_scope() as session:
            run_status = repo_get_run(session, persisted.run_id)
        polling_url = str(request.url_for("get_run_status", run_id=persisted.run_id))
        return CreateRunResponse(run=run_status, polling_url=polling_url, message="Run created.")

    @app.get(
        "/runs",
        tags=["runs"],
        summary="List runs",
        response_model=ListRunsResponse,
        description="Retrieve a list of all runs in the MVP in-memory store.",
    )
    # PUBLIC_INTERFACE
    def list_runs() -> ListRunsResponse:
        """List all runs from the database."""
        with session_scope() as session:
            runs = repo_list_runs(session)
            return ListRunsResponse(runs=runs)

    @app.get(
        "/runs/{run_id}",
        tags=["runs"],
        summary="Get run status",
        response_model=RunStatus,
        description="Fetch the current status of a run including attempts and assets.",
    )
    # PUBLIC_INTERFACE
    def get_run_status(run_id: str = FPath(..., description="Run identifier")) -> RunStatus:
        """Get current run status including attempts and assets from DB."""
        with session_scope() as session:
            run = repo_get_run(session, run_id)
            if not run:
                raise HTTPException(status_code=404, detail="Run not found")
            return run

    @app.post(
        "/runs/{run_id}/attempts",
        tags=["attempts"],
        status_code=status.HTTP_202_ACCEPTED,
        summary="Create a new attempt for a run",
        response_model=AttemptStatus,
        description="Create an attempt under an existing run and trigger execution.",
    )
    # PUBLIC_INTERFACE
    async def create_attempt(
        run_id: str,
        payload: CreateAttemptRequest = Body(...),
        background_tasks: BackgroundTasks = None,
    ) -> AttemptStatus:
        """Create a new attempt for the given run and schedule execution."""
        with session_scope() as session:
            run = repo_get_run(session, run_id)
            if not run:
                raise HTTPException(status_code=404, detail="Run not found")

            # Idempotency via correlation key
            attempt = repo_create_attempt(
                session=session,
                run_id=run_id,
                types=payload.certification_types,
                correlation_key=payload.correlation_key,
            )

        # Choose executor and orchestrate
        attempt.executor = resolve_executor(payload.certification_types)

        if payload.async_execution:
            assert background_tasks is not None
            background_tasks.add_task(
                orchestrate_attempt_db,
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                payload.notification,
            )
        else:
            await orchestrate_attempt_db(
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                payload.notification,
            )

        with session_scope() as session:
            fresh = repo_get_attempt(session, run_id, attempt.attempt_id)
            assert fresh is not None
            return fresh

    @app.get(
        "/runs/{run_id}/attempts/{attempt_id}",
        tags=["attempts"],
        summary="Get attempt status",
        response_model=AttemptStatus,
        description="Fetch the current status of an attempt including assets.",
    )
    # PUBLIC_INTERFACE
    def get_attempt_status(
        run_id: str = FPath(..., description="Run id"),
        attempt_id: str = FPath(..., description="Attempt id"),
    ) -> AttemptStatus:
        """Return attempt status for given run and attempt ids from DB."""
        with session_scope() as session:
            attempt = repo_get_attempt(session, run_id, attempt_id)
            if not attempt:
                raise HTTPException(status_code=404, detail="Attempt not found")
            return attempt

    # WebSocket usage help note (route)
    @app.get(
        "/docs/websocket",
        tags=["health"],
        summary="WebSocket usage",
        description="This service does not expose WebSockets in MVP. Poll REST endpoints for status.",
    )
    # PUBLIC_INTERFACE
    def websocket_usage_note():
        """Explain real-time options (none in MVP)."""
        return {"message": "No WebSockets in MVP; use polling via GET /runs/{run_id} and GET /runs/{run_id}/attempts/{attempt_id}."}

    return app


# ==== Helpers, Runners, and Stubs (unchanged) ====

class LocalRunner:
    """Executes static analysis and unit tests locally and collects artifacts."""
    def __init__(self, storage_root: Path):
        self.storage_root = storage_root

    def _artifact_dir(self, attempt_id: str) -> Path:
        d = self.storage_root / "attempts" / attempt_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _run_cmd(self, cmd: List[str], cwd: Optional[str], log_file: Path) -> int:
        with log_file.open("w") as lf:
            proc = subprocess.Popen(
                cmd,
                cwd=cwd,
                stdout=lf,
                stderr=subprocess.STDOUT,
                text=True,
            )
            return_code = proc.wait()
        return return_code

    # PUBLIC_INTERFACE
    def run_pylint(self, attempt_id: str, paths: Optional[List[str]] = None) -> Dict:
        """Run pylint on provided paths and store logs."""
        paths = paths or ["."]
        artifact_dir = self._artifact_dir(attempt_id)
        log_file = artifact_dir / "pylint.log"
        cmd = [sys.executable, "-m", "pylint", *paths]
        rc = self._run_cmd(cmd, cwd=None, log_file=log_file)
        return {
            "rc": rc,
            "log": str(log_file),
            "assets": [
                {"name": "pylint_log", "path": str(log_file), "content_type": "text/plain", "size_bytes": log_file.stat().st_size if log_file.exists() else None}
            ],
        }

    # PUBLIC_INTERFACE
    def run_bandit(self, attempt_id: str, paths: Optional[List[str]] = None) -> Dict:
        """Run bandit on provided paths and store logs."""
        paths = paths or ["."]
        artifact_dir = self._artifact_dir(attempt_id)
        log_file = artifact_dir / "bandit.log"
        cmd = [sys.executable, "-m", "bandit", "-r", *paths]
        rc = self._run_cmd(cmd, cwd=None, log_file=log_file)
        return {
            "rc": rc,
            "log": str(log_file),
            "assets": [
                {"name": "bandit_log", "path": str(log_file), "content_type": "text/plain", "size_bytes": log_file.stat().st_size if log_file.exists() else None}
            ],
        }

    # PUBLIC_INTERFACE
    def run_pytest(self, attempt_id: str, tests_path: Optional[str] = None) -> Dict:
        """Run pytest for the repository and store report."""
        tests_path = tests_path or "."
        artifact_dir = self._artifact_dir(attempt_id)
        log_file = artifact_dir / "pytest.log"
        junit_xml = artifact_dir / "junit.xml"
        cmd = [sys.executable, "-m", "pytest", tests_path, "-q", f"--junitxml={junit_xml}"]
        rc = self._run_cmd(cmd, cwd=None, log_file=log_file)
        assets = [
            {"name": "pytest_log", "path": str(log_file), "content_type": "text/plain", "size_bytes": log_file.stat().st_size if log_file.exists() else None}
        ]
        if junit_xml.exists():
            assets.append({"name": "junit_xml", "path": str(junit_xml), "content_type": "application/xml", "size_bytes": junit_xml.stat().st_size})
        return {"rc": rc, "log": str(log_file), "junit": str(junit_xml), "assets": assets}


class AirflowClientStub:
    """Stub client for triggering Airflow DAG runs for e2e/performance/soak."""
    # PUBLIC_INTERFACE
    async def trigger(self, dag_id: str, conf: Dict) -> Dict:
        """Trigger a DAG run (stub)."""
        # In MVP, simulate a DAG trigger and completion after delay.
        run_id = f"sim-{__import__('uuid').uuid4()}"
        await asyncio.sleep(0.1)
        return {"dag_id": dag_id, "dag_run_id": run_id, "state": "queued", "conf": conf}

    # PUBLIC_INTERFACE
    async def wait_for_completion(self, dag_id: str, dag_run_id: str) -> Dict:
        """Wait for DAG completion (stubbed with sleep and success)."""
        await asyncio.sleep(0.5)
        return {"dag_id": dag_id, "dag_run_id": dag_run_id, "state": "success", "message": "Simulated success"}


def resolve_executor(types: List[str]) -> Literal["local", "airflow"]:
    airflow_types = {"e2e", "performance", "soak"}
    return "airflow" if any(t in airflow_types for t in types) else "local"


async def orchestrate_attempt_db(
    attempt_id: str,
    types: List[str],
    local_runner: LocalRunner,
    airflow: AirflowClientStub,
    notification: Optional["NotificationTarget"],
):
    """Core orchestration using database persistence for attempt status and assets."""
    # mark running
    with session_scope() as session:
        # Fetch directly to get run_id for status updates
        from .models import Attempt as AttemptModel
        a_model = session.get(AttemptModel, attempt_id)
        if not a_model:
            return
        run_id = a_model.run_id

        repo_update_attempt_status_and_assets(
            session=session,
            attempt_id=attempt_id,
            status="running",
            message=None,
            started_at=datetime.now(timezone.utc),
            finished_at=None,
            assets=[],
        )

    # Instantiate storage service if configured
    storage_service = None
    if S3StorageService and StorageConfig:
        try:
            storage_service = S3StorageService(StorageConfig.from_env())
        except Exception:
            storage_service = None

    def maybe_upload(asset: Dict) -> Dict:
        """
        Upload a single local asset to object storage if configured.
        Returns updated asset dict including 'path' updated to s3://bucket/key and preserves content_type and size.
        """
        if not storage_service:
            return asset
        try:
            local_path = asset.get("path")
            name = asset.get("name") or Path(str(local_path)).name
            # key convention: runs/<run_id>/attempts/<attempt_id>/<filename>
            # obtain run_id safely by directory from artifact dir or DB lookup not ideal here; we use DB lookup done earlier.
            filename = Path(str(local_path)).name if local_path else name
            key = f"attempts/{attempt_id}/{filename}"
            storage_service.upload_file(local_path=str(local_path), storage_key=key, content_type=asset.get("content_type"))
            asset["path"] = f"s3://{storage_service.config.bucket}/{key}"
            # include storage_key hint (picked up by repo to sign)
            asset["storage_key"] = key
        except Exception:
            # keep local path on failures
            pass
        return asset

    assets: List[Dict] = []
    messages: List[str] = []

    try:
        # Local suite
        local_types = [t for t in types if t in {"pylint", "bandit", "pytest"}]
        rc_accum = []
        for t in local_types:
            if t == "pylint":
                res = local_runner.run_pylint(attempt_id=attempt_id)
            elif t == "bandit":
                res = local_runner.run_bandit(attempt_id=attempt_id)
            elif t == "pytest":
                res = local_runner.run_pytest(attempt_id=attempt_id)
            else:
                continue
            produced = [maybe_upload(a) for a in res.get("assets", [])]
            assets.extend(produced)
            messages.append(f"{t} rc={res.get('rc')}")
            rc_accum.append(res.get("rc"))

        # Airflow suite
        airflow_types = [t for t in types if t in {"e2e", "performance", "soak"}]
        for t in airflow_types:
            dag_id = f"cert_{t}"
            try:
                trigger = await airflow.trigger(dag_id=dag_id, conf={"attempt_id": attempt_id})
                dag_run_id = str(trigger.get("dag_run_id") or trigger.get("run_id") or trigger.get("dag_run_id".upper()) or "")
                if not dag_run_id:
                    # Some Airflow instances return "id" or "dag_run_id" in different casing
                    dag_run_id = str(trigger.get("id") or "")
                if not dag_run_id:
                    raise RuntimeError(f"Airflow did not return a dag_run_id for dag {dag_id}: {trigger}")

                # Periodically poll for completion while syncing status to DB
                # and write a progressive log artifact for transparency
                storage = local_runner._artifact_dir(attempt_id)
                af_log = storage / f"{t}_airflow.log"
                step_assets = []

                # write initial trigger payload
                af_log.write_text(json.dumps({"trigger": trigger}, indent=2))
                if af_log.exists():
                    step_assets.append({"name": f"{t}_airflow_log", "path": str(af_log), "content_type": "application/json", "size_bytes": af_log.stat().st_size})

                # Stream status by polling the client, but if the client is stub it will still work
                complete = await airflow.wait_for_completion(dag_id=dag_id, dag_run_id=dag_run_id)

                # append completion info
                try:
                    prior = {}
                    if af_log.exists():
                        prior = json.loads(af_log.read_text() or "{}")
                    prior["complete"] = complete
                    af_log.write_text(json.dumps(prior, indent=2))
                except Exception:
                    pass

                # update final asset size info
                if af_log.exists():
                    step_assets = [{"name": f"{t}_airflow_log", "path": str(af_log), "content_type": "application/json", "size_bytes": af_log.stat().st_size}]

                produced = [maybe_upload(a) for a in step_assets]
                assets.extend(produced)
                messages.append(f"{t} state={str(complete.get('state') or complete.get('status') or '').lower()}")
            except Exception as af_exc:
                messages.append(f"{t} state=failed error={af_exc}")
                # continue to next certification type

        # Determine final status
        failed_rc = any(isinstance(x, int) and x not in (0, None) for x in rc_accum)
        af_failed = any("state=failed" in m.lower() for m in messages)
        final_status = "failed" if (failed_rc or af_failed) else "succeeded"

        with session_scope() as session:
            repo_update_attempt_status_and_assets(
                session=session,
                attempt_id=attempt_id,
                status=final_status,
                message="; ".join(messages),
                started_at=None,  # keep existing
                finished_at=datetime.now(timezone.utc),
                assets=[AssetRef(**a) for a in assets],
            )

    except Exception as exc:
        with session_scope() as session:
            repo_update_attempt_status_and_assets(
                session=session,
                attempt_id=attempt_id,
                status="failed",
                message=f"Execution error: {exc}",
                started_at=None,
                finished_at=datetime.now(timezone.utc),
                assets=[],
            )

    # Notifications
    with session_scope() as session:
        fresh = repo_get_attempt(session, run_id, attempt_id)
    await maybe_notify(notification, fresh)


async def maybe_notify(notification: Optional["NotificationTarget"], attempt_obj) -> None:
    """Send optional notifications. Supports webhook and email (SMTP/SendGrid/Mailgun) if configured via env."""
    if not notification:
        return

    # Webhook notification
    url = notification.notification_url if isinstance(notification, NotificationTarget) else getattr(notification, "notification_url", None)
    if url:
        try:
            import httpx  # type: ignore
            payload = {
                "attempt_id": attempt_obj.attempt_id,
                "run_id": attempt_obj.run_id,
                "status": attempt_obj.status,
                "message": attempt_obj.message,
                "assets": [a.model_dump() for a in getattr(attempt_obj, "assets", [])],
                "finished_at": attempt_obj.finished_at.isoformat() if attempt_obj.finished_at else None,
            }
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(str(url), json=payload)
        except Exception:
            # Best-effort; do not raise
            pass

    # Email notification
    email = notification.notification_email if isinstance(notification, NotificationTarget) else getattr(notification, "notification_email", None)
    if email:
        try:
            # Lazy import to avoid mandatory dependency during runtime if unused
            from .notifications import (
                EmailConfig,
                EmailSender,
                format_attempt_email_subject,
                format_attempt_email_body,
                format_attempt_email_body_html,
            )
            cfg = EmailConfig.from_env()
            sender = EmailSender(cfg)
            subject = format_attempt_email_subject(status=attempt_obj.status, run_id=attempt_obj.run_id, attempt_id=attempt_obj.attempt_id)
            body_text = format_attempt_email_body(attempt_obj)
            body_html = format_attempt_email_body_html(attempt_obj)
            # Send synchronously in this async context (smtplib/httpx are blocking here)
            # For production, consider using a background task or thread executor.
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sender.send_email, email, subject, body_text, body_html)
        except Exception:
            # best-effort; ignore failures
            pass


# Create application instance for ASGI
app = get_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
