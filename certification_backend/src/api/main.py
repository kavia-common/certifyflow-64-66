import asyncio
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Path as FPath, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, HttpUrl, EmailStr

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

    # In-memory store for MVP
    registry = TaskRegistry()

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routes

    @app.get("/", tags=["health"], summary="Health Check")
    # PUBLIC_INTERFACE
    def health_check():
        """Basic health check endpoint returning service status."""
        return {"message": "Healthy", "time": datetime.now(timezone.utc).isoformat()}

    # Models moved to module level

    # Services and helpers
    storage_root = Path(os.getenv("CERT_STORAGE_DIR", "data/storage")).resolve()
    storage_root.mkdir(parents=True, exist_ok=True)

    local_runner = LocalRunner(storage_root=storage_root)
    airflow = AirflowClientStub()

    # Routes implementation

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
        # Idempotent handling
        existing_run = registry.find_run_by_correlation_key(payload.correlation_key) if payload.correlation_key else None
        if existing_run:
            polling_url = str(request.url_for("get_run_status", run_id=existing_run.run_id))
            return CreateRunResponse(run=existing_run, polling_url=polling_url, message="Run already exists (idempotent).")

        run_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        run_status = RunStatus(
            run_id=run_id,
            correlation_key=payload.correlation_key,
            branch=payload.branch,
            target_env=payload.target_env,
            certification_types=list(payload.certification_types),
            created_at=now,
            last_updated=now,
            status="queued",
            attempts=[],
            assets=[],
        )
        registry.store_run(run_status)

        # Create attempt for the run
        attempt = registry.create_attempt(run_id=run_id, types=payload.certification_types, correlation_key=payload.correlation_key)
        attempt.executor = resolve_executor(payload.certification_types)
        registry.update_attempt(attempt)

        # Execute async
        if payload.async_execution:
            assert background_tasks is not None
            background_tasks.add_task(
                orchestrate_attempt,
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                registry,
                payload.notification,
            )
        else:
            await orchestrate_attempt(
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                registry,
                payload.notification,
            )

        polling_url = str(request.url_for("get_run_status", run_id=run_id))
        return CreateRunResponse(run=registry.get_run(run_id), polling_url=polling_url, message="Run created.")

    @app.get(
        "/runs",
        tags=["runs"],
        summary="List runs",
        response_model=ListRunsResponse,
        description="Retrieve a list of all runs in the MVP in-memory store.",
    )
    # PUBLIC_INTERFACE
    def list_runs() -> ListRunsResponse:
        """List all runs from the in-memory registry."""
        return ListRunsResponse(runs=registry.list_runs())

    @app.get(
        "/runs/{run_id}",
        tags=["runs"],
        summary="Get run status",
        response_model=RunStatus,
        description="Fetch the current status of a run including attempts and assets.",
    )
    # PUBLIC_INTERFACE
    def get_run_status(run_id: str = FPath(..., description="Run identifier")) -> RunStatus:
        """Get current run status including attempts and assets."""
        run = registry.get_run(run_id)
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
        run = registry.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")

        # Idempotency for attempts within this run via correlation_key
        if payload.correlation_key:
            exist = registry.find_attempt_by_correlation_key(run_id, payload.correlation_key)
            if exist:
                return exist

        attempt = registry.create_attempt(run_id=run_id, types=payload.certification_types, correlation_key=payload.correlation_key)
        attempt.executor = resolve_executor(payload.certification_types)
        registry.update_attempt(attempt)

        if payload.async_execution:
            assert background_tasks is not None
            background_tasks.add_task(
                orchestrate_attempt,
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                registry,
                payload.notification,
            )
        else:
            await orchestrate_attempt(
                attempt.attempt_id,
                payload.certification_types,
                local_runner,
                airflow,
                registry,
                payload.notification,
            )

        return registry.get_attempt(run_id, attempt.attempt_id)

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
        """Return attempt status for given run and attempt ids."""
        attempt = registry.get_attempt(run_id, attempt_id)
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


# ==== Helpers, Registry, Runners, and Stubs ====

class TaskRegistry:
    """In-memory registry for runs and attempts for MVP; replace with DB in future."""
    def __init__(self):
        self._runs: Dict[str, Dict] = {}  # run_id -> RunStatus.model_dump()
        self._attempts: Dict[str, Dict[str, Dict]] = {}  # run_id -> attempt_id -> AttemptStatus
        self._run_by_ckey: Dict[str, str] = {}  # correlation -> run_id
        self._attempt_by_ckey: Dict[str, str] = {}  # f"{run_id}:{ckey}" -> attempt_id

    def store_run(self, run_status):
        self._runs[run_status.run_id] = run_status.model_dump()
        if run_status.correlation_key:
            self._run_by_ckey[run_status.correlation_key] = run_status.run_id

    def list_runs(self):
        return [RunStatus(**r) for r in self._runs.values()]

    def get_run(self, run_id: str):
        r = self._runs.get(run_id)
        if not r:
            return None
        # hydrate attempts
        attempts = list(self._attempts.get(run_id, {}).values())
        rs = RunStatus(**r)
        rs.attempts = [AttemptStatus(**a) for a in attempts]
        return rs

    def update_run_status(self, run_id: str):
        run = self.get_run(run_id)
        if not run:
            return
        # aggregate from attempts
        statuses = {a.status for a in run.attempts}
        if "running" in statuses:
            run.status = "running"
        elif "failed" in statuses and "succeeded" in statuses:
            run.status = "partial"
        elif statuses == {"succeeded"}:
            run.status = "succeeded"
        elif statuses == {"failed"}:
            run.status = "failed"
        elif statuses == {"queued"}:
            run.status = "queued"
        else:
            # default
            if "failed" in statuses:
                run.status = "partial"
            elif "succeeded" in statuses:
                run.status = "partial"
        run.last_updated = datetime.now(timezone.utc)
        # persist back
        self._runs[run_id] = run.model_dump()

    def create_attempt(self, run_id: str, types: List[str], correlation_key: Optional[str] = None):
        attempt_id = str(uuid.uuid4())
        attempt = AttemptStatus(
            attempt_id=attempt_id,
            run_id=run_id,
            status="queued",
            started_at=None,
            finished_at=None,
            message=None,
            assets=[],
            metrics={"requested_types": ",".join(types)},
            executor="local",
            correlation_key=correlation_key,
        )
        self._attempts.setdefault(run_id, {})[attempt_id] = attempt.model_dump()
        if correlation_key:
            self._attempt_by_ckey[f"{run_id}:{correlation_key}"] = attempt_id
        self.update_run_status(run_id)
        return AttemptStatus(**self._attempts[run_id][attempt_id])

    def update_attempt(self, attempt: "AttemptStatus"):
        self._attempts.setdefault(attempt.run_id, {})[attempt.attempt_id] = attempt.model_dump()
        self.update_run_status(attempt.run_id)

    def get_attempt(self, run_id: str, attempt_id: str):
        a = self._attempts.get(run_id, {}).get(attempt_id)
        if not a:
            return None
        return AttemptStatus(**a)

    def list_attempts(self, run_id: str):
        return [AttemptStatus(**a) for a in self._attempts.get(run_id, {}).values()]

    def find_run_by_correlation_key(self, ckey: Optional[str]):
        if not ckey:
            return None
        run_id = self._run_by_ckey.get(ckey)
        if not run_id:
            return None
        return self.get_run(run_id)

    def find_attempt_by_correlation_key(self, run_id: str, ckey: Optional[str]):
        if not ckey:
            return None
        attempt_id = self._attempt_by_ckey.get(f"{run_id}:{ckey}")
        if not attempt_id:
            return None
        return self.get_attempt(run_id, attempt_id)


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
        # Use python -m pip to ensure module availability context; fallback to pylint command if available
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
        run_id = f"sim-{uuid.uuid4()}"
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


async def orchestrate_attempt(
    attempt_id: str,
    types: List[str],
    local_runner: LocalRunner,
    airflow: AirflowClientStub,
    registry: TaskRegistry,
    notification: Optional["NotificationTarget"],
):
    """Core orchestration: dispatch to local or Airflow and aggregate assets."""
    # locate attempt
    run_id = None
    attempt = None
    # find attempt in registry
    for r_id, attempts in registry._attempts.items():
        if attempt_id in attempts:
            run_id = r_id
            attempt = registry.get_attempt(r_id, attempt_id)
            break
    if not attempt:
        return

    # update status to running
    attempt.status = "running"
    attempt.started_at = datetime.now(timezone.utc)
    registry.update_attempt(attempt)

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
            assets.extend(res.get("assets", []))
            messages.append(f"{t} rc={res.get('rc')}")
            rc_accum.append(res.get("rc"))

        # Airflow suite
        airflow_types = [t for t in types if t in {"e2e", "performance", "soak"}]
        for t in airflow_types:
            dag_id = f"cert_{t}"
            trigger = await airflow.trigger(dag_id=dag_id, conf={"attempt_id": attempt_id})
            complete = await airflow.wait_for_completion(dag_id=dag_id, dag_run_id=trigger["dag_run_id"])
            # simulate artifact
            # write a small log
            storage = local_runner._artifact_dir(attempt_id)
            af_log = storage / f"{t}_airflow.log"
            af_log.write_text(json.dumps({"trigger": trigger, "complete": complete}, indent=2))
            assets.append({"name": f"{t}_airflow_log", "path": str(af_log), "content_type": "application/json", "size_bytes": af_log.stat().st_size})
            messages.append(f"{t} state={complete.get('state')}")

        # Determine final status
        failed_rc = any(isinstance(x, int) and x not in (0, None) for x in rc_accum)
        af_failed = any("state=failed" in m.lower() for m in messages)
        if failed_rc or af_failed:
            attempt.status = "failed"
        else:
            attempt.status = "succeeded"

        attempt.finished_at = datetime.now(timezone.utc)
        attempt.message = "; ".join(messages)
        attempt.assets = [AssetRef(**a) for a in assets]
        registry.update_attempt(attempt)

    except Exception as exc:
        attempt.status = "failed"
        attempt.finished_at = datetime.now(timezone.utc)
        attempt.message = f"Execution error: {exc}"
        registry.update_attempt(attempt)

    # Update run aggregate
    if run_id:
        registry.update_run_status(run_id)

    # Notifications (stub)
    await maybe_notify(notification, attempt)


async def maybe_notify(notification: Optional["NotificationTarget"], attempt_obj) -> None:
    """Send optional notifications. MVP: webhook only; email is a no-op."""
    if not notification:
        return
    url = notification.notification_url if isinstance(notification, NotificationTarget) else getattr(notification, "notification_url", None)
    if url:
        try:
            # Lazy import httpx to avoid mandatory dependency during import time
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
            # swallow in MVP
            pass
    # email stub intentionally omitted


# Create application instance for ASGI
# ==== Pydantic Models (module-level to avoid forward reference issues) ====

class AssetRef(BaseModel):
    name: str = Field(..., description="Logical name of the asset/log/report")
    path: str = Field(..., description="Local path or storage URI")
    content_type: Optional[str] = Field(None, description="MIME type if known")
    size_bytes: Optional[int] = Field(None, description="Size in bytes if known")


class AttemptStatus(BaseModel):
    attempt_id: str
    run_id: str
    status: Literal["queued", "running", "succeeded", "failed", "canceled"]
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    message: Optional[str] = None
    assets: List[AssetRef] = Field(default_factory=list)
    metrics: Dict[str, Union[int, float, str]] = Field(default_factory=dict)
    executor: Literal["local", "airflow"] = "local"
    correlation_key: Optional[str] = None


class RunStatus(BaseModel):
    run_id: str
    correlation_key: Optional[str] = None
    branch: Optional[str] = None
    target_env: Optional[str] = None
    certification_types: List[str] = Field(default_factory=list)
    created_at: datetime
    last_updated: datetime
    status: Literal["queued", "running", "succeeded", "failed", "partial"]
    attempts: List[AttemptStatus] = Field(default_factory=list)
    assets: List[AssetRef] = Field(default_factory=list)


class NotificationTarget(BaseModel):
    notification_url: Optional[HttpUrl] = Field(
        None, description="Webhook to be called upon status change."
    )
    notification_email: Optional[EmailStr] = Field(
        None, description="Email address to notify (no-op stub)."
    )


class CreateRunRequest(BaseModel):
    correlation_key: Optional[str] = Field(
        None, description="Idempotency key to deduplicate run creation."
    )
    branch: Optional[str] = Field(None, description="SCM branch for mapping.")
    target_env: Optional[str] = Field(None, description="Target environment.")
    certification_types: List[
        Literal["pylint", "bandit", "pytest", "e2e", "performance", "soak"]
    ] = Field(..., description="Types of certifications to execute.")
    async_execution: bool = Field(
        True, description="Return immediately and allow polling for status."
    )
    notification: Optional[NotificationTarget] = Field(
        None, description="Optional notification destinations."
    )
    extra: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict, description="Additional parameters."
    )


class CreateRunResponse(BaseModel):
    run: RunStatus
    polling_url: Optional[HttpUrl] = None
    message: str


class CreateAttemptRequest(BaseModel):
    correlation_key: Optional[str] = Field(
        None, description="Idempotency key to deduplicate attempt creation."
    )
    certification_types: List[
        Literal["pylint", "bandit", "pytest", "e2e", "performance", "soak"]
    ] = Field(..., description="Types of certifications to execute for this attempt.")
    async_execution: bool = True
    notification: Optional[NotificationTarget] = None
    extra: Dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)


class ListRunsResponse(BaseModel):
    runs: List[RunStatus]


# Rebuild app after model definitions
app = get_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
