import asyncio
import base64
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass
class AirflowAuthConfig:
    """Configuration for authenticating to Airflow REST API."""
    scheme: str  # "basic" | "bearer" | "none"
    username: Optional[str] = None
    password: Optional[str] = None
    bearer_token: Optional[str] = None


class AirflowAPIError(RuntimeError):
    """Raised when Airflow API returns an error response."""


# PUBLIC_INTERFACE
class AirflowClient:
    """Real Airflow REST API client for DAG submission and status polling.

    Env variables (must be set externally via .env):
      - AIRFLOW_API_URL: Base URL, e.g., https://airflow.example.com/api/v1
      - AIRFLOW_AUTH_SCHEME: one of basic | bearer | none (default: none)
      - AIRFLOW_USERNAME: for basic auth
      - AIRFLOW_PASSWORD: for basic auth
      - AIRFLOW_BEARER_TOKEN: for bearer auth
      - AIRFLOW_VERIFY_SSL: "true" | "false" (default: true)
      - AIRFLOW_TIMEOUT_SECONDS: request timeout (default: 15)
      - AIRFLOW_POLL_INTERVAL_SECONDS: poll wait interval (default: 5)
      - AIRFLOW_POLL_TIMEOUT_SECONDS: max wait for a DAG run (default: 7200 = 2h)
    """

    def __init__(self) -> None:
        base_url = os.getenv("AIRFLOW_API_URL", "").rstrip("/")
        if not base_url:
            raise ValueError("AIRFLOW_API_URL is required for Airflow integration")

        scheme = os.getenv("AIRFLOW_AUTH_SCHEME", "none").strip().lower()
        auth = AirflowAuthConfig(
            scheme=scheme,
            username=os.getenv("AIRFLOW_USERNAME"),
            password=os.getenv("AIRFLOW_PASSWORD"),
            bearer_token=os.getenv("AIRFLOW_BEARER_TOKEN"),
        )
        self.base_url = base_url
        self.auth = auth
        self.verify_ssl = os.getenv("AIRFLOW_VERIFY_SSL", "true").strip().lower() != "false"
        self.timeout = float(os.getenv("AIRFLOW_TIMEOUT_SECONDS", "15"))
        self.poll_interval = float(os.getenv("AIRFLOW_POLL_INTERVAL_SECONDS", "5"))
        self.poll_timeout = float(os.getenv("AIRFLOW_POLL_TIMEOUT_SECONDS", "7200"))

        # Prepare default headers
        self._headers: Dict[str, str] = {"Accept": "application/json"}
        if self.auth.scheme == "basic":
            if not (self.auth.username and self.auth.password):
                raise ValueError("AIRFLOW_USERNAME and AIRFLOW_PASSWORD are required for basic auth")
            token = base64.b64encode(f"{self.auth.username}:{self.auth.password}".encode()).decode()
            self._headers["Authorization"] = f"Basic {token}"
        elif self.auth.scheme == "bearer":
            if not self.auth.bearer_token:
                raise ValueError("AIRFLOW_BEARER_TOKEN is required for bearer auth")
            self._headers["Authorization"] = f"Bearer {self.auth.bearer_token}"
        else:
            # none: no Authorization header
            pass

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    async def _request(self, method: str, path: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout, headers=self._headers) as client:
            resp = await client.request(method, self._url(path), json=json)
            # Raise for unexpected http statuses
            if resp.status_code >= 400:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise AirflowAPIError(f"Airflow API error {resp.status_code}: {detail}")
            try:
                return resp.json()
            except Exception:
                return {}

    # PUBLIC_INTERFACE
    async def trigger(self, dag_id: str, conf: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger a DAG run using Airflow stable API.

        POST /dags/{dag_id}/dagRuns with payload:
        {
          "conf": {...}
        }

        Returns parsed JSON of the created dag run.
        """
        path = f"/dags/{dag_id}/dagRuns"
        payload = {"conf": conf}
        return await self._request("POST", path, json=payload)

    # PUBLIC_INTERFACE
    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get a DAG run by ID.

        GET /dags/{dag_id}/dagRuns/{dag_run_id}
        """
        path = f"/dags/{dag_id}/dagRuns/{dag_run_id}"
        return await self._request("GET", path)

    # PUBLIC_INTERFACE
    async def wait_for_completion(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Poll the DAG run state until it finishes or timeout occurs.

        Terminal states expected:
          success, failed, error (and treat others not running/queued as terminal if returned).
        """
        deadline = asyncio.get_event_loop().time() + self.poll_timeout
        last_payload: Dict[str, Any] = {}
        while True:
            last_payload = await self.get_dag_run(dag_id, dag_run_id)
            state = str(last_payload.get("state") or last_payload.get("status") or "").lower()
            if state in {"success", "failed", "error"}:
                return last_payload
            # Some Airflow versions: "queued", "running"
            if asyncio.get_event_loop().time() > deadline:
                raise AirflowAPIError(f"Timeout waiting for DAG run {dag_run_id} state; last={state}")
            await asyncio.sleep(self.poll_interval)
