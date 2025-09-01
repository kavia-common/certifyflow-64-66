from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, HttpUrl, EmailStr


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
