from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, HttpUrl, EmailStr


class AssetRef(BaseModel):
    name: str = Field(..., description="Logical name of the asset/log/report")
    path: str = Field(..., description="Local path or storage URI")
    content_type: Optional[str] = Field(None, description="MIME type if known")
    size_bytes: Optional[int] = Field(None, description="Size in bytes if known")
    storage_key: Optional[str] = Field(
        None, description="Object storage key if asset uploaded to S3/MinIO"
    )
    signed_url: Optional[HttpUrl] = Field(
        None, description="Presigned URL for temporary access to the asset"
    )


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
    # Generalized grouping of certification families for forward compatibility.
    # Values are normalized to: code_quality, security, functional_test, e2e, performance, soak
    certification_types: List[str] = Field(default_factory=list)
    created_at: datetime
    last_updated: datetime
    status: Literal["queued", "running", "succeeded", "failed", "partial"]
    attempts: List[AttemptStatus] = Field(default_factory=list)
    assets: List[AssetRef] = Field(default_factory=list)


class WebhookSignature(BaseModel):
    """Webhook signature metadata included as headers for verification."""
    algorithm: Literal["hmac-sha256"] = Field(
        "hmac-sha256", description="Signature algorithm used"
    )
    signature: str = Field(
        ..., description="Hex-encoded HMAC signature of the payload"
    )
    timestamp: int = Field(
        ..., description="Unix epoch seconds when signature was generated"
    )


class NotificationTarget(BaseModel):
    notification_url: Optional[HttpUrl] = Field(
        None, description="Webhook to be called upon status change."
    )
    notification_email: Optional[EmailStr] = Field(
        None, description="Email address to notify (no-op stub)."
    )
    # Optional shared secret for signing webhook requests
    webhook_secret: Optional[str] = Field(
        None, description="Shared secret for HMAC signing of webhooks (HMAC-SHA256)."
    )


# Generalized certification selection with tool-specific option
GeneralizedType = Literal["code_quality", "security", "functional_test", "e2e", "performance", "soak"]
CodeQualityTool = Literal["pylint"]  # extensible in future
SecurityTool = Literal["bandit"]     # extensible in future
FunctionalTestTool = Literal["pytest"]  # extensible in future

class CertificationSelection(BaseModel):
    """Select a generalized certification type with optional per-tool selection."""
    type: GeneralizedType = Field(..., description="Generalized certification type")
    tool: Optional[str] = Field(
        None,
        description="Optional specific tool name within the generalized type (e.g., pylint | bandit | pytest).",
    )


def _normalize_legacy_types(values: List[str]) -> List[CertificationSelection]:
    """Normalize legacy certification_types array into new generalized schema selections."""
    normalized: List[CertificationSelection] = []
    for v in values:
        v_lower = v.lower()
        if v_lower == "pylint":
            normalized.append(CertificationSelection(type="code_quality", tool="pylint"))
        elif v_lower == "bandit":
            normalized.append(CertificationSelection(type="security", tool="bandit"))
        elif v_lower == "pytest":
            normalized.append(CertificationSelection(type="functional_test", tool="pytest"))
        elif v_lower in {"e2e", "performance", "soak"}:
            normalized.append(CertificationSelection(type=v_lower))
        else:
            # Unknown legacy value; keep as-is under type field to avoid hard failure
            normalized.append(CertificationSelection(type=v_lower))  # type: ignore[arg-type]
    return normalized


class CreateRunRequest(BaseModel):
    correlation_key: Optional[str] = Field(
        None, description="Idempotency key to deduplicate run creation."
    )
    branch: Optional[str] = Field(None, description="SCM branch for mapping.")
    target_env: Optional[str] = Field(None, description="Target environment.")
    # New extensible input: either selections or legacy list still accepted via 'legacy_certification_types'
    certification_selections: Optional[List[CertificationSelection]] = Field(
        None,
        description="Generalized certification selections with optional tool per type.",
    )
    legacy_certification_types: Optional[
        List[Literal["pylint", "bandit", "pytest", "e2e", "performance", "soak"]]
    ] = Field(
        None,
        description="Deprecated: legacy specific types. Prefer certification_selections."
    )
    async_execution: bool = Field(
        True, description="Return immediately and allow polling for status."
    )
    notification: Optional[NotificationTarget] = Field(
        None, description="Optional notification destinations."
    )
    extra: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict, description="Additional parameters."
    )

    # PUBLIC_INTERFACE
    def effective_selections(self) -> List[CertificationSelection]:
        """Return normalized list of certification selections regardless of legacy or new input."""
        if self.certification_selections and len(self.certification_selections) > 0:
            return self.certification_selections
        legacy = self.legacy_certification_types or []
        return _normalize_legacy_types(legacy)


class CreateRunResponse(BaseModel):
    run: RunStatus
    polling_url: Optional[HttpUrl] = None
    message: str


class CreateAttemptRequest(BaseModel):
    correlation_key: Optional[str] = Field(
        None, description="Idempotency key to deduplicate attempt creation."
    )
    certification_selections: Optional[List[CertificationSelection]] = Field(
        None,
        description="Generalized certification selections with optional tool per type."
    )
    legacy_certification_types: Optional[
        List[Literal["pylint", "bandit", "pytest", "e2e", "performance", "soak"]]
    ] = Field(
        None,
        description="Deprecated: legacy specific types. Prefer certification_selections."
    )
    async_execution: bool = True
    notification: Optional[NotificationTarget] = None
    extra: Dict[str, Union[str, int, float, bool]] = Field(default_factory=dict)

    # PUBLIC_INTERFACE
    def effective_selections(self) -> List[CertificationSelection]:
        """Return normalized list of certification selections regardless of legacy or new input."""
        if self.certification_selections and len(self.certification_selections) > 0:
            return self.certification_selections
        legacy = self.legacy_certification_types or []
        return _normalize_legacy_types(legacy)


class PageMeta(BaseModel):
    page: int = Field(..., ge=1, description="Current page (1-based)")
    per_page: int = Field(..., ge=1, le=200, description="Items per page")
    total: int = Field(..., ge=0, description="Total number of items available")
    next_page: Optional[int] = Field(None, description="Next page if available")
    prev_page: Optional[int] = Field(None, description="Previous page if available")


class ListRunsResponse(BaseModel):
    runs: List[RunStatus]
    page: Optional[PageMeta] = Field(None, description="Pagination metadata when pagination is applied")


class ListAttemptsResponse(BaseModel):
    attempts: List[AttemptStatus]
    page: Optional[PageMeta] = Field(None, description="Pagination metadata when pagination is applied")


class APIError(BaseModel):
    code: str = Field(..., description="Stable application error code")
    message: str = Field(..., description="Human-readable error message")
    detail: Optional[Dict[str, Union[str, int, float, bool]]] = Field(
        default=None, description="Optional detail payload"
    )
