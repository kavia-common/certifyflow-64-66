import os
from dataclasses import dataclass
from typing import Optional, Tuple

import boto3
from botocore.client import Config


@dataclass
class StorageConfig:
    """Configuration for S3/MinIO-compatible object storage loaded from environment."""
    endpoint_url: Optional[str]
    region_name: Optional[str]
    access_key: Optional[str]
    secret_key: Optional[str]
    bucket: str
    use_ssl: bool
    url_expiry_seconds: int
    public_base_url: Optional[str]
    path_style: bool

    @staticmethod
    def from_env() -> "StorageConfig":
        """
        Load config from environment variables.
        Required:
          - STORAGE_S3_BUCKET
        Optional:
          - STORAGE_S3_ENDPOINT
          - STORAGE_S3_REGION
          - STORAGE_S3_ACCESS_KEY
          - STORAGE_S3_SECRET_KEY
          - STORAGE_S3_USE_SSL (default: true)
          - STORAGE_S3_URL_EXPIRY_SECONDS (default: 3600)
          - STORAGE_S3_PUBLIC_BASE_URL (if provided, used to construct non-signed HTTP URLs)
          - STORAGE_S3_PATH_STYLE (default: true for MinIO compatibility)
        """
        bucket = os.getenv("STORAGE_S3_BUCKET", "").strip()
        if not bucket:
            raise ValueError("STORAGE_S3_BUCKET must be set for asset storage")

        return StorageConfig(
            endpoint_url=os.getenv("STORAGE_S3_ENDPOINT") or None,
            region_name=os.getenv("STORAGE_S3_REGION") or None,
            access_key=os.getenv("STORAGE_S3_ACCESS_KEY") or None,
            secret_key=os.getenv("STORAGE_S3_SECRET_KEY") or None,
            bucket=bucket,
            use_ssl=(os.getenv("STORAGE_S3_USE_SSL", "true").lower() != "false"),
            url_expiry_seconds=int(os.getenv("STORAGE_S3_URL_EXPIRY_SECONDS", "3600")),
            public_base_url=os.getenv("STORAGE_S3_PUBLIC_BASE_URL") or None,
            path_style=(os.getenv("STORAGE_S3_PATH_STYLE", "true").lower() != "false"),
        )


class S3StorageService:
    """S3/MinIO storage abstraction for uploading assets and generating signed URLs."""

    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        session = boto3.session.Session()
        self.s3 = session.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            use_ssl=config.use_ssl,
            config=Config(s3={"addressing_style": "path" if config.path_style else "virtual"}),
        )

    # PUBLIC_INTERFACE
    def upload_file(self, local_path: str, storage_key: str, content_type: Optional[str] = None) -> Tuple[str, str]:
        """
        Upload a local file to object storage.

        Returns (bucket, key). Raises on failure.
        """
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        self.s3.upload_file(local_path, self.config.bucket, storage_key, ExtraArgs=extra_args)
        return self.config.bucket, storage_key

    # PUBLIC_INTERFACE
    def generate_presigned_url(self, storage_key: str, method: str = "get_object") -> Optional[str]:
        """
        Generate a presigned URL for an object key. Returns None if generation fails.
        """
        try:
            return self.s3.generate_presigned_url(
                ClientMethod=method,
                Params={"Bucket": self.config.bucket, "Key": storage_key},
                ExpiresIn=self.config.url_expiry_seconds,
            )
        except Exception:
            return None

    # PUBLIC_INTERFACE
    def object_url(self, storage_key: str) -> Optional[str]:
        """
        Build a non-signed URL if a public base URL is provided. Useful when using CDN/Ingress.
        """
        if not self.config.public_base_url:
            return None
        base = self.config.public_base_url.rstrip("/")
        return f"{base}/{storage_key.lstrip('/')}"
