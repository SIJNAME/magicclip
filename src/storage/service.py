import tempfile
from pathlib import Path

import boto3

from src.config import settings


def _client():
    if not settings.s3_bucket:
        raise RuntimeError("S3_BUCKET is required")
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
    )


def upload_file(local_path: str, key: str) -> str:
    client = _client()
    client.upload_file(local_path, settings.s3_bucket, key)
    return key


def upload_bytes(content: bytes, key: str) -> str:
    client = _client()
    client.put_object(Bucket=settings.s3_bucket, Key=key, Body=content)
    return key


def download_to_temp(key: str) -> str:
    client = _client()
    suffix = Path(key).suffix or ".bin"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        client.download_fileobj(settings.s3_bucket, key, tmp)
        return tmp.name


def signed_download_url(key: str, expires_seconds: int = 3600) -> str:
    client = _client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.s3_bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )

