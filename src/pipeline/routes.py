import logging
import threading
import time
from collections import defaultdict
from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, Field, HttpUrl
from redis import Redis

from src.auth.deps import get_current_user
from src.config import settings
from src.db.repository import get_job, list_jobs_by_user
from src.pipeline.service import enqueue_pipeline_job
from src.pipeline.youtube_ingest import encode_youtube_source_key, fetch_youtube_metadata, validate_youtube_url
from src.storage.service import signed_download_url, upload_bytes

logger = logging.getLogger(__name__)

_YOUTUBE_INGEST_LIMIT = 5
_YOUTUBE_INGEST_WINDOW_SECONDS = 3600
_youtube_fallback_lock = threading.Lock()
_youtube_fallback_buckets: dict[str, list[float]] = defaultdict(list)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


class SubmitJobRequest(BaseModel):
    source_key: str = Field(min_length=1)
    requested_minutes: float = Field(gt=0)


class SubmitYouTubeJobRequest(BaseModel):
    youtube_url: HttpUrl
    requested_minutes: float | None = Field(default=None, gt=0)


def _enforce_youtube_ingest_rate_limit(user_id: str) -> None:
    bucket_id = int(time.time()) // _YOUTUBE_INGEST_WINDOW_SECONDS
    bucket_key = f"yt_ingest:{user_id}:{bucket_id}"

    if settings.redis_url:
        client = Redis.from_url(settings.redis_url)
        current = client.incr(bucket_key)
        if current == 1:
            client.expire(bucket_key, _YOUTUBE_INGEST_WINDOW_SECONDS + 5)
        if current > _YOUTUBE_INGEST_LIMIT:
            raise HTTPException(status_code=429, detail="YouTube ingest rate limit exceeded")
        return

    now = time.time()
    cutoff = now - _YOUTUBE_INGEST_WINDOW_SECONDS
    with _youtube_fallback_lock:
        bucket = [ts for ts in _youtube_fallback_buckets[bucket_key] if ts >= cutoff]
        bucket.append(now)
        _youtube_fallback_buckets[bucket_key] = bucket
        if len(bucket) > _YOUTUBE_INGEST_LIMIT:
            raise HTTPException(status_code=429, detail="YouTube ingest rate limit exceeded")


@router.post("/upload")
async def upload_source(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    data = await file.read()
    key = f"{settings.s3_input_prefix}/{current_user['id']}/{uuid4().hex}_{file.filename or 'upload.mp4'}"
    upload_bytes(data, key)
    return {"source_key": key}


@router.post("/jobs")
def submit_job(payload: SubmitJobRequest, current_user: dict = Depends(get_current_user)):
    try:
        return enqueue_pipeline_job(
            user_id=current_user["id"],
            source_key=payload.source_key,
            requested_minutes=payload.requested_minutes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/from-youtube")
def submit_youtube_job(payload: SubmitYouTubeJobRequest, current_user: dict = Depends(get_current_user)):
    try:
        _enforce_youtube_ingest_rate_limit(current_user["id"])

        youtube_url = str(payload.youtube_url)
        validate_youtube_url(youtube_url)
        metadata = fetch_youtube_metadata(youtube_url)

        logger.info(
            "youtube_ingest",
            extra={
                "event": "youtube_ingest",
                "user_id": current_user["id"],
                "video_id": metadata["video_id"],
                "duration": metadata["duration_seconds"],
            },
        )

        requested_minutes = payload.requested_minutes
        if requested_minutes is None:
            requested_minutes = float(metadata["duration_seconds"]) / 60.0

        return enqueue_pipeline_job(
            user_id=current_user["id"],
            source_key=encode_youtube_source_key(youtube_url),
            requested_minutes=requested_minutes,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/jobs")
def list_my_jobs(current_user: dict = Depends(get_current_user)):
    jobs = list_jobs_by_user(current_user["id"])
    for job in jobs:
        if job.get("output_key") and job.get("status") == "completed":
            job["output_url"] = signed_download_url(job["output_key"])
    return jobs


@router.get("/jobs/{job_id}")
def get_my_job(job_id: str, current_user: dict = Depends(get_current_user)):
    job = get_job(job_id)
    if not job or job["user_id"] != current_user["id"]:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("output_key") and job.get("status") == "completed":
        job["output_url"] = signed_download_url(job["output_key"])
    return job
