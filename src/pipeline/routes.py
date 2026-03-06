from uuid import uuid4

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from src.auth.deps import get_current_user
from src.config import settings
from src.db.repository import get_job, list_jobs_by_user
from src.pipeline.service import enqueue_pipeline_job
from src.storage.service import signed_download_url, upload_bytes

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


class SubmitJobRequest(BaseModel):
    source_key: str = Field(min_length=1)
    requested_minutes: float = Field(gt=0)


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
