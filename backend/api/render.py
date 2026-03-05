from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from core.project_service import get_video_render_job
from schemas.render_schema import RenderCreateRequest, VideoRenderJobResponse
from video_renderer.worker import enqueue_render_job

router = APIRouter()


def _build_output_url(request: Request, job_id: str) -> str:
    return str(request.url_for("download_render", job_id=job_id))


@router.post("", response_model=VideoRenderJobResponse)
def create_render_job(payload: RenderCreateRequest, request: Request):
    try:
        job = enqueue_render_job(
            project_id=payload.project_id,
            clip_id=payload.clip_id,
            start=payload.start,
            end=payload.end,
            callback_url=payload.callback_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    output_url = _build_output_url(request, job["id"])
    from core.project_service import update_video_render_job

    job = update_video_render_job(job["id"], status=job["status"], output_url=output_url)
    return job


@router.get("/{job_id}/status", response_model=VideoRenderJobResponse)
def render_status(job_id: str):
    job = get_video_render_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Render job not found")
    return job


@router.get("/{job_id}/download", name="download_render")
def download_render(job_id: str):
    job = get_video_render_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Render job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=409, detail="Render is not completed")

    path = Path(job["output_file"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="Rendered file missing")
    return FileResponse(path=str(path), media_type="video/mp4", filename=path.name)
