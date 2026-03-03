from fastapi import APIRouter, HTTPException

from backend.core.project_service import (
    create_clip_performance,
    get_clip,
    get_video_render_job,
)
from backend.schemas.clip_schema import ClipPerformanceCreateRequest, ClipPerformanceItem
from backend.schemas.render_schema import VideoRenderJobResponse
from backend.video_renderer.worker import enqueue_render_job

router = APIRouter()


@router.post("/{clip_id}/performance", response_model=ClipPerformanceItem)
def create_performance_entry(clip_id: str, payload: ClipPerformanceCreateRequest):
    clip = get_clip(clip_id)
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")

    record = create_clip_performance(
        clip_id=clip_id,
        project_id=clip["projectId"],
        avg_watch_time=payload.avg_watch_time,
        completion_rate=payload.completion_rate,
        rewatch_rate=payload.rewatch_rate,
    )
    return record


@router.post("/{clip_id}/render", response_model=VideoRenderJobResponse)
def enqueue_clip_render(clip_id: str):
    clip = get_clip(clip_id)
    if not clip:
        raise HTTPException(status_code=404, detail="Clip not found")
    try:
        return enqueue_render_job(project_id=clip["projectId"], clip_id=clip_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/render-jobs/{job_id}", response_model=VideoRenderJobResponse)
def get_render_job(job_id: str):
    job = get_video_render_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Render job not found")
    return job
