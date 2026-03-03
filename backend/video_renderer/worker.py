from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

from core.project_service import (
    create_video_render_job,
    get_clip,
    get_next_queued_render_job,
    get_project,
    get_project_words,
    get_video_render_job,
    update_video_render_job,
)
from video_renderer.config import CONFIG, default_mp4_options
from video_renderer.renderer import render_clip

logger = logging.getLogger(__name__)

try:
    from redis import Redis
    from rq import Queue
except Exception:  # pragma: no cover
    Redis = None
    Queue = None


@dataclass
class VideoRenderJob:
    project_id: str
    clip_id: str
    input_file: str
    output_file: str
    start: float
    end: float
    mp4_options: dict[str, Any]
    status: str
    logs: str


def _redis_queue() -> Queue | None:
    redis_url = os.getenv("MC_REDIS_URL")
    if not redis_url or Redis is None or Queue is None:
        return None
    conn = Redis.from_url(redis_url)
    return Queue("magicclip-render", connection=conn)


def _send_callback_if_needed(job: dict[str, Any]) -> None:
    callback = job.get("callback_url")
    if not callback:
        return
    payload = {
        "job_id": job["id"],
        "status": job["status"],
        "output_url": job.get("output_url"),
        "output_file": job.get("output_file"),
        "render_time": job.get("render_time_sec"),
        "output_file_size": job.get("output_file_size"),
        "encoding_params": job.get("encoding_params"),
    }
    try:
        requests.post(callback, json=payload, timeout=5)
    except Exception:
        logger.exception("render_callback_failed", extra={"job_id": job["id"], "callback": callback})


def enqueue_render_job(
    project_id: str,
    clip_id: str,
    output_file: str | None = None,
    start: float | None = None,
    end: float | None = None,
    callback_url: str | None = None,
) -> dict[str, Any]:
    clip = get_clip(clip_id)
    project = get_project(project_id)
    if not clip or not project:
        raise ValueError("Project or clip not found")

    input_file = project.get("videoFile") or project.get("inputFile") or project.get("audioFile")
    if not input_file:
        raise ValueError("No input media available for render")

    clip_start = float(clip["start"]) if start is None else float(start)
    clip_end = float(clip["end"]) if end is None else float(end)
    if clip_end <= clip_start:
        raise ValueError("Invalid render range")

    out = output_file or str(
        Path(__file__).resolve().parents[1]
        / "storage"
        / "renders"
        / f"{project_id}_{clip_id}.mp4"
    )
    db_job = create_video_render_job(
        project_id=project_id,
        clip_id=clip_id,
        input_file=input_file,
        output_file=out,
        start=clip_start,
        end=clip_end,
        mp4_options=default_mp4_options(),
        status="queued",
        max_retries=2,
        callback_url=callback_url,
    )

    rq_q = _redis_queue()
    if rq_q:
        rq_q.enqueue(process_job_by_id, db_job["id"], job_timeout="20m")
        logger.info("render_job_enqueued_rq", extra={"job_id": db_job["id"]})
    return db_job


def process_job_by_id(job_id: str, renderer: Callable[..., dict[str, Any]] = render_clip) -> dict[str, Any] | None:
    job = get_video_render_job(job_id)
    if not job:
        return None
    return _process_job(job, renderer=renderer)


def _process_job(job: dict[str, Any], renderer: Callable[..., dict[str, Any]]) -> dict[str, Any]:
    started = time.time()
    clip_duration = max(1.0, float(job["end"]) - float(job["start"]))
    update_video_render_job(
        job["id"],
        status="processing",
        logs="worker_started",
        progress=0.1,
        eta_seconds=clip_duration,
    )
    try:
        words = get_project_words(job["project_id"])
        update_video_render_job(job["id"], status="processing", progress=0.25, eta_seconds=clip_duration * 0.75)
        result = renderer(
            input_file=job["input_file"],
            output_file=job["output_file"],
            start=job["start"],
            end=job["end"],
            words=words,
            mp4_options=job.get("mp4_options") or default_mp4_options(),
        )
        status = result.get("status", "failed")
        logs = result.get("log", "")

        render_time = round(time.time() - started, 3)
        file_size = None
        try:
            file_size = os.path.getsize(job["output_file"]) if os.path.exists(job["output_file"]) else None
        except OSError:
            file_size = None

        output_url = job.get("output_url")

        if status == "failed" and job["retries"] < job["max_retries"]:
            retries = int(job["retries"]) + 1
            updated = update_video_render_job(
                job["id"],
                status="retrying",
                logs=logs,
                progress=0.0,
                eta_seconds=clip_duration,
                retries=retries,
                render_time_sec=render_time,
                output_file_size=file_size,
                encoding_params=job.get("mp4_options") or default_mp4_options(),
                output_url=output_url,
            )
        else:
            updated = update_video_render_job(
                job["id"],
                status=status,
                logs=logs,
                progress=1.0 if status == "completed" else 0.0,
                eta_seconds=0.0,
                render_time_sec=render_time,
                output_file_size=file_size,
                encoding_params=job.get("mp4_options") or default_mp4_options(),
                output_url=output_url,
            )
            if status == "completed":
                _send_callback_if_needed(updated)

        logger.info(
            "video_render_job_completed",
            extra={
                "job_id": job["id"],
                "status": status,
                "render_time": render_time,
                "output_file_size": file_size,
                "encoding_params": job.get("mp4_options") or default_mp4_options(),
            },
        )
    except Exception as exc:
        retries = int(job["retries"]) + 1
        new_status = "retrying" if retries <= int(job["max_retries"]) else "failed"
        updated = update_video_render_job(
            job["id"],
            status=new_status,
            logs=str(exc),
            progress=0.0,
            eta_seconds=clip_duration,
            retries=retries,
            render_time_sec=round(time.time() - started, 3),
            encoding_params=job.get("mp4_options") or default_mp4_options(),
            output_url=job.get("output_url"),
        )
        if new_status == "failed":
            _send_callback_if_needed(updated)
        logger.exception("video_render_job_failed", extra={"job_id": job["id"], "retries": retries})

    return get_video_render_job(job["id"]) or job


def process_next_render_job(renderer: Callable[..., dict[str, Any]] = render_clip) -> dict[str, Any] | None:
    job = get_next_queued_render_job()
    if not job:
        return None
    return _process_job(job, renderer=renderer)


class RenderWorker(threading.Thread):
    daemon = True

    def __init__(self) -> None:
        super().__init__(name="magicclip-render-worker")
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            processed = process_next_render_job()
            if processed is None:
                time.sleep(CONFIG.queue_poll_interval_sec)

    def stop(self) -> None:
        self._stop.set()
