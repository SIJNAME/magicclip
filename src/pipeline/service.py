import os
import tempfile
import threading

from src.clip.service import suggest_clips
from src.config import settings
from src.db.repository import claim_job_for_processing, get_job, mark_job_completed, mark_job_failed, recover_timed_out_jobs, touch_job_heartbeat
from src.queue.service import enqueue_dead_letter, get_queue
from src.render.service import render_clip
from src.storage.service import download_to_temp, upload_file
from src.stt.service import transcribe_segments
from src.usage.service import get_plan_policy, reserve_job_capacity


def _estimate_reserved_storage_bytes(requested_minutes: float) -> int:
    # Conservative estimate for short-form H.264 output reservation.
    bytes_per_minute = 15 * 1024 * 1024
    return max(1, int(requested_minutes * bytes_per_minute))


def enqueue_pipeline_job(user_id: str, source_key: str, requested_minutes: float) -> dict:
    recovery = recover_timed_out_jobs(settings.job_timeout_sec, settings.job_max_attempts)
    if recovery["requeued"]:
        queue = get_queue()
        for stale_job_id in recovery["requeued"]:
            queue.enqueue("src.pipeline.worker.process_pipeline_job", stale_job_id, job_timeout="60m")

    job = reserve_job_capacity(
        user_id=user_id,
        source_key=source_key,
        requested_minutes=requested_minutes,
        reserved_storage_bytes=_estimate_reserved_storage_bytes(requested_minutes),
    )
    queue = get_queue()
    try:
        queue.enqueue("src.pipeline.worker.process_pipeline_job", job["id"], job_timeout="60m")
    except Exception:
        mark_job_failed(job["id"], "Queue enqueue failed")
        enqueue_dead_letter(job["id"], "Queue enqueue failed")
        raise
    return get_job(job["id"]) or job


def run_pipeline(job_id: str) -> None:
    job = get_job(job_id)
    if not job:
        return
    if not claim_job_for_processing(job_id):
        return
    source_temp_path = ""
    output_temp_path = ""
    stop_heartbeat = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_heartbeat.wait(settings.job_heartbeat_sec):
            touch_job_heartbeat(job_id)

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, name=f"job-heartbeat-{job_id}", daemon=True)
    heartbeat_thread.start()
    try:
        source_temp_path = download_to_temp(job["source_key"])
        segments = transcribe_segments(source_temp_path)
        clips = suggest_clips(segments)
        selected = clips[0]

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_out:
            output_temp_path = tmp_out.name
        render_clip(
            input_path=source_temp_path,
            output_path=output_temp_path,
            start=float(selected["start"]),
            end=float(selected["end"]),
        )
        output_size_bytes = os.path.getsize(output_temp_path)
        output_key = f"{settings.s3_output_prefix}/{job['user_id']}/{job['id']}.mp4"
        upload_file(output_temp_path, output_key)
        policy = get_plan_policy(job["user_id"])
        mark_job_completed(
            job_id=job_id,
            transcript=segments,
            clips=clips,
            selected_clip=selected,
            output_key=output_key,
            output_size_bytes=output_size_bytes,
            storage_limit_bytes=policy["storage_limit_bytes"],
        )
    except Exception as exc:
        mark_job_failed(job_id, str(exc))
        enqueue_dead_letter(job_id, str(exc))
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=2)
        if source_temp_path and os.path.exists(source_temp_path):
            os.remove(source_temp_path)
        if output_temp_path and os.path.exists(output_temp_path):
            os.remove(output_temp_path)
