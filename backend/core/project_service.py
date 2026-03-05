import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from db import get_connection


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_project(
    *,
    name: str,
    source_type: str,
    source_url: Optional[str] = None,
    input_file: Optional[str] = None,
    video_file: Optional[str] = None,
    audio_file: Optional[str] = None,
    status: str = "processing",
) -> Dict[str, Any]:
    project_id = str(uuid4())
    now = _now_iso()

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO projects (
                id, name, source_type, source_url, input_file, video_file, audio_file,
                status, transcript_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                name,
                source_type,
                source_url,
                input_file,
                video_file,
                audio_file,
                status,
                "[]",
                now,
                now,
            ),
        )
        conn.commit()

    return get_project_or_404(project_id)


def update_project(project_id: str, **fields: Any) -> Dict[str, Any]:
    if not fields:
        return get_project_or_404(project_id)

    allowed_fields = {
        "name",
        "source_type",
        "source_url",
        "input_file",
        "video_file",
        "audio_file",
        "status",
        "transcript_json",
    }

    columns = []
    values = []
    for key, value in fields.items():
        if key not in allowed_fields:
            raise ValueError(f"Unsupported update field: {key}")
        columns.append(f"{key} = ?")
        values.append(value)

    values.extend([_now_iso(), project_id])
    set_clause = ", ".join(columns + ["updated_at = ?"])

    with get_connection() as conn:
        result = conn.execute(
            f"UPDATE projects SET {set_clause} WHERE id = ?",
            values,
        )
        conn.commit()

    if result.rowcount == 0:
        raise ValueError("Project not found")

    return get_project_or_404(project_id)


def list_projects() -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM projects ORDER BY created_at DESC"
        ).fetchall()
    return [_project_row_to_dict(row) for row in rows]


def get_project(project_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM projects WHERE id = ?",
            (project_id,),
        ).fetchone()
    return _project_row_to_dict(row) if row else None


def get_project_or_404(project_id: str) -> Dict[str, Any]:
    project = get_project(project_id)
    if not project:
        raise ValueError("Project not found")
    return project


def replace_project_clips(project_id: str, clips: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = _now_iso()
    with get_connection() as conn:
        conn.execute("DELETE FROM clips WHERE project_id = ?", (project_id,))
        for clip in clips:
            conn.execute(
                """
                INSERT INTO clips (
                    id, project_id, start_time, end_time, score, title, summary,
                    scoring_breakdown_json, retention_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    project_id,
                    float(clip["start"]),
                    float(clip["end"]),
                    int(clip.get("score", 0)),
                    str(clip.get("title", "Untitled Clip")),
                    str(clip.get("summary", "")),
                    json.dumps(clip.get("scoring_breakdown") or {}, ensure_ascii=False),
                    json.dumps(clip.get("retention") or {}, ensure_ascii=False),
                    now,
                ),
            )
        conn.commit()
    return list_clips(project_id)


def get_clip(clip_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM clips WHERE id = ?", (clip_id,)).fetchone()
    return _clip_row_to_dict(row) if row else None


def list_clips(project_id: str) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM clips
            WHERE project_id = ?
            ORDER BY score DESC, start_time ASC
            """,
            (project_id,),
        ).fetchall()
    return [_clip_row_to_dict(row) for row in rows]


def create_clip_performance(
    clip_id: str,
    project_id: str,
    avg_watch_time: float,
    completion_rate: float,
    rewatch_rate: float,
) -> Dict[str, Any]:
    record_id = str(uuid4())
    now = _now_iso()
    clip = get_clip(clip_id)
    if not clip:
        raise ValueError("Clip not found")

    clip_length = max(1e-3, float(clip["end"]) - float(clip["start"]))
    normalized_watch_time = max(0.0, min(1.0, float(avg_watch_time) / clip_length))
    engagement_score = (
        0.5 * float(completion_rate)
        + 0.3 * float(rewatch_rate)
        + 0.2 * normalized_watch_time
    )

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO clip_performance (
                id, clip_id, project_id, avg_watch_time, completion_rate,
                rewatch_rate, engagement_score, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                clip_id,
                project_id,
                float(avg_watch_time),
                float(completion_rate),
                float(rewatch_rate),
                float(engagement_score),
                now,
            ),
        )
        conn.commit()

    return {
        "id": record_id,
        "clipId": clip_id,
        "projectId": project_id,
        "avgWatchTime": float(avg_watch_time),
        "completionRate": float(completion_rate),
        "rewatchRate": float(rewatch_rate),
        "engagementScore": round(float(engagement_score), 4),
        "createdAt": now,
    }


def list_clip_training_samples(limit: int = 5000) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              c.id AS clip_id,
              c.score,
              c.start_time,
              c.end_time,
              c.scoring_breakdown_json,
              cp.engagement_score,
              cp.rewatch_rate
            FROM clip_performance cp
            JOIN clips c ON c.id = cp.clip_id
            ORDER BY cp.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    samples: List[Dict[str, Any]] = []
    for row in rows:
        breakdown = json.loads(row["scoring_breakdown_json"] or "{}")
        samples.append(
            {
                "clip_id": row["clip_id"],
                "score": float(row["score"]),
                "segment_length": max(0.1, float(row["end_time"]) - float(row["start_time"])),
                "engagement_score": float(row["engagement_score"]),
                "rewatch_rate": float(row["rewatch_rate"]),
                "breakdown": breakdown,
            }
        )
    return samples


def create_export(
    project_id: str,
    *,
    clip_id: Optional[str],
    fmt: str,
    output_path: Optional[str],
    status: str = "queued",
) -> Dict[str, Any]:
    export_id = str(uuid4())
    now = _now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO exports (id, project_id, clip_id, status, output_path, format, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (export_id, project_id, clip_id, status, output_path, fmt, now, now),
        )
        conn.commit()
    return get_export(export_id)


def get_export(export_id: str) -> Dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM exports WHERE id = ?",
            (export_id,),
        ).fetchone()
    if not row:
        raise ValueError("Export not found")
    return _export_row_to_dict(row)


def list_exports(project_id: str) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM exports
            WHERE project_id = ?
            ORDER BY created_at DESC
            """,
            (project_id,),
        ).fetchall()
    return [_export_row_to_dict(row) for row in rows]


def _project_row_to_dict(row: Any) -> Dict[str, Any]:
    transcript_raw = row["transcript_json"] or "[]"
    transcript_words = json.loads(transcript_raw)
    return {
        "id": row["id"],
        "name": row["name"],
        "sourceType": row["source_type"],
        "sourceUrl": row["source_url"],
        "inputFile": row["input_file"],
        "videoFile": row["video_file"],
        "audioFile": row["audio_file"],
        "status": row["status"],
        "transcriptWords": transcript_words,
        "transcriptWordCount": len(transcript_words),
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def _clip_row_to_dict(row: Any) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "projectId": row["project_id"],
        "start": row["start_time"],
        "end": row["end_time"],
        "score": row["score"],
        "title": row["title"],
        "summary": row["summary"],
        "createdAt": row["created_at"],
    }


def _export_row_to_dict(row: Any) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "projectId": row["project_id"],
        "clipId": row["clip_id"],
        "status": row["status"],
        "outputPath": row["output_path"],
        "format": row["format"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def create_video_render_job(
    *,
    project_id: str,
    clip_id: str,
    input_file: str,
    output_file: str,
    start: float,
    end: float,
    mp4_options: dict[str, Any] | None = None,
    status: str = "queued",
    max_retries: int = 2,
    callback_url: str | None = None,
    output_url: str | None = None,
) -> Dict[str, Any]:
    job_id = str(uuid4())
    now = _now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO video_render_jobs (
                id, project_id, clip_id, input_file, output_file, start, end,
                mp4_options_json, status, logs, progress, eta_seconds,
                retries, max_retries, render_time_sec, output_file_size,
                encoding_params_json, callback_url, output_url, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                project_id,
                clip_id,
                input_file,
                output_file,
                float(start),
                float(end),
                json.dumps(mp4_options or {}, ensure_ascii=False),
                status,
                "",
                0.0,
                max(1.0, float(end) - float(start)),
                0,
                int(max_retries),
                None,
                None,
                json.dumps(mp4_options or {}, ensure_ascii=False),
                callback_url,
                output_url,
                now,
                now,
            ),
        )
        conn.commit()
    return get_video_render_job(job_id)


def get_video_render_job(job_id: str) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM video_render_jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return None
    return _video_render_job_to_dict(row)


def update_video_render_job(
    job_id: str,
    *,
    status: str,
    logs: str | None = None,
    progress: float | None = None,
    eta_seconds: float | None = None,
    retries: int | None = None,
    render_time_sec: float | None = None,
    output_file_size: int | None = None,
    encoding_params: dict[str, Any] | None = None,
    output_url: str | None = None,
) -> Dict[str, Any]:
    now = _now_iso()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE video_render_jobs
            SET status = ?,
                logs = COALESCE(?, logs),
                progress = COALESCE(?, progress),
                eta_seconds = COALESCE(?, eta_seconds),
                retries = COALESCE(?, retries),
                render_time_sec = COALESCE(?, render_time_sec),
                output_file_size = COALESCE(?, output_file_size),
                encoding_params_json = COALESCE(?, encoding_params_json),
                output_url = COALESCE(?, output_url),
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                logs,
                progress,
                eta_seconds,
                retries,
                render_time_sec,
                output_file_size,
                json.dumps(encoding_params, ensure_ascii=False) if encoding_params is not None else None,
                output_url,
                now,
                job_id,
            ),
        )
        conn.commit()
    job = get_video_render_job(job_id)
    if not job:
        raise ValueError("Render job not found")
    return job


def get_next_queued_render_job() -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM video_render_jobs WHERE status IN ('queued', 'retrying') ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
    return _video_render_job_to_dict(row) if row else None


def list_video_render_jobs(project_id: str) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM video_render_jobs WHERE project_id = ? ORDER BY created_at DESC",
            (project_id,),
        ).fetchall()
    return [_video_render_job_to_dict(r) for r in rows]


def get_project_words(project_id: str) -> List[Dict[str, Any]]:
    project = get_project_or_404(project_id)
    return list(project.get("transcriptWords", []))


def _video_render_job_to_dict(row: Any) -> Dict[str, Any]:
    encoding_params = json.loads(row["encoding_params_json"] or "{}")
    encoding_profile = (
        f"{encoding_params.get('vcodec', 'libx264')}:{encoding_params.get('preset', 'medium')}:"
        f"crf{encoding_params.get('crf', 20)}"
    )
    return {
        "id": row["id"],
        "project_id": row["project_id"],
        "clip_id": row["clip_id"],
        "input_file": row["input_file"],
        "output_file": row["output_file"],
        "start": row["start"],
        "end": row["end"],
        "mp4_options": json.loads(row["mp4_options_json"] or "{}"),
        "status": row["status"],
        "logs": row["logs"],
        "progress": float(row["progress"] or 0.0),
        "eta_seconds": float(row["eta_seconds"] or 0.0),
        "retries": int(row["retries"] or 0),
        "max_retries": int(row["max_retries"] or 0),
        "render_time_sec": row["render_time_sec"],
        "output_file_size": row["output_file_size"],
        "encoding_params": encoding_params,
        "encoding_profile": encoding_profile,
        "callback_url": row["callback_url"],
        "output_url": row["output_url"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
