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
                INSERT INTO clips (id, project_id, start_time, end_time, score, title, summary, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    project_id,
                    float(clip["start"]),
                    float(clip["end"]),
                    int(clip.get("score", 0)),
                    str(clip.get("title", "Untitled Clip")),
                    str(clip.get("summary", "")),
                    now,
                ),
            )
        conn.commit()
    return list_clips(project_id)


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
