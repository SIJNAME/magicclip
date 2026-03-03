import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, UploadFile

from backend.core.ai_enrich_service import enrich_words_with_ai, select_clips
from backend.core.mapping_service import map_words_to_segments
from backend.core.project_service import (
    create_export,
    create_project,
    get_project,
    get_project_or_404,
    list_clips,
    list_exports,
    list_projects,
    replace_project_clips,
    update_project,
)
from backend.core.stt_service import extract_audio, transcribe_audio
from backend.core.youtube_service import download_youtube_video
from backend.schemas.project_schema import (
    CreateYoutubeProjectRequest,
    ExportCreateRequest,
    ProjectDetail,
    ProjectItem,
)

router = APIRouter()
BASE_DIR = Path(__file__).resolve().parents[1]
STORAGE_DIR = BASE_DIR / "storage"
VIDEO_DIR = STORAGE_DIR / "videos"
UPLOAD_DIR = STORAGE_DIR / "uploads"
OUTPUT_DIR = STORAGE_DIR / "outputs"


def ensure_storage_dirs() -> None:
    for folder in (VIDEO_DIR, UPLOAD_DIR, OUTPUT_DIR):
        folder.mkdir(parents=True, exist_ok=True)


def _project_detail(project_id: str) -> ProjectDetail:
    project = get_project_or_404(project_id)
    return ProjectDetail(
        **project,
        clips=list_clips(project_id),
        exports=list_exports(project_id),
    )


def _run_pipeline(
    *,
    project_id: str,
    audio_path: str,
    output_name_prefix: str,
) -> None:
    words = transcribe_audio(audio_path)
    mapped_words = map_words_to_segments(words)
    enriched_words = enrich_words_with_ai(mapped_words)
    clips = select_clips(enriched_words)

    replace_project_clips(project_id, clips)
    update_project(
        project_id,
        transcript_json=json.dumps(enriched_words, ensure_ascii=False),
        status="ready",
    )

    output_path = OUTPUT_DIR / f"{output_name_prefix}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    payload = {
        "project_id": project_id,
        "transcript_words": enriched_words,
        "clips": clips,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    create_export(project_id, clip_id=None, fmt="json", output_path=str(output_path), status="completed")


@router.post("/upload", response_model=ProjectDetail)
async def create_project_from_upload(file: UploadFile = File(...)):
    ensure_storage_dirs()

    original_name = file.filename or "uploaded_media"
    saved_name = f"{uuid4().hex}_{Path(original_name).name}"
    file_path = UPLOAD_DIR / saved_name

    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    project = create_project(
        name=Path(original_name).stem,
        source_type="upload",
        input_file=str(file_path),
        audio_file=str(file_path),
        status="processing",
    )

    try:
        _run_pipeline(
            project_id=project["id"],
            audio_path=str(file_path),
            output_name_prefix=f"upload_{file_path.stem}",
        )
    except Exception as exc:
        update_project(project["id"], status="failed")
        raise HTTPException(status_code=500, detail=f"Upload processing failed: {exc}") from exc

    return _project_detail(project["id"])


@router.post("/from-youtube", response_model=ProjectDetail)
async def create_project_from_youtube(data: CreateYoutubeProjectRequest):
    ensure_storage_dirs()

    project_name = data.name or "YouTube Project"
    project = create_project(
        name=project_name,
        source_type="youtube",
        source_url=data.url,
        status="processing",
    )

    try:
        downloaded_path = download_youtube_video(data.url, output_dir=str(VIDEO_DIR), audio_only=True)
        path_obj = Path(downloaded_path)
        if path_obj.suffix.lower() in {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac"}:
            audio_path = downloaded_path
        else:
            audio_output = str((VIDEO_DIR / f"{path_obj.stem}_audio.mp3"))
            audio_path = extract_audio(downloaded_path, output_path=audio_output)

        update_project(
            project["id"],
            input_file=downloaded_path,
            audio_file=audio_path,
            video_file=downloaded_path if path_obj.suffix.lower() in {".mp4", ".mkv", ".webm", ".mov"} else None,
        )

        _run_pipeline(
            project_id=project["id"],
            audio_path=audio_path,
            output_name_prefix=f"youtube_{Path(downloaded_path).stem}",
        )
    except Exception as exc:
        update_project(project["id"], status="failed")
        raise HTTPException(status_code=500, detail=f"YouTube processing failed: {exc}") from exc

    return _project_detail(project["id"])


@router.get("/", response_model=list[ProjectItem])
def get_projects():
    return list_projects()


@router.get("/{project_id}", response_model=ProjectDetail)
def get_project_detail(project_id: str):
    project = get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return _project_detail(project_id)


@router.get("/{project_id}/clips")
def get_project_clips(project_id: str):
    if not get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    return {"projectId": project_id, "clips": list_clips(project_id)}


@router.post("/{project_id}/exports")
def create_project_export(project_id: str, request: ExportCreateRequest):
    if not get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    export = create_export(
        project_id,
        clip_id=request.clipId,
        fmt=request.format,
        output_path=request.outputPath,
        status="queued",
    )
    return export


@router.get("/{project_id}/exports")
def get_project_exports(project_id: str):
    if not get_project(project_id):
        raise HTTPException(status_code=404, detail="Project not found")
    return {"projectId": project_id, "exports": list_exports(project_id)}
