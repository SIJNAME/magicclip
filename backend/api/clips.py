import json
from datetime import datetime
from time import perf_counter
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, UploadFile, File
from pydantic import BaseModel
from schemas.clip_schema import ClipRequest, ClipResponse
from core.clip_service import generate_clip_suggestions
from core.youtube_service import download_youtube_video
from core.stt_service import extract_audio, transcribe_audio
from core.mapping_service import map_words_to_segments
from core.ai_enrich_service import generate_clips

router = APIRouter()
BASE_DIR = Path(__file__).resolve().parents[1]
STORAGE_DIR = BASE_DIR / "storage"
VIDEO_DIR = STORAGE_DIR / "videos"
UPLOAD_DIR = STORAGE_DIR / "uploads"
OUTPUT_DIR = STORAGE_DIR / "outputs"


def ensure_storage_dirs():
    for folder in (VIDEO_DIR, UPLOAD_DIR, OUTPUT_DIR):
        folder.mkdir(parents=True, exist_ok=True)

@router.post("/generate", response_model=ClipResponse)
def generate_clips_from_text(request: ClipRequest):
    result = generate_clip_suggestions(request.transcript)

    return {"clips": result}


@router.post("/upload")
async def upload_audio(file: UploadFile = File(...)):
    ensure_storage_dirs()

    file_name = file.filename or "uploaded_audio"
    saved_name = f"{uuid4().hex}_{Path(file_name).name}"
    file_path = UPLOAD_DIR / saved_name

    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    # 1 STT word-level
    words = transcribe_audio(file_path)

    # 2 map words -> segments
    segments = map_words_to_segments(words)

    # 3 AI clip analysis
    clips = generate_clips(segments)
    output_path = OUTPUT_DIR / f"upload_{file_path.stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    payload = {
        "source": "upload",
        "input_file": str(file_path),
        "segments": segments,
        "clips": clips,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "segments": segments,
        "clips": clips,
        "input_file": str(file_path),
        "output_file": str(output_path),
    }


class YoutubeRequest(BaseModel):
    url: str


@router.post("/from-youtube")
async def generate_from_youtube(data: YoutubeRequest):
    ensure_storage_dirs()
    total_start = perf_counter()
    timings = {}

    # 1 download
    t0 = perf_counter()
    downloaded_path = download_youtube_video(data.url, output_dir=str(VIDEO_DIR), audio_only=True)
    timings["download_sec"] = round(perf_counter() - t0, 3)

    # 2 extract audio
    t0 = perf_counter()
    path_obj = Path(downloaded_path)
    if path_obj.suffix.lower() in {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac"}:
        audio_path = downloaded_path
    else:
        audio_path = extract_audio(downloaded_path)
    timings["extract_audio_sec"] = round(perf_counter() - t0, 3)

    # 3 STT
    t0 = perf_counter()
    words = transcribe_audio(audio_path)
    timings["stt_sec"] = round(perf_counter() - t0, 3)

    # 4 mapping
    t0 = perf_counter()
    segments = map_words_to_segments(words)
    timings["mapping_sec"] = round(perf_counter() - t0, 3)

    # 5 AI clip
    t0 = perf_counter()
    clips = generate_clips(segments)
    timings["ai_sec"] = round(perf_counter() - t0, 3)
    timings["total_sec"] = round(perf_counter() - total_start, 3)

    output_path = OUTPUT_DIR / f"youtube_{Path(downloaded_path).stem}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
    video_file = downloaded_path if Path(downloaded_path).suffix.lower() in {".mp4", ".mkv", ".webm", ".mov"} else None
    payload = {
        "source": "youtube",
        "url": data.url,
        "downloaded_file": downloaded_path,
        "video_file": video_file,
        "audio_file": audio_path,
        "segments": segments,
        "clips": clips,
        "timings": timings,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "segments": segments,
        "clips": clips,
        "downloaded_file": downloaded_path,
        "video_file": video_file,
        "audio_file": audio_path,
        "output_file": str(output_path),
        "timings": timings,
    }
