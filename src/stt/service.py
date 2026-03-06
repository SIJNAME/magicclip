import subprocess
from pathlib import Path
from typing import Any

from groq import Groq

from src.config import settings

GROQ_STT_MODEL = "whisper-large-v3"
SUPPORTED_EXTENSIONS = {".mp3", ".wav", ".mp4"}


def _client() -> Groq:
    if not settings.groq_api_key:
        raise RuntimeError("GROQ_API_KEY is required")
    return Groq(api_key=settings.groq_api_key)


def _validate_input(path: str) -> Path:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        raise RuntimeError(f"Input file not found: {path}")
    suffix = file_path.suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise RuntimeError(f"Unsupported media format '{suffix}'. Supported: mp3, wav, mp4")
    return file_path


def transcribe_to_payload(input_media_path: str) -> dict[str, Any]:
    file_path = _validate_input(input_media_path)
    try:
        with file_path.open("rb") as media_file:
            response = _client().audio.transcriptions.create(
                model=GROQ_STT_MODEL,
                file=media_file,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )
    except Exception as exc:
        raise RuntimeError(f"Groq transcription failed: {exc}") from exc

    payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
    raw_segments = payload.get("segments") or []
    segments: list[dict[str, Any]] = []
    for seg in raw_segments:
        segments.append(
            {
                "start": float(seg.get("start", 0.0)),
                "end": float(seg.get("end", seg.get("start", 0.0))),
                "text": str(seg.get("text", "")).strip(),
            }
        )
    return {
        "text": str(payload.get("text", "")).strip(),
        "segments": segments,
    }


def transcribe_segments(input_media_path: str) -> list[dict[str, Any]]:
    # Keep pipeline contract unchanged.
    return transcribe_to_payload(input_media_path)["segments"]


def video_duration_minutes(video_path: str) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        video_path,
    ]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()
    seconds = float(out)
    return max(0.0, seconds / 60.0)


def estimate_minutes_from_source_path(source_path: str) -> float:
    return video_duration_minutes(source_path)
