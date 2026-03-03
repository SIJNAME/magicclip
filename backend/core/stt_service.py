import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from groq import Groq

load_dotenv()

logger = logging.getLogger(__name__)

GROQ_MODEL = "whisper-large-v3"
DEFAULT_CHUNK_MINUTES = int(os.getenv("STT_CHUNK_MINUTES", "5"))
DEFAULT_OVERLAP_SECONDS = float(os.getenv("STT_OVERLAP_SECONDS", "3"))


class STTChunkError(RuntimeError):
    """Raised when a chunk transcription fails."""


def _get_client() -> Groq:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is required for transcription")
    return Groq(api_key=api_key)


def _run_ffmpeg(command: list[str]) -> None:
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(f"ffmpeg command failed: {' '.join(command)} | {stderr}") from exc


def _get_audio_duration_seconds(audio_path: str) -> float:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        audio_path,
    ]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        return max(float(result.stdout.strip()), 0.0)
    except (ValueError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"Unable to read audio duration for {audio_path}") from exc


def extract_audio(input_path: str, output_path: str | None = None) -> str:
    """Extract optimized WAV audio for STT (mono, 16kHz, 32k bitrate)."""
    source = Path(input_path)
    target = Path(output_path) if output_path else source.with_name(f"{source.stem}_stt.wav")
    target.parent.mkdir(parents=True, exist_ok=True)

    _run_ffmpeg(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(input_path),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-b:a",
            "32k",
            "-f",
            "wav",
            str(target),
        ]
    )

    return str(target)


def split_audio_chunks(
    audio_path: str,
    chunk_minutes: int | None = None,
    overlap_seconds: float | None = None,
) -> list[tuple[str, float]]:
    """Split audio into chunk files and return (chunk_path, chunk_start_seconds)."""
    minutes = chunk_minutes if chunk_minutes and chunk_minutes > 0 else DEFAULT_CHUNK_MINUTES
    overlap = max(overlap_seconds if overlap_seconds is not None else DEFAULT_OVERLAP_SECONDS, 0.0)

    chunk_seconds = float(minutes * 60)
    if chunk_seconds <= 0:
        raise ValueError("Chunk duration must be > 0")

    duration = _get_audio_duration_seconds(audio_path)
    if duration <= 0:
        return []

    step = max(chunk_seconds - overlap, 1.0)
    output_dir = Path(tempfile.mkdtemp(prefix="stt_chunks_"))
    chunks: list[tuple[str, float]] = []

    cursor = 0.0
    chunk_index = 0
    while cursor < duration:
        chunk_length = min(chunk_seconds, duration - cursor)
        chunk_path = output_dir / f"chunk_{chunk_index:04d}.wav"

        _run_ffmpeg(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{cursor:.3f}",
                "-t",
                f"{chunk_length:.3f}",
                "-i",
                audio_path,
                "-ac",
                "1",
                "-ar",
                "16000",
                "-b:a",
                "32k",
                "-f",
                "wav",
                str(chunk_path),
            ]
        )

        chunks.append((str(chunk_path), cursor))
        cursor += step
        chunk_index += 1

    return chunks


def transcribe_chunk(chunk_path: str) -> dict[str, Any]:
    """Transcribe one chunk with Groq Whisper and request segment+word timestamps."""
    with open(chunk_path, "rb") as chunk_file:
        response = _get_client().audio.transcriptions.create(
            file=(Path(chunk_path).name, chunk_file.read()),
            model=GROQ_MODEL,
            temperature=0,
            response_format="verbose_json",
            timestamp_granularities=["word", "segment"],
        )

    if hasattr(response, "model_dump"):
        return response.model_dump()
    if isinstance(response, dict):
        return response
    return dict(response)


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def _segments_similar(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_text = _normalize_text(str(left.get("text", "")))
    right_text = _normalize_text(str(right.get("text", "")))

    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True

    left_tokens = set(left_text.split())
    right_tokens = set(right_text.split())
    union = left_tokens | right_tokens
    if not union:
        return False

    similarity = len(left_tokens & right_tokens) / len(union)
    return similarity >= 0.8


def merge_segments(all_segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort segments globally and remove overlap duplicates."""
    sorted_segments = sorted(all_segments, key=lambda item: float(item.get("start", 0.0)))
    merged: list[dict[str, Any]] = []

    for segment in sorted_segments:
        start = float(segment.get("start", 0.0))
        end = float(segment.get("end", start))
        text = str(segment.get("text", "")).strip()
        normalized = {"start": start, "end": max(end, start), "text": text}

        if not merged:
            merged.append(normalized)
            continue

        previous = merged[-1]
        if abs(start - previous["start"]) < 1.0 and _segments_similar(previous, normalized):
            if normalized["end"] > previous["end"]:
                previous["end"] = normalized["end"]
            continue

        merged.append(normalized)

    return merged


def _offset_segment(segment: dict[str, Any], chunk_start: float) -> dict[str, Any]:
    local_start = float(segment.get("start", 0.0))
    local_end = float(segment.get("end", local_start))
    global_start = chunk_start + local_start
    global_end = chunk_start + max(local_end, local_start)
    return {
        "start": global_start,
        "end": global_end,
        "text": str(segment.get("text", "")).strip(),
    }


def _offset_words(words: list[dict[str, Any]], chunk_start: float) -> list[dict[str, Any]]:
    adjusted_words: list[dict[str, Any]] = []
    for word in words:
        text = word.get("word") or word.get("text") or ""
        local_start = float(word.get("start", 0.0))
        local_end = float(word.get("end", local_start))
        adjusted_words.append(
            {
                "word": str(text),
                "start": chunk_start + local_start,
                "end": chunk_start + max(local_end, local_start),
                "confidence": word.get("confidence"),
            }
        )
    return adjusted_words


def _transcribe_chunk_stream(chunks: list[tuple[str, float]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Sequential chunk transcription stream (ready for future async parallelization)."""
    all_segments: list[dict[str, Any]] = []
    all_words: list[dict[str, Any]] = []

    for chunk_index, (chunk_path, chunk_start) in enumerate(chunks):
        logger.info(
            "Transcribing chunk %s/%s start=%.3f path=%s",
            chunk_index + 1,
            len(chunks),
            chunk_start,
            chunk_path,
        )
        try:
            chunk_result = transcribe_chunk(chunk_path)
        except Exception as exc:  # noqa: BLE001
            message = (
                f"Chunk transcription failed at chunk_index={chunk_index}, "
                f"start={chunk_start:.3f}, path={chunk_path}"
            )
            logger.exception(message)
            raise STTChunkError(message) from exc

        chunk_segments = chunk_result.get("segments") or []
        all_segments.extend(_offset_segment(segment, chunk_start) for segment in chunk_segments)

        chunk_words = chunk_result.get("words") or []
        all_words.extend(_offset_words(chunk_words, chunk_start))

    return all_segments, all_words


def _transcribe_audio_internal(file_path: str) -> dict[str, Any]:
    source = Path(file_path)
    with tempfile.TemporaryDirectory(prefix="stt_work_") as temp_dir:
        optimized_audio = extract_audio(
            str(source),
            output_path=str(Path(temp_dir) / f"{source.stem}_optimized.wav"),
        )
        chunks = split_audio_chunks(optimized_audio)
        all_segments, all_words = _transcribe_chunk_stream(chunks)

    merged_segments = merge_segments(all_segments)
    full_text = " ".join(segment["text"] for segment in merged_segments if segment["text"]).strip()

    return {
        "segments": merged_segments,
        "full_text": full_text,
        "words": all_words,
    }


def transcribe_audio_to_segments(file_path: str) -> dict[str, Any]:
    """Chunk-based STT pipeline returning merged segments and full text."""
    result = _transcribe_audio_internal(file_path)
    return {
        "segments": result["segments"],
        "full_text": result["full_text"],
    }


def transcribe_audio(file_path: str):
    """Backward-compatible entrypoint returning word-level timestamps."""
    result = _transcribe_audio_internal(file_path)
    return result.get("words", [])
