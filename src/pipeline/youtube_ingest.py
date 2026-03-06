import json
import subprocess
from pathlib import Path
from urllib.parse import urlparse

ALLOWED_YOUTUBE_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "youtu.be",
}
MAX_DURATION_SECONDS = 2 * 60 * 60
MAX_ESTIMATED_BYTES = 500 * 1024 * 1024
YOUTUBE_FORMAT_SELECTOR = "bv*[ext=mp4][height<=1080]+ba[ext=m4a]/b[ext=mp4][height<=1080]/b[height<=1080]"
YOUTUBE_SOURCE_KEY_PREFIX = "youtube_url:"


def validate_youtube_url(youtube_url: str) -> None:
    parsed = urlparse(youtube_url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        raise RuntimeError("Invalid youtube_url: only http/https are allowed")
    if host not in ALLOWED_YOUTUBE_HOSTS:
        raise RuntimeError("Invalid youtube_url: host is not allowed")


def encode_youtube_source_key(youtube_url: str) -> str:
    validate_youtube_url(youtube_url)
    return f"{YOUTUBE_SOURCE_KEY_PREFIX}{youtube_url}"


def decode_youtube_source_key(source_key: str) -> str | None:
    if not source_key.startswith(YOUTUBE_SOURCE_KEY_PREFIX):
        return None
    youtube_url = source_key[len(YOUTUBE_SOURCE_KEY_PREFIX) :]
    validate_youtube_url(youtube_url)
    return youtube_url


def _run_yt_dlp(cmd: list[str], timeout_seconds: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=timeout_seconds)
    except FileNotFoundError as exc:
        raise RuntimeError("yt-dlp is required to ingest YouTube URLs") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("YouTube download timed out") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        raise RuntimeError(f"YouTube ingest failed: {detail}") from exc


def _extract_size_bytes(format_payload: dict) -> int | None:
    raw = format_payload.get("filesize")
    if raw is None:
        raw = format_payload.get("filesize_approx")
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def _estimate_selected_size_bytes(info: dict) -> int | None:
    requested_formats = info.get("requested_formats") or []
    if requested_formats:
        total = 0
        for item in requested_formats:
            size = _extract_size_bytes(item)
            if size is None:
                return None
            total += size
        return total if total > 0 else None

    single_size = _extract_size_bytes(info)
    if single_size is not None:
        return single_size

    requested_downloads = info.get("requested_downloads") or []
    if requested_downloads:
        total = 0
        for item in requested_downloads:
            size = _extract_size_bytes(item)
            if size is None:
                return None
            total += size
        return total if total > 0 else None
    return None


def _resolve_downloaded_path(stdout_text: str, temp_dir: str) -> str:
    final_path_lines = [line.strip() for line in stdout_text.splitlines() if line.strip()]
    if not final_path_lines:
        raise RuntimeError("YouTube download failed: could not resolve downloaded file path")

    downloaded_file = Path(final_path_lines[-1]).resolve()
    temp_root = Path(temp_dir).resolve()
    if temp_root not in downloaded_file.parents:
        raise RuntimeError("YouTube download failed: output escaped temp directory")
    if not downloaded_file.exists() or not downloaded_file.is_file():
        raise RuntimeError("YouTube download failed: downloaded file not found")
    return str(downloaded_file)


def fetch_youtube_metadata(youtube_url: str) -> dict[str, int | str]:
    validate_youtube_url(youtube_url)
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--no-download",
        "--no-playlist",
        "--no-warnings",
        "-f",
        YOUTUBE_FORMAT_SELECTOR,
        youtube_url,
    ]
    result = _run_yt_dlp(cmd, timeout_seconds=60)
    payload_text = (result.stdout or "").strip()
    if not payload_text:
        raise RuntimeError("YouTube ingest failed: empty metadata response")

    try:
        info = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("YouTube ingest failed: invalid metadata response") from exc

    video_id = str(info.get("id") or "").strip()
    if not video_id:
        raise RuntimeError("YouTube ingest failed: missing video id")

    try:
        duration_seconds = int(info.get("duration") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("YouTube ingest failed: invalid duration metadata") from exc

    if duration_seconds <= 0:
        raise RuntimeError("YouTube ingest failed: missing duration metadata")
    if duration_seconds > MAX_DURATION_SECONDS:
        raise RuntimeError("YouTube ingest failed: video longer than 2 hours")

    estimated_bytes = _estimate_selected_size_bytes(info)
    if estimated_bytes is None:
        raise RuntimeError("YouTube ingest failed: could not estimate selected format size")
    if estimated_bytes > MAX_ESTIMATED_BYTES:
        raise RuntimeError("YouTube ingest failed: selected format exceeds 500MB")

    return {
        "video_id": video_id,
        "duration_seconds": duration_seconds,
        "estimated_bytes": estimated_bytes,
    }


def download_youtube_audio_to_temp(youtube_url: str, temp_dir: str) -> str:
    validate_youtube_url(youtube_url)
    output_template = str(Path(temp_dir) / "%(id)s.%(ext)s")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--no-warnings",
        "--restrict-filenames",
        "--extract-audio",
        "--audio-format",
        "mp3",
        "--audio-quality",
        "0",
        "--max-filesize",
        "500M",
        "-o",
        output_template,
        "--print",
        "after_move:filepath",
        youtube_url,
    ]
    result = _run_yt_dlp(cmd, timeout_seconds=180)
    return _resolve_downloaded_path(result.stdout or "", temp_dir)


def download_youtube_video_segment_to_temp(youtube_url: str, temp_dir: str, start: float, end: float) -> str:
    validate_youtube_url(youtube_url)
    if end <= start:
        raise RuntimeError("Invalid clip range")

    start_sec = max(0.0, float(start))
    end_sec = float(end)
    sections = f"*{start_sec:.3f}-{end_sec:.3f}"
    output_template = str(Path(temp_dir) / "%(id)s.%(ext)s")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--no-warnings",
        "--restrict-filenames",
        "--merge-output-format",
        "mp4",
        "--max-filesize",
        "500M",
        "-f",
        YOUTUBE_FORMAT_SELECTOR,
        "--download-sections",
        sections,
        "-o",
        output_template,
        "--print",
        "after_move:filepath",
        youtube_url,
    ]
    result = _run_yt_dlp(cmd, timeout_seconds=180)
    return _resolve_downloaded_path(result.stdout or "", temp_dir)
