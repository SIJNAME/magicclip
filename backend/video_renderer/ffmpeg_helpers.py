from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)
DEFAULT_CROP_GRAPH = "crop=ih*(9/16):ih,scale=1080:1920"


def run_ffmpeg(args: list[str]) -> tuple[int, str, str]:
    cmd = ["ffmpeg", "-y", *args]
    logger.info("ffmpeg_command", extra={"cmd": " ".join(cmd)})
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def ffprobe_video_info(input_file: str) -> dict:
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,r_frame_rate",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                input_file,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return {"width": 0, "height": 0, "duration": 0.0}
    if proc.returncode != 0:
        return {"width": 0, "height": 0, "duration": 0.0}
    payload = json.loads(proc.stdout or "{}")
    stream = (payload.get("streams") or [{}])[0]
    fmt = payload.get("format") or {}
    return {
        "width": int(stream.get("width", 0) or 0),
        "height": int(stream.get("height", 0) or 0),
        "duration": float(fmt.get("duration", 0.0) or 0.0),
        "r_frame_rate": stream.get("r_frame_rate", "0/1"),
    }


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def ass_to_ffmpeg_filter(subtitle_file: str) -> str:
    safe = subtitle_file.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")
    return f"ass='{safe}'"


def build_filter_chain(crop_filter: str, subtitle_file: str | None = None) -> str:
    chain = crop_filter
    if subtitle_file:
        chain = f"{chain},{ass_to_ffmpeg_filter(subtitle_file)}"
    return chain


def cut_and_crop(input_file: str, start: float, end: float, crop_graph: str, raw_clip: str) -> tuple[int, str, str]:
    ensure_parent(raw_clip)
    vf = crop_graph or DEFAULT_CROP_GRAPH
    args = [
        "-ss",
        str(start),
        "-to",
        str(end),
        "-i",
        input_file,
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        raw_clip,
    ]
    return run_ffmpeg(args)


def overlay_subtitles(raw_clip: str, ass_subtitle: str, subtitled_clip: str) -> tuple[int, str, str]:
    ensure_parent(subtitled_clip)
    filter_complex = ass_to_ffmpeg_filter(ass_subtitle)
    args = [
        "-i",
        raw_clip,
        "-filter_complex",
        filter_complex,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        subtitled_clip,
    ]
    return run_ffmpeg(args)


def encode_for_tiktok(subtitled_clip: str, output_file: str, crf: int = 20) -> tuple[int, str, str]:
    ensure_parent(output_file)
    bounded_crf = str(max(18, min(21, int(crf))))
    args = [
        "-i",
        subtitled_clip,
        "-vf",
        "scale=1080:1920:flags=lanczos,setsar=1",
        "-c:v",
        "libx264",
        "-crf",
        bounded_crf,
        "-preset",
        "medium",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        output_file,
    ]
    return run_ffmpeg(args)
