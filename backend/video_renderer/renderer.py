from __future__ import annotations

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Any

from video_renderer.caption_engine import render_ass_subtitles
from video_renderer.ffmpeg_helpers import cut_and_crop, encode_for_tiktok, ffprobe_video_info, overlay_subtitles
from video_renderer.smart_crop import build_smart_crop_filter

logger = logging.getLogger(__name__)


def render_clip(
    *,
    input_file: str,
    output_file: str,
    start: float,
    end: float,
    words: list[dict] | None = None,
    mp4_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.time()
    crop_filter = build_smart_crop_filter(input_file)

    logs: dict[str, Any] = {"crop_decisions": {"filter": crop_filter}, "ffmpeg_commands": []}

    with tempfile.TemporaryDirectory(prefix="magicclip_render_") as td:
        raw_clip = str(Path(td) / "raw_clip.mp4")
        subtitled_clip = str(Path(td) / "subtitled_clip.mp4")

        code, out, err = cut_and_crop(input_file, start, end, crop_filter, raw_clip)
        logs["ffmpeg_commands"].append("cut_and_crop")
        logs["cut_and_crop"] = {"code": code, "stderr": err[-500:]}
        if code != 0:
            return {
                "status": "failed",
                "output_file": output_file,
                "duration_sec": round(time.time() - started, 3),
                "log": json.dumps(logs),
            }

        stage_input = raw_clip
        if words:
            subtitle_file = str(Path(td) / "captions.ass")
            subtitle_started = time.time()
            render_ass_subtitles(words, start=start, end=end, output_ass_path=subtitle_file)
            logs["subtitle_creation_time"] = round(time.time() - subtitle_started, 3)
            code, out, err = overlay_subtitles(raw_clip, subtitle_file, subtitled_clip)
            logs["ffmpeg_commands"].append("overlay_subtitles")
            logs["overlay_subtitles"] = {"code": code, "stderr": err[-500:]}
            if code != 0:
                return {
                    "status": "failed",
                    "output_file": output_file,
                    "duration_sec": round(time.time() - started, 3),
                    "log": json.dumps(logs),
                }
            stage_input = subtitled_clip

        crf = int((mp4_options or {}).get("crf", 20))
        code, out, err = encode_for_tiktok(stage_input, output_file, crf=crf)
        logs["ffmpeg_commands"].append("encode_for_tiktok")
        logs["encode_for_tiktok"] = {"code": code, "stderr": err[-1000:]}

    ok = code == 0
    render_duration = round(time.time() - started, 3)
    output_info = ffprobe_video_info(output_file) if ok else {"width": 0, "height": 0}
    logs["total_render_duration"] = render_duration
    logs["output_resolution"] = [output_info.get("width", 0), output_info.get("height", 0)]

    logger.info(
        "render_complete",
        extra={
            "output_file": output_file,
            "total_render_duration": render_duration,
            "output_resolution": logs["output_resolution"],
            "crop_decisions": logs.get("crop_decisions"),
        },
    )

    return {
        "status": "completed" if ok else "failed",
        "output_file": output_file,
        "duration_sec": render_duration,
        "log": json.dumps(logs),
    }
