from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class RenderConfig:
    output_width: int = _env_int("MC_RENDER_WIDTH", 1080)
    output_height: int = _env_int("MC_RENDER_HEIGHT", 1920)
    fps: int = _env_int("MC_RENDER_FPS", 30)
    video_codec: str = os.getenv("MC_RENDER_CODEC", "libx264")
    audio_codec: str = os.getenv("MC_RENDER_AUDIO_CODEC", "aac")
    preset: str = os.getenv("MC_RENDER_PRESET", "medium")
    crf: int = _env_int("MC_RENDER_CRF", 20)
    queue_poll_interval_sec: int = _env_int("MC_RENDER_POLL_SEC", 2)
    max_workers: int = _env_int("MC_RENDER_WORKERS", 2)
    caption_fontsize: int = _env_int("MC_CAPTION_FONTSIZE", 60)
    caption_margin_v: int = _env_int("MC_CAPTION_MARGIN_V", 180)
    tiktok_faststart: bool = True


CONFIG = RenderConfig()


def default_mp4_options() -> dict[str, str | int]:
    return {
        "vcodec": CONFIG.video_codec,
        "acodec": CONFIG.audio_codec,
        "crf": CONFIG.crf,
        "preset": CONFIG.preset,
        "movflags": "+faststart",
        "pix_fmt": "yuv420p",
        "profile:v": "high",
        "level": "4.1",
    }
