from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from pathlib import Path

from backend.video_renderer.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class CaptionStyle:
    font_name: str = "Arial"
    font_size: int = CONFIG.caption_fontsize
    primary_color: str = "&H00FFFFFF"  # white
    accent_color: str = "&H0048D1FF"  # orange-ish BGR for ASS
    outline_color: str = "&H00000000"
    back_color: str = "&H5A000000"
    margin_v: int = CONFIG.caption_margin_v
    bold: int = 1


EMOJI_MAP = {
    "wow": "😮",
    "insane": "🤯",
    "crazy": "🔥",
    "love": "❤️",
    "win": "🏆",
    "secret": "🤫",
    "money": "💸",
}


def _fmt_ass_time(seconds: float) -> str:
    total = max(0.0, float(seconds))
    h = int(total // 3600)
    m = int((total % 3600) // 60)
    s = total % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def _sanitize_ass_text(text: str) -> str:
    return text.replace("{", "(").replace("}", ")").replace("\n", " ").strip()


def _decorate_word(word: str) -> str:
    clean = _sanitize_ass_text(word)
    emoji = EMOJI_MAP.get(clean.lower().strip(".,!?"))
    return f"{clean} {emoji}" if emoji else clean


def _chunk_words(words: list[dict], start: float, end: float, chunk_size: int = 4) -> list[list[dict]]:
    scoped = []
    for w in words:
        ws = float(w.get("startTime", 0.0))
        we = float(w.get("endTime", ws + 0.3))
        if we < start or ws > end:
            continue
        scoped.append(w)

    chunks: list[list[dict]] = []
    current: list[dict] = []
    for w in scoped:
        current.append(w)
        token = str(w.get("word", ""))
        if len(current) >= chunk_size or any(p in token for p in (".", "!", "?", ",")):
            chunks.append(current)
            current = []
    if current:
        chunks.append(current)
    return chunks


def _karaoke_event_text(chunk: list[dict], start: float) -> str:
    parts = []
    for w in chunk:
        ws = float(w.get("startTime", 0.0))
        we = float(w.get("endTime", ws + 0.25))
        dur_cs = max(1, int(round((we - ws) * 100)))
        parts.append(f"{{\\k{dur_cs}\\c&H00FFFFFF&}}{_decorate_word(str(w.get('word', '')))}")
    return " ".join(parts)


def render_ass_subtitles(
    words: list[dict],
    *,
    start: float,
    end: float,
    output_ass_path: str,
    style: CaptionStyle | None = None,
) -> str:
    started = time.time()
    style = style or CaptionStyle()
    Path(output_ass_path).parent.mkdir(parents=True, exist_ok=True)

    header = [
        "[Script Info]",
        "ScriptType: v4.00+",
        "PlayResX: 1080",
        "PlayResY: 1920",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
        (
            f"Style: Default,{style.font_name},{style.font_size},{style.primary_color},{style.accent_color},"
            f"{style.outline_color},{style.back_color},{style.bold},0,0,0,100,100,0,0,1,3,0,2,80,80,{style.margin_v},1"
        ),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    events = []
    chunks = _chunk_words(words, start, end)
    for chunk in chunks:
        ws = max(start, float(chunk[0].get("startTime", start)))
        we = min(end, float(chunk[-1].get("endTime", ws + 0.4)))
        rel_start = max(0.0, ws - start)
        rel_end = max(rel_start + 0.05, we - start)
        text = _karaoke_event_text(chunk, start)
        events.append(
            f"Dialogue: 0,{_fmt_ass_time(rel_start)},{_fmt_ass_time(rel_end)},Default,,0,0,0,,{text}"
        )

    Path(output_ass_path).write_text("\n".join(header + events), encoding="utf-8")
    logger.info("subtitle_creation", extra={"output_ass": output_ass_path, "event_count": len(events), "subtitle_creation_time": round(time.time()-started, 3)})
    return output_ass_path


def build_ass_captions(words: list[dict], start: float, end: float, output_ass_path: str) -> str:
    # Backward-compatible wrapper
    return render_ass_subtitles(words, start=start, end=end, output_ass_path=output_ass_path)
