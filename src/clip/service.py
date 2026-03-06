import json
from typing import Any

from openai import OpenAI

from src.config import settings


def _client() -> OpenAI:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is required")
    return OpenAI(api_key=settings.openai_api_key)


def suggest_clips(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not segments:
        raise RuntimeError("No segments available for clip suggestion")
    prompt = (
        "From transcript segments, return JSON with key clips. "
        "Each clip: start (number), end (number), score (0-100), title (string). "
        "Return max 3 clips.\n"
        f"Segments:\n{json.dumps(segments, ensure_ascii=False)}"
    )
    response = _client().chat.completions.create(
        model=settings.clip_model,
        response_format={"type": "json_object"},
        temperature=0,
        messages=[
            {"role": "system", "content": "You select short-form video clips."},
            {"role": "user", "content": prompt},
        ],
    )
    content = response.choices[0].message.content or "{}"
    payload = json.loads(content)
    clips = payload.get("clips")
    if not isinstance(clips, list) or not clips:
        raise RuntimeError("Clip suggestion returned no clips")
    result: list[dict[str, Any]] = []
    for clip in clips:
        start = float(clip["start"])
        end = float(clip["end"])
        if end <= start:
            continue
        result.append(
            {
                "start": start,
                "end": end,
                "score": int(clip.get("score", 0)),
                "title": str(clip.get("title", "Clip")),
            }
        )
    if not result:
        raise RuntimeError("No valid clip candidate from suggestion engine")
    result.sort(key=lambda item: item["score"], reverse=True)
    return result

