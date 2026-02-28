def build_clip_prompt(transcript_segments):
    transcript_text = ""

    for seg in transcript_segments:
        transcript_text += f"[{seg.segment_id}] ({seg.start}-{seg.end}) {seg.text}\n"

    prompt = f"""
You are an expert short-form content strategist.

From the transcript below, select 10–20 high-quality short-form clip candidates.

Rules:
- Each clip must be 20–60 seconds long
- Must feel complete
- Strong hook within first 3 seconds
- Avoid incomplete thoughts

For each clip:
- Generate a clickbait-style title (max 60 characters)
- Generate a 2–3 sentence insight-based summary (under 250 characters)
- Give a score (0-100)

Return JSON only:

[
  {{
    "start": float,
    "end": float,
    "score": int,
    "title": "",
    "summary": ""
  }}
]

Transcript:
{transcript_text}
"""
    return prompt