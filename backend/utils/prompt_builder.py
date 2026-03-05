def build_clip_prompt(transcript_segments):
    transcript_text = ""

    for seg in transcript_segments:
        transcript_text += f"[{seg.segment_id}] ({seg.start}-{seg.end}) {seg.text}\n"

    prompt = f"""
You are an expert short-form content strategist.

From the transcript below, select high-quality short-form clip candidates.
Generate maximum 5 clips only.

Rules:
- Each clip must be 20-60 seconds long
- Must feel complete
- Strong hook within first 3 seconds
- Avoid incomplete thoughts

For each clip:
- Generate a clickbait-style title (max 60 characters)
- Generate a 2-3 sentence insight-based summary (under 250 characters)
- Give a score (0-100)

Return JSON only:

[
  {{
    "s": float,
    "e": float,
    "score": int,
    "t": "",
    "summary": ""
  }}
]

Transcript:
{transcript_text}

IMPORTANT:
If the output may exceed limits, shorten all strings.
Ensure the final character is }}.
"""
    return prompt
