from __future__ import annotations

import re
from typing import Iterable

EMOTION_WORDS = {
    "amazing", "shocking", "crazy", "insane", "love", "hate", "fear", "wow", "unbelievable", "urgent",
    "warning", "secret", "pain", "excited", "angry", "happy", "sad", "best", "worst", "never",
}
HOOK_PHRASES = {"imagine", "what if", "here is", "listen", "stop", "the truth", "today", "you need"}
PUNCHLINE_MARKERS = {"so", "therefore", "that is why", "in short", "bottom line", "the point"}
KEYWORD_RE = re.compile(r"[A-Za-z0-9']+")


def _normalize(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return max(0.0, min(1.0, value / max_value))


def _tokenize(text: str) -> list[str]:
    return [w.lower() for w in KEYWORD_RE.findall(text)]


def _sentence_chunks(words: Iterable[dict]) -> list[list[dict]]:
    chunks: list[list[dict]] = []
    current: list[dict] = []
    for word in words:
        if word.get("type") != "word":
            continue
        current.append(word)
        token = str(word.get("word", ""))
        if any(p in token for p in (".", "?", "!")):
            chunks.append(current)
            current = []
    if current:
        chunks.append(current)
    return chunks


def semantic_segment(words: list[dict]) -> list[dict]:
    sentences = _sentence_chunks(words)
    if not sentences:
        return []

    segments: list[dict] = []
    current = [sentences[0]]
    previous_len = len(sentences[0])

    def flush(segment_sentences: list[list[dict]], segment_id: int) -> dict:
        flat = [w for s in segment_sentences for w in s]
        text = " ".join(str(w.get("word", "")) for w in flat).strip()
        tokens = _tokenize(text)
        emotion_hits = sum(1 for t in tokens if t in EMOTION_WORDS)
        hook_hits = sum(1 for phrase in HOOK_PHRASES if phrase in text.lower())
        punchline_hits = sum(1 for phrase in PUNCHLINE_MARKERS if phrase in text.lower())
        topic_score = _normalize(len(set(tokens)), max(8, len(tokens)))
        emotion_score = min(1.0, _normalize(emotion_hits, 4) + _normalize(punchline_hits, 3) * 0.2)
        hook_score = min(1.0, _normalize(hook_hits, 3) + (0.25 if "?" in text else 0.0) + _normalize(punchline_hits, 4) * 0.2)
        return {
            "segment_id": segment_id,
            "start": float(flat[0].get("startTime", 0.0)),
            "end": float(flat[-1].get("endTime", 0.0)),
            "text": text,
            "topic_score": round(topic_score, 3),
            "emotion_score": round(emotion_score, 3),
            "hook_score": round(hook_score, 3),
        }

    segment_id = 1
    for sentence in sentences[1:]:
        sentence_text = " ".join(str(w.get("word", "")) for w in sentence)
        sentence_tokens = set(_tokenize(sentence_text))
        prev_tokens = set(_tokenize(" ".join(str(w.get("word", "")) for w in current[-1])))
        jaccard = len(sentence_tokens & prev_tokens) / max(1, len(sentence_tokens | prev_tokens))
        length_delta = abs(len(sentence) - previous_len) / max(1, previous_len)
        boundary = jaccard < 0.18 or length_delta > 0.75 or any(k in sentence_text.lower() for k in ("however", "anyway", "but", "meanwhile"))
        if boundary and len(current) >= 1:
            segments.append(flush(current, segment_id))
            segment_id += 1
            current = [sentence]
        else:
            current.append(sentence)
        previous_len = len(sentence)

    if current:
        segments.append(flush(current, segment_id))
    return segments
