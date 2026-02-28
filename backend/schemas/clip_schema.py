from pydantic import BaseModel
from typing import List

class TranscriptSegment(BaseModel):
    segment_id: int
    start: float
    end: float
    text: str

class ClipRequest(BaseModel):
    transcript: List[TranscriptSegment]

class ClipSuggestion(BaseModel):
    start: float
    end: float
    score: int
    title: str
    summary: str

class ClipResponse(BaseModel):
    clips: List[ClipSuggestion]