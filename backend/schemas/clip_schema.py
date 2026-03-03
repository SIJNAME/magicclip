from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


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


class ClipPerformanceCreateRequest(BaseModel):
    avg_watch_time: float = Field(ge=0)
    completion_rate: float = Field(ge=0, le=1)
    rewatch_rate: float = Field(ge=0, le=1)


class ClipPerformanceItem(BaseModel):
    id: str
    clipId: str
    projectId: str
    avgWatchTime: float
    completionRate: float
    rewatchRate: float
    engagementScore: float
    createdAt: datetime
