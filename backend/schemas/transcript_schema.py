from pydantic import BaseModel
from typing import Literal, Optional

class RawWord(BaseModel):
    word: str
    start: float
    end: float
    confidence: Optional[float] = 1.0

class EnrichedWord(BaseModel):
    id: int
    pcId: int
    word: str
    startTime: float
    endTime: float
    type: Literal["word", "silence", "punctuation"]
    speaker: Optional[str] = None
    important: bool
    emoji: Optional[str] = None
    clipDisabled: bool
