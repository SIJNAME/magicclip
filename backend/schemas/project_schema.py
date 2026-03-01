from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class CreateYoutubeProjectRequest(BaseModel):
    url: str
    name: Optional[str] = None


class ClipItem(BaseModel):
    id: str
    projectId: str
    start: float
    end: float
    score: int
    title: str
    summary: str
    createdAt: datetime


class ExportCreateRequest(BaseModel):
    clipId: Optional[str] = None
    format: Literal["json", "mp4"] = "json"
    outputPath: Optional[str] = None


class ExportItem(BaseModel):
    id: str
    projectId: str
    clipId: Optional[str] = None
    status: Literal["queued", "completed", "failed"]
    format: str
    outputPath: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime


class ProjectItem(BaseModel):
    id: str
    name: str
    sourceType: Literal["upload", "youtube"]
    sourceUrl: Optional[str] = None
    inputFile: Optional[str] = None
    videoFile: Optional[str] = None
    audioFile: Optional[str] = None
    status: Literal["processing", "ready", "failed"]
    transcriptWordCount: int = Field(ge=0)
    createdAt: datetime
    updatedAt: datetime


class ProjectDetail(ProjectItem):
    clips: List[ClipItem]
    exports: List[ExportItem]
