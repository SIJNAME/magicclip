from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RenderCreateRequest(BaseModel):
    project_id: str
    clip_id: str
    start: float = Field(ge=0)
    end: float = Field(gt=0)
    callback_url: str | None = None


class VideoRenderJobResponse(BaseModel):
    id: str
    project_id: str
    clip_id: str
    input_file: str
    output_file: str
    output_url: str | None = None
    start: float
    end: float
    mp4_options: dict[str, Any]
    status: str
    logs: str
    progress: float
    eta_seconds: float
    retries: int
    max_retries: int
    render_time_sec: float | None = None
    output_file_size: int | None = None
    encoding_params: dict[str, Any]
    encoding_profile: str
    callback_url: str | None = None
    created_at: datetime
    updated_at: datetime
