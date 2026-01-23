from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


class FeedbackPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(..., description="Schema version")
    tracking_id: str
    trace_id: Optional[str] = None
    site_id: Optional[str] = None
    user_id: Optional[str] = None
    pims_id: Optional[str] = None
    pims: str
    feedback_boolean: Optional[bool] = None
    feedback_score_1: Optional[float] = None
    feedback_score_2: Optional[float] = None
    feedback_comment_1: Optional[str] = None
    feedback_comment_2: Optional[str] = None
    timestamp: datetime
    source_app: Optional[str] = None
    service_name: Optional[str] = None
    consumer_id: Optional[str] = None
    request_id: Optional[str] = None


class FeedbackResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    message: str
    feedback_id: Optional[str] = None
    validation_errors: Optional[List[str]] = None


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: str
    details: Optional[List[str]] = None
