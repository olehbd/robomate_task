from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any


class EventSchema(BaseModel):
    event_id: str = Field(min_length=5)
    occurred_at: datetime
    user_id: int
    event_type: str
    properties: Dict[str, Any]
