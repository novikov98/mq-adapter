from datetime import datetime

from pydantic import BaseModel


class Payload(BaseModel):
    pass


class Envelope(BaseModel):
    message_id: str
    event_type: str
    event_action: str
    event_version: float
    source_system: str
    destination_system: str
    occurred_at: datetime
    sent_at: datetime
    correlation_id: str | None
    payload: Payload
