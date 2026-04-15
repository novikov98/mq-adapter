from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel


class QueueConfig(BaseModel):
    queue_name: str
    exchange_name: str
    binding_key: str
    exchange_type: Literal["fanout", "topic", "direct"] = "topic"
    durable: bool = True
    auto_delete: bool = False
    dlq_name: str
    dlx_name: str
    dlx_routing_key: str
    prefetch_count: int
    message_ttl: int | None = None
    max_length: int | None = None
    max_priority: int | None = None


class Payload(BaseModel):
    pass


class Envelope(BaseModel):
    message_id: UUID
    event_type: str
    event_action: str
    event_version: float
    source_system: str
    destination_system: str
    occurred_at: datetime
    sent_at: datetime
    correlation_id: UUID | None = None
    payload: dict[str, Any]
