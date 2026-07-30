from pydantic import BaseModel
from typing import Optional

class ConversationUpdatedEvent(BaseModel):
    """
    Schema for the consumed conversation.updated Kafka event.
    """
    conversation_id: str
    title: Optional[str] = None
    status: Optional[str] = None
    updated_at: Optional[str] = None
    user_id: Optional[str] = None
    event_id: Optional[str] = None
    event_version: Optional[int] = None
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None
