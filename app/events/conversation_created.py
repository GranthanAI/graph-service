from pydantic import BaseModel
from typing import Optional

class ConversationCreatedEvent(BaseModel):
    """
    Schema for the consumed conversation.created Kafka event.
    """
    conversation_id: str
    user_id: Optional[str] = "unknown_user"
    title: Optional[str] = None
    parent_conversation_id: Optional[str] = None
    conversation_status: Optional[str] = "ACTIVE"
    created_at: Optional[str] = None
    event_id: Optional[str] = None
    event_version: Optional[int] = None
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None

