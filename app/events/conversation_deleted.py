from pydantic import BaseModel
from typing import Optional

class ConversationDeletedEvent(BaseModel):
    """
    Schema for the consumed conversation.deleted Kafka event.
    """
    conversation_id: str
    user_id: Optional[str] = "unknown_user"
    deleted_at: Optional[str] = None
    event_id: Optional[str] = None
    event_version: Optional[int] = None
    trace_id: Optional[str] = None
    correlation_id: Optional[str] = None

