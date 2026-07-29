from pydantic import BaseModel

class ConversationCreatedEvent(BaseModel):
    """
    Schema for the consumed conversation.created Kafka event.
    """
    event_id: str
    event_version: int
    conversation_id: str
    parent_conversation_id: str | None = None
    conversation_status: str
    user_id: str
    created_at: str
    trace_id: str
    correlation_id: str
