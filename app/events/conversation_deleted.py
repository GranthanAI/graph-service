from pydantic import BaseModel

class ConversationDeletedEvent(BaseModel):
    """
    Schema for the consumed conversation.deleted Kafka event.
    """
    event_id: str
    event_version: int
    conversation_id: str
    user_id: str
    deleted_at: str
    trace_id: str
    correlation_id: str
