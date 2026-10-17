"""
Canonical schema for the conversation.updated Kafka event
produced by Conversation Service and consumed by Graph Service.
"""

from pydantic import BaseModel
from typing import Optional


class ConversationUpdatedPayload(BaseModel):
    """Inner domain payload for conversation.updated."""
    conversation_id: str
    user_id:         Optional[str] = None
    title:           Optional[str] = None
    status:          Optional[str] = None
    updated_at:      Optional[str] = None


class ConversationUpdatedEvent(BaseModel):
    """
    Canonical versioned envelope for the conversation.updated event.
    Produced by: Conversation Service (via Cassandra Outbox → Kafka).
    Consumed by: Graph Service.

    Supports both canonical nested format and legacy flat format.
    """
    event_id:       Optional[str] = None
    event_type:     Optional[str] = None
    event_version:  Optional[int] = None
    occurred_at:    Optional[str] = None
    source_service: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id:   Optional[str] = None
    payload:        Optional[ConversationUpdatedPayload] = None

    # Legacy flat fields
    conversation_id: Optional[str] = None
    user_id:         Optional[str] = None
    title:           Optional[str] = None
    status:          Optional[str] = None
    updated_at:      Optional[str] = None

    def get_payload(self) -> ConversationUpdatedPayload:
        """Normalises canonical and legacy event formats."""
        if self.payload:
            return self.payload
        return ConversationUpdatedPayload(
            conversation_id=self.conversation_id or "",
            user_id=self.user_id,
            title=self.title,
            status=self.status,
            updated_at=self.updated_at,
        )
