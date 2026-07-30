"""
Canonical schema for the conversation.deleted Kafka event
produced by Conversation Service and consumed by Graph Service.
"""

from pydantic import BaseModel
from typing import Optional


class ConversationDeletedPayload(BaseModel):
    """Inner domain payload for conversation.deleted."""
    conversation_id: str
    user_id:         Optional[str] = None
    deleted_at:      Optional[str] = None


class ConversationDeletedEvent(BaseModel):
    """
    Canonical versioned envelope for the conversation.deleted event.
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
    payload:        Optional[ConversationDeletedPayload] = None

    # Legacy flat fields
    conversation_id: Optional[str] = None
    user_id:         Optional[str] = None
    deleted_at:      Optional[str] = None

    def get_payload(self) -> ConversationDeletedPayload:
        """Normalises canonical and legacy event formats."""
        if self.payload:
            return self.payload
        return ConversationDeletedPayload(
            conversation_id=self.conversation_id or "",
            user_id=self.user_id,
            deleted_at=self.deleted_at,
        )
