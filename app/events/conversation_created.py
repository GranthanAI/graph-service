"""
Canonical schema for the conversation.created Kafka event
produced by Conversation Service and consumed by Graph Service.
"""

from pydantic import BaseModel
from typing import Optional


class ConversationCreatedPayload(BaseModel):
    """Inner domain payload for conversation.created."""
    conversation_id:        str
    user_id:                Optional[str] = "unknown_user"
    title:                  Optional[str] = None
    status:                 Optional[str] = "ACTIVE"
    parent_conversation_id: Optional[str] = None
    created_at:             Optional[str] = None


class ConversationCreatedEvent(BaseModel):
    """
    Canonical versioned envelope for the conversation.created event.
    Produced by: Conversation Service (via Cassandra Outbox → Kafka).
    Consumed by: Graph Service.

    Supports both the legacy flat format and the new nested-payload format
    for backwards-compatible migration.
    """
    event_id:       Optional[str] = None
    event_type:     Optional[str] = None
    event_version:  Optional[int] = None
    occurred_at:    Optional[str] = None
    source_service: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id:   Optional[str] = None
    payload:        Optional[ConversationCreatedPayload] = None

    # Legacy flat fields — present when payload is None (old format)
    conversation_id:        Optional[str] = None
    user_id:                Optional[str] = None
    title:                  Optional[str] = None
    conversation_status:    Optional[str] = None
    status:                 Optional[str] = None
    parent_conversation_id: Optional[str] = None
    created_at:             Optional[str] = None

    def get_payload(self) -> ConversationCreatedPayload:
        """
        Returns a normalised ConversationCreatedPayload regardless of whether
        the event arrived in canonical (nested payload) or legacy (flat) format.
        """
        if self.payload:
            return self.payload
        # Fall back to flat fields for backwards compatibility
        status = self.conversation_status or self.status or "ACTIVE"
        return ConversationCreatedPayload(
            conversation_id=self.conversation_id or "",
            user_id=self.user_id or "unknown_user",
            title=self.title,
            status=status,
            parent_conversation_id=self.parent_conversation_id,
            created_at=self.created_at,
        )
