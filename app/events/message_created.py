"""
Canonical schema for the chat.message.created Kafka event
produced by Conversation Service and consumed by Graph Service.

Envelope shape (frozen at v1):
    {
        "event_id":       str,
        "event_type":     "chat.message.created",
        "event_version":  1,
        "occurred_at":    str (ISO-8601),
        "source_service": "conversation-service",
        "correlation_id": str,
        "causation_id":   str,
        "payload": {
            "conversation_id": str,
            "message_id":      str,
            "role":            str,   # "user" | "assistant"
            "content":         str,
            "user_id":         str | None
        }
    }
"""

from pydantic import BaseModel
from typing import Optional


class MessageCreatedPayload(BaseModel):
    """Inner domain payload for chat.message.created."""
    conversation_id: str
    message_id:      str
    role:            str            # "user" | "assistant"
    content:         str
    user_id:         Optional[str] = None
    # Optional: present only on regeneration requests
    prompt_content:  Optional[str] = None


class MessageCreatedEnvelope(BaseModel):
    """
    Canonical versioned envelope for the chat.message.created event.
    Produced by: Conversation Service (via Cassandra Outbox → Kafka).
    Consumed by: Graph Service.
    """
    event_id:       str
    event_type:     str
    event_version:  int = 1
    occurred_at:    Optional[str] = None
    source_service: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id:   Optional[str] = None
    payload:        MessageCreatedPayload
