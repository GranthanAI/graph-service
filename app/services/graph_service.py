"""
Graph Service.
Business logic layer orchestrating repository queries and handling Kafka events.
All handlers unpack the canonical versioned event envelope before processing.
"""

import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

from app.repositories.graph_repository import BaseGraphRepository
from app.events.conversation_created import ConversationCreatedEvent
from app.events.conversation_deleted import ConversationDeletedEvent
from app.events.conversation_updated import ConversationUpdatedEvent
from app.events.message_created import MessageCreatedEnvelope
from app.core.exceptions import NodeNotFoundError

logger = logging.getLogger(__name__)


class GraphService:
    """
    Business logic layer orchestrating repository queries and handling Kafka events.
    """

    def __init__(self, repo: BaseGraphRepository) -> None:
        self.repo = repo

    # -------------------------------------------------------------------------
    # Idempotency helpers
    # -------------------------------------------------------------------------

    def is_event_processed(self, event_id: str) -> bool:
        """Returns True if this event_id was already fully handled."""
        return self.repo.is_event_processed(event_id)

    def mark_event_processed(self, event_id: str) -> None:
        """Permanently records this event_id as processed in Neo4j."""
        self.repo.mark_event_processed(event_id)

    async def get_message_count(self) -> int:
        """Retrieves total message count from the Neo4j repository."""
        return await asyncio.to_thread(self.repo.get_message_count)


    # -------------------------------------------------------------------------
    # Conversation event handlers
    # -------------------------------------------------------------------------

    async def process_conversation_created(self, event_data: dict) -> None:
        """
        Handles conversation.created events.
        Unpacks the canonical envelope, creates a Conversation node in Neo4j,
        and wires parent–child relationships if applicable.
        """
        event = ConversationCreatedEvent(**event_data)
        p = event.get_payload()
        correlation_id = event.correlation_id or ""

        logger.info(
            "Processing conversation.created",
            extra={"conversation_id": p.conversation_id, "correlation_id": correlation_id},
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        created_at = p.created_at or now_iso
        title = p.title or f"Conversation {p.conversation_id[:8]}"

        await asyncio.to_thread(
            self.repo.create_conversation_with_parent,
            conversation_id=p.conversation_id,
            user_id=p.user_id or "unknown_user",
            title=title,
            status=p.status or "ACTIVE",
            created_at=created_at,
            updated_at=created_at,
            parent_id=p.parent_conversation_id,
        )
        logger.info(
            "Conversation node synchronised",
            extra={"conversation_id": p.conversation_id, "correlation_id": correlation_id},
        )

    async def process_conversation_deleted(self, event_data: dict) -> None:
        """
        Handles conversation.deleted events.
        Soft-deletes or hard-deletes the Conversation node per config.
        """
        event = ConversationDeletedEvent(**event_data)
        p = event.get_payload()
        correlation_id = event.correlation_id or ""

        logger.info(
            "Processing conversation.deleted",
            extra={"conversation_id": p.conversation_id, "correlation_id": correlation_id},
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        deleted_at = p.deleted_at or now_iso

        from app.core.config import settings
        if settings.SOFT_DELETE_ENABLED:
            await asyncio.to_thread(
                self.repo.soft_delete_conversation,
                conversation_id=p.conversation_id,
                updated_at=deleted_at,
            )
            logger.info("Conversation soft-deleted", extra={"conversation_id": p.conversation_id})
        else:
            await asyncio.to_thread(
                self.repo.delete_conversation,
                conversation_id=p.conversation_id,
            )
            logger.info("Conversation hard-deleted", extra={"conversation_id": p.conversation_id})

    async def process_conversation_updated(self, event_data: dict) -> None:
        """
        Handles conversation.updated events.
        Updates mutable properties (title, status) on the Conversation node.
        """
        event = ConversationUpdatedEvent(**event_data)
        p = event.get_payload()
        correlation_id = event.correlation_id or ""

        logger.info(
            "Processing conversation.updated",
            extra={"conversation_id": p.conversation_id, "correlation_id": correlation_id},
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        updated_at = p.updated_at or event.occurred_at or now_iso

        await asyncio.to_thread(
            self.repo.update_conversation_node,
            conversation_id=p.conversation_id,
            title=p.title,
            status=p.status,
            updated_at=updated_at,
        )
        logger.info("Conversation node updated", extra={"conversation_id": p.conversation_id})

    @staticmethod
    def _normalize_message_event(event_data: dict) -> dict:
        """Accept both wire shapes of chat.message.created.

        Enveloped (original contract): {event_id, ..., payload: {...}}.
        Flat (what conversation-service publishes today for llm-service):
        {message_id, conversation_id, user_id, content, timestamp, ...}. Flat
        events are user messages (assistant output travels on
        chat.response.generated) and carry no event_id; the Message write is a
        MERGE on message_id, so redelivery is idempotent either way.
        """
        if "payload" in event_data:
            return event_data
        trace = event_data.get("trace_context") or {}
        return {
            "event_id": event_data.get("event_id") or f"msg-{event_data.get('message_id', '')}",
            "event_type": event_data.get("event_type", "chat.message.created"),
            "occurred_at": event_data.get("timestamp"),
            "source_service": "conversation-service",
            "correlation_id": trace.get("traceparent") if isinstance(trace, dict) else None,
            "payload": {
                "conversation_id": event_data.get("conversation_id"),
                "message_id": event_data.get("message_id"),
                "role": event_data.get("role", "user"),
                "content": event_data.get("content", ""),
                "user_id": event_data.get("user_id") or None,
            },
        }

    async def process_message_created(self, event_data: dict) -> None:
        """
        Handles chat.message.created events.

        Creates:
            (:Message) node with message content and metadata.
            (:User)-[:SENT]->(:Message)-[:BELONGS_TO]->(:Conversation) relationships.

        Graph Service is AI-agnostic — no entity extraction happens here.
        Entity extraction is deferred to a future LLM Service pipeline.
        """
        envelope = MessageCreatedEnvelope(**self._normalize_message_event(event_data))
        p = envelope.payload
        correlation_id = envelope.correlation_id or ""

        logger.info(
            "Processing chat.message.created",
            extra={
                "message_id": p.message_id,
                "conversation_id": p.conversation_id,
                "role": p.role,
                "correlation_id": correlation_id,
            },
        )

        occurred_at = envelope.occurred_at or datetime.now(timezone.utc).isoformat()
        # Use message sender as user_id fallback if no explicit user_id (e.g. assistant messages)
        user_id = p.user_id or f"agent:{p.role}"

        # 1. Merge the Message node
        await asyncio.to_thread(
            self.repo.create_message_node,
            message_id=p.message_id,
            conversation_id=p.conversation_id,
            user_id=user_id,
            role=p.role,
            content=p.content,
            occurred_at=occurred_at,
        )

        # 2. Wire relationships: (:User)-[:SENT]->(:Message)-[:BELONGS_TO]->(:Conversation)
        await asyncio.to_thread(
            self.repo.create_message_relationships,
            message_id=p.message_id,
            conversation_id=p.conversation_id,
            user_id=user_id,
        )

        logger.info(
            "Message node and relationships synchronised",
            extra={
                "message_id": p.message_id,
                "conversation_id": p.conversation_id,
                "correlation_id": correlation_id,
            },
        )

    # -------------------------------------------------------------------------
    # Query handlers (REST API)
    # -------------------------------------------------------------------------

    async def get_conversation(self, conversation_id: str, user_id: str) -> dict:
        """Retrieve a conversation node the caller owns.

        A missing node and a node owned by someone else raise the same
        NodeNotFoundError, so conversation IDs cannot be enumerated across users.
        """
        node = await asyncio.to_thread(self.repo.get_conversation, conversation_id)
        if not node or not user_id or node.get("user_id") != user_id:
            raise NodeNotFoundError(conversation_id)
        return node

    @staticmethod
    def _owned_by(nodes: list[dict], user_id: str) -> list[dict]:
        return [n for n in nodes if n.get("user_id") == user_id]

    async def get_parent(self, conversation_id: str, user_id: str) -> dict:
        """Retrieve the caller's parent conversation, raising NodeNotFoundError if missing or not owned."""
        await self.get_conversation(conversation_id, user_id)
        parent = await asyncio.to_thread(self.repo.get_parent, conversation_id)
        if not parent or parent.get("user_id") != user_id:
            raise NodeNotFoundError(f"Parent for conversation {conversation_id}")
        return parent

    async def get_children(self, conversation_id: str, user_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve immediate children the caller owns."""
        await self.get_conversation(conversation_id, user_id)
        children = await asyncio.to_thread(self.repo.get_children, conversation_id, skip=skip, limit=limit)
        return self._owned_by(children, user_id)

    async def get_ancestors(self, conversation_id: str, user_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve the caller-owned ancestors (parent chain)."""
        await self.get_conversation(conversation_id, user_id)
        ancestors = await asyncio.to_thread(self.repo.get_ancestors, conversation_id, skip=skip, limit=limit)
        return self._owned_by(ancestors, user_id)

    async def get_descendants(self, conversation_id: str, user_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve the caller-owned descendants subtree."""
        await self.get_conversation(conversation_id, user_id)
        descendants = await asyncio.to_thread(self.repo.get_descendants, conversation_id, skip=skip, limit=limit)
        return self._owned_by(descendants, user_id)

    async def get_memory_context(self, conversation_id: str, user_id: str) -> dict:
        """Retrieve lineage memory context for a conversation the caller owns."""
        await self.get_conversation(conversation_id, user_id)
        context = await asyncio.to_thread(self.repo.get_memory_context, conversation_id)
        if not context:
            raise NodeNotFoundError(conversation_id)
        return context
