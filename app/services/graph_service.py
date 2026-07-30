import logging
import asyncio
from app.repositories.graph_repository import BaseGraphRepository
from app.events.conversation_created import ConversationCreatedEvent
from app.events.conversation_deleted import ConversationDeletedEvent

logger = logging.getLogger(__name__)

class GraphService:
    """
    Business logic layer orchestrating repository queries and handling Kafka events.
    """
    def __init__(self, repo: BaseGraphRepository) -> None:
        self.repo = repo

    async def process_conversation_created(self, event_data: dict) -> None:
        logger.info(f"Processing conversation.created event: {event_data.get('conversation_id')}")
        
        # Handle nested payload format
        if "payload" in event_data and isinstance(event_data["payload"], dict):
            payload = event_data["payload"]
            for k, v in payload.items():
                if k not in event_data:
                    event_data[k] = v

        if "status" in event_data and "conversation_status" not in event_data:
            event_data["conversation_status"] = event_data["status"]
            
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        if not event_data.get("created_at"):
            event_data["created_at"] = now_iso

        event = ConversationCreatedEvent(**event_data)
        title = event.title or f"Conversation {event.conversation_id[:8]}"
        
        # Execute DB call in a separate thread to keep FastAPI event loop unblocked
        await asyncio.to_thread(
            self.repo.create_conversation_with_parent,
            conversation_id=event.conversation_id,
            user_id=event.user_id,
            title=title,
            status=event.conversation_status,
            created_at=event.created_at,
            updated_at=event.created_at,
            parent_id=event.parent_conversation_id
        )
        logger.info(f"Successfully synchronized conversation {event.conversation_id} in graph.")

    async def process_conversation_deleted(self, event_data: dict) -> None:
        logger.info(f"Processing conversation.deleted event: {event_data.get('conversation_id')}")
        
        # Handle nested payload format
        if "payload" in event_data and isinstance(event_data["payload"], dict):
            payload = event_data["payload"]
            for k, v in payload.items():
                if k not in event_data:
                    event_data[k] = v

        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        if not event_data.get("deleted_at"):
            event_data["deleted_at"] = now_iso

        event = ConversationDeletedEvent(**event_data)
        
        # Execute soft or hard delete based on config setting
        from app.core.config import settings
        if settings.SOFT_DELETE_ENABLED:
            await asyncio.to_thread(
                self.repo.soft_delete_conversation,
                conversation_id=event.conversation_id,
                updated_at=event.deleted_at
            )
            logger.info(f"Successfully soft-deleted conversation {event.conversation_id} in graph.")
        else:
            await asyncio.to_thread(
                self.repo.delete_conversation,
                conversation_id=event.conversation_id
            )
            logger.info(f"Successfully hard-deleted (removed) conversation {event.conversation_id} from graph.")

    async def process_conversation_updated(self, event_data: dict) -> None:
        logger.info(f"Processing conversation.updated event: {event_data.get('conversation_id')}")
        
        # Handle nested payload format
        if "payload" in event_data and isinstance(event_data["payload"], dict):
            payload = event_data["payload"]
            for k, v in payload.items():
                if k not in event_data:
                    event_data[k] = v

        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        if not event_data.get("updated_at"):
            event_data["updated_at"] = now_iso

        from app.events.conversation_updated import ConversationUpdatedEvent
        event = ConversationUpdatedEvent(**event_data)
        
        # Execute DB call in a separate thread to keep FastAPI event loop unblocked
        await asyncio.to_thread(
            self.repo.update_conversation_node,
            conversation_id=event.conversation_id,
            title=event.title,
            status=event.status,
            updated_at=event.updated_at
        )
        logger.info(f"Successfully updated conversation {event.conversation_id} properties in graph.")

    async def get_conversation(self, conversation_id: str) -> dict:
        """Retrieve node info for a specific conversation, raising NodeNotFoundError if missing."""
        from app.core.exceptions import NodeNotFoundError
        node = await asyncio.to_thread(self.repo.get_conversation, conversation_id)
        if not node:
            raise NodeNotFoundError(conversation_id)
        return node

    async def get_parent(self, conversation_id: str) -> dict:
        """Retrieve parent conversation, raising NodeNotFoundError if conversation or its parent is missing."""
        from app.core.exceptions import NodeNotFoundError
        await self.get_conversation(conversation_id)  # Validate node existence
        parent = await asyncio.to_thread(self.repo.get_parent, conversation_id)
        if not parent:
            raise NodeNotFoundError(f"Parent for conversation {conversation_id}")
        return parent

    async def get_children(self, conversation_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve immediate children conversations, validating conversation node existence."""
        await self.get_conversation(conversation_id)  # Validate node existence
        return await asyncio.to_thread(self.repo.get_children, conversation_id, skip=skip, limit=limit)

    async def get_ancestors(self, conversation_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve ancestors (complete parent chain), validating conversation node existence."""
        await self.get_conversation(conversation_id)  # Validate node existence
        return await asyncio.to_thread(self.repo.get_ancestors, conversation_id, skip=skip, limit=limit)

    async def get_descendants(self, conversation_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
        """Retrieve descendants subtree, validating conversation node existence."""
        await self.get_conversation(conversation_id)  # Validate node existence
        return await asyncio.to_thread(self.repo.get_descendants, conversation_id, skip=skip, limit=limit)


