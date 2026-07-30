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
            updated_at=now_iso,
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

        event = ConversationDeletedEvent(**event_data)
        
        # Execute DB call in a separate thread to keep FastAPI event loop unblocked
        await asyncio.to_thread(
            self.repo.delete_conversation,
            conversation_id=event.conversation_id
        )
        logger.info(f"Successfully deleted/removed conversation {event.conversation_id} from graph.")

