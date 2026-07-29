import logging
from app.repositories.graph_repository import BaseGraphRepository

logger = logging.getLogger(__name__)

class GraphService:
    """
    Business logic layer orchestrating repository queries and handling Kafka events.
    """
    def __init__(self, repo: BaseGraphRepository) -> None:
        self.repo = repo

    def process_conversation_created(self, event_data: dict) -> None:
        logger.info(f"Processing conversation.created event: {event_data}")
        # Will be implemented in Phase 2

    def process_conversation_deleted(self, event_data: dict) -> None:
        logger.info(f"Processing conversation.deleted event: {event_data}")
        # Will be implemented in Phase 2
