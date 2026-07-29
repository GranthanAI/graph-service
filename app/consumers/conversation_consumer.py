# Kafka consumer for conversation events (created, deleted)
import logging

logger = logging.getLogger(__name__)

class ConversationConsumer:
    """
    Kafka consumer class to poll and process conversation events.
    """
    def __init__(self) -> None:
        pass

    def start(self) -> None:
        logger.info("Starting conversation event consumer...")

    def stop(self) -> None:
        logger.info("Stopping conversation event consumer...")
