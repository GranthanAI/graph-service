# Background worker to execute Kafka consumer loops
import logging
import asyncio

logger = logging.getLogger(__name__)

async def run_consumer_worker() -> None:
    """
    Asynchronous loop for polling and processing messages.
    """
    logger.info("Kafka consumer worker starting...")
    while True:
        try:
            await asyncio.sleep(3600)  # Keep running indefinitely
        except asyncio.CancelledError:
            logger.info("Kafka consumer worker stopped.")
            break
