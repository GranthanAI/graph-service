import logging
import asyncio
import json
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.services.graph_service import GraphService

logger = logging.getLogger(__name__)

class KafkaConsumerWorker:
    """
    Background worker running the Kafka consumer loop to synchronize 
    incoming events into the graph database.
    """
    def __init__(self, graph_service: GraphService) -> None:
        self.graph_service = graph_service
        self.consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background consumer worker task."""
        if self._running:
            return
        
        logger.info(f"Starting Kafka consumer worker with group ID: {settings.KAFKA_CONSUMER_GROUP_ID}...")
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            settings.KAFKA_CONVERSATION_UPDATED_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False  # Manual commit for at-least-once semantics
        )
        try:
            await self.consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info("Kafka consumer worker successfully started.")
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer worker: {e}")
            raise e

    async def stop(self) -> None:
        """Stop the background consumer worker task."""
        if not self._running:
            return
        
        logger.info("Stopping Kafka consumer worker...")
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        logger.info("Kafka consumer worker successfully stopped.")

    async def _consume_loop(self) -> None:
        """Main polling and event processing loop."""
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                try:
                    payload = json.loads(msg.value.decode("utf-8"))
                    
                    if msg.topic == settings.KAFKA_CONVERSATION_CREATED_TOPIC:
                        await self.graph_service.process_conversation_created(payload)
                    elif msg.topic == settings.KAFKA_CONVERSATION_DELETED_TOPIC:
                        await self.graph_service.process_conversation_deleted(payload)
                    elif msg.topic == settings.KAFKA_CONVERSATION_UPDATED_TOPIC:
                        await self.graph_service.process_conversation_updated(payload)
                    
                    # Manually commit offset after processing successfully
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(
                        f"Error processing message from topic {msg.topic} at offset {msg.offset}: {e}",
                        exc_info=True
                    )
                    # For production: here is where we would send messages to a DLQ
        except asyncio.CancelledError:
            logger.debug("Consume loop task cancelled.")
        except Exception as e:
            logger.critical(f"Unhandled exception in Kafka consumer loop: {e}", exc_info=True)

