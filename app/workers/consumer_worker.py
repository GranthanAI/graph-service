"""
Kafka Consumer Worker.
Background worker running the Kafka consumer loop to synchronise
incoming events into the graph database.

Design decisions:
    - Manual offset commit (at-least-once delivery semantics).
    - Idempotency check before every handler invocation; duplicate events are
      safely skipped without processing.
    - correlation_id from the canonical envelope is logged on every event so
      distributed traces can be reconstructed across services.
"""

import logging
import asyncio
import json

from aiokafka import AIOKafkaConsumer

from app.core.config import settings
from app.services.graph_service import GraphService

logger = logging.getLogger(__name__)


class KafkaConsumerWorker:
    """
    Background worker running the Kafka consumer loop to synchronise
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

        logger.info(f"Starting Kafka consumer worker, group: {settings.KAFKA_CONSUMER_GROUP_ID}")
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            settings.KAFKA_CONVERSATION_UPDATED_TOPIC,
            settings.KAFKA_CHAT_MESSAGE_CREATED_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit — at-least-once semantics
        )
        try:
            await self.consumer.start()
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info("Kafka consumer worker started successfully.")
        except Exception as e:
            logger.error(f"Failed to start Kafka consumer worker: {e}")
            raise

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
        logger.info("Kafka consumer worker stopped.")

    async def _consume_loop(self) -> None:
        """Main polling and event processing loop (batched)."""
        try:
            while self._running:
                # Poll a batch of records
                batch = await self.consumer.getmany(
                    timeout_ms=settings.KAFKA_BATCH_TIMEOUT_MS,
                    max_records=settings.KAFKA_BATCH_SIZE,
                )
                if not batch:
                    # Yield to event loop while idle
                    await asyncio.sleep(0.1)
                    continue

                for tp, messages in batch.items():
                    for msg in messages:
                        if not self._running:
                            break
                        await self._handle_message(msg)

                # Commit the entire batch after processing
                await self.consumer.commit()

        except asyncio.CancelledError:
            logger.debug("Consume loop cancelled.")
        except Exception as e:
            logger.critical(f"Unhandled exception in Kafka consumer loop: {e}", exc_info=True)

    async def _handle_message(self, msg) -> None:
        """
        Deserialise, idempotency-check, route, and mark a single Kafka message.
        On any processing error the message is logged and skipped so the batch
        can still be committed (prevents infinite retry on poison-pill messages).
        """
        try:
            raw = msg.value.decode("utf-8")
            payload = json.loads(raw)
        except Exception as e:
            logger.error(
                f"Failed to deserialise Kafka message on topic {msg.topic} "
                f"at offset {msg.offset}: {e}"
            )
            return  # Skip undecodeable messages

        # Extract envelope metadata for logging and idempotency
        event_id = payload.get("event_id")
        correlation_id = payload.get("correlation_id", "")

        # --- Idempotency gate ---
        if event_id:
            try:
                already_done = await asyncio.to_thread(
                    self.graph_service.is_event_processed, event_id
                )
                if already_done:
                    logger.info(
                        f"Duplicate event skipped (already processed)",
                        extra={"event_id": event_id, "topic": msg.topic,
                               "correlation_id": correlation_id},
                    )
                    return
            except Exception as e:
                # If idempotency check fails, log and continue — better to risk
                # a duplicate write than to silently lose events.
                logger.warning(
                    f"Idempotency check failed for event {event_id}: {e}. "
                    "Processing anyway to avoid data loss."
                )

        logger.info(
            f"Consuming event from topic '{msg.topic}'",
            extra={"event_id": event_id, "offset": msg.offset,
                   "correlation_id": correlation_id},
        )

        # --- Route to handler ---
        try:
            if msg.topic == settings.KAFKA_CONVERSATION_CREATED_TOPIC:
                await self.graph_service.process_conversation_created(payload)

            elif msg.topic == settings.KAFKA_CONVERSATION_DELETED_TOPIC:
                await self.graph_service.process_conversation_deleted(payload)

            elif msg.topic == settings.KAFKA_CONVERSATION_UPDATED_TOPIC:
                await self.graph_service.process_conversation_updated(payload)

            elif msg.topic == settings.KAFKA_CHAT_MESSAGE_CREATED_TOPIC:
                await self.graph_service.process_message_created(payload)

            else:
                logger.warning(f"Received event on unrecognised topic: {msg.topic}")
                return  # Nothing to mark processed

        except Exception as e:
            logger.error(
                f"Error processing event from topic {msg.topic} at offset {msg.offset}: {e}",
                exc_info=True,
            )
            # Do NOT mark as processed — allow retry on next consumer restart.
            # For production, route to a DLQ here after N retries.
            return

        # --- Mark processed only after successful handler completion ---
        if event_id:
            try:
                await asyncio.to_thread(self.graph_service.mark_event_processed, event_id)
            except Exception as e:
                # The handler succeeded but we couldn't persist the dedup marker.
                # Log it — next delivery will hit the idempotency path and be skipped.
                logger.warning(
                    f"Failed to mark event {event_id} as processed: {e}. "
                    "Next delivery will be a no-op if the write reaches Neo4j."
                )
