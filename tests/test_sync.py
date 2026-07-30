import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from aiokafka import AIOKafkaProducer
from app.main import app
from app.core.config import settings

@pytest.mark.anyio
async def test_kafka_neo4j_sync() -> None:
    # Use TestClient as context manager to invoke app lifespan (starts/stops consumer worker)
    with TestClient(app) as client:
        # Initialize producer inside test to publish mock events to local Kafka broker
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        
        test_conv_id = "test-conv-12345"
        test_parent_id = "test-parent-12345"
        
        # 1. Publish conversation.created event
        event_created = {
            "event_id": "event-1",
            "event_version": 1,
            "conversation_id": test_conv_id,
            "parent_conversation_id": test_parent_id,
            "conversation_status": "ACTIVE",
            "user_id": "user-123",
            "created_at": "2026-07-30T10:00:00Z",
            "trace_id": "trace-1",
            "correlation_id": "corr-1"
        }
        
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            json.dumps(event_created).encode("utf-8")
        )
        
        # Give the background consumer worker a moment to poll and write to Neo4j
        await asyncio.sleep(3.0)
        
        # Verify node creation and property synchronization
        response_child = client.get(f"/graph/conversations/{test_conv_id}")
        assert response_child.status_code == 200
        data_child = response_child.json()
        assert data_child["conversation_id"] == test_conv_id
        assert data_child["user_id"] == "user-123"
        assert data_child["status"] == "ACTIVE"
        
        # Verify parent stub node auto-creation
        response_parent = client.get(f"/graph/conversations/{test_parent_id}")
        assert response_parent.status_code == 200
        data_parent = response_parent.json()
        assert data_parent["conversation_id"] == test_parent_id
        
        # Verify relationships (CREATED_FROM relationship)
        response_parent_rel = client.get(f"/graph/conversations/{test_conv_id}/parent")
        assert response_parent_rel.status_code == 200
        assert response_parent_rel.json()["conversation_id"] == test_parent_id

        # 2. Publish conversation.deleted event
        event_deleted = {
            "event_id": "event-2",
            "event_version": 1,
            "conversation_id": test_conv_id,
            "user_id": "user-123",
            "deleted_at": "2026-07-30T10:05:00Z",
            "trace_id": "trace-2",
            "correlation_id": "corr-2"
        }
        
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            json.dumps(event_deleted).encode("utf-8")
        )
        
        # Give background consumer worker time to execute deletion query
        await asyncio.sleep(3.0)
        
        # Verify child node is removed from graph database
        response_child_post_delete = client.get(f"/graph/conversations/{test_conv_id}")
        assert response_child_post_delete.status_code == 404
        
        await producer.stop()
