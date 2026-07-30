import asyncio
import json
import pytest
from fastapi.testclient import TestClient
from aiokafka import AIOKafkaProducer
from app.main import app
from app.core.config import settings

async def assert_eventually(client, path, assert_fn, timeout=10.0, interval=0.5):
    """Helper to poll an endpoint until assertions pass or timeout is reached."""
    start = asyncio.get_running_loop().time()
    last_ex = None
    while asyncio.get_running_loop().time() - start < timeout:
        try:
            resp = client.get(path)
            assert_fn(resp)
            return resp
        except AssertionError as e:
            last_ex = e
            await asyncio.sleep(interval)
    if last_ex:
        raise last_ex
    raise AssertionError(f"Timeout waiting for assertion on path: {path}")

@pytest.mark.anyio
async def test_kafka_neo4j_sync() -> None:
    # Isolate consumer group for tests to avoid partition sharing with active local dev server
    settings.KAFKA_CONSUMER_GROUP_ID = "graph-service-consumer-test-sync"
    # Use TestClient as context manager to invoke app lifespan (starts/stops consumer worker)
    with TestClient(app) as client:
        # Initialize producer inside test to publish mock events to local Kafka broker
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        
        import uuid
        test_conv_id = f"test-conv-{uuid.uuid4()}"
        test_parent_id = f"test-parent-{uuid.uuid4()}"
        
        # Clean up database first to ensure no dirty state from previous runs
        from app.api.dependencies import get_repository, get_db
        repo = get_repository(get_db())
        repo.delete_conversation(test_conv_id)
        repo.delete_conversation(test_parent_id)
        
        # 1. Publish conversation.created event
        event_created = {
            "event_id": str(uuid.uuid4()),
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
        
        # Verify node creation and property synchronization eventually
        def assert_created(r):
            assert r.status_code == 200
            data = r.json()
            assert data["conversation_id"] == test_conv_id
            assert data["user_id"] == "user-123"
            assert data["status"] == "ACTIVE"
        await assert_eventually(client, f"/graph/conversations/{test_conv_id}", assert_created)
        
        # Verify parent stub node auto-creation eventually
        def assert_parent_created(r):
            assert r.status_code == 200
            assert r.json()["conversation_id"] == test_parent_id
        await assert_eventually(client, f"/graph/conversations/{test_parent_id}", assert_parent_created)
        
        # Verify relationships (CREATED_FROM relationship) eventually
        def assert_parent_relationship(r):
            assert r.status_code == 200
            assert r.json()["conversation_id"] == test_parent_id
        await assert_eventually(client, f"/graph/conversations/{test_conv_id}/parent", assert_parent_relationship)
 
        # 2. Publish conversation.deleted event
        # Toggle soft delete off to verify physical hard delete
        settings.SOFT_DELETE_ENABLED = False
        event_deleted = {
            "event_id": str(uuid.uuid4()),
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
        
        # Verify child node is removed from graph database eventually
        def assert_deleted(r):
            assert r.status_code == 404
        await assert_eventually(client, f"/graph/conversations/{test_conv_id}", assert_deleted)
        
        await producer.stop()
