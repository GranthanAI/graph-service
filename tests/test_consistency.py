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
async def test_updates_and_soft_delete_consistency() -> None:
    # Isolate consumer group for tests to avoid partition sharing with active local dev server
    settings.KAFKA_CONSUMER_GROUP_ID = "graph-service-consumer-test-consistency"
    # Use TestClient context manager to run FastAPI app lifespan
    with TestClient(app) as client:
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        
        test_root_id = "consist-root-1"
        test_child_id = "consist-child-1"
        
        # Clean up database first to ensure no dirty state from previous runs
        from app.api.dependencies import get_repository, get_db
        repo = get_repository(get_db())
        repo.delete_conversation(test_child_id)
        repo.delete_conversation(test_root_id)
        
        # 1. Publish Created Event for Root
        event_created_root = {
            "conversation_id": test_root_id,
            "user_id": "user-consist",
            "conversation_status": "ACTIVE",
            "created_at": "2026-07-30T10:00:00Z"
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            json.dumps(event_created_root).encode("utf-8")
        )
        
        # 2. Publish Created Event for Child (linked to Root)
        event_created_child = {
            "conversation_id": test_child_id,
            "parent_conversation_id": test_root_id,
            "user_id": "user-consist",
            "conversation_status": "ACTIVE",
            "created_at": "2026-07-30T10:01:00Z"
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            json.dumps(event_created_child).encode("utf-8")
        )
        
        # Verify node creation eventually (to handle async latency)
        def assert_created(r):
            assert r.status_code == 200
            assert r.json()["title"] == f"Conversation {test_root_id[:8]}"
        await assert_eventually(client, f"/graph/conversations/{test_root_id}", assert_created)
        
        # 3. Publish newer updated event to update title
        event_update_new = {
            "conversation_id": test_root_id,
            "title": "Updated Title Root",
            "status": "ACTIVE",
            "updated_at": "2026-07-30T10:05:00Z"
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_UPDATED_TOPIC,
            json.dumps(event_update_new).encode("utf-8")
        )
        
        # Verify title was successfully updated eventually
        def assert_updated_title(r):
            assert r.status_code == 200
            assert r.json()["title"] == "Updated Title Root"
        await assert_eventually(client, f"/graph/conversations/{test_root_id}", assert_updated_title)
        
        # 4. Out-of-Order Replay check: Publish older updated event (should be ignored)
        event_update_stale = {
            "conversation_id": test_root_id,
            "title": "Stale Out of Order Title",
            "status": "ACTIVE",
            "updated_at": "2026-07-30T10:02:00Z" # Older timestamp
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_UPDATED_TOPIC,
            json.dumps(event_update_stale).encode("utf-8")
        )
        
        # Wait a bit to ensure stale event had time to be processed and ignored
        await asyncio.sleep(2.0)
        
        # Verify title is STILL "Updated Title Root" (stale update safely ignored)
        resp = client.get(f"/graph/conversations/{test_root_id}")
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated Title Root"
        
        # 5. Soft delete check: Publish conversation.deleted event for root
        event_delete_root = {
            "conversation_id": test_root_id,
            "deleted_at": "2026-07-30T10:10:00Z"
        }
        
        # Toggle soft delete toggle ON
        settings.SOFT_DELETE_ENABLED = True
        
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            json.dumps(event_delete_root).encode("utf-8")
        )
        
        # Verify node status is updated to DELETED but structural node remains in graph database
        def assert_soft_deleted(r):
            assert r.status_code == 200
            assert r.json()["status"] == "DELETED"
        await assert_eventually(client, f"/graph/conversations/{test_root_id}", assert_soft_deleted)
        
        # Verify parent-child lineage connections are preserved
        resp_parent = client.get(f"/graph/conversations/{test_child_id}/parent")
        assert resp_parent.status_code == 200
        assert resp_parent.json()["conversation_id"] == test_root_id
        
        # 6. Hard delete check: Publish conversation.deleted event with SOFT_DELETE_ENABLED=False
        settings.SOFT_DELETE_ENABLED = False
        event_delete_child = {
            "conversation_id": test_child_id,
            "deleted_at": "2026-07-30T10:11:00Z"
        }
        
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            json.dumps(event_delete_child).encode("utf-8")
        )
        
        # Verify child node is physically removed from the graph database
        def assert_child_removed(r):
            assert r.status_code == 404
        await assert_eventually(client, f"/graph/conversations/{test_child_id}", assert_child_removed)
        
        # Cleanup root node physically using hard delete
        event_hard_delete_root = {
            "conversation_id": test_root_id,
            "deleted_at": "2026-07-30T10:12:00Z"
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_DELETED_TOPIC,
            json.dumps(event_hard_delete_root).encode("utf-8")
        )
        
        # Verify root node is also physically removed
        def assert_root_removed(r):
            assert r.status_code == 404
        await assert_eventually(client, f"/graph/conversations/{test_root_id}", assert_root_removed)
        
        await producer.stop()
