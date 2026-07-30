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
async def test_pagination_and_load_performance() -> None:
    # Isolate consumer group for tests to avoid partition sharing with active local dev server
    settings.KAFKA_CONSUMER_GROUP_ID = "graph-service-consumer-test-performance"
    
    with TestClient(app) as client:
        producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        
        parent_id = "perf-parent-1"
        child_ids = [f"perf-child-{i}" for i in range(1, 11)]  # 10 child nodes
        
        # Clean up database first to ensure clean state
        from app.api.dependencies import get_repository, get_db
        repo = get_repository(get_db())
        for cid in child_ids:
            repo.delete_conversation(cid)
        repo.delete_conversation(parent_id)
        
        # 1. Publish Created Event for Parent
        event_parent = {
            "conversation_id": parent_id,
            "user_id": "user-perf",
            "conversation_status": "ACTIVE",
            "created_at": "2026-07-30T10:00:00Z"
        }
        await producer.send_and_wait(
            settings.KAFKA_CONVERSATION_CREATED_TOPIC,
            json.dumps(event_parent).encode("utf-8")
        )
        
        # 2. Publish 10 children in batch to verify high throughput batch loading
        for i, cid in enumerate(child_ids):
            # Format timestamp chronologically (e.g. 10:01:00, 10:01:01, ...) to ensure deterministic ordering
            sec = str(i).zfill(2)
            timestamp = f"2026-07-30T10:01:{sec}Z"
            
            event_child = {
                "conversation_id": cid,
                "parent_conversation_id": parent_id,
                "user_id": "user-perf",
                "conversation_status": "ACTIVE",
                "created_at": timestamp
            }
            # Send asynchronously without waiting for each one, to test concurrent loading
            await producer.send(
                settings.KAFKA_CONVERSATION_CREATED_TOPIC,
                json.dumps(event_child).encode("utf-8")
            )
            
        await asyncio.sleep(3.0)
        
        # Verify parent node creation eventually
        def assert_parent(r):
            assert r.status_code == 200
        await assert_eventually(client, f"/graph/conversations/{parent_id}", assert_parent)
        
        # Verify child nodes count is exactly 10 eventually
        def assert_all_children(r):
            assert r.status_code == 200
            assert len(r.json()) == 10
        await assert_eventually(client, f"/graph/conversations/{parent_id}/children?limit=100", assert_all_children)
        
        # 3. Verify Pagination on Children
        # Page 1: limit=3, skip=0 (expect perf-child-1, perf-child-2, perf-child-3)
        resp_p1 = client.get(f"/graph/conversations/{parent_id}/children?skip=0&limit=3")
        assert resp_p1.status_code == 200
        p1_data = resp_p1.json()
        assert len(p1_data) == 3
        assert p1_data[0]["conversation_id"] == "perf-child-1"
        assert p1_data[1]["conversation_id"] == "perf-child-2"
        assert p1_data[2]["conversation_id"] == "perf-child-3"
        
        # Page 2: limit=3, skip=3 (expect perf-child-4, perf-child-5, perf-child-6)
        resp_p2 = client.get(f"/graph/conversations/{parent_id}/children?skip=3&limit=3")
        assert resp_p2.status_code == 200
        p2_data = resp_p2.json()
        assert len(p2_data) == 3
        assert p2_data[0]["conversation_id"] == "perf-child-4"
        assert p2_data[1]["conversation_id"] == "perf-child-5"
        assert p2_data[2]["conversation_id"] == "perf-child-6"
        
        # Page 3: limit=5, skip=6 (expect perf-child-7, perf-child-8, perf-child-9, perf-child-10)
        resp_p3 = client.get(f"/graph/conversations/{parent_id}/children?skip=6&limit=5")
        assert resp_p3.status_code == 200
        p3_data = resp_p3.json()
        assert len(p3_data) == 4
        assert p3_data[0]["conversation_id"] == "perf-child-7"
        assert p3_data[3]["conversation_id"] == "perf-child-10"

        # 4. Verify Pagination parameter validations (HTTP 422 Unprocessable Entity)
        resp_val1 = client.get(f"/graph/conversations/{parent_id}/children?limit=-5")
        assert resp_val1.status_code == 422
        
        resp_val2 = client.get(f"/graph/conversations/{parent_id}/children?skip=-1")
        assert resp_val2.status_code == 422
        
        resp_val3 = client.get(f"/graph/conversations/{parent_id}/children?limit=5000")
        assert resp_val3.status_code == 422
        
        # Clean up database test entries
        for cid in child_ids:
            repo.delete_conversation(cid)
        repo.delete_conversation(parent_id)
        
        await producer.stop()
