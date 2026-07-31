import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.api.dependencies import get_repository, get_db

client = TestClient(app)

@pytest.fixture(scope="module")
def setup_test_graph():
    # Setup test nodes
    db = get_db()
    repo = get_repository(db)
    
    root_id = "mem-test-root"
    child1_id = "mem-test-child1"
    grandchild_id = "mem-test-grandchild"
    
    # Clean up first to ensure clean state
    repo.delete_conversation(grandchild_id)
    repo.delete_conversation(child1_id)
    repo.delete_conversation(root_id)
    
    # Create test nodes and branches
    repo.create_conversation_with_parent(
        conversation_id=root_id,
        user_id="user-mem",
        title="Root Node",
        status="ACTIVE",
        created_at="2026-07-30T12:00:00Z",
        updated_at="2026-07-30T12:00:00Z",
        parent_id=None
    )
    repo.create_conversation_with_parent(
        conversation_id=child1_id,
        user_id="user-mem",
        title="Child 1",
        status="ACTIVE",
        created_at="2026-07-30T12:01:00Z",
        updated_at="2026-07-30T12:01:00Z",
        parent_id=root_id
    )
    repo.create_conversation_with_parent(
        conversation_id=grandchild_id,
        user_id="user-mem",
        title="Grandchild 1",
        status="ACTIVE",
        created_at="2026-07-30T12:03:00Z",
        updated_at="2026-07-30T12:03:00Z",
        parent_id=child1_id
    )
    
    yield {
        "root": root_id,
        "child": child1_id,
        "grandchild": grandchild_id
    }
    
    # Clean up after tests run
    repo.delete_conversation(grandchild_id)
    repo.delete_conversation(child1_id)
    repo.delete_conversation(root_id)


def test_memory_context_endpoint(setup_test_graph) -> None:
    ids = setup_test_graph
    
    # 1. Root conversation memory-context check
    resp = client.get(f"/internal/graph/conversations/{ids['root']}/memory-context")
    assert resp.status_code == 200
    data = resp.json()
    assert data["current_conversation_id"] == ids["root"]
    assert data["root_conversation_id"] == ids["root"]
    assert data["lineage_depth"] == 1
    assert len(data["lineage"]) == 1
    assert data["lineage"][0]["conversation_id"] == ids["root"]
    assert data["lineage"][0]["depth"] == 0
    
    # 2. Grandchild conversation memory-context check
    resp = client.get(f"/internal/graph/conversations/{ids['grandchild']}/memory-context")
    assert resp.status_code == 200
    data = resp.json()
    assert data["current_conversation_id"] == ids["grandchild"]
    assert data["root_conversation_id"] == ids["root"]
    assert data["lineage_depth"] == 3
    assert len(data["lineage"]) == 3
    
    # Verify depth sorting order (root -> child -> grandchild)
    assert data["lineage"][0]["conversation_id"] == ids["root"]
    assert data["lineage"][0]["depth"] == 0
    
    assert data["lineage"][1]["conversation_id"] == ids["child"]
    assert data["lineage"][1]["depth"] == 1
    
    assert data["lineage"][2]["conversation_id"] == ids["grandchild"]
    assert data["lineage"][2]["depth"] == 2

    # 3. Non-existent conversation should return 404
    resp = client.get("/internal/graph/conversations/non-existent-id/memory-context")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()
