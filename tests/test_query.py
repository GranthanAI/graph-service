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
    
    root_id = "query-test-root"
    child1_id = "query-test-child1"
    child2_id = "query-test-child2"
    grandchild_id = "query-test-grandchild"
    
    # Clean up first to ensure clean state
    repo.delete_conversation(grandchild_id)
    repo.delete_conversation(child1_id)
    repo.delete_conversation(child2_id)
    repo.delete_conversation(root_id)
    
    # Create test nodes and branches
    repo.create_conversation_with_parent(
        conversation_id=root_id,
        user_id="user-q",
        title="Root Node",
        status="ACTIVE",
        created_at="2026-07-30T12:00:00Z",
        updated_at="2026-07-30T12:00:00Z",
        parent_id=None
    )
    repo.create_conversation_with_parent(
        conversation_id=child1_id,
        user_id="user-q",
        title="Child 1",
        status="ACTIVE",
        created_at="2026-07-30T12:01:00Z",
        updated_at="2026-07-30T12:01:00Z",
        parent_id=root_id
    )
    repo.create_conversation_with_parent(
        conversation_id=child2_id,
        user_id="user-q",
        title="Child 2",
        status="ACTIVE",
        created_at="2026-07-30T12:02:00Z",
        updated_at="2026-07-30T12:02:00Z",
        parent_id=root_id
    )
    repo.create_conversation_with_parent(
        conversation_id=grandchild_id,
        user_id="user-q",
        title="Grandchild 1",
        status="ACTIVE",
        created_at="2026-07-30T12:03:00Z",
        updated_at="2026-07-30T12:03:00Z",
        parent_id=child1_id
    )
    
    yield {
        "root": root_id,
        "child1": child1_id,
        "child2": child2_id,
        "grandchild": grandchild_id
    }
    
    # Clean up after tests run
    repo.delete_conversation(grandchild_id)
    repo.delete_conversation(child1_id)
    repo.delete_conversation(child2_id)
    repo.delete_conversation(root_id)


def test_query_endpoints(setup_test_graph) -> None:
    ids = setup_test_graph
    
    # 1. Non-existent conversation
    resp = client.get("/graph/conversations/non-existent-id")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]
    
    # 2. Get root node
    resp = client.get(f"/graph/conversations/{ids['root']}")
    assert resp.status_code == 200
    assert resp.json()["conversation_id"] == ids["root"]
    
    # 3. Root parent should return 404 (no parent exists)
    resp = client.get(f"/graph/conversations/{ids['root']}/parent")
    assert resp.status_code == 404
    assert "Parent for conversation" in resp.json()["detail"]
    
    # 4. Child 1 parent should return root
    resp = client.get(f"/graph/conversations/{ids['child1']}/parent")
    assert resp.status_code == 200
    assert resp.json()["conversation_id"] == ids["root"]
    
    # 5. Root children should return [Child 1, Child 2]
    resp = client.get(f"/graph/conversations/{ids['root']}/children")
    assert resp.status_code == 200
    children = [c["conversation_id"] for c in resp.json()]
    assert len(children) == 2
    assert ids["child1"] in children
    assert ids["child2"] in children
    
    # 6. Grandchild ancestors should return [Child 1, Root]
    resp = client.get(f"/graph/conversations/{ids['grandchild']}/ancestors")
    assert resp.status_code == 200
    ancestors = [a["conversation_id"] for a in resp.json()]
    assert len(ancestors) == 2
    assert ids["child1"] in ancestors
    assert ids["root"] in ancestors
    
    # 7. Root descendants should return [Child 1, Child 2, Grandchild]
    resp = client.get(f"/graph/conversations/{ids['root']}/descendants")
    assert resp.status_code == 200
    descendants = [d["conversation_id"] for d in resp.json()]
    assert len(descendants) == 3
    assert ids["child1"] in descendants
    assert ids["child2"] in descendants
    assert ids["grandchild"] in descendants

    # 8. Traversal from non-existent node should fail with 404
    resp = client.get("/graph/conversations/non-existent-id/children")
    assert resp.status_code == 404
