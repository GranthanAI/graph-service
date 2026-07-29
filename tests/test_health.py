from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_read_root() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Welcome" in response.json()["message"]

def test_health_check() -> None:
    response = client.get("/graph/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "Neo4j" in data["database"]
