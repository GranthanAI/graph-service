"""Ownership and service-credential enforcement, against an in-memory fake
repository (no Neo4j): non-owners must be indistinguishable from "not found"."""

import asyncio
from unittest.mock import MagicMock

import grpc
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.dependencies import get_graph_service
from app.api.routes import graph as graph_routes
from app.api.routes import internal as internal_routes
from app.core.config import settings
from app.grpc.graph_context_handler import GraphContextHandler
from app.proto import graph_pb2
from app.services.graph_service import GraphService

NODES = {
    "a-root": {"conversation_id": "a-root", "user_id": "alice", "title": "root", "status": "ACTIVE", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"},
    "a-child": {"conversation_id": "a-child", "user_id": "alice", "title": "child", "status": "ACTIVE", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"},
    "b-conv": {"conversation_id": "b-conv", "user_id": "bob", "title": "secret", "status": "ACTIVE", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"},
}


class FakeRepo:
    def get_conversation(self, cid):
        return NODES.get(cid)

    def get_parent(self, cid):
        return NODES["a-root"] if cid == "a-child" else None

    def get_children(self, cid, skip=0, limit=100):
        # Includes a cross-tenant node to prove results are filtered by owner.
        return [NODES["a-child"], NODES["b-conv"]] if cid == "a-root" else []

    def get_ancestors(self, cid, skip=0, limit=100):
        return [NODES["a-root"], NODES["b-conv"]] if cid == "a-child" else []

    def get_descendants(self, cid, skip=0, limit=100):
        return [NODES["a-child"], NODES["b-conv"]] if cid == "a-root" else []

    def get_memory_context(self, cid):
        return {
            "current_conversation_id": cid,
            "root_conversation_id": "a-root",
            "lineage_depth": 1,
            "lineage": [{"conversation_id": "a-root", "depth": 0}],
        }


@pytest.fixture
def service():
    return GraphService(FakeRepo())


@pytest.fixture
def client(service, monkeypatch):
    monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "svc-token")
    from app.main import app  # registers the real exception handlers

    app.dependency_overrides[get_graph_service] = lambda: service
    yield TestClient(app)
    app.dependency_overrides.clear()


def _h(user):
    return {"X-User-Id": user}


# ---- public HTTP ----------------------------------------------------------


def test_owner_can_read_own_conversation(client):
    assert client.get("/graph/conversations/a-root", headers=_h("alice")).status_code == 200


def test_other_user_gets_same_404_as_missing(client):
    other = client.get("/graph/conversations/a-root", headers=_h("bob"))
    missing = client.get("/graph/conversations/does-not-exist", headers=_h("bob"))
    assert other.status_code == missing.status_code == 404


def test_missing_identity_is_401(client):
    assert client.get("/graph/conversations/a-root").status_code == 401


@pytest.mark.parametrize("suffix", ["", "/parent", "/children", "/ancestors", "/descendants"])
def test_non_owner_denied_on_every_route(client, suffix):
    assert client.get(f"/graph/conversations/a-child{suffix}", headers=_h("bob")).status_code == 404


def test_lineage_results_exclude_other_users_nodes(client):
    children = client.get("/graph/conversations/a-root/children", headers=_h("alice")).json()
    assert [c["conversation_id"] for c in children] == ["a-child"]
    ancestors = client.get("/graph/conversations/a-child/ancestors", headers=_h("alice")).json()
    assert [c["conversation_id"] for c in ancestors] == ["a-root"]


# ---- internal HTTP --------------------------------------------------------


def test_internal_requires_service_credential(client):
    url = "/internal/graph/conversations/a-root/memory-context?user_id=alice"
    assert client.get(url).status_code == 401
    assert client.get(url, headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get(url, headers=_h("alice")).status_code == 401  # a user identity is not a service credential
    assert client.get(url, headers={"Authorization": "Bearer svc-token"}).status_code == 200


def test_internal_fails_closed_without_configured_tokens(client, monkeypatch):
    monkeypatch.setattr(settings, "INTERNAL_SERVICE_TOKENS", "")
    url = "/internal/graph/conversations/a-root/memory-context?user_id=alice"
    assert client.get(url, headers={"Authorization": "Bearer "}).status_code == 401


def test_internal_enforces_ownership(client):
    auth = {"Authorization": "Bearer svc-token"}
    assert client.get("/internal/graph/conversations/a-root/memory-context?user_id=bob", headers=auth).status_code == 404
    assert client.get("/internal/graph/conversations/a-root/memory-context", headers=auth).status_code == 422


# ---- gRPC -----------------------------------------------------------------


def _ctx():
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = []
    return ctx


def _handler(service):
    return GraphContextHandler(service)


def test_grpc_context_owner_gets_only_own_lineage(service):
    req = graph_pb2.GetGraphContextRequest(user_id="alice", conversation_id="a-root")
    resp = asyncio.run(_handler(service).GetGraphContext(req, _ctx()))
    assert {n.node_id for n in resp.nodes} == {"a-child"}  # bob's node filtered out


@pytest.mark.parametrize("user,conv", [("bob", "a-root"), ("alice", "b-conv"), ("alice", "nope"), ("", "a-root")])
def test_grpc_context_denied_looks_empty(service, user, conv):
    req = graph_pb2.GetGraphContextRequest(user_id=user, conversation_id=conv)
    resp = asyncio.run(_handler(service).GetGraphContext(req, _ctx()))
    assert len(resp.nodes) == 0 and len(resp.relationships) == 0


def test_grpc_nodes_by_ids_returns_only_owned(service):
    req = graph_pb2.GetNodesByIdsRequest(user_id="alice", node_ids=["a-root", "b-conv", "x"])
    resp = asyncio.run(_handler(service).GetNodesByIds(req, _ctx()))
    assert [n.node_id for n in resp.nodes] == ["a-root"]


def test_grpc_nodes_by_ids_requires_user_and_bounds_batch(service):
    h = _handler(service)
    no_user = asyncio.run(h.GetNodesByIds(graph_pb2.GetNodesByIdsRequest(node_ids=["a-root"]), _ctx()))
    assert len(no_user.nodes) == 0
    ctx = _ctx()
    big = graph_pb2.GetNodesByIdsRequest(user_id="alice", node_ids=[str(i) for i in range(101)])
    asyncio.run(h.GetNodesByIds(big, ctx))
    ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


# ---- chat.message.created wire compatibility --------------------------------


def test_message_event_accepts_flat_conversation_service_shape():
    flat = {
        "event_type": "chat.message.created",
        "message_id": "m1",
        "conversation_id": "a-root",
        "user_id": "alice",
        "content": "hello",
        "timestamp": "2026-10-17T00:00:00+00:00",
        "trace_context": {"traceparent": "trace-1"},
    }
    from app.events.message_created import MessageCreatedEnvelope

    env = MessageCreatedEnvelope(**GraphService._normalize_message_event(flat))
    assert (env.payload.message_id, env.payload.role, env.payload.user_id) == ("m1", "user", "alice")
    assert env.correlation_id == "trace-1"


def test_message_event_keeps_enveloped_shape_unchanged():
    enveloped = {"event_id": "e1", "event_type": "chat.message.created", "payload": {"message_id": "m1"}}
    assert GraphService._normalize_message_event(enveloped) is enveloped
