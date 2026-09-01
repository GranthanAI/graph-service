# Task Update 1: Graph Context gRPC Server for LLM Service Integration

This is the first entry in `docs/updates/` for Graph Service, documenting the addition of a gRPC server so llm-service's Context Collector can actually reach this service — previously it had no gRPC surface at all.

---

## 1. Executive Summary

llm-service's `ContextCollector` calls a gRPC `GraphServiceClient` on port 50052 expecting `llm-service/proto/graph.proto`'s `GraphService.GetGraphContext` / `GetNodesByIds`. Graph Service had no gRPC server whatsoever (REST-only, via `app/api/routes/graph.py` and `internal.py`) — every call from llm-service was timing out and degrading to empty graph context.

This service's Neo4j data model is **conversation lineage only** — `(:User)-[:SENT]->(:Message)-[:BELONGS_TO]->(:Conversation)-[:HAS_CHILD]/[:CREATED_FROM]->(:Conversation)`. There is no generic entity/knowledge-graph model, no relevance scoring, and no free-text/semantic node search. `graph.proto`'s `GetGraphContext` was designed against a fuller knowledge-graph shape (arbitrary `query`, `GraphNode.label`/`node_type`/`properties`/`relevance_score`), which this service cannot honor in full.

Rather than fabricate a knowledge-graph feature that doesn't exist, this task implements the gRPC contract as an **honest, degenerate mapping onto what Graph Service actually has**: conversation ancestor/descendant lineage, using the existing `get_ancestors`/`get_descendants`/`get_conversation` repository methods unchanged. `relevance_score` is a fixed `1.0` placeholder and `subgraph_summary` is a deterministic count string — neither claims to be more sophisticated than the underlying data supports.

---

## 2. gRPC Server

### 2.1 Proto
Copied `llm-service/proto/graph.proto` into `app/proto/graph.proto` verbatim (field-for-field match with llm-service's `GraphServiceClient`, confirmed against its actual call sites in `app/grpc/clients/graph_client.py` rather than assumed from the proto alone — its `GetNodesByIdsRequest` carries a `user_id` field the plan's initial draft had omitted). Compiled with `grpc_tools.protoc` into `app/proto/graph_pb2.py` / `graph_pb2_grpc.py`. This is the first proto/gRPC infrastructure in this service — `grpcio`/`grpcio-tools` added to `requirements.txt` and `pyproject.toml`.

### 2.2 Handler — `app/grpc/graph_context_handler.py`
`GraphContextHandler(graph_pb2_grpc.GraphServiceServicer)`, constructed with the existing `GraphService` facade:

- **`GetGraphContext`**: calls `repo.get_ancestors()` and `repo.get_descendants()` (both pre-existing, run concurrently via `asyncio.gather` + `asyncio.to_thread` since the underlying Neo4j driver calls are synchronous — the same offloading pattern `GraphService.get_message_count()` already uses). Each returned `Conversation` node maps to a `GraphNode` (`node_id` = conversation_id, `label` = title, `node_type = "conversation"`, `relevance_score = 1.0`). Ancestor chain and descendant links are emitted as `HAS_CHILD` `GraphRelationship` edges. `subgraph_summary` is a plain count sentence (e.g. `"3 related conversation(s) in lineage (2 ancestor(s), 1 descendant(s))."`) — not LLM-generated, since no such capability exists in this service. `request.query` and `request.max_depth` beyond what `get_ancestors`/`get_descendants` already accept as a depth parameter are not treated as semantic filters, since there is nothing to filter against.
- **`GetNodesByIds`**: calls `repo.get_conversation(id)` per requested ID concurrently, skipping any that don't resolve rather than failing the whole call.

### 2.3 Wiring
`app/main.py`'s lifespan starts a `grpc.aio.server()` after the Neo4j driver/repository/`GraphService` singletons are already constructed, registering `GraphContextHandler`, bound to a new `GRAPH_GRPC_PORT` setting (`app/core/config.py`, default `50052` — matching llm-service's `graph_service_port` default exactly, so no config change was needed on llm-service's side). Stopped alongside the existing Neo4j `db.close()` in the shutdown block.

---

## 3. Verification

Confirmed live against the running stack: with the new server started, an ad-hoc gRPC client call to `GetGraphContext` for a real (deleted, in this test) conversation returned a clean, correctly-shaped empty response (`0 nodes, 0 relationships`) rather than a timeout. More significantly, restarting llm-service afterward and sending a real message through the full pipeline (API Gateway → Conversation Service → Kafka → LLM Service) showed `Context collection completed` with `graph` no longer appearing in `missing_sources` — previously every request logged a `GRPC_TIMEOUT` for this service.

---

## 4. What Was NOT Changed

- No new Cypher queries beyond what `get_ancestors`/`get_descendants`/`get_conversation` already provide — no entity extraction, no relevance model, no embedding/vector search added to this service.
- Existing REST API (`app/api/routes/graph.py`), Kafka consumer worker (`consumer_worker.py`), and the conversation-lineage write path are entirely unchanged.

## 5. Known Gaps / Follow-ups

- Running this service's own `pytest` suite while the live dev server is also running causes 3 test failures (`test_consistency`, `test_performance`, `test_sync`) — each spins up its own `TestClient(app)` lifespan, which tries to rebind the same gRPC port (`50052`) the live server already holds. This is a test/dev-server port collision, not a defect in the new handler; run tests with the dev server stopped, or pass a distinct port for test runs.
- `GetGraphContext`'s `relevance_score` and `subgraph_summary` are intentionally simplistic placeholders (fixed `1.0`, deterministic count string) — a real scoring model or graph-based summary would require an actual knowledge-graph feature this service does not currently implement.
