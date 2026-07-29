

# Graph Service HLD (v1)

## 1. Overview

The Graph Service is responsible for maintaining the **conversation relationship graph** of the GraphGPT platform. It consumes immutable Kafka domain events from the Conversation Service and synchronizes them into Neo4j.

The service provides graph traversal APIs for downstream services such as the Memory Service while remaining completely decoupled from the Conversation Service.

---

# 2. Objectives

* Build the conversation graph asynchronously.
* Maintain parent-child relationships between conversations.
* Support conversation branching.
* Provide graph traversal APIs.
* Keep graph data eventually consistent with Conversation Service.
* Ensure idempotent event processing.

---

# 3. Responsibilities

The Graph Service is responsible for:

* Consuming conversation lifecycle events from Kafka.
* Creating Conversation nodes in Neo4j.
* Creating conversation relationships.
* Maintaining graph consistency.
* Exposing graph traversal APIs.
* Handling duplicate events safely.

---

# 4. Non-Responsibilities

The Graph Service does **not**:

* Create conversations.
* Store conversation metadata.
* Store messages.
* Authenticate users.
* Generate AI responses.
* Manage memories.
* Perform vector search.
* Store uploaded files.

---

# 5. Architecture

```text
                 REST APIs
                     │
                     ▼
             Graph Service API
                     │
                     ▼
             Graph Service Layer
            ┌────────┴────────┐
            │                 │
            ▼                 ▼
     Graph Repository    Kafka Consumer
            │                 │
            ▼                 ▼
          Neo4j       conversation.created
                      conversation.deleted
```

---

# 6. Technology Stack

| Component       | Technology           |
| --------------- | -------------------- |
| Framework       | FastAPI              |
| Language        | Python               |
| Database        | Neo4j                |
| Event Streaming | Kafka                |
| Cache           | Redis (Optional)     |
| Container       | Docker               |
| Orchestration   | Kubernetes           |
| Monitoring      | Prometheus + Grafana |
| Logging         | Structured JSON Logs |
| Tracing         | OpenTelemetry        |

---

# 7. Data Ownership

| Service              | Owns                             |
| -------------------- | -------------------------------- |
| Conversation Service | Conversation metadata            |
| Graph Service        | Conversation graph               |
| Memory Service       | Memory graph enrichment (future) |

---

# 8. Graph Model

## Node

### Conversation

Properties

```text
conversation_id
user_id
title
status
created_at
updated_at
```

---

## Relationships

### HAS_CHILD

```text
(parent)-[:HAS_CHILD]->(child)
```

Represents the direct child conversations.

---

### CREATED_FROM

```text
(child)-[:CREATED_FROM]->(parent)
```

Represents the origin of a branched conversation.

---

# 9. Kafka Integration

## Consumed Events

### conversation.created

Payload

```json
{
  "event_id": "...",
  "event_version": 1,
  "conversation_id": "...",
  "parent_conversation_id": "...",
  "conversation_status": "ACTIVE",
  "user_id": "...",
  "created_at": "...",
  "trace_id": "...",
  "correlation_id": "..."
}
```

Processing:

* Create conversation node.
* If `parent_conversation_id` exists:

  * Create `HAS_CHILD`
  * Create `CREATED_FROM`

---

### conversation.deleted

Processing:

* Mark node as deleted or remove it (based on business policy).
* Maintain graph consistency.

---

# 10. High-Level Data Flow

## Root Conversation

```text
Client
      │
Conversation Service
      │
Persist Conversation
      │
Transactional Outbox
      │
Kafka
      │
Graph Service
      │
MERGE Conversation Node
```

---

## Branched Conversation

```text
Client
      │
Create Branch
      │
Conversation Service
      │
Persist parent_conversation_id
      │
Transactional Outbox
      │
Kafka
      │
Graph Service
      │
MERGE Parent
MERGE Child
CREATE HAS_CHILD
CREATE CREATED_FROM
```

---

# 11. Public APIs

### Get Conversation

```http
GET /graph/conversations/{conversation_id}
```

Returns node information.

---

### Get Parent

```http
GET /graph/conversations/{conversation_id}/parent
```

Returns the parent conversation.

---

### Get Children

```http
GET /graph/conversations/{conversation_id}/children
```

Returns immediate child conversations.

---

### Get Ancestors

```http
GET /graph/conversations/{conversation_id}/ancestors
```

Returns the complete parent chain.

---

### Get Descendants

```http
GET /graph/conversations/{conversation_id}/descendants
```

Returns the conversation subtree.

---

# 12. Reliability

* Kafka consumer groups for scalability.
* Idempotent Neo4j `MERGE` operations.
* Dead Letter Queue (DLQ) for failed events.
* Retry with exponential backoff.
* Offset commit only after successful Neo4j transaction.

---

# 13. Non-Functional Requirements

* Event-driven architecture.
* Eventual consistency.
* High availability.
* Horizontal scalability.
* Low-latency graph traversal.
* Idempotent event processing.
* No synchronous dependency on Conversation Service.

---

# 14. Service Boundaries

## Graph Service Owns

* Conversation nodes.
* Conversation relationships.
* Graph traversals.
* Neo4j persistence.
* Graph query APIs.

---

## Graph Service Does Not Own

* Conversation creation.
* Conversation metadata.
* Message storage.
* Authentication.
* LLM responses.
* Memory management.
* File management.
* Vector search.

---

# 15. Future Scope

The Graph Service is intentionally designed for incremental evolution. While the initial implementation manages only conversation nodes and branching relationships, it will later expand to support:

* File nodes and `USES_FILE` relationships.
* Memory nodes and memory lineage.
* Concept nodes extracted from conversations and documents.
* Project/workspace graphs.
* Cross-conversation semantic relationships.
* Knowledge graph traversal for Retrieval and Memory Services.

This evolution will be achieved by consuming additional domain events from future File, Memory, and Retrieval Services without requiring architectural changes to the Graph Service.
