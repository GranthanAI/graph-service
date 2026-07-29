# Phase 1 — Project Foundation & Neo4j Setup

## HLD Scope

* Graph Service overview
* Responsibilities
* Service boundaries
* Technology stack
* High-level architecture
* Neo4j selection
* Data ownership
* Future scope (brief)

## LLD Scope

* Project structure
* Configuration
* Dependency injection
* Neo4j connection manager
* Neo4j driver configuration
* Health endpoint
* Docker Compose
* Neo4j constraints and indexes
* Basic GraphRepository skeleton

### Deliverables

* FastAPI service running
* Neo4j connected
* Health API
* Repository initialized
* Docker working

---

# Phase 2 — Conversation Graph Synchronization

## HLD Scope

* Kafka integration
* Conversation graph model
* Event-driven synchronization
* Conversation nodes
* HAS_CHILD
* CREATED_FROM
* Idempotent processing

## LLD Scope

* Kafka Consumer
* Event models
* Consumer worker
* GraphService business logic
* Repository implementation
* Neo4j MERGE queries
* Transaction handling
* Retry logic
* DLQ
* Unit & integration tests

### Deliverables

Consumes

```text
conversation.created
conversation.deleted
```

Creates

```text
Conversation Node

HAS_CHILD

CREATED_FROM
```

Graph is synchronized from Kafka.

---

# Phase 3 — Graph Query APIs

## HLD Scope

* Graph traversal APIs
* Read architecture
* Traversal capabilities

## LLD Scope

REST APIs

```text
GET /parent

GET /children

GET /ancestors

GET /descendants

GET /conversation
```

Repository

* Parent query
* Child query
* Ancestor traversal
* Descendant traversal

Cypher queries

Response DTOs

API tests

### Deliverables

Graph traversal works.

Neo4j can now be queried.

---

# Phase 4 — Graph Management & Consistency

## HLD Scope

* Graph lifecycle
* Consistency model
* Delete/update strategy

## LLD Scope

Support

```text
conversation.deleted

conversation.updated
```

Node updates

Relationship cleanup

Soft delete

Hard delete policy

Replay handling

Idempotency improvements

Recovery scenarios

Integration tests

### Deliverables

Graph stays consistent when conversations change.

---

# Phase 5 — Production Hardening

*(Skipping observability for now, as requested.)*

## HLD Scope

* Reliability
* Scalability
* Fault tolerance

## LLD Scope

* Consumer concurrency
* Neo4j transaction optimization
* Batch event processing
* Performance optimization
* Pagination for traversal APIs
* Configuration tuning
* Load testing
* End-to-end testing
* Deployment documentation

### Deliverables

Production-ready Graph Service v1.

---

# Overall Roadmap

| Phase | Goal                  | Output                                        |
| ----- | --------------------- | --------------------------------------------- |
| **1** | Foundation            | FastAPI + Neo4j + Repository + Docker         |
| **2** | Event Synchronization | Kafka Consumer → Neo4j Conversation Graph     |
| **3** | Graph APIs            | Parent, Children, Ancestors, Descendants APIs |
| **4** | Graph Consistency     | Delete/Update events, replay, cleanup         |
| **5** | Production Readiness  | Performance, batching, reliability, E2E tests |

This progression is natural because each phase depends on the previous one and produces a usable milestone before moving on. Once Phase 5 is complete, your Graph Service will be ready for the next major integration—adding File, Memory, Concept, and Project nodes as those services are built.
