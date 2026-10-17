# Cypher Queries for Conversation Nodes and Relationships

CREATE_CONVERSATION_CONSTRAINT = """
CREATE CONSTRAINT conversation_id_unique IF NOT EXISTS
FOR (c:Conversation)
REQUIRE c.conversation_id IS UNIQUE
"""

CREATE_USER_INDEX = """
CREATE INDEX conversation_user_id_idx IF NOT EXISTS
FOR (c:Conversation)
ON (c.user_id)
"""

MERGE_CONVERSATION_NODE = """
MERGE (c:Conversation {conversation_id: $conversation_id})
ON CREATE SET c.user_id = $user_id,
              c.title = $title,
              c.status = $status,
              c.created_at = $created_at,
              c.updated_at = $updated_at
ON MATCH SET c.title = case when c.updated_at IS NULL OR $updated_at >= c.updated_at then $title else c.title end,
             c.status = case when c.updated_at IS NULL OR $updated_at >= c.updated_at then $status else c.status end,
             c.updated_at = case when c.updated_at IS NULL OR $updated_at >= c.updated_at then $updated_at else c.updated_at end
RETURN c
"""

UPDATE_CONVERSATION_NODE = """
MATCH (c:Conversation {conversation_id: $conversation_id})
WHERE c.updated_at IS NULL OR $updated_at >= c.updated_at
SET c.title = case when $title IS NOT NULL then $title else c.title end,
    c.status = case when $status IS NOT NULL then $status else c.status end,
    c.updated_at = case when $updated_at IS NOT NULL then $updated_at else c.updated_at end
RETURN c
"""

MERGE_HAS_CHILD_RELATIONSHIP = """
MATCH (parent:Conversation {conversation_id: $parent_id})
MATCH (child:Conversation {conversation_id: $child_id})
MERGE (parent)-[r:HAS_CHILD]->(child)
RETURN r
"""

MERGE_CREATED_FROM_RELATIONSHIP = """
MATCH (parent:Conversation {conversation_id: $parent_id})
MATCH (child:Conversation {conversation_id: $child_id})
MERGE (child)-[r:CREATED_FROM]->(parent)
RETURN r
"""

DELETE_CONVERSATION_NODE = """
MATCH (c:Conversation {conversation_id: $conversation_id})
DETACH DELETE c
"""

SOFT_DELETE_CONVERSATION_NODE = """
MATCH (c:Conversation {conversation_id: $conversation_id})
WHERE c.updated_at IS NULL OR $updated_at >= c.updated_at
SET c.status = "DELETED",
    c.updated_at = $updated_at
RETURN c
"""

# ---------------------------------------------------------------------------
# Message Node Queries
# ---------------------------------------------------------------------------

CREATE_MESSAGE_CONSTRAINT = """
CREATE CONSTRAINT message_id_unique IF NOT EXISTS
FOR (m:Message)
REQUIRE m.message_id IS UNIQUE
"""

CREATE_USER_NODE_CONSTRAINT = """
CREATE CONSTRAINT user_id_unique IF NOT EXISTS
FOR (u:User)
REQUIRE u.user_id IS UNIQUE
"""

MERGE_MESSAGE_NODE = """
MERGE (m:Message {message_id: $message_id})
ON CREATE SET
    m.conversation_id = $conversation_id,
    m.user_id         = $user_id,
    m.role            = $role,
    m.content         = $content,
    m.occurred_at     = $occurred_at
RETURN m
"""

MERGE_MESSAGE_RELATIONSHIPS = """
MERGE (u:User {user_id: $user_id})
MERGE (c:Conversation {conversation_id: $conversation_id})
MERGE (m:Message {message_id: $message_id})
MERGE (u)-[:SENT]->(m)
MERGE (m)-[:BELONGS_TO]->(c)
"""

# ---------------------------------------------------------------------------
# Idempotency: ProcessedEvent Nodes
# ---------------------------------------------------------------------------

CREATE_PROCESSED_EVENT_CONSTRAINT = """
CREATE CONSTRAINT processed_event_id_unique IF NOT EXISTS
FOR (pe:ProcessedEvent)
REQUIRE pe.event_id IS UNIQUE
"""

MERGE_PROCESSED_EVENT = """
MERGE (pe:ProcessedEvent {event_id: $event_id})
ON CREATE SET pe.processed_at = $processed_at
RETURN pe, (pe.processed_at = $processed_at) AS is_new
"""

GET_PROCESSED_EVENT = """
MATCH (pe:ProcessedEvent {event_id: $event_id})
RETURN pe
"""

