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
ON MATCH SET c.title = $title,
             c.status = $status,
             c.updated_at = $updated_at
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
