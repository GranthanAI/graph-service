# Cypher Queries for Traversal

GET_CONVERSATION_NODE = """
MATCH (c:Conversation {conversation_id: $conversation_id})
RETURN c
"""

GET_PARENT = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:CREATED_FROM]->(parent:Conversation)
RETURN parent
"""

GET_CHILDREN = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:HAS_CHILD]->(child:Conversation)
RETURN child
ORDER BY child.created_at ASC, child.conversation_id ASC
SKIP $skip LIMIT $limit
"""

GET_ANCESTORS = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:CREATED_FROM*]->(ancestor:Conversation)
RETURN ancestor
ORDER BY ancestor.created_at ASC, ancestor.conversation_id ASC
SKIP $skip LIMIT $limit
"""

GET_DESCENDANTS = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:HAS_CHILD*]->(descendant:Conversation)
RETURN descendant
ORDER BY descendant.created_at ASC, descendant.conversation_id ASC
SKIP $skip LIMIT $limit
"""
