# Cypher Queries for Traversal
#
# A conversation deleted in Conversation Service is soft-deleted here (status = "DELETED"; the node and its HAS_CHILD /
# CREATED_FROM edges stay so lineage history is not rewritten). Navigation lists (children / ancestors / descendants) must not
# offer it: opening it fails because the conversation no longer exists. GET_MEMORY_CONTEXT is deliberately not filtered.

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
WHERE coalesce(child.status, "") <> "DELETED"
RETURN child
ORDER BY child.created_at ASC, child.conversation_id ASC
SKIP $skip LIMIT $limit
"""

GET_ANCESTORS = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:CREATED_FROM*]->(ancestor:Conversation)
WHERE coalesce(ancestor.status, "") <> "DELETED"
RETURN ancestor
ORDER BY ancestor.created_at ASC, ancestor.conversation_id ASC
SKIP $skip LIMIT $limit
"""

GET_DESCENDANTS = """
MATCH (c:Conversation {conversation_id: $conversation_id})-[:HAS_CHILD*]->(descendant:Conversation)
WHERE coalesce(descendant.status, "") <> "DELETED"
RETURN descendant
ORDER BY descendant.created_at ASC, descendant.conversation_id ASC
SKIP $skip LIMIT $limit
"""

GET_MEMORY_CONTEXT = """
MATCH (c:Conversation {conversation_id: $conversation_id})
MATCH path = (root:Conversation)-[:HAS_CHILD*0..]->(c)
WHERE NOT ()-[:HAS_CHILD]->(root)
RETURN nodes(path) as lineage
LIMIT 1
"""
