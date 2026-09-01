"""
app/grpc/graph_context_handler.py

gRPC handler serving the Graph Service context API consumed by the LLM
Service's ContextCollector (llm-service/proto/graph.proto). Backed entirely
by the existing conversation-lineage Neo4j model (ancestors/descendants) —
this service has no generic entity/knowledge-graph model, so relevance
scoring and free-text query matching are not implemented; this is an honest,
degenerate mapping of "graph context" onto "conversation lineage", not a
fabricated knowledge-graph feature.
"""

import asyncio
import logging
import uuid

import grpc

from app.proto import graph_pb2, graph_pb2_grpc
from app.services.graph_service import GraphService

logger = logging.getLogger("graph_service.grpc.graph_context_handler")

_DEFAULT_MAX_NODES = 50


def _extract_trace_id(context) -> str:
    for key, val in (context.invocation_metadata() or []):
        if key.lower() in ("x-trace-id", "x-request-id"):
            return val
    return str(uuid.uuid4())


def _conversation_to_node(conv: dict) -> "graph_pb2.GraphNode":
    conversation_id = str(conv.get("conversation_id", ""))
    title = conv.get("title") or conversation_id
    return graph_pb2.GraphNode(
        node_id=conversation_id,
        label=title,
        node_type="conversation",
        properties={"status": str(conv.get("status", ""))},
        relevance_score=1.0,
    )


class GraphContextHandler(graph_pb2_grpc.GraphServiceServicer):
    """
    gRPC handler for GraphService.GetGraphContext / GetNodesByIds.
    Delegates to the existing Neo4jGraphRepository lineage queries.
    """

    def __init__(self, graph_service: GraphService):
        self.graph_service = graph_service

    async def GetGraphContext(self, request, context):
        trace_id = _extract_trace_id(context)
        try:
            logger.info(
                f"gRPC GetGraphContext request received for conversation={request.conversation_id}"
            )
            limit = request.max_nodes or _DEFAULT_MAX_NODES
            depth = request.max_depth or 100

            ancestors, descendants = await asyncio.gather(
                asyncio.to_thread(
                    self.graph_service.repo.get_ancestors,
                    request.conversation_id,
                    0,
                    min(limit, depth),
                ),
                asyncio.to_thread(
                    self.graph_service.repo.get_descendants,
                    request.conversation_id,
                    0,
                    min(limit, depth),
                ),
            )

            nodes = [_conversation_to_node(c) for c in ancestors] + [
                _conversation_to_node(c) for c in descendants
            ]

            relationships = []
            # Ancestors are returned root-first ending at the current conversation's
            # parent; chain them, then link the last ancestor to the current node.
            chain = ancestors + [{"conversation_id": request.conversation_id}]
            for parent, child in zip(chain, chain[1:]):
                relationships.append(
                    graph_pb2.GraphRelationship(
                        from_node_id=str(parent.get("conversation_id", "")),
                        to_node_id=str(child.get("conversation_id", "")),
                        relationship_type="HAS_CHILD",
                    )
                )
            for descendant in descendants:
                relationships.append(
                    graph_pb2.GraphRelationship(
                        from_node_id=request.conversation_id,
                        to_node_id=str(descendant.get("conversation_id", "")),
                        relationship_type="HAS_CHILD",
                    )
                )

            summary = (
                f"{len(nodes)} related conversation(s) in lineage "
                f"({len(ancestors)} ancestor(s), {len(descendants)} descendant(s))."
            )

            return graph_pb2.GetGraphContextResponse(
                nodes=nodes,
                relationships=relationships,
                subgraph_summary=summary,
                total_tokens=0,
                request_id=trace_id,
            )
        except Exception as e:
            logger.error(f"gRPC GetGraphContext failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            raise

    async def GetNodesByIds(self, request, context):
        try:
            logger.info(f"gRPC GetNodesByIds request received for {len(request.node_ids)} id(s)")
            results = await asyncio.gather(
                *[
                    asyncio.to_thread(self.graph_service.repo.get_conversation, node_id)
                    for node_id in request.node_ids
                ]
            )
            nodes = [_conversation_to_node(conv) for conv in results if conv]
            return graph_pb2.GetNodesByIdsResponse(nodes=nodes)
        except Exception as e:
            logger.error(f"gRPC GetNodesByIds failed: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            raise
