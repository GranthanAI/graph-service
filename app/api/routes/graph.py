from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Dict, Any
from app.api.dependencies import get_db, get_graph_service
from app.db.neo4j import Neo4jDatabase
from app.services.graph_service import GraphService
from app.api.schemas.conversation import ConversationResponse

router = APIRouter(prefix="/graph", tags=["Graph Operations"])

@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check(
    db: Neo4jDatabase = Depends(get_db),
    service: GraphService = Depends(get_graph_service)
) -> Dict[str, Any]:
    """
    Health check endpoint. Verifies that the service is running and
    that the Neo4j database is reachable.
    """
    is_healthy = db.verify_connectivity()
    if not is_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Neo4j database connectivity check failed."
        )
    message_count = await service.get_message_count()
    return {
        "status": "healthy",
        "database": "Neo4j connected",
        "message_count": message_count
    }


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: str,
    service: GraphService = Depends(get_graph_service)
):
    """
    Retrieve details of a single conversation node.
    """
    return await service.get_conversation(conversation_id)

@router.get("/conversations/{conversation_id}/parent", response_model=ConversationResponse)
async def get_parent(
    conversation_id: str,
    service: GraphService = Depends(get_graph_service)
):
    """
    Retrieve parent conversation of a conversation node.
    """
    return await service.get_parent(conversation_id)

@router.get("/conversations/{conversation_id}/children", response_model=List[ConversationResponse])
async def get_children(
    conversation_id: str,
    skip: int = Query(0, ge=0, description="Number of children to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Max number of children to return"),
    service: GraphService = Depends(get_graph_service)
):
    """
    Retrieve immediate children of a conversation node with pagination.
    """
    return await service.get_children(conversation_id, skip=skip, limit=limit)

@router.get("/conversations/{conversation_id}/ancestors", response_model=List[ConversationResponse])
async def get_ancestors(
    conversation_id: str,
    skip: int = Query(0, ge=0, description="Number of ancestors to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Max number of ancestors to return"),
    service: GraphService = Depends(get_graph_service)
):
    """
    Retrieve all ancestors in the chain of a conversation node with pagination.
    """
    return await service.get_ancestors(conversation_id, skip=skip, limit=limit)

@router.get("/conversations/{conversation_id}/descendants", response_model=List[ConversationResponse])
async def get_descendants(
    conversation_id: str,
    skip: int = Query(0, ge=0, description="Number of descendants to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Max number of descendants to return"),
    service: GraphService = Depends(get_graph_service)
):
    """
    Retrieve all descendants of a conversation node with pagination.
    """
    return await service.get_descendants(conversation_id, skip=skip, limit=limit)
