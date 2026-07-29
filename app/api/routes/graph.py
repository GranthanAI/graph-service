from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any
from app.api.dependencies import get_db, get_repository
from app.db.neo4j import Neo4jDatabase
from app.repositories.graph_repository import BaseGraphRepository
from app.api.schemas.conversation import ConversationResponse

router = APIRouter(prefix="/graph", tags=["Graph Operations"])

@router.get("/health", status_code=status.HTTP_200_OK)
def health_check(db: Neo4jDatabase = Depends(get_db)) -> Dict[str, str]:
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
    return {"status": "healthy", "database": "Neo4j connected"}

@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
def get_conversation(
    conversation_id: str,
    repo: BaseGraphRepository = Depends(get_repository)
):
    """
    Retrieve details of a single conversation node.
    """
    node = repo.get_conversation(conversation_id)
    if not node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conversation {conversation_id} not found."
        )
    return node

@router.get("/conversations/{conversation_id}/parent", response_model=ConversationResponse)
def get_parent(
    conversation_id: str,
    repo: BaseGraphRepository = Depends(get_repository)
):
    """
    Retrieve parent conversation of a conversation node.
    """
    parent = repo.get_parent(conversation_id)
    if not parent:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Parent for conversation {conversation_id} not found."
        )
    return parent

@router.get("/conversations/{conversation_id}/children", response_model=List[ConversationResponse])
def get_children(
    conversation_id: str,
    repo: BaseGraphRepository = Depends(get_repository)
):
    """
    Retrieve immediate children of a conversation node.
    """
    return repo.get_children(conversation_id)

@router.get("/conversations/{conversation_id}/ancestors", response_model=List[ConversationResponse])
def get_ancestors(
    conversation_id: str,
    repo: BaseGraphRepository = Depends(get_repository)
):
    """
    Retrieve all ancestors in the chain of a conversation node.
    """
    return repo.get_ancestors(conversation_id)

@router.get("/conversations/{conversation_id}/descendants", response_model=List[ConversationResponse])
def get_descendants(
    conversation_id: str,
    repo: BaseGraphRepository = Depends(get_repository)
):
    """
    Retrieve all descendants of a conversation node.
    """
    return repo.get_descendants(conversation_id)
