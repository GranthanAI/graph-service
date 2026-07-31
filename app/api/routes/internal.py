from fastapi import APIRouter, Depends, status
from app.api.dependencies import get_graph_service
from app.services.graph_service import GraphService
from app.api.schemas.internal import MemoryContextResponse

router = APIRouter(prefix="/internal/graph", tags=["Internal Graph Operations"])

@router.get("/conversations/{conversation_id}/memory-context", response_model=MemoryContextResponse, status_code=status.HTTP_200_OK)
async def get_memory_context(
    conversation_id: str,
    service: GraphService = Depends(get_graph_service)
):
    """
    Exposes an internal service-to-service lineage mapping endpoint.
    Computes root conversation and ordered depths list for Memory Services.
    """
    return await service.get_memory_context(conversation_id)
