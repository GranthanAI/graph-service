from fastapi import APIRouter, Depends, Query, status
from app.api.dependencies import get_graph_service
from app.core.security import require_internal_service
from app.services.graph_service import GraphService
from app.api.schemas.internal import MemoryContextResponse

# Service-to-service only: requires the shared service credential, and the
# gateway must never proxy /internal/**.
router = APIRouter(
    prefix="/internal/graph",
    tags=["Internal Graph Operations"],
    dependencies=[Depends(require_internal_service)],
)

@router.get("/conversations/{conversation_id}/memory-context", response_model=MemoryContextResponse, status_code=status.HTTP_200_OK)
async def get_memory_context(
    conversation_id: str,
    user_id: str = Query(..., min_length=1, description="Owner the lineage is being read on behalf of"),
    service: GraphService = Depends(get_graph_service)
):
    """
    Internal lineage mapping endpoint for Memory Service. The conversation must
    belong to `user_id`; otherwise the response is the same 404 as a missing one.
    """
    return await service.get_memory_context(conversation_id, user_id)
