from pydantic import BaseModel

class LineageNode(BaseModel):
    conversation_id: str
    depth: int

class MemoryContextResponse(BaseModel):
    current_conversation_id: str
    root_conversation_id: str
    lineage_depth: int
    lineage: list[LineageNode]
