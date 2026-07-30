from pydantic import BaseModel
from typing import Optional


class ConversationResponse(BaseModel):
    """
    API response schema for a Conversation node retrieved from Neo4j.
    """
    conversation_id:        str
    user_id:                str
    title:                  str
    status:                 str
    created_at:             str
    updated_at:             str
    parent_conversation_id: Optional[str] = None
