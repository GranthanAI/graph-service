from pydantic import BaseModel
from typing import Optional

class ConversationNodeModel(BaseModel):
    """
    Domain model representing a Conversation node in the graph database.
    """
    conversation_id: str
    user_id: str
    title: str
    status: str
    created_at: str
    updated_at: str
