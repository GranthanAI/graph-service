from pydantic import BaseModel
from typing import Optional

class ConversationResponse(BaseModel):
    conversation_id: str
    user_id: str
    title: str
    status: str
    created_at: str
    updated_at: str
