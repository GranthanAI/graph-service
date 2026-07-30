from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from neo4j import Driver
from app.db.cypher.conversation_queries import (
    CREATE_CONVERSATION_CONSTRAINT,
    CREATE_USER_INDEX,
    MERGE_CONVERSATION_NODE,
    MERGE_HAS_CHILD_RELATIONSHIP,
    MERGE_CREATED_FROM_RELATIONSHIP,
    DELETE_CONVERSATION_NODE,
    UPDATE_CONVERSATION_NODE,
    SOFT_DELETE_CONVERSATION_NODE
)
from app.db.cypher.traversal_queries import (
    GET_CONVERSATION_NODE,
    GET_PARENT,
    GET_CHILDREN,
    GET_ANCESTORS,
    GET_DESCENDANTS
)

class BaseGraphRepository(ABC):
    """
    Abstract base class for Graph Service repository operations (Dependency Inversion).
    """
    @abstractmethod
    def create_constraints_and_indexes(self) -> None:
        pass

    @abstractmethod
    def create_conversation_node(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_relationships(self, parent_id: str, child_id: str) -> None:
        pass

    @abstractmethod
    def delete_conversation(self, conversation_id: str) -> None:
        pass

    @abstractmethod
    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_parent(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_children(self, conversation_id: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_ancestors(self, conversation_id: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_descendants(self, conversation_id: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def create_conversation_with_parent(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str,
        parent_id: Optional[str] = None
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def soft_delete_conversation(self, conversation_id: str, updated_at: str) -> None:
        pass

    @abstractmethod
    def update_conversation_node(
        self,
        conversation_id: str,
        title: Optional[str],
        status: Optional[str],
        updated_at: Optional[str]
    ) -> Dict[str, Any]:
        pass


class Neo4jGraphRepository(BaseGraphRepository):
    """
    Concrete implementation of Graph Repository for Neo4j database.
    """
    def __init__(self, driver: Driver) -> None:
        self.driver = driver

    def create_constraints_and_indexes(self) -> None:
        with self.driver.session() as session:
            session.run(CREATE_CONVERSATION_CONSTRAINT)
            session.run(CREATE_USER_INDEX)

    def create_conversation_node(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            result = session.run(
                MERGE_CONVERSATION_NODE,
                conversation_id=conversation_id,
                user_id=user_id,
                title=title,
                status=status,
                created_at=created_at,
                updated_at=updated_at
            )
            record = result.single()
            if record:
                return dict(record["c"])
            return {}

    def create_relationships(self, parent_id: str, child_id: str) -> None:
        with self.driver.session() as session:
            session.run(MERGE_HAS_CHILD_RELATIONSHIP, parent_id=parent_id, child_id=child_id)
            session.run(MERGE_CREATED_FROM_RELATIONSHIP, parent_id=parent_id, child_id=child_id)

    def delete_conversation(self, conversation_id: str) -> None:
        with self.driver.session() as session:
            session.run(DELETE_CONVERSATION_NODE, conversation_id=conversation_id)

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(GET_CONVERSATION_NODE, conversation_id=conversation_id)
            record = result.single()
            if record:
                return dict(record["c"])
            return None

    def get_parent(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(GET_PARENT, conversation_id=conversation_id)
            record = result.single()
            if record:
                return dict(record["parent"])
            return None

    def get_children(self, conversation_id: str) -> List[Dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(GET_CHILDREN, conversation_id=conversation_id)
            return [dict(record["child"]) for record in result]

    def get_ancestors(self, conversation_id: str) -> List[Dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(GET_ANCESTORS, conversation_id=conversation_id)
            return [dict(record["ancestor"]) for record in result]

    def get_descendants(self, conversation_id: str) -> List[Dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(GET_DESCENDANTS, conversation_id=conversation_id)
            return [dict(record["descendant"]) for record in result]

    def create_conversation_with_parent(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str,
        parent_id: Optional[str] = None
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            def _tx(tx):
                # 1. Merge child conversation node
                result = tx.run(
                    MERGE_CONVERSATION_NODE,
                    conversation_id=conversation_id,
                    user_id=user_id,
                    title=title,
                    status=status,
                    created_at=created_at,
                    updated_at=updated_at
                )
                record = result.single()
                child_node = dict(record["c"]) if record else {}

                # 2. If parent_id exists, merge parent stub node and relationships
                # Use Unix Epoch for stub creation timestamps so real events can override it
                if parent_id:
                    tx.run(
                        MERGE_CONVERSATION_NODE,
                        conversation_id=parent_id,
                        user_id=user_id,
                        title="Stub Conversation",
                        status="ACTIVE",
                        created_at="1970-01-01T00:00:00Z",
                        updated_at="1970-01-01T00:00:00Z"
                    )
                    tx.run(MERGE_HAS_CHILD_RELATIONSHIP, parent_id=parent_id, child_id=conversation_id)
                    tx.run(MERGE_CREATED_FROM_RELATIONSHIP, parent_id=parent_id, child_id=conversation_id)
                
                return child_node

            return session.execute_write(_tx)

    def soft_delete_conversation(self, conversation_id: str, updated_at: str) -> None:
        with self.driver.session() as session:
            session.run(SOFT_DELETE_CONVERSATION_NODE, conversation_id=conversation_id, updated_at=updated_at)

    def update_conversation_node(
        self,
        conversation_id: str,
        title: Optional[str],
        status: Optional[str],
        updated_at: Optional[str]
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            result = session.run(
                UPDATE_CONVERSATION_NODE,
                conversation_id=conversation_id,
                title=title,
                status=status,
                updated_at=updated_at
            )
            record = result.single()
            if record:
                return dict(record["c"])
            return {}
