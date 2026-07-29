from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from neo4j import Driver
from app.db.cypher.conversation_queries import (
    CREATE_CONVERSATION_CONSTRAINT,
    CREATE_USER_INDEX,
    MERGE_CONVERSATION_NODE,
    MERGE_HAS_CHILD_RELATIONSHIP,
    MERGE_CREATED_FROM_RELATIONSHIP,
    DELETE_CONVERSATION_NODE
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
