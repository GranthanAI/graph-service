from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from neo4j import Driver
from app.db.cypher.conversation_queries import (
    CREATE_CONVERSATION_CONSTRAINT,
    CREATE_USER_INDEX,
    CREATE_MESSAGE_CONSTRAINT,
    CREATE_USER_NODE_CONSTRAINT,
    CREATE_PROCESSED_EVENT_CONSTRAINT,
    MERGE_CONVERSATION_NODE,
    MERGE_HAS_CHILD_RELATIONSHIP,
    MERGE_CREATED_FROM_RELATIONSHIP,
    MERGE_MESSAGE_NODE,
    MERGE_MESSAGE_RELATIONSHIPS,
    MERGE_PROCESSED_EVENT,
    GET_PROCESSED_EVENT,
    DELETE_CONVERSATION_NODE,
    UPDATE_CONVERSATION_NODE,
    SOFT_DELETE_CONVERSATION_NODE,
)
from app.db.cypher.traversal_queries import (
    GET_CONVERSATION_NODE,
    GET_PARENT,
    GET_CHILDREN,
    GET_ANCESTORS,
    GET_DESCENDANTS,
)


class BaseGraphRepository(ABC):
    """
    Abstract base class for Graph Service repository operations (Dependency Inversion).
    """

    # --- Schema bootstrap ---

    @abstractmethod
    def create_constraints_and_indexes(self) -> None:
        pass

    # --- Conversation operations ---

    @abstractmethod
    def create_conversation_node(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str,
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
    def get_children(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_ancestors(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_descendants(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
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
        parent_id: Optional[str] = None,
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
        updated_at: Optional[str],
    ) -> Dict[str, Any]:
        pass

    # --- Message operations ---

    @abstractmethod
    def create_message_node(
        self,
        message_id: str,
        conversation_id: str,
        user_id: str,
        role: str,
        content: str,
        occurred_at: str,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_message_relationships(
        self,
        message_id: str,
        conversation_id: str,
        user_id: str,
    ) -> None:
        pass

    # --- Idempotency operations ---

    @abstractmethod
    def is_event_processed(self, event_id: str) -> bool:
        pass

    @abstractmethod
    def mark_event_processed(self, event_id: str) -> None:
        pass

    @abstractmethod
    def get_message_count(self) -> int:
        pass



class Neo4jGraphRepository(BaseGraphRepository):
    """
    Concrete implementation of Graph Repository for Neo4j database.
    """

    def __init__(self, driver: Driver) -> None:
        self.driver = driver

    # --- Schema bootstrap ---

    def create_constraints_and_indexes(self) -> None:
        with self.driver.session() as session:
            session.run(CREATE_CONVERSATION_CONSTRAINT)
            session.run(CREATE_USER_INDEX)
            session.run(CREATE_MESSAGE_CONSTRAINT)
            session.run(CREATE_USER_NODE_CONSTRAINT)
            session.run(CREATE_PROCESSED_EVENT_CONSTRAINT)

    # --- Conversation operations ---

    def create_conversation_node(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str,
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            result = session.run(
                MERGE_CONVERSATION_NODE,
                conversation_id=conversation_id,
                user_id=user_id,
                title=title,
                status=status,
                created_at=created_at,
                updated_at=updated_at,
            )
            record = result.single()
            return dict(record["c"]) if record else {}

    def create_relationships(self, parent_id: str, child_id: str) -> None:
        with self.driver.session() as session:
            session.run(MERGE_HAS_CHILD_RELATIONSHIP, parent_id=parent_id, child_id=child_id)
            session.run(MERGE_CREATED_FROM_RELATIONSHIP, parent_id=parent_id, child_id=child_id)

    def delete_conversation(self, conversation_id: str) -> None:
        with self.driver.session() as session:
            session.run(DELETE_CONVERSATION_NODE, conversation_id=conversation_id)

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        def _work(tx):
            result = tx.run(GET_CONVERSATION_NODE, conversation_id=conversation_id)
            record = result.single()
            return dict(record["c"]) if record else None
        with self.driver.session() as session:
            return session.execute_read(_work)

    def get_parent(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        def _work(tx):
            result = tx.run(GET_PARENT, conversation_id=conversation_id)
            record = result.single()
            return dict(record["parent"]) if record else None
        with self.driver.session() as session:
            return session.execute_read(_work)

    def get_children(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        def _work(tx):
            result = tx.run(GET_CHILDREN, conversation_id=conversation_id, skip=skip, limit=limit)
            return [dict(record["child"]) for record in result]
        with self.driver.session() as session:
            return session.execute_read(_work)

    def get_ancestors(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        def _work(tx):
            result = tx.run(GET_ANCESTORS, conversation_id=conversation_id, skip=skip, limit=limit)
            return [dict(record["ancestor"]) for record in result]
        with self.driver.session() as session:
            return session.execute_read(_work)

    def get_descendants(self, conversation_id: str, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        def _work(tx):
            result = tx.run(GET_DESCENDANTS, conversation_id=conversation_id, skip=skip, limit=limit)
            return [dict(record["descendant"]) for record in result]
        with self.driver.session() as session:
            return session.execute_read(_work)

    def create_conversation_with_parent(
        self,
        conversation_id: str,
        user_id: str,
        title: str,
        status: str,
        created_at: str,
        updated_at: str,
        parent_id: Optional[str] = None,
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
                    updated_at=updated_at,
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
                        updated_at="1970-01-01T00:00:00Z",
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
        updated_at: Optional[str],
    ) -> Dict[str, Any]:
        with self.driver.session() as session:
            result = session.run(
                UPDATE_CONVERSATION_NODE,
                conversation_id=conversation_id,
                title=title,
                status=status,
                updated_at=updated_at,
            )
            record = result.single()
            return dict(record["c"]) if record else {}

    # --- Message operations ---

    def create_message_node(
        self,
        message_id: str,
        conversation_id: str,
        user_id: str,
        role: str,
        content: str,
        occurred_at: str,
    ) -> Dict[str, Any]:
        """
        Merges a Message node into Neo4j. ON CREATE only — duplicate event_id
        will be caught by idempotency check before this method is called.
        """
        def _tx(tx):
            result = tx.run(
                MERGE_MESSAGE_NODE,
                message_id=message_id,
                conversation_id=conversation_id,
                user_id=user_id,
                role=role,
                content=content,
                occurred_at=occurred_at,
            )
            record = result.single()
            return dict(record["m"]) if record else {}

        with self.driver.session() as session:
            return session.execute_write(_tx)

    def create_message_relationships(
        self,
        message_id: str,
        conversation_id: str,
        user_id: str,
    ) -> None:
        """
        Creates:
            (:User {user_id})-[:SENT]->(:Message {message_id})-[:BELONGS_TO]->(:Conversation {conversation_id})

        All three nodes are MERGEd so this is safe to call even if the conversation
        node arrives before the message or vice-versa.
        """
        with self.driver.session() as session:
            def _tx(tx):
                tx.run(
                    MERGE_MESSAGE_RELATIONSHIPS,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    message_id=message_id,
                )

            session.execute_write(_tx)

    # --- Idempotency operations ---

    def is_event_processed(self, event_id: str) -> bool:
        """
        Returns True if this event_id has already been fully processed.
        Uses a Neo4j MATCH (no write) so it is safe to call from the consumer loop.
        """
        def _work(tx):
            result = tx.run(GET_PROCESSED_EVENT, event_id=event_id)
            return result.single() is not None

        with self.driver.session() as session:
            return session.execute_read(_work)

    def mark_event_processed(self, event_id: str) -> None:
        """
        Merges a ProcessedEvent node so this event_id is permanently marked done.
        The unique constraint on event_id guarantees exactly-once writes even
        under concurrent consumer replicas.
        """
        processed_at = datetime.now(timezone.utc).isoformat()

        def _tx(tx):
            tx.run(MERGE_PROCESSED_EVENT, event_id=event_id, processed_at=processed_at)

        with self.driver.session() as session:
            session.execute_write(_tx)

    def get_message_count(self) -> int:
        """
        Returns total number of Message nodes in Neo4j.
        """
        def _work(tx):
            result = tx.run("MATCH (m:Message) RETURN count(m) as count")
            record = result.single()
            return record["count"] if record else 0
        with self.driver.session() as session:
            return session.execute_read(_work)

