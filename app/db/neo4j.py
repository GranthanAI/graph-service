import threading
from typing import Optional
from neo4j import GraphDatabase, Driver
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class Neo4jDatabase:
    """
    Neo4j connection manager implemented as a thread-safe Singleton.
    Manages the lifecycle of the official Neo4j driver.
    """
    _instance: Optional["Neo4jDatabase"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(Neo4jDatabase, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        
        self.uri = settings.NEO4J_URI
        self.user = settings.NEO4J_USER
        self.password = settings.NEO4J_PASSWORD
        self._driver: Optional[Driver] = None
        self._initialized = True

    def connect(self) -> None:
        """Initializes the Neo4j driver."""
        if not self._driver:
            try:
                self._driver = GraphDatabase.driver(
                    self.uri,
                    auth=(self.user, self.password)
                )
                logger.info(f"Connected to Neo4j at {self.uri}")
            except Exception as e:
                logger.error(f"Could not connect to Neo4j at {self.uri}: {e}")
                raise

    def close(self) -> None:
        """Closes the Neo4j driver session pool."""
        if self._driver:
            try:
                self._driver.close()
                logger.info("Neo4j driver connection closed.")
            except Exception as e:
                logger.error(f"Error closing Neo4j driver: {e}")
            finally:
                self._driver = None

    def get_driver(self) -> Driver:
        """Returns the active Neo4j driver instance, initializing if necessary."""
        if not self._driver:
            self.connect()
        return self._driver

    def verify_connectivity(self) -> bool:
        """Checks if the database is reachable and accepting connections."""
        try:
            drv = self.get_driver()
            # verify_connectivity sends a quick ping/request to the server
            drv.verify_connectivity()
            return True
        except Exception as e:
            logger.error(f"Neo4j connectivity check failed: {e}")
            return False
