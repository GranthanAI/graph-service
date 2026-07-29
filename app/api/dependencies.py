from fastapi import Depends
from neo4j import Driver
from app.db.neo4j import Neo4jDatabase
from app.repositories.graph_repository import BaseGraphRepository, Neo4jGraphRepository

def get_db() -> Neo4jDatabase:
    """
    Dependency resolver for the Neo4jDatabase connection manager.
    Returns the Singleton database connection manager instance.
    """
    return Neo4jDatabase()

def get_repository(db: Neo4jDatabase = Depends(get_db)) -> BaseGraphRepository:
    """
    Dependency resolver for the repository layer.
    Acts as a Factory Method to construct the BaseGraphRepository implementation.
    """
    driver: Driver = db.get_driver()
    return Neo4jGraphRepository(driver)
