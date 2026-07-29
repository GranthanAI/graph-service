from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.config import settings
from app.core.logging import setup_logging
from app.db.neo4j import Neo4jDatabase
from app.api.dependencies import get_repository
from app.api.routes import graph
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup structured logging
    setup_logging()
    logger.info("Starting Graph Service...")
    
    # Establish connection to Neo4j database singleton
    db = Neo4jDatabase()
    try:
        db.connect()
        db.verify_connectivity()
        
        # Apply Neo4j constraints and indexes
        repo = get_repository(db)
        repo.create_constraints_and_indexes()
        logger.info("Neo4j connectivity verified and database schema constraints successfully applied.")
    except Exception as e:
        logger.critical(f"Critical error initializing Neo4j connection on startup: {e}")
        # Crash fast if database cannot be connected on startup
        raise e

    yield

    # Clean shutdown of DB connections
    logger.info("Shutting down Graph Service...")
    db.close()
    logger.info("Graph Service successfully stopped.")

app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan
)

# Register routes
app.include_router(graph.router)

@app.get("/")
def read_root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}!"}
