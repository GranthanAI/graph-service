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
        
        # Instantiate service and background worker
        from app.services.graph_service import GraphService
        from app.workers.consumer_worker import KafkaConsumerWorker
        
        graph_service = GraphService(repo)
        app.state.consumer_worker = KafkaConsumerWorker(graph_service)
        await app.state.consumer_worker.start()
    except Exception as e:
        logger.critical(f"Critical error initializing Neo4j connection on startup: {e}")
        # Crash fast if database cannot be connected on startup
        raise e

    yield

    # Clean shutdown of DB connections and consumer worker
    logger.info("Shutting down Graph Service...")
    if hasattr(app.state, "consumer_worker"):
        await app.state.consumer_worker.stop()
    db.close()
    logger.info("Graph Service successfully stopped.")

app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan
)

# Register custom exception handler
from app.core.exceptions import NodeNotFoundError
from fastapi import Request
from fastapi.responses import JSONResponse

@app.exception_handler(NodeNotFoundError)
def node_not_found_exception_handler(request: Request, exc: NodeNotFoundError):
    return JSONResponse(
        status_code=404,
        content={"detail": str(exc)}
    )

# Register routes
app.include_router(graph.router)

@app.get("/")
def read_root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}!"}
