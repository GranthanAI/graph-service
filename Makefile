.PHONY: install infra-up infra-down infra-status infra-logs-kafka run test clean lint help

# Default target
help:
	@echo "Graph Service Makefile targets:"
	@echo "  install           Install python dependencies"
	@echo "  infra-up          Start shared GraphGPT docker containers (Kafka, Redis, Neo4j, Cassandra, Milvus)"
	@echo "  infra-down        Stop shared GraphGPT docker containers"
	@echo "  infra-status      Check status of shared GraphGPT containers"
	@echo "  infra-logs-kafka  Follow Kafka container logs"
	@echo "  run               Run the FastAPI application locally"
	@echo "  test              Run unit tests using pytest"
	@echo "  clean             Clean temporary python files and cache"

install:
	uv pip install -r requirements.txt

# Shared infrastructure lives in the root-level ../docker-compose.yml
infra-up:
	docker compose -f ../docker-compose.yml up -d

infra-down:
	docker compose -f ../docker-compose.yml down

infra-status:
	docker compose -f ../docker-compose.yml ps

infra-logs-kafka:
	docker compose -f ../docker-compose.yml logs -f kafka

run:
	uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

test:
	uv run pytest

clean:
	rm -rf __pycache__ .pytest_cache .coverage

