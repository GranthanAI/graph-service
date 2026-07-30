.PHONY: install docker-up docker-down run test clean lint help

# Default target
help:
	@echo "Graph Service Makefile targets:"
	@echo "  install      Install python dependencies"
	@echo "  docker-up    Start Neo4j docker container"
	@echo "  docker-down  Stop Neo4j docker container"
	@echo "  run          Run the FastAPI application locally"
	@echo "  test         Run unit tests using pytest"
	@echo "  clean        Clean temporary python files and cache"

install:
	uv pip install -r requirements.txt

docker-up:
	docker compose up -d

docker-down:
	docker compose down

run:
	uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

test:
	uv run pytest

clean:
	rm -rf __pycache__ .pytest_cache .coverage

