from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Graph Service"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    # Neo4j Configurations
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # External services
    REDIS_URL: str = "redis://localhost:6379"
    CASSANDRA_HOSTS: str = "localhost"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONVERSATION_CREATED_TOPIC: str = "conversation.created"
    KAFKA_CONVERSATION_DELETED_TOPIC: str = "conversation.deleted"
    KAFKA_CONVERSATION_UPDATED_TOPIC: str = "conversation.updated"
    KAFKA_CONSUMER_GROUP_ID: str = "graph-service-consumer"
    
    # Consistency policy
    SOFT_DELETE_ENABLED: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Singleton settings instance
settings = Settings()
