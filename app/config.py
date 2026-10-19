import os
from functools import lru_cache
from typing import List
from pydantic import BaseModel, ConfigDict, Field


class Settings(BaseModel):
    """Configuration for Rundir Ingestion and Transformation Engine."""

    model_config = ConfigDict(extra="ignore")

    # Core App
    app_name: str = Field(default="Rundir Ingestion & Transformation Pipeline", description="Service name")
    app_version: str = Field(default="1.0.0", description="API version")
    environment: str = Field(default="development", description="Runtime environment")
    debug: bool = Field(default=False, description="Debug mode")
    host: str = Field(default="0.0.0.0", description="HTTP host")
    port: int = Field(default=8086, description="HTTP port")
    cors_origins: List[str] = Field(default=["*"], description="Allowed CORS origins")

    # Ingestion & Concurrency Limits
    ingestion_worker_concurrency: int = Field(default=8, description="Max concurrent ingestion workers")
    ingestion_batch_size: int = Field(default=100, description="Records per batch bulk-insert")
    ingestion_fetch_timeout_sec: float = Field(default=10.0, description="HTTP request timeout in seconds")
    ingestion_max_retries: int = Field(default=3, description="HTTP retry attempts with backoff")

    # Database Configuration (PostgreSQL / asyncpg, with sqlite fallback)
    database_url: str = Field(
        default="sqlite+aiosqlite:///./rundir_ingestion.db",
        description="Async database connection string",
    )
    db_pool_size: int = Field(default=10, description="Database connection pool size")
    db_max_overflow: int = Field(default=20, description="Database connection overflow size")

    # Logging
    log_level: str = Field(default="INFO", description="Log level")
    log_json_format: bool = Field(default=True, description="Structured JSON logging")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
