import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ExternalStreamSource(BaseModel):
    """External API endpoint configuration for ingestion."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_id: str = Field(..., min_length=3, max_length=64, description="Unique source identifier")
    url: str = Field(..., description="REST endpoint URL")
    auth_header: Optional[str] = Field(default=None, description="Optional Bearer/API token")
    data_format: str = Field(default="json", description="Expected format: json, csv")
    rate_limit_per_min: int = Field(default=60, ge=1, le=1000)

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class NormalizedRecord(BaseModel):
    """Normalized structured data model ready for bulk database insertion."""

    entity_id: str = Field(..., min_length=1, max_length=128)
    source_id: str = Field(..., min_length=1, max_length=64)
    event_timestamp: datetime
    category: str = Field(default="general", max_length=64)
    payload_hash: str = Field(..., min_length=8, max_length=64)
    numeric_value: float = Field(default=0.0)
    raw_payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class IngestionJobRequest(BaseModel):
    """Request to initiate asynchronous ingestion from one or more streams."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    job_name: str = Field(..., min_length=3, max_length=64)
    sources: List[ExternalStreamSource] = Field(..., min_length=1, max_length=20)
    concurrency_limit: int = Field(default=4, ge=1, le=16)
    batch_size: int = Field(default=50, ge=10, le=500)
    simulate_mock_stream: bool = Field(default=False, description="Generate high-volume synthetic payloads for demonstration")
    mock_record_count: int = Field(default=500, ge=10, le=10000)


class IngestionJobResponse(BaseModel):
    """Instant acknowledgement of ingestion job dispatch."""

    job_id: str
    job_name: str
    status: str
    sources_count: int
    concurrency_limit: int
    started_at: datetime
    tracking_url: str


class DeadLetterRecord(BaseModel):
    """Quarantined malformed payload record."""

    id: int
    source_id: str
    error_reason: str
    raw_data: str
    quarantined_at: str


class IngestionStatsResponse(BaseModel):
    """Live telemetry on pipeline throughput and error boundaries."""

    total_records_fetched: int
    total_records_normalized: int
    total_records_inserted: int
    total_records_quarantined: int
    active_workers: int
    completed_jobs: int
    ingestion_rate_rec_sec: float
