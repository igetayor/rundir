"""
IngestionController — FastAPI route handlers for the rundir ingestion pipeline.

Thin adapter: validates HTTP input, calls IngestionService, returns responses.
No ORM, no pipeline, no business logic here.
"""

import uuid
from typing import Any, Dict, List
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import (
    DeadLetterRecord,
    IngestionJobRequest,
    IngestionJobResponse,
    IngestionStatsResponse,
)
from app.api.services.ingestion_service import IngestionService

router = APIRouter(prefix="/api/v1", tags=["Ingestion Pipeline"])


@router.post(
    "/ingest",
    response_model=IngestionJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Dispatch asynchronous data ingestion job across external streams",
)
async def dispatch_ingestion_job(
    payload: IngestionJobRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
) -> IngestionJobResponse:
    service = IngestionService(db)
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    mock_items = service.build_mock_stream(payload.mock_record_count) if payload.simulate_mock_stream else None
    background_tasks.add_task(
        service.dispatch_job,
        job_id=job_id,
        job_name=payload.job_name,
        sources=payload.sources,
        concurrency=payload.concurrency_limit,
        batch_size=payload.batch_size,
        mock_items=mock_items,
    )
    return IngestionJobResponse(
        job_id=job_id,
        job_name=payload.job_name,
        status="ACCEPTED",
        sources_count=len(payload.sources),
        concurrency_limit=payload.concurrency_limit,
        started_at=__import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        tracking_url="/api/v1/records",
    )


@router.get(
    "/records",
    summary="Retrieve latest normalized records from database",
)
async def list_ingested_records(
    limit: int = Query(default=20, ge=1, le=100),
    category: str = Query(default=None),
    db: AsyncSession = Depends(get_db),
) -> List[Dict[str, Any]]:
    service = IngestionService(db)
    return await service.list_records(limit=limit, category=category)


@router.get(
    "/quarantine",
    response_model=List[DeadLetterRecord],
    summary="Inspect malformed records caught in Dead Letter Queue",
)
async def list_quarantine_records(
    limit: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> List[DeadLetterRecord]:
    service = IngestionService(db)
    return await service.list_quarantine(limit=limit)


@router.get(
    "/stats",
    response_model=IngestionStatsResponse,
    summary="Live pipeline ingestion rate and telemetry",
)
async def get_pipeline_stats(
    db: AsyncSession = Depends(get_db),
) -> IngestionStatsResponse:
    service = IngestionService(db)
    return service.get_stats()
