import asyncio
import time
import uuid
from typing import Any, Dict, List
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response, status
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import DeadLetterPayload, IngestedRecord
from app.pipeline import pipeline
from app.schemas import (
    DeadLetterRecord,
    IngestionJobRequest,
    IngestionJobResponse,
    IngestionStatsResponse,
)

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
) -> IngestionJobResponse:
    job_id = f"job_{uuid.uuid4().hex[:12]}"

    # Synthesize synthetic payloads if demo flag is enabled
    mock_items = None
    if payload.simulate_mock_stream:
        mock_items = [
            {
                "id": f"event_{uuid.uuid4().hex[:10]}",
                "timestamp": time.time(),
                "category": "blockchain_tick" if i % 2 == 0 else "iot_telemetry",
                "value": round(10.5 + (i * 1.5), 2),
                "device_id": f"dev_{i % 5}",
                "metadata": {"batch_seq": i},
            }
            # Inject 5% deliberate malformed payloads to exercise quarantine boundary
            if i % 20 != 0
            else "malformed_string_payload_triggering_dlq_boundary"
            for i in range(payload.mock_record_count)
        ]

    # Run in background task
    background_tasks.add_task(
        pipeline.run_ingestion_job,
        job_id=job_id,
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
        started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        tracking_url=f"/api/v1/records",
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
    stmt = select(IngestedRecord).order_by(desc(IngestedRecord.id))
    if category:
        stmt = stmt.where(IngestedRecord.category == category)
    stmt = stmt.limit(limit)
    res = await db.execute(stmt)
    records = res.scalars().all()
    return [r.to_dict() for r in records]


@router.get(
    "/quarantine",
    response_model=List[DeadLetterRecord],
    summary="Inspect malformed records caught in Dead Letter Queue",
)
async def list_quarantine_records(
    limit: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> List[DeadLetterRecord]:
    stmt = select(DeadLetterPayload).order_by(desc(DeadLetterPayload.id)).limit(limit)
    res = await db.execute(stmt)
    items = res.scalars().all()
    return [DeadLetterRecord(**item.to_dict()) for item in items]


@router.get(
    "/stats",
    response_model=IngestionStatsResponse,
    summary="Live pipeline ingestion rate and telemetry",
)
async def get_pipeline_stats() -> IngestionStatsResponse:
    stats = pipeline.get_stats()
    return IngestionStatsResponse(**stats)
