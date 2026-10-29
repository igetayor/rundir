"""
IngestionService — business logic for the rundir ingestion pipeline.

Orchestrates job dispatch, record retrieval, and telemetry reporting.
Controllers depend on this; pipeline and repository are injected.
"""

import time
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.pipeline import pipeline
from app.schemas import IngestionJobResponse, IngestionStatsResponse, DeadLetterRecord
from app.api.repositories.ingestion_repository import IngestionRepository


class IngestionService:
    def __init__(self, db: AsyncSession):
        self._repo = IngestionRepository(db)

    def build_mock_stream(self, count: int) -> List[Any]:
        """Synthesize mock payloads including deliberate 5% malformed entries for DLQ testing."""
        items = []
        for i in range(count):
            if i % 20 == 0:
                items.append("malformed_string_payload_triggering_dlq_boundary")
            else:
                items.append({
                    "id": f"event_{uuid.uuid4().hex[:10]}",
                    "timestamp": time.time(),
                    "category": "blockchain_tick" if i % 2 == 0 else "iot_telemetry",
                    "value": round(10.5 + (i * 1.5), 2),
                    "device_id": f"dev_{i % 5}",
                    "metadata": {"batch_seq": i},
                })
        return items

    def dispatch_job(
        self,
        job_id: str,
        job_name: str,
        sources: List[str],
        concurrency: int,
        batch_size: int,
        mock_items: Optional[List[Any]],
    ) -> IngestionJobResponse:
        pipeline.run_ingestion_job(
            job_id=job_id,
            sources=sources,
            concurrency=concurrency,
            batch_size=batch_size,
            mock_items=mock_items,
        )
        return IngestionJobResponse(
            job_id=job_id,
            job_name=job_name,
            status="ACCEPTED",
            sources_count=len(sources),
            concurrency_limit=concurrency,
            started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            tracking_url="/api/v1/records",
        )

    async def list_records(self, limit: int, category: Optional[str]) -> List[Dict[str, Any]]:
        records = await self._repo.list_records(limit=limit, category=category)
        return [r.to_dict() for r in records]

    async def list_quarantine(self, limit: int) -> List[DeadLetterRecord]:
        items = await self._repo.list_quarantine(limit=limit)
        return [DeadLetterRecord(**item.to_dict()) for item in items]

    def get_stats(self) -> IngestionStatsResponse:
        return IngestionStatsResponse(**pipeline.get_stats())
