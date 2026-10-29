"""
IngestionRepository — database access layer for IngestedRecord and DeadLetterPayload.

All raw ORM queries are isolated here; services call this layer.
"""

from typing import List, Optional
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DeadLetterPayload, IngestedRecord


class IngestionRepository:
    def __init__(self, db: AsyncSession):
        self._db = db

    async def list_records(self, limit: int = 20, category: Optional[str] = None) -> List[IngestedRecord]:
        stmt = select(IngestedRecord).order_by(desc(IngestedRecord.id))
        if category:
            stmt = stmt.where(IngestedRecord.category == category)
        stmt = stmt.limit(limit)
        result = await self._db.execute(stmt)
        return list(result.scalars().all())

    async def list_quarantine(self, limit: int = 20) -> List[DeadLetterPayload]:
        stmt = select(DeadLetterPayload).order_by(desc(DeadLetterPayload.id)).limit(limit)
        result = await self._db.execute(stmt)
        return list(result.scalars().all())
