import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import httpx
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import async_session_factory
from app.logger import logger
from app.models import DeadLetterPayload, IngestedRecord
from app.schemas import ExternalStreamSource, NormalizedRecord

settings = get_settings()


class IngestionPipeline:
    """
    High-performance asynchronous data ingestion and transformation pipeline.
    
    Features:
    - Bounded worker pools with asyncio.Semaphore concurrency limit.
    - Concurrent fetching from multiple external REST endpoints via httpx.AsyncClient.
    - Robust exception recovery boundaries for malformed payloads (quarantining to DLQ).
    - Batch transformation & bulk-insert into database.
    """

    def __init__(self):
        self.stats = {
            "total_fetched": 0,
            "total_normalized": 0,
            "total_inserted": 0,
            "total_quarantined": 0,
            "active_workers": 0,
            "completed_jobs": 0,
            "start_time": time.time(),
        }

    def _normalize_raw_payload(self, source_id: str, raw_item: Any) -> NormalizedRecord:
        """
        Transformation boundary: parses heterogeneous raw records,
        validates structure, computes deterministic payload hash, and normalizes fields.
        Raises ValueError if item is unrecoverably malformed.
        """
        if not isinstance(raw_item, dict):
            raise ValueError(f"Malformed payload: expected dictionary, got {type(raw_item).__name__}")

        # Extract or generate entity identifier
        entity_id = str(
            raw_item.get("id")
            or raw_item.get("uuid")
            or raw_item.get("tx_hash")
            or raw_item.get("account_id")
            or raw_item.get("sensor_id")
            or ""
        ).strip()

        if not entity_id:
            raise ValueError("Malformed record: missing required entity identifier (id, uuid, tx_hash, etc.)")

        # Extract timestamp
        ts_val = raw_item.get("timestamp") or raw_item.get("created_at") or raw_item.get("time")
        event_ts = datetime.now(timezone.utc)
        if ts_val:
            try:
                if isinstance(ts_val, (int, float)):
                    event_ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                elif isinstance(ts_val, str):
                    event_ts = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            except Exception:
                event_ts = datetime.now(timezone.utc)

        # Extract numeric metric
        numeric_val = 0.0
        for num_field in ("value", "amount", "price", "metric", "temperature", "balance"):
            if num_field in raw_item:
                try:
                    numeric_val = float(raw_item[num_field])
                    break
                except (ValueError, TypeError):
                    pass

        category = str(raw_item.get("category") or raw_item.get("type") or "telemetry")[:64]
        serialized_raw = json.dumps(raw_item, sort_keys=True)
        payload_hash = hashlib.sha256(serialized_raw.encode("utf-8")).hexdigest()

        return NormalizedRecord(
            entity_id=entity_id,
            source_id=source_id,
            event_timestamp=event_ts,
            category=category,
            payload_hash=payload_hash,
            numeric_value=numeric_val,
            raw_payload=raw_item,
            metadata={"ingested_at": datetime.now(timezone.utc).isoformat()},
        )

    async def fetch_source_stream(
        self,
        client: httpx.AsyncClient,
        source: ExternalStreamSource,
    ) -> List[Dict[str, Any]]:
        """Fetches raw stream items from an external REST API with retry boundaries."""
        headers = {}
        if source.auth_header:
            headers["Authorization"] = source.auth_header

        for attempt in range(1, settings.ingestion_max_retries + 1):
            try:
                res = await client.get(source.url, headers=headers, timeout=settings.ingestion_fetch_timeout_sec)
                res.raise_for_status()
                data = res.json()
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # Check common envelope keys (data, items, results)
                    for k in ("data", "items", "results", "records"):
                        if k in data and isinstance(data[k], list):
                            return data[k]
                    return [data]
                return []
            except Exception as exc:
                if attempt == settings.ingestion_max_retries:
                    logger.error(
                        f"Exhausted HTTP retries fetching stream {source.source_id}: {exc}",
                        extra={"source_id": source.source_id},
                    )
                    raise
                await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        return []

    async def bulk_insert_records(
        self,
        records: List[NormalizedRecord],
        quarantined: List[Tuple[str, str, str]],
    ) -> Tuple[int, int]:
        """
        Executes high-performance bulk database persistence.
        Persists normalized records and routes any malformed items to dead-letter quarantine.
        """
        inserted_count = 0
        quarantined_count = 0

        async with async_session_factory() as session:
            try:
                if records:
                    insert_dicts = [
                        {
                            "entity_id": r.entity_id,
                            "source_id": r.source_id,
                            "event_timestamp": r.event_timestamp,
                            "category": r.category,
                            "payload_hash": r.payload_hash,
                            "numeric_value": r.numeric_value,
                            "raw_payload": json.dumps(r.raw_payload),
                            "metadata_json": json.dumps(r.metadata),
                            "created_at": datetime.now(timezone.utc),
                        }
                        for r in records
                    ]
                    # Bulk insert
                    await session.execute(insert(IngestedRecord), insert_dicts)
                    inserted_count = len(insert_dicts)

                if quarantined:
                    quarantine_dicts = [
                        {
                            "source_id": q[0],
                            "error_reason": q[1],
                            "raw_data": q[2],
                            "quarantined_at": datetime.now(timezone.utc),
                        }
                        for q in quarantined
                    ]
                    await session.execute(insert(DeadLetterPayload), quarantine_dicts)
                    quarantined_count = len(quarantine_dicts)

                await session.commit()
            except Exception as exc:
                await session.rollback()
                logger.error(f"Bulk insert failed, rolling back transaction: {exc}", exc_info=True)
                raise

        return inserted_count, quarantined_count

    async def process_batch(
        self,
        source_id: str,
        raw_items: List[Any],
    ) -> Tuple[int, int]:
        """Transforms batch items with strict exception recovery boundaries."""
        normalized: List[NormalizedRecord] = []
        quarantined: List[Tuple[str, str, str]] = []

        for item in raw_items:
            try:
                rec = self._normalize_raw_payload(source_id, item)
                normalized.append(rec)
            except Exception as err:
                # Exception recovery boundary: capture malformed payload without crashing pipeline
                raw_str = json.dumps(item) if isinstance(item, (dict, list)) else str(item)
                quarantined.append((source_id, str(err), raw_str))
                logger.warning(
                    f"Payload quarantined to dead-letter store: {err}",
                    extra={"source_id": source_id},
                )

        inserted, quaran = await self.bulk_insert_records(normalized, quarantined)
        self.stats["total_normalized"] += len(normalized)
        self.stats["total_inserted"] += inserted
        self.stats["total_quarantined"] += quaran
        return inserted, quaran

    async def run_ingestion_job(
        self,
        job_id: str,
        sources: List[ExternalStreamSource],
        concurrency: int = 4,
        batch_size: int = 50,
        mock_items: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Executes end-to-end ingestion job with bounded concurrency.
        """
        self.stats["active_workers"] += 1
        sem = asyncio.Semaphore(concurrency)
        start = time.perf_counter()

        async def worker_task(source: ExternalStreamSource):
            async with sem:
                raw_records = []
                if mock_items is not None:
                    raw_records = mock_items
                else:
                    async with httpx.AsyncClient() as client:
                        raw_records = await self.fetch_source_stream(client, source)

                self.stats["total_fetched"] += len(raw_records)

                # Process in batches
                for i in range(0, len(raw_records), batch_size):
                    batch = raw_records[i : i + batch_size]
                    await self.process_batch(source.source_id, batch)

        try:
            tasks = [worker_task(src) for src in sources]
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.stats["active_workers"] -= 1
            self.stats["completed_jobs"] += 1

        elapsed = time.perf_counter() - start
        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "duration_seconds": round(elapsed, 3),
            "sources_processed": len(sources),
            "stats": self.get_stats(),
        }

    def get_stats(self) -> Dict[str, Any]:
        uptime = time.time() - self.stats["start_time"]
        rate = round(self.stats["total_inserted"] / uptime, 2) if uptime > 0 else 0.0
        return {
            "total_records_fetched": self.stats["total_fetched"],
            "total_records_normalized": self.stats["total_normalized"],
            "total_records_inserted": self.stats["total_inserted"],
            "total_records_quarantined": self.stats["total_quarantined"],
            "active_workers": self.stats["active_workers"],
            "completed_jobs": self.stats["completed_jobs"],
            "ingestion_rate_rec_sec": rate,
        }


pipeline = IngestionPipeline()
