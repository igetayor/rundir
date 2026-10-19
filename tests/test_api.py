import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_healthz_endpoint(client: AsyncClient):
    res = await client.get("/healthz")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_dispatch_ingestion_job(client: AsyncClient):
    payload = {
        "job_name": "crypto_and_iot_live_feed",
        "sources": [
            {"source_id": "binance_ticks", "url": "https://api.binance.com/api/v3/ticker/price"},
            {"source_id": "iot_sensors", "url": "https://api.sensors.internal/feed"},
        ],
        "concurrency_limit": 4,
        "batch_size": 50,
        "simulate_mock_stream": True,
        "mock_record_count": 50,
    }

    res = await client.post("/api/v1/ingest", json=payload)
    assert res.status_code == 202
    data = res.json()
    assert "job_id" in data
    assert data["status"] == "ACCEPTED"
    assert data["sources_count"] == 2


@pytest.mark.asyncio
async def test_stats_and_quarantine_endpoints(client: AsyncClient):
    # Stats
    stats_res = await client.get("/api/v1/stats")
    assert stats_res.status_code == 200
    stats = stats_res.json()
    assert "total_records_inserted" in stats
    assert "ingestion_rate_rec_sec" in stats

    # Quarantine DLQ
    quaran_res = await client.get("/api/v1/quarantine")
    assert quaran_res.status_code == 200
    assert isinstance(quaran_res.json(), list)
