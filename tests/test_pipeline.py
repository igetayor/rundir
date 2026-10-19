import pytest
from app.pipeline import pipeline
from app.schemas import ExternalStreamSource


@pytest.mark.asyncio
async def test_normalization_valid_record():
    raw_item = {
        "id": "tx_alpha_999",
        "timestamp": "2026-01-01T12:00:00Z",
        "category": "financial",
        "value": 142.50,
        "payload_extra": "nested_content",
    }
    rec = pipeline._normalize_raw_payload("test_source", raw_item)
    assert rec.entity_id == "tx_alpha_999"
    assert rec.source_id == "test_source"
    assert rec.numeric_value == 142.50
    assert rec.category == "financial"
    assert len(rec.payload_hash) == 64


@pytest.mark.asyncio
async def test_normalization_malformed_record_raises_error():
    # Record missing entity identifier
    raw_item = {
        "timestamp": "2026-01-01T12:00:00Z",
        "value": 50.0,
    }
    with pytest.raises(ValueError) as exc:
        pipeline._normalize_raw_payload("test_source", raw_item)
    assert "missing required entity identifier" in str(exc.value)


@pytest.mark.asyncio
async def test_exception_recovery_boundary_and_quarantine():
    # Batch with 2 valid and 2 malformed records
    raw_batch = [
        {"id": "valid_01", "value": 10.0},
        {"malformed_no_id": True},
        {"id": "valid_02", "value": 20.0},
        "not_even_a_dictionary",
    ]

    inserted, quarantined = await pipeline.process_batch("test_stream", raw_batch)
    assert inserted == 2
    assert quarantined == 2


@pytest.mark.asyncio
async def test_concurrent_worker_pool_mock_stream():
    sources = [
        ExternalStreamSource(source_id="src_1", url="https://mock.api/stream1"),
        ExternalStreamSource(source_id="src_2", url="https://mock.api/stream2"),
    ]

    mock_items = [
        {"id": f"event_{i}", "value": float(i), "category": "metrics"}
        for i in range(100)
    ]

    job_result = await pipeline.run_ingestion_job(
        job_id="test_job_123",
        sources=sources,
        concurrency=4,
        batch_size=25,
        mock_items=mock_items,
    )

    assert job_result["status"] == "COMPLETED"
    assert job_result["sources_processed"] == 2
    assert job_result["stats"]["total_records_inserted"] >= 100
