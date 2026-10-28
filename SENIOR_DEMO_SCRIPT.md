# Senior Engineering Walkthrough: Rundir Async Stream Ingestion

> **Target Audience:** Principal Systems Engineers, Data Pipeline Leads, Distributed Ingestion Architects.  
> **Key Architecture Highlights:** Asynchronous HTTP Stream Ingestion (`httpx` + `asyncio.gather`), Bounded Concurrency (`asyncio.Semaphore`), Schema Normalization & Dynamic Type Coercion, Zero-Drop Dead-Letter Queue (DLQ), Bulk Batch Persistence (SQLAlchemy Async).

---

## 1. Executive Summary & Problem Space

Ingesting high-velocity event streams from external third-party partner APIs poses several critical engineering challenges:
- External network latency variance and rate-limit throttling
- Inconsistent and dirty data schemas (mixed timestamp formats, missing fields, corrupted JSON)
- Database insertion bottlenecks when records are inserted row-by-row.

**Rundir** addresses these challenges through an end-to-end resilient ingestion pipeline:
- **Bounded Concurrency:** Uses `asyncio.Semaphore(10)` and `httpx.AsyncClient` connection pools to pull from multiple external endpoints in parallel without triggering rate limits.
- **Strict Normalization:** Pydantic v2 schemas dynamically coerce ISO-8601 strings, UNIX epoch integers, and legacy formats into clean models.
- **Zero-Drop DLQ Quarantine:** Any record that fails schema validation or business logic is never discarded; it is stored in a dedicated Dead-Letter Queue table with detailed failure traces and the raw payload snapshot.
- **Bulk Insert Transactions:** Persists validated batches in single transactional blocks (`session.execute(insert(...))`), eliminating per-record roundtrip latency.

---

## 2. File & Function Code Architecture

### `app/main.py` — Ingestion Gateway & Telemetry Dashboard (Port 8086)
- Exposes `POST /api/v1/ingest` for initiating concurrent stream ingestion jobs.
- Exposes `GET /api/v1/records` and `GET /api/v1/dlq` for browsing successfully normalized data vs quarantined anomalies.
- Serves the live ingestion telemetry UI at `GET /`.

### `app/services/ingestion.py` — Pipeline Orchestrator
- `fetch_stream(client, url, semaphore)`: Asynchronously pulls JSON chunks from external providers under semaphore concurrency bounds.
- `process_and_normalize(raw_record)`: Applies Pydantic validation; returns valid model or captures validation error into DLQ record.
- `bulk_persist(records, dlq_records)`: Executes transactional database inserts.

---

## 3. How to Run the Live Demo

### Starting the Server (Port 8086)
```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8086
```
Open your browser to: **http://127.0.0.1:8086**

### Demo Walkthrough Script
1. **Show the Dashboard:** Point out the dark-mode dashboard displaying Ingested Record Count, Throughput, and Dead-Letter Queue Isolation Rate.
2. **Dispatch Live Ingestion Batch:** Click **Start Stream Ingestion Batch** (simulating 500 events from disparate external REST APIs).
3. **Inspect Real-Time Ingestion:** Watch the throughput meter spike to several hundred records per second.
4. **Inspect Normalized Data:** Open the **Ingested Data Ledger** tab to show uniformly parsed timestamps and currency values.
5. **Inspect the Dead-Letter Queue:** Switch to the **DLQ Anomalies** tab to show that malformed records were quarantined with full error context without interrupting the pipeline.
