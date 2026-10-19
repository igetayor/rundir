# Rundir — High-Performance Async Data Ingestion & Transformation Pipeline

A production-grade, asynchronous stream ingestion engine built with **Python 3.13**, **FastAPI**, **asyncio**, **httpx**, and **SQLAlchemy (asyncpg/SQLite)**. Rundir concurrently pulls real-time JSON/event streams from external REST sources, normalizes payloads, runs bulk transformations, isolates malformed records into a Dead-Letter Queue (DLQ), and persists validated data in transactional batches.

---

## Architecture Overview

```
                          ┌─────────────────────────────┐
                          │ External REST APIs / Streams│
                          └──────────────┬──────────────┘
                                         │ concurrent HTTP
                                         ▼ (httpx / asyncio.gather)
┌─────────────────────────────────────────────────────────────────────────────┐
│ Rundir Ingestion Engine (FastAPI + Async Worker Pool)                       │
│                                                                             │
│  ┌────────────────────────┐    ┌──────────────────────┐    ┌─────────────┐  │
│  │ Bounded Concurrency    ├───►│ Batch Normalization  ├───►│ Bulk Insert │  │
│  │ (asyncio.Semaphore(10))│    │ & Schema Validation  │    │ (Asyncpg)   │  │
│  └────────────────────────┘    └──────────┬───────────┘    └──────┬──────┘  │
│                                           │ (malformed payload)   │         │
│                                           ▼                       ▼         │
│                                ┌──────────────────────┐  ┌───────────────┐  │
│                                │ Dead-Letter Q (DLQ)  │  │ Ingested DB   │  │
│                                └──────────────────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                         ▲
                                         │
                          ┌──────────────┴──────────────┐
                          │ Web Telemetry UI (Port 8086)│
                          └─────────────────────────────┘
```

---

## Key Features

1. **Concurrent Stream Orchestration**:
   - `httpx.AsyncClient` HTTP connection pool with configurable timeouts and connection limits.
   - Bounded concurrency with `asyncio.Semaphore` preventing rate-limit cascades on external servers.
2. **Robust Schema Normalization & Validation**:
   - Pydantic v2 schemas normalizing variable API timestamp formats (`unix`, ISO 8601, string dates).
   - Dynamic payload parsing supporting nested objects, arrays, and multi-record batches.
3. **Dead-Letter Queue (DLQ) Quarantine**:
   - Zero-drop architecture: records failing validation or type coercion are isolated into `DeadLetterPayload` with error descriptions and raw JSON snapshots.
4. **Bulk Database Ingestion**:
   - Optimized multi-row batch execution using SQLAlchemy async sessions (`session.execute(insert(...))`) avoiding single-insert overhead.
5. **Real-Time Web Telemetry UI**:
   - Interactive control dashboard at `http://localhost:8086` for dispatching streams, viewing ingestion rates, querying DLQ anomalies, and browsing ingested records.

---

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/` | `GET` | Web Telemetry & Stream Ingestion Dashboard |
| `/healthz` | `GET` | Service & DB connectivity health probe |
| `/api/v1/ingest` | `POST` | Dispatch synchronous or background ingestion across streams |
| `/api/v1/records` | `GET` | Query normalized records with filtering by stream/status |
| `/api/v1/quarantine` | `GET` | Query quarantined DLQ records |
| `/api/v1/stats` | `GET` | Live ingestion metrics, success rate, and error counters |

---

## Local Development & Testing

### 1. Run with Virtual Environment
```bash
cd rundir
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

# Run server (default port: 8086)
uvicorn app.main:app --port 8086 --reload
```

### 2. Run Automated Pytest Suite
```bash
python -m pytest tests/ -v
```

---

## Docker & Docker Compose

### Start PostgreSQL & Pipeline
```bash
docker-compose up --build
```
Web Dashboard will be accessible at: `http://localhost:8086`

---

## Render Deployment (`render.yaml`)

This repository includes a ready-to-deploy `render.yaml` Blueprint:
1. Connect your Git repository to [Render](https://render.com).
2. Create a new **Blueprint** and select `rundir/render.yaml`.
3. Render automatically provisions the managed PostgreSQL database and the web service.
