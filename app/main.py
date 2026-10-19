from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.api.router import router as api_router
from app.config import get_settings
from app.database import init_db
from app.logger import logger

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.app_name} on {settings.host}:{settings.port}")
    await init_db()
    yield
    logger.info("Shutting down Rundir ingestion pipeline...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="High-performance asynchronous data ingestion and transformation pipeline in Python using asyncio, httpx, and asyncpg/PostgreSQL with DLQ exception recovery.",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

web_dir = Path(__file__).resolve().parent.parent / "web"
if web_dir.exists():
    app.mount("/static", StaticFiles(directory=str(web_dir)), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    @app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    async def serve_index():
        index_file = web_dir / "index.html"
        if index_file.exists():
            return FileResponse(index_file)
        return HTMLResponse("<h1>Rundir Ingestion Engine UI</h1>")


@app.get("/healthz", tags=["System"])
async def healthz():
    return {"status": "ok", "service": "rundir", "version": settings.app_version}
