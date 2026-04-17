
"""
DataPulse - FastAPI REST Service
Exposes KPI data from Cosmos DB over a clean REST API.
Runs as a containerised service on AKS.

Endpoints:
  GET /health                  - Health check (used by AKS liveness probe)
  GET /metrics                 - Prometheus metrics
  GET /api/v1/kpis/summary     - Overall dashboard summary
  GET /api/v1/kpis/revenue     - Hourly revenue by region
  GET /api/v1/kpis/products    - Top products
  GET /api/v1/kpis/conversion  - Conversion funnel
  GET /api/v1/kpis/inventory   - Inventory alerts
"""

import time
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from routers import kpis
from cosmos_client import CosmosDBClient

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("datapulse.api")

# ── Prometheus Metrics ────────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "datapulse_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "datapulse_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)
ACTIVE_REQUESTS = Gauge(
    "datapulse_active_requests",
    "Number of active requests"
)
COSMOS_ERRORS = Counter(
    "datapulse_cosmos_errors_total",
    "Total Cosmos DB errors",
    ["operation"]
)


# ── Lifespan (startup / shutdown) ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("DataPulse API starting up...")
    app.state.cosmos = CosmosDBClient()
    await app.state.cosmos.initialize()
    logger.info("Cosmos DB client initialised.")
    yield
    logger.info("DataPulse API shutting down...")
    await app.state.cosmos.close()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="DataPulse API",
    description="Real-time e-commerce analytics KPI service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Middleware: metrics + request logging ─────────────────────────────────────
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start   = time.perf_counter()
    ACTIVE_REQUESTS.inc()
    try:
        response = await call_next(request)
        duration = time.perf_counter() - start
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        ).inc()
        REQUEST_LATENCY.labels(
            method=request.method,
            endpoint=request.url.path
        ).observe(duration)
        logger.info(
            f"{request.method} {request.url.path} "
            f"→ {response.status_code} ({duration:.3f}s)"
        )
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        raise
    finally:
        ACTIVE_REQUESTS.dec()


# ── Routes ────────────────────────────────────────────────────────────────────
app.include_router(kpis.router, prefix="/api/v1")


@app.get("/health", tags=["system"])
async def health():
    """Kubernetes liveness & readiness probe endpoint."""
    return {
        "status": "healthy",
        "service": "datapulse-api",
        "version": "1.0.0",
    }


@app.get("/metrics", tags=["system"], response_class=PlainTextResponse)
async def metrics():
    """Prometheus metrics scrape endpoint."""
    return PlainTextResponse(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


@app.get("/", tags=["system"])
async def root():
    return {
        "service": "DataPulse API",
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }
