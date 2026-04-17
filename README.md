# DataPulse — Real-Time E-Commerce Analytics Platform

> A production-grade streaming analytics platform built on Databricks, Apache Spark, Azure, Cosmos DB, Docker, and AKS.  
> Designed as a senior data engineering portfolio project covering the full stack: ingestion → processing → storage → serving → observability → CI/CD.

![CI/CD](https://github.com/krishnach45/datapulse/actions/workflows/ci-cd.yml/badge.svg)
![Tests](https://img.shields.io/badge/tests-48%20passing-brightgreen)
![Python](https://img.shields.io/badge/python-3.11-blue)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Event Producers                        │
│        Orders · Product Clicks · Inventory Updates        │
└─────────────────────────┬────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────┐
│           Azure Event Hubs  (Kafka-compatible)            │
│   datapulse-orders · datapulse-clicks · datapulse-inventory│
└─────────────────────────┬────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────┐
│        Databricks · Apache Spark · Delta Lake             │
│                                                          │
│  ┌──────────┐    ┌──────────┐    ┌────────────────────┐  │
│  │  BRONZE  │───▶│  SILVER  │───▶│       GOLD         │  │
│  │ Raw JSON │    │ Cleaned  │    │  KPI Aggregates    │  │
│  │ events   │    │ Deduped  │    │  revenue, products │  │
│  └──────────┘    └──────────┘    └─────────┬──────────┘  │
└────────────────────────────────────────────┼─────────────┘
│
▼
┌─────────────────────────┐
│       Cosmos DB          │
│   KPI Documents          │
│   sub-10ms reads         │
└────────────┬────────────┘
│
┌────────────▼────────────┐
│   FastAPI REST API       │
│   Docker · AKS           │
│   HPA: 2–10 pods         │
└────────────┬────────────┘
│
┌─────────────┴────────────┐
│                          │
┌─────────▼────────┐    ┌────────────▼──────┐
│    Grafana       │    │  Postman/Newman    │
│  Dashboards      │    │  API Tests         │
└──────────────────┘    └───────────────────┘
```

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Azure Event Hubs / Kafka | Streaming event bus — 3 topics, 4 partitions each |
| Processing | Databricks + PySpark | Structured Streaming, Delta Lake medallion architecture |
| Infrastructure | Azure IAAS | VMs, VNet, NSG, ADLS Gen2 — provisioned via script |
| Storage | Cosmos DB | NoSQL KPI documents, partitioned by `kpi_type`, sub-10ms reads |
| API | Python FastAPI | 6 REST endpoints, Prometheus metrics, async Cosmos client |
| Containers | Docker + AKS | Multi-stage build, 3 replicas, HPA scales 2–10 pods on CPU |
| Observability | Grafana + Prometheus | p50/p95/p99 latency, request rate, error rate dashboards |
| Testing | pytest + Postman/Newman | 21 unit + 27 integration + 12 Postman contract tests |
| Quality | SonarQube | Coverage ≥ 80%, 0 critical bugs, maintainability A |
| CI/CD | GitHub Actions | 6-stage automated pipeline on every push |

---

## Project Structure
datapulse/
├── ingestion/
│   └── event_producer.py           # Generates orders, clicks, inventory events → Kafka
│                                   # Modes: seed | continuous (rate/sec) | burst (count)
├── spark_jobs/
│   ├── bronze_ingest.py            # Kafka → Delta Bronze (Structured Streaming)
│   ├── silver_clean.py             # Bronze → Silver (dedup, validate, quarantine)
│   └── gold_aggregate.py           # Silver → Gold KPIs → Cosmos DB
├── api/
│   ├── main.py                     # FastAPI app with Prometheus metrics middleware
│   ├── cosmos_client.py            # Async Cosmos DB client with retry + mock fallback
│   ├── routers/kpis.py             # All /api/v1/kpis/* endpoints
│   ├── Dockerfile                  # Multi-stage production Docker build
│   └── requirements.txt
├── tests/
│   ├── unit/test_transformations.py    # 21 pytest unit tests — pure Python, no JVM
│   ├── integration/test_api.py         # 27 integration tests against live API
│   └── postman/DataPulse.postman_collection.json
├── infra/k8s/
│   ├── deployment.yaml             # AKS Deployment — 3 replicas, rolling update
│   ├── service.yaml                # LoadBalancer service
│   └── hpa.yaml                    # HPA: scales 2–10 pods on CPU/memory
├── grafana/dashboards/
│   └── datapulse-overview.json     # Pre-built Grafana dashboard (auto-provisioned)
├── config/
│   ├── prometheus.yml              # Prometheus scrape config
│   └── grafana-datasources.yml     # Grafana datasource auto-provisioning
├── scripts/
│   ├── setup_local.sh              # One-command local stack setup + data seed
│   └── deploy_azure.sh             # Full Azure resource provisioning script
├── .github/workflows/ci-cd.yml     # 6-stage CI/CD pipeline
├── docker-compose.yml              # Full local stack (9 services)
├── sonar-project.properties        # SonarQube quality gate config
└── pyproject.toml                  # pytest + ruff + black + isort config

---

## Quick Start — Local (No Azure Required)

### Option A — API only (fastest, 30 seconds)

```bash
git clone https://github.com/krishnach45/datapulse.git
cd datapulse/api

pip install fastapi uvicorn azure-cosmos prometheus-client httpx
uvicorn main:app --reload --port 8000
```

Open **http://localhost:8000/docs** — Swagger UI with all 6 KPI endpoints live.

> The API has a built-in mock store seeded with 82 realistic documents. Everything works immediately with no Spark or Cosmos DB required.

### Option B — Full stack (Docker)

```bash
cd datapulse
chmod +x scripts/setup_local.sh
./scripts/setup_local.sh
```

Starts: Kafka · Zookeeper · Spark · Cosmos DB emulator · FastAPI · Grafana · Prometheus · SonarQube
Seeds: 500 orders + 1000 clicks + 100 inventory events → Kafka

### Run the Spark pipeline

```bash
pip install pyspark==3.5.0 delta-spark==3.1.0 kafka-python azure-cosmos

# Bronze: Kafka → Delta (streaming)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/bronze_ingest.py

# Silver: clean + dedup
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_jobs/silver_clean.py

# Gold: KPI aggregation → Cosmos DB
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 spark_jobs/gold_aggregate.py
```

### Seed & produce events

```bash
# Seed sample data (500 orders, 1000 clicks, 100 inventory)
python3 ingestion/event_producer.py --mode seed

# Continuous stream at 10 events/sec
python3 ingestion/event_producer.py --mode continuous --rate 10

# One-shot burst
python3 ingestion/event_producer.py --mode burst --count 5000
```

---

## Test Results

pytest tests/unit/ -v
══════════════════════════════════════════════
21 passed in 0.16s ✅
══════════════════════════════════════════════
pytest tests/integration/ -v
══════════════════════════════════════════════
27 passed in 2.77s ✅
══════════════════════════════════════════════

### Run tests yourself

```bash
# Unit tests
pip install pytest pytest-cov
pytest tests/unit/ -v

# Integration tests (API must be running)
pip install httpx
pytest tests/integration/ -v

# Postman / Newman contract tests
npm install -g newman
newman run tests/postman/DataPulse.postman_collection.json \
  --env-var "base_url=http://localhost:8000"
```

---

## REST API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Kubernetes liveness & readiness probe |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint |
| `GET` | `/api/v1/kpis/summary` | Overall dashboard — revenue, orders, AOV, clicks |
| `GET` | `/api/v1/kpis/revenue` | Hourly revenue by region (`?region=Europe&limit=24`) |
| `GET` | `/api/v1/kpis/products` | Top products by revenue (`?category=Electronics`) |
| `GET` | `/api/v1/kpis/conversion` | Conversion funnel — clicks → cart → completed orders |
| `GET` | `/api/v1/kpis/inventory` | Stock levels and low-stock alerts (`?alerts_only=true`) |
| `GET` | `/api/v1/kpis/regions` | Available region names |

---

## CI/CD Pipeline

Runs automatically on every push to `main` or `develop`, and on all pull requests.

push to main / pull request
│
▼

Lint & Type Check ─── ruff · black · isort          ✅  10s
│
▼
Unit Tests ─────────── pytest 21 tests              ✅  19s
│
▼
SonarQube ──────────── quality gate                 ✅  50s
│
▼
Docker Build & Push ── push to Azure ACR            ⚙️  ~2m
│
▼
Deploy to AKS ──────── rolling update               ⚙️  ~3m
│
▼
Integration Tests ───── pytest + Newman vs AKS      ⚙️  ~1m


Stages 4–6 activate once Azure credentials are added as GitHub Secrets.

### Required GitHub Secrets

| Secret | Value |
|---|---|
| `AZURE_CREDENTIALS` | Output of `az ad sp create-for-rbac --sdk-auth` |
| `ACR_REGISTRY` | e.g. `datapulseacr.azurecr.io` |
| `SONAR_TOKEN` | SonarCloud project token |
| `SONAR_HOST_URL` | `https://sonarcloud.io` |
| `STAGING_API_URL` | AKS LoadBalancer IP after deploy |

---

## Local Service URLs

| Service | URL | Credentials |
|---|---|---|
| API Swagger UI | http://localhost:8000/docs | — |
| API Health | http://localhost:8000/health | — |
| Kafka UI | http://localhost:8090 | — |
| Spark Master | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / datapulse123 |
| Prometheus | http://localhost:9090 | — |
| SonarQube | http://localhost:9000 | admin / admin |
| Cosmos Emulator | https://localhost:8081 | emulator key |

---

## Deploy to Azure

```bash
az login
export AZURE_SUBSCRIPTION_ID="your-subscription-id"

chmod +x scripts/deploy_azure.sh
./scripts/deploy_azure.sh
```

Provisions: Azure Container Registry · AKS (2 nodes) · Cosmos DB · Event Hubs (3 hubs) · Azure Databricks · VNet + NSG

---

## Key Design Decisions

**Why Cosmos DB over Azure SQL?**
Sub-10ms reads at scale, flexible JSON schema (KPI shapes vary per type), and horizontal partitioning by `kpi_type`. Ideal for the high-throughput write pattern from Spark Gold jobs and the read-heavy API serving layer.

**Why Medallion Architecture?**
Bronze preserves raw events — full reprocessing is always possible. Silver enforces schema and removes duplicates. Gold computes KPIs. Business rule changes mean rerunning from Silver, never from scratch.

**Why AKS over App Service?**
HPA provides elastic scaling under traffic bursts. Rolling updates enable zero-downtime deployments. Prometheus pod annotations let Grafana auto-discover all replicas.

**Why FastAPI with an in-memory mock?**
The API is fully functional on day one without Cosmos DB populated, making local development fast and CI integration tests independent of the Spark pipeline. Mock store seeds 82 realistic documents on startup.

---

## Interview Demo Script

1. **GitHub repo** — show structure, README, commit history
2. **Actions tab** — green CI/CD pipeline running on every push
3. **Terminal** — `pytest tests/unit/ -v` — 21 green in 0.16s
4. **Terminal** — `pytest tests/integration/ -v` — 27 green in 2.77s
5. **Browser** — `http://localhost:8000/docs` — demo each KPI endpoint live
6. **Code walkthrough** — `bronze_ingest.py` → `silver_clean.py` → `gold_aggregate.py`
7. **K8s manifests** — `deployment.yaml` (rolling update) + `hpa.yaml` (auto-scaling)
8. **Grafana** — latency histogram + request rate panels

---

## License

MIT — built as a senior data engineering portfolio project.
