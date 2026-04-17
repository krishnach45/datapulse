# DataPulse — Real-Time E-Commerce Analytics Platform

> A production-grade streaming analytics platform built on Databricks, Apache Spark, Azure, Cosmos DB, Docker, and AKS.  
> Designed as a senior data engineering portfolio project covering the full stack: ingestion → processing → storage → serving → observability → CI/CD.

---

## Architecture

```
Event Producers
    │
    ▼
Azure Event Hubs (Kafka-compatible)
    │
    ▼
Databricks Structured Streaming
    │
    ├── Bronze Layer (Delta Lake) — raw events, schema-on-read
    ├── Silver Layer (Delta Lake) — cleaned, validated, deduplicated
    └── Gold Layer  (Delta Lake) — KPI aggregates
                │
                ▼
         Cosmos DB (NoSQL)
                │
                ▼
         FastAPI REST Service
         (Docker → AKS)
                │
         ┌──────┴──────┐
         │             │
      Grafana      Postman/
      Dashboards   Newman Tests
```

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Azure Event Hubs / Kafka | Streaming event bus |
| Processing | Databricks + PySpark | Structured Streaming, Delta Lake |
| Infrastructure | Azure IAAS (VMs, VNet, NSG) | Cloud infrastructure |
| Storage | Cosmos DB + ADLS Gen2 | Real-time KPIs + Delta archive |
| API | Python FastAPI | REST endpoints on AKS |
| Containers | Docker + Azure Kubernetes Service | Scalable API deployment |
| Observability | Grafana + Prometheus | Dashboards + alerting |
| Testing | JUnit/pytest + Postman/Newman | Unit + integration tests |
| Quality | SonarQube | Code quality gates |
| CI/CD | Azure DevOps / GitHub Actions | Automated pipeline |

---

## Project Structure

```
datapulse/
├── ingestion/
│   └── event_producer.py          # Simulates orders, clicks, inventory events
├── spark_jobs/
│   ├── bronze_ingest.py            # Kafka → Delta Bronze (Structured Streaming)
│   ├── silver_clean.py             # Bronze → Silver (clean, dedup, validate)
│   └── gold_aggregate.py           # Silver → Gold KPIs → Cosmos DB
├── api/
│   ├── main.py                     # FastAPI app with Prometheus metrics
│   ├── cosmos_client.py            # Async Cosmos DB client with retry
│   ├── routers/kpis.py             # All /api/v1/kpis/* endpoints
│   ├── Dockerfile                  # Multi-stage Docker build
│   └── requirements.txt
├── tests/
│   ├── unit/test_transformations.py    # 15+ pytest unit tests for Spark logic
│   ├── integration/test_api.py         # Full API integration test suite
│   └── postman/DataPulse.postman_collection.json
├── infra/k8s/
│   ├── deployment.yaml             # AKS Deployment (3 replicas, rolling update)
│   ├── service.yaml                # LoadBalancer service
│   └── hpa.yaml                    # Horizontal Pod Autoscaler (2-10 pods)
├── grafana/dashboards/
│   └── datapulse-overview.json     # Pre-built Grafana dashboard
├── config/
│   ├── prometheus.yml              # Prometheus scrape config
│   └── grafana-datasources.yml     # Auto-provisioned datasource
├── scripts/
│   ├── setup_local.sh              # One-command local setup + seed
│   └── deploy_azure.sh             # Full Azure provisioning script
├── .github/workflows/ci-cd.yml     # Full CI/CD pipeline
├── sonar-project.properties        # SonarQube quality gate config
├── docker-compose.yml              # Full local stack
└── pyproject.toml                  # pytest + ruff + black config
```

---

## Quick Start — Local (No Azure Required)

### Prerequisites
- Docker Desktop running
- Python 3.11+
- ~8 GB RAM available for Docker

### 1. Clone and start

```bash
git clone https://github.com/YOUR_USERNAME/datapulse.git
cd datapulse

# Start everything and seed sample data in one command
chmod +x scripts/setup_local.sh
./scripts/setup_local.sh
```

This script:
- Starts Kafka, Zookeeper, Spark, Cosmos DB emulator, FastAPI, Grafana, Prometheus, SonarQube
- Creates Kafka topics (`datapulse-orders`, `datapulse-clicks`, `datapulse-inventory`)
- Seeds **500 orders + 1000 clicks + 100 inventory events** to Kafka

### 2. Run the Spark pipeline

Open a new terminal for each job (or run sequentially):

```bash
# Install Spark deps
pip install pyspark==3.5.0 delta-spark==3.1.0 kafka-python azure-cosmos

# Bronze: read from Kafka, write to Delta
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/bronze_ingest.py

# Silver: clean and deduplicate (run after bronze has processed)
spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/silver_clean.py

# Gold: aggregate KPIs and write to Cosmos DB
COSMOS_ENDPOINT=https://localhost:8081 \
COSMOS_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==" \
spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  spark_jobs/gold_aggregate.py
```

> **Note:** The API has a built-in in-memory mock store — all endpoints work immediately even before running Spark. The mock store is seeded with realistic sample data automatically.

### 3. Explore the platform

| Service | URL | Credentials |
|---|---|---|
| **API Docs (Swagger)** | http://localhost:8000/docs | — |
| **API Health** | http://localhost:8000/health | — |
| **Kafka UI** | http://localhost:8090 | — |
| **Spark Master UI** | http://localhost:8080 | — |
| **Grafana** | http://localhost:3000 | admin / datapulse123 |
| **Prometheus** | http://localhost:9090 | — |
| **SonarQube** | http://localhost:9000 | admin / admin |
| **Cosmos DB Emulator** | https://localhost:8081 | Use the key above |

### 4. Test the API

```bash
# Summary dashboard KPI
curl http://localhost:8000/api/v1/kpis/summary | python3 -m json.tool

# Hourly revenue (last 24 buckets)
curl "http://localhost:8000/api/v1/kpis/revenue?limit=24" | python3 -m json.tool

# Top products
curl http://localhost:8000/api/v1/kpis/products | python3 -m json.tool

# Conversion funnel
curl http://localhost:8000/api/v1/kpis/conversion | python3 -m json.tool

# Inventory alerts (low stock items only)
curl "http://localhost:8000/api/v1/kpis/inventory?alerts_only=true" | python3 -m json.tool
```

### 5. Produce more events

```bash
# Continuous stream at 10 events/sec
python3 ingestion/event_producer.py --mode continuous --rate 10

# One-shot burst of 5000 events
python3 ingestion/event_producer.py --mode burst --count 5000
```

---

## Running Tests

### Unit Tests (Spark transformations)

```bash
pip install pytest pytest-cov pyspark==3.5.0 delta-spark==3.1.0
pytest tests/unit/ -v --cov=spark_jobs --cov-report=html
open htmlcov/index.html
```

### Integration Tests (live API)

```bash
pip install pytest httpx
pytest tests/integration/ -v
```

### Postman / Newman (API contract tests)

```bash
npm install -g newman
newman run tests/postman/DataPulse.postman_collection.json \
  --env-var "base_url=http://localhost:8000"
```

### SonarQube Analysis

```bash
# Start SonarQube (already running via docker-compose)
# Then run sonar-scanner (requires sonar-scanner CLI installed)
sonar-scanner \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=YOUR_SONAR_TOKEN
```

---

## Deploy to Azure

### Prerequisites
- Azure CLI installed: `brew install azure-cli` or https://aka.ms/installazurecli
- Azure subscription

```bash
# Login and set your subscription
az login
export AZURE_SUBSCRIPTION_ID="your-subscription-id"

# Full Azure provisioning (ACR, AKS, Cosmos DB, Event Hubs, Databricks)
chmod +x scripts/deploy_azure.sh
./scripts/deploy_azure.sh
```

The script provisions:
- **Azure Container Registry** — Docker image hosting
- **Azure Kubernetes Service** (2 nodes, Standard_B2s)
- **Cosmos DB** (serverless, `datapulse` database)
- **Event Hubs** (3 hubs, 4 partitions each)
- **Azure Databricks** workspace
- All K8s resources (Deployment, Service, HPA, Secrets)

---

## CI/CD Pipeline

The GitHub Actions pipeline (`.github/workflows/ci-cd.yml`) runs on every push:

```
push to main/PR
      │
      ▼
  1. Lint (ruff, black, isort)
      │
      ▼
  2. Unit Tests (pytest + Spark, JUnit XML report)
      │
      ▼
  3. SonarQube Quality Gate
     (coverage ≥ 80%, 0 critical bugs, maintainability A)
      │
      ▼
  4. Docker Build & Push → ACR
      │
      ▼
  5. Deploy → AKS (rolling update, zero downtime)
      │
      ▼
  6. Integration Tests (pytest + Newman against live AKS)
```

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `AZURE_CREDENTIALS` | Service principal JSON from `az ad sp create-for-rbac` |
| `ACR_REGISTRY` | e.g. `datapulseacr1234.azurecr.io` |
| `SONAR_TOKEN` | SonarQube project token |
| `SONAR_HOST_URL` | e.g. `http://your-sonar-host:9000` |
| `STAGING_API_URL` | AKS external IP, e.g. `http://20.x.x.x` |

---

## Key Design Decisions

**Why Cosmos DB over SQL?**  
Cosmos DB offers sub-10ms reads, flexible JSON document schema (KPI shapes vary between types), and horizontal partitioning by `kpi_type`. Perfect for the high-throughput write pattern from Spark Gold jobs.

**Why Medallion Architecture?**  
Bronze preserves the raw event for reprocessing. Silver enforces schema, removes duplicates. Gold computes KPIs. If a business rule changes, you rerun from Silver — no data loss.

**Why AKS over App Service?**  
HPA gives elastic scaling under load bursts. Rolling updates enable zero-downtime deploys. Prometheus annotations allow Grafana to auto-discover pods.

**Why FastAPI + in-memory mock?**  
The API works day-one without Cosmos DB being populated, making local development and CI integration tests fast and independent of the Spark pipeline.

---

## Interview Demo Script

1. Show the running `docker-compose ps` — all 9 services healthy
2. Open Kafka UI → show 1600 seeded events across 3 topics
3. Walk through `bronze_ingest.py` — Structured Streaming, Delta write
4. Walk through `silver_clean.py` — dedup logic, quarantine pattern
5. Walk through `gold_aggregate.py` — KPI computation, Cosmos write
6. Open Swagger UI → live-demo each endpoint
7. Open Grafana → show latency + request rate panels
8. Show CI/CD YAML — highlight SonarQube gate and Newman stage
9. Show K8s manifests — HPA config, rolling update strategy
10. Run `pytest tests/unit/ -v` live — all green

---

## License

MIT — built as a senior data engineering portfolio project.
