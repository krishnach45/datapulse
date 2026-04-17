#!/usr/bin/env bash
# =============================================================================
# DataPulse - Local Setup & Seed Script
# Spins up the full stack locally and seeds sample data.
#
# Usage:
#   chmod +x scripts/setup_local.sh
#   ./scripts/setup_local.sh
#
# Prerequisites:
#   - Docker Desktop running
#   - Python 3.11+
#   - pip
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

echo ""
echo -e "${BLUE}╔══════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     DataPulse — Local Setup & Seed       ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════╝${NC}"
echo ""

# ── 1. Check prerequisites ───────────────────────────────────────────────────
info "Checking prerequisites..."
command -v docker  >/dev/null 2>&1 || error "Docker not found. Install Docker Desktop."
command -v python3 >/dev/null 2>&1 || error "Python3 not found."
docker info >/dev/null 2>&1        || error "Docker daemon not running. Start Docker Desktop."
success "Prerequisites OK"

# ── 2. Install Python dependencies ───────────────────────────────────────────
info "Installing Python dependencies..."
python3 -m pip install --quiet kafka-python azure-cosmos 2>/dev/null || \
  warn "Pip install had warnings — continuing"
success "Python deps installed"

# ── 3. Start Docker Compose stack ────────────────────────────────────────────
info "Starting Docker Compose services..."
docker compose up -d --build 2>&1 | tail -5
success "Docker Compose services started"

# ── 4. Wait for Kafka ────────────────────────────────────────────────────────
info "Waiting for Kafka to be ready..."
MAX_WAIT=90; WAITED=0; INTERVAL=5
until docker exec datapulse-kafka kafka-topics \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  if [ $WAITED -ge $MAX_WAIT ]; then
    error "Kafka did not start within ${MAX_WAIT}s"
  fi
  printf "  Waiting... (%ds)\r" "$WAITED"
  sleep $INTERVAL; WAITED=$((WAITED + INTERVAL))
done
success "Kafka is ready"

# ── 5. Create Kafka topics ────────────────────────────────────────────────────
info "Creating Kafka topics..."
for TOPIC in datapulse-orders datapulse-clicks datapulse-inventory; do
  docker exec datapulse-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 1 2>/dev/null && \
    success "  Topic: $TOPIC" || warn "  Topic may already exist: $TOPIC"
done

# ── 6. Wait for API to be healthy ─────────────────────────────────────────────
info "Waiting for DataPulse API..."
MAX_WAIT=60; WAITED=0
until curl -sf http://localhost:8000/health >/dev/null 2>&1; do
  if [ $WAITED -ge $MAX_WAIT ]; then
    warn "API not responding yet — it may still be starting"
    break
  fi
  printf "  Waiting... (%ds)\r" "$WAITED"
  sleep 3; WAITED=$((WAITED + 3))
done
if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
  success "API is healthy at http://localhost:8000"
fi

# ── 7. Seed sample data ───────────────────────────────────────────────────────
info "Seeding sample events to Kafka..."
python3 ingestion/event_producer.py --broker localhost:9092 --mode seed
success "Sample data seeded (500 orders, 1000 clicks, 100 inventory updates)"

# ── 8. Summary ────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              DataPulse Stack is Ready!                   ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  API (docs)     →  http://localhost:8000/docs            ║${NC}"
echo -e "${GREEN}║  API (health)   →  http://localhost:8000/health          ║${NC}"
echo -e "${GREEN}║  Kafka UI       →  http://localhost:8090                 ║${NC}"
echo -e "${GREEN}║  Spark UI       →  http://localhost:8080                 ║${NC}"
echo -e "${GREEN}║  Grafana        →  http://localhost:3000  (admin/datapulse123) ║${NC}"
echo -e "${GREEN}║  Prometheus     →  http://localhost:9090                 ║${NC}"
echo -e "${GREEN}║  SonarQube      →  http://localhost:9000  (admin/admin)  ║${NC}"
echo -e "${GREEN}║  Cosmos Emul.   →  https://localhost:8081                ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  Next steps:                                             ║${NC}"
echo -e "${GREEN}║  1. spark-submit spark_jobs/bronze_ingest.py             ║${NC}"
echo -e "${GREEN}║  2. spark-submit spark_jobs/silver_clean.py              ║${NC}"
echo -e "${GREEN}║  3. spark-submit spark_jobs/gold_aggregate.py            ║${NC}"
echo -e "${GREEN}║  4. curl http://localhost:8000/api/v1/kpis/summary       ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
