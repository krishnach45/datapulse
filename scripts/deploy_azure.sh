#!/usr/bin/env bash
# =============================================================================
# DataPulse - Azure Deployment Script
# Provisions all Azure resources and deploys the stack.
#
# Usage:
#   export AZURE_SUBSCRIPTION_ID="your-sub-id"
#   chmod +x scripts/deploy_azure.sh
#   ./scripts/deploy_azure.sh
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - kubectl installed
#   - Docker installed
# =============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Config ───────────────────────────────────────────────────────────────────
SUBSCRIPTION="${AZURE_SUBSCRIPTION_ID:-}"
RESOURCE_GROUP="datapulse-rg"
LOCATION="eastus"
ACR_NAME="datapulseacr${RANDOM}"           # Must be globally unique
AKS_CLUSTER="datapulse-aks"
COSMOS_ACCOUNT="datapulse-cosmos-${RANDOM}"
EVENT_HUB_NS="datapulse-eh-${RANDOM}"
DATABRICKS_WS="datapulse-databricks"
IMAGE_NAME="datapulse-api"

[ -z "$SUBSCRIPTION" ] && error "Set AZURE_SUBSCRIPTION_ID env var first"

echo ""
echo -e "${BLUE}╔══════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   DataPulse → Azure Deployment       ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════╝${NC}"
echo ""

# ── Login check ───────────────────────────────────────────────────────────────
info "Checking Azure login..."
az account show >/dev/null 2>&1 || error "Not logged in. Run: az login"
az account set --subscription "$SUBSCRIPTION"
success "Logged in to subscription: $SUBSCRIPTION"

# ── Resource Group ────────────────────────────────────────────────────────────
info "Creating resource group: $RESOURCE_GROUP"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" --output none
success "Resource group ready"

# ── Azure Container Registry ──────────────────────────────────────────────────
info "Creating Azure Container Registry: $ACR_NAME"
az acr create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ACR_NAME" \
  --sku Basic \
  --admin-enabled true \
  --output none
ACR_LOGIN_SERVER=$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)
success "ACR created: $ACR_LOGIN_SERVER"

# ── Build & Push Docker image ─────────────────────────────────────────────────
info "Building and pushing API Docker image..."
az acr login --name "$ACR_NAME"
docker build -t "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest" ./api
docker push "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest"
success "Docker image pushed"

# ── Cosmos DB ─────────────────────────────────────────────────────────────────
info "Creating Cosmos DB account: $COSMOS_ACCOUNT (this takes ~3 min)"
az cosmosdb create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$COSMOS_ACCOUNT" \
  --kind GlobalDocumentDB \
  --default-consistency-level Session \
  --locations regionName="$LOCATION" failoverPriority=0 isZoneRedundant=false \
  --output none

az cosmosdb sql database create \
  --resource-group "$RESOURCE_GROUP" \
  --account-name "$COSMOS_ACCOUNT" \
  --name datapulse \
  --output none

az cosmosdb sql container create \
  --resource-group "$RESOURCE_GROUP" \
  --account-name "$COSMOS_ACCOUNT" \
  --database-name datapulse \
  --name kpis \
  --partition-key-path "/kpi_type" \
  --throughput 400 \
  --output none

COSMOS_ENDPOINT=$(az cosmosdb show --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" --query documentEndpoint -o tsv)
COSMOS_KEY=$(az cosmosdb keys list --resource-group "$RESOURCE_GROUP" --name "$COSMOS_ACCOUNT" --query primaryMasterKey -o tsv)
success "Cosmos DB ready: $COSMOS_ENDPOINT"

# ── Event Hubs ────────────────────────────────────────────────────────────────
info "Creating Azure Event Hubs namespace: $EVENT_HUB_NS"
az eventhubs namespace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$EVENT_HUB_NS" \
  --location "$LOCATION" \
  --sku Standard \
  --output none

for HUB in datapulse-orders datapulse-clicks datapulse-inventory; do
  az eventhubs eventhub create \
    --resource-group "$RESOURCE_GROUP" \
    --namespace-name "$EVENT_HUB_NS" \
    --name "$HUB" \
    --partition-count 4 \
    --message-retention 1 \
    --output none
  success "  Event Hub: $HUB"
done

EH_CONN_STRING=$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENT_HUB_NS" \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)

# ── AKS Cluster ───────────────────────────────────────────────────────────────
info "Creating AKS cluster: $AKS_CLUSTER (this takes ~5 min)"
az aks create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --node-count 2 \
  --node-vm-size Standard_B2s \
  --enable-managed-identity \
  --attach-acr "$ACR_NAME" \
  --generate-ssh-keys \
  --output none
success "AKS cluster ready"

# ── AKS credentials ───────────────────────────────────────────────────────────
az aks get-credentials \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --overwrite-existing
success "kubectl configured for AKS"

# ── Kubernetes secrets ────────────────────────────────────────────────────────
info "Creating Kubernetes namespace and secrets..."
kubectl create namespace datapulse --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic datapulse-secrets \
  --namespace datapulse \
  --from-literal=COSMOS_ENDPOINT="$COSMOS_ENDPOINT" \
  --from-literal=COSMOS_KEY="$COSMOS_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
success "Kubernetes secrets created"

# ── Deploy to AKS ─────────────────────────────────────────────────────────────
info "Deploying DataPulse API to AKS..."
IMAGE_TAG="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:latest"
sed "s|IMAGE_PLACEHOLDER|${IMAGE_TAG}|g" infra/k8s/deployment.yaml | kubectl apply -f -
kubectl apply -f infra/k8s/service.yaml -n datapulse
kubectl apply -f infra/k8s/hpa.yaml     -n datapulse
kubectl rollout status deployment/datapulse-api -n datapulse --timeout=5m
success "DataPulse API deployed to AKS"

# ── Get public IP ─────────────────────────────────────────────────────────────
info "Waiting for external IP (may take 60s)..."
sleep 30
EXTERNAL_IP=""
for i in {1..10}; do
  EXTERNAL_IP=$(kubectl get svc datapulse-api -n datapulse -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  [ -n "$EXTERNAL_IP" ] && break
  sleep 10
done

# ── Databricks workspace ──────────────────────────────────────────────────────
info "Creating Azure Databricks workspace: $DATABRICKS_WS"
az databricks workspace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DATABRICKS_WS" \
  --location "$LOCATION" \
  --sku standard \
  --output none
DATABRICKS_URL=$(az databricks workspace show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DATABRICKS_WS" \
  --query workspaceUrl -o tsv)
success "Databricks workspace: https://${DATABRICKS_URL}"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              DataPulse deployed to Azure!                    ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  API URL         → http://${EXTERNAL_IP}/docs${NC}"
echo -e "${GREEN}║  Cosmos DB       → ${COSMOS_ENDPOINT}${NC}"
echo -e "${GREEN}║  Event Hub NS    → ${EVENT_HUB_NS}${NC}"
echo -e "${GREEN}║  ACR             → ${ACR_LOGIN_SERVER}${NC}"
echo -e "${GREEN}║  Databricks      → https://${DATABRICKS_URL}${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  Save these secrets to your GitHub repo:                     ║${NC}"
echo -e "${GREEN}║  COSMOS_ENDPOINT=${COSMOS_ENDPOINT}${NC}"
echo -e "${GREEN}║  COSMOS_KEY=${COSMOS_KEY:0:20}...${NC}"
echo -e "${GREEN}║  ACR_REGISTRY=${ACR_LOGIN_SERVER}${NC}"
echo -e "${GREEN}║  EH_CONNECTION=${EH_CONN_STRING:0:40}...${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
