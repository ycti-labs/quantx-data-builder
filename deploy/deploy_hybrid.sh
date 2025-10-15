#!/bin/bash
set -e

#============================================
# QuantX Data Builder - Hybrid Deployment
# Deploys both Azure Functions and Container Apps
#============================================

echo "🚀 QuantX Data Builder - Hybrid Architecture Deployment"
echo "=========================================================="
echo ""

# Configuration
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-finsight-data}"
LOCATION="${LOCATION:-eastus}"
STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-stfinsightdata}"
CONTAINER_NAME="${CONTAINER_NAME:-finsight-data}"
ACR_NAME="${ACR_NAME:-acrfinsightdata}"
ACA_ENV_NAME="${ACA_ENV_NAME:-env-finsight-data}"
FUNCTION_APP_NAME="${FUNCTION_APP_NAME:-func-finsight-data}"
IMAGE_NAME="finsight-data-fetcher"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 Configuration:${NC}"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  Storage Account: $STORAGE_ACCOUNT"
echo "  ACR: $ACR_NAME"
echo "  Function App: $FUNCTION_APP_NAME"
echo ""

# ============================================
# Step 1: Deploy Azure Functions (Daily Updates)
# ============================================
echo -e "${BLUE}📦 Step 1: Deploying Azure Functions (Daily Updates)...${NC}"

# Check if Function App exists
if az functionapp show --name $FUNCTION_APP_NAME --resource-group $RESOURCE_GROUP &>/dev/null; then
    echo "  Function App already exists, updating..."
else
    echo "  Creating Function App..."
    az functionapp create \
        --resource-group $RESOURCE_GROUP \
        --consumption-plan-location $LOCATION \
        --runtime python \
        --runtime-version 3.11 \
        --functions-version 4 \
        --name $FUNCTION_APP_NAME \
        --storage-account $STORAGE_ACCOUNT \
        --os-type Linux \
        --disable-app-insights false
fi

# Enable managed identity
echo "  Enabling managed identity..."
FUNCTION_IDENTITY=$(az functionapp identity assign \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --query principalId \
    --output tsv)

# Grant storage access
echo "  Granting storage access..."
STORAGE_ID=$(az storage account show \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query id \
    --output tsv)

az role assignment create \
    --assignee $FUNCTION_IDENTITY \
    --role "Storage Blob Data Contributor" \
    --scope $STORAGE_ID \
    --output none 2>/dev/null || echo "  Role assignment already exists"

# Configure app settings
echo "  Configuring app settings..."
az functionapp config appsettings set \
    --name $FUNCTION_APP_NAME \
    --resource-group $RESOURCE_GROUP \
    --settings \
        AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT \
        AZURE_CONTAINER_NAME=$CONTAINER_NAME \
        MAX_WORKERS=10 \
        LOOKBACK_DAYS=5 \
        UNIVERSE_PATH="meta/universe_phase_1.csv" \
        UNIVERSE_PHASE="phase_1" \
    --output none

# Deploy function code
echo "  Deploying function code..."
cd azure_functions
func azure functionapp publish $FUNCTION_APP_NAME --python
cd ..

echo -e "${GREEN}✅ Azure Functions deployed${NC}"
echo ""

# ============================================
# Step 2: Deploy Container Apps (Backfill)
# ============================================
echo -e "${BLUE}🐳 Step 2: Deploying Container Apps (Heavy Backfill)...${NC}"

# Build and push container image
echo "  Building container image..."
FULL_IMAGE="${ACR_NAME}.azurecr.io/${IMAGE_NAME}:latest"

docker build -t $FULL_IMAGE -f container/Dockerfile .

echo "  Pushing to ACR..."
az acr login --name $ACR_NAME
docker push $FULL_IMAGE

# Create Container Apps Job for backfill
echo "  Creating Container Apps Job..."
az containerapp job create \
    --name "job-finsight-backfill" \
    --resource-group $RESOURCE_GROUP \
    --environment $ACA_ENV_NAME \
    --trigger-type "Manual" \
    --replica-timeout 7200 \
    --replica-completion-count 1 \
    --parallelism 1 \
    --image $FULL_IMAGE \
    --registry-server "${ACR_NAME}.azurecr.io" \
    --registry-identity "system" \
    --cpu 2.0 \
    --memory 4.0Gi \
    --env-vars \
        "FETCHER_AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT" \
        "FETCHER_AZURE_CONTAINER_NAME=$CONTAINER_NAME" \
        "FETCHER_LOG_LEVEL=INFO" \
        "USE_MANAGED_IDENTITY=true" \
    --command "python" "cli.py" "backfill" \
        "--start" "2020-01-01" \
        "--end" "2024-12-31" \
        "--max-workers" "20" \
        "--chunk-size" "100" \
    --output none 2>/dev/null || echo "  Job already exists, updating..."

# Update existing job
az containerapp job update \
    --name "job-finsight-backfill" \
    --resource-group $RESOURCE_GROUP \
    --image $FULL_IMAGE \
    --output none 2>/dev/null || true

echo -e "${GREEN}✅ Container Apps Jobs deployed${NC}"
echo ""

# ============================================
# Step 3: Create daily update job (optional)
# ============================================
echo -e "${BLUE}📅 Step 3: Creating Daily Update Job (Container Apps)...${NC}"

az containerapp job create \
    --name "job-finsight-daily" \
    --resource-group $RESOURCE_GROUP \
    --environment $ACA_ENV_NAME \
    --trigger-type "Manual" \
    --replica-timeout 600 \
    --replica-completion-count 1 \
    --parallelism 1 \
    --image $FULL_IMAGE \
    --registry-server "${ACR_NAME}.azurecr.io" \
    --registry-identity "system" \
    --cpu 1.0 \
    --memory 2.0Gi \
    --env-vars \
        "FETCHER_AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT" \
        "FETCHER_AZURE_CONTAINER_NAME=$CONTAINER_NAME" \
        "FETCHER_LOG_LEVEL=INFO" \
        "USE_MANAGED_IDENTITY=true" \
    --command "python" "cli.py" "update-daily" \
        "--lookback-days" "5" \
        "--max-workers" "10" \
    --output none 2>/dev/null || echo "  Job already exists"

echo -e "${GREEN}✅ Daily update job created${NC}"
echo ""

# ============================================
# Summary
# ============================================
echo ""
echo -e "${GREEN}🎉 Deployment Complete!${NC}"
echo "=========================================================="
echo ""
echo -e "${BLUE}📊 Deployed Services:${NC}"
echo "  ✅ Azure Functions (Scheduled Daily Updates)"
echo "     - Function App: $FUNCTION_APP_NAME"
echo "     - Schedule: Weekdays at 6 AM UTC"
echo "     - URL: https://${FUNCTION_APP_NAME}.azurewebsites.net"
echo ""
echo "  ✅ Container Apps (Heavy Operations)"
echo "     - Backfill Job: job-finsight-backfill"
echo "     - Daily Update Job: job-finsight-daily"
echo "     - Trigger: Manual"
echo ""
echo -e "${BLUE}🔧 Both services use SHARED code from src/${NC}"
echo ""
echo -e "${BLUE}💰 Estimated Monthly Cost:${NC}"
echo "  • Azure Functions: \$2-5/month (Consumption Plan)"
echo "  • Container Apps: \$5-10/month (Manual trigger only)"
echo "  • Total: ~\$10-15/month"
echo ""
echo -e "${BLUE}🚀 Next Steps:${NC}"
echo ""
echo "  1. Functions will run automatically on schedule"
echo ""
echo "  2. Trigger backfill manually:"
echo "     ${YELLOW}az containerapp job start --name job-finsight-backfill --resource-group $RESOURCE_GROUP${NC}"
echo ""
echo "  3. Trigger daily update manually:"
echo "     ${YELLOW}az containerapp job start --name job-finsight-daily --resource-group $RESOURCE_GROUP${NC}"
echo ""
echo "  4. View function logs:"
echo "     ${YELLOW}az functionapp logs tail --name $FUNCTION_APP_NAME --resource-group $RESOURCE_GROUP${NC}"
echo ""
echo "  5. Test function endpoint:"
echo "     ${YELLOW}curl https://${FUNCTION_APP_NAME}.azurewebsites.net/api/health${NC}"
echo ""
echo "=========================================================="
