# Getting Started with QuantX Data Builder v2.0

## Quick Start Guide

This guide will help you get the QuantX Data Builder up and running in minutes.

---

## 🎯 Prerequisites

Before you begin, ensure you have:

- **Python 3.11+** installed
- **Azure CLI** configured with an active subscription
- **Docker** (for container deployment)
- **Azure Functions Core Tools** (for local Functions development)

---

## 📦 Installation

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/ycti-com/quantx-data-builder.git
cd quantx-data-builder

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file:

```bash
# Azure Storage (for cloud deployment)
AZURE_STORAGE_ACCOUNT=your_storage_account
AZURE_CONTAINER_NAME=finsight-data
AZURE_STORAGE_CONNECTION_STRING=your_connection_string

# Container Apps (for CLI)
FETCHER_AZURE_STORAGE_ACCOUNT=your_storage_account
FETCHER_AZURE_CONTAINER_NAME=finsight-data
FETCHER_LOG_LEVEL=INFO
USE_MANAGED_IDENTITY=false

# Local Development
LOCAL_STORAGE_ROOT=./data
```

---

## 🧪 Test Locally

### Test Universe Building

```bash
# Build universe from config (saves to local storage)
python container/cli.py refresh-universe --phase phase_1 --use-local

# Check created files
ls -la data/meta/
```

### Test Data Fetching

```bash
# Fetch recent data for universe (last 5 days)
python container/cli.py update-daily --use-local --lookback-days 5

# Check downloaded data
ls -la data/data/exchange=us/ticker=AAPL/year=2024/
```

### Test Backfill (Small Sample)

```bash
# Backfill for a short period
python container/cli.py backfill \
  --start 2024-01-01 \
  --end 2024-01-31 \
  --phase phase_1 \
  --use-local \
  --max-workers 5
```

---

## ☁️ Deploy to Azure

### 1. Create Azure Resources

```bash
# Set variables
export RESOURCE_GROUP="rg-finsight-data"
export LOCATION="eastus"
export STORAGE_ACCOUNT="stfinsightdata"
export ACR_NAME="acrfinsightdata"
export ACA_ENV_NAME="env-finsight-data"

# Create resource group
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# Create storage account
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS

# Create container
az storage container create \
  --name finsight-data \
  --account-name $STORAGE_ACCOUNT

# Create Azure Container Registry
az acr create \
  --name $ACR_NAME \
  --resource-group $RESOURCE_GROUP \
  --sku Basic \
  --admin-enabled true

# Create Container Apps Environment
az containerapp env create \
  --name $ACA_ENV_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION
```

### 2. Deploy with Script

```bash
# Run deployment script (deploys both Functions + Container Apps)
./deploy/deploy_hybrid.sh
```

### 3. Verify Deployment

```bash
# Check Function App
az functionapp show \
  --name func-finsight-data \
  --resource-group $RESOURCE_GROUP

# Test health endpoint
curl https://func-finsight-data.azurewebsites.net/api/health

# Check Container Apps Jobs
az containerapp job list \
  --resource-group $RESOURCE_GROUP \
  --output table
```

---

## 🚀 Run Your First Job

### Option 1: Automatic (Azure Functions)

Azure Functions will automatically run:
- **Daily updates**: Weekdays at 6 AM UTC
- **Weekly universe refresh**: Saturdays at 2 AM UTC

Monitor executions:
```bash
az functionapp logs tail \
  --name func-finsight-data \
  --resource-group $RESOURCE_GROUP
```

### Option 2: Manual (Container Apps)

Trigger a backfill job:
```bash
az containerapp job start \
  --name job-finsight-backfill \
  --resource-group $RESOURCE_GROUP

# Check execution status
az containerapp job execution list \
  --name job-finsight-backfill \
  --resource-group $RESOURCE_GROUP \
  --output table
```

---

## 📊 View Results

### Check Blob Storage

```bash
# List files in storage
az storage blob list \
  --account-name stfinsightdata \
  --container-name finsight-data \
  --output table

# Download a sample file
az storage blob download \
  --account-name stfinsightdata \
  --container-name finsight-data \
  --name data/exchange=us/ticker=AAPL/year=2024/data.parquet \
  --file ./aapl_sample.parquet
```

### Read Parquet Files

```python
import pandas as pd

# Read downloaded file
df = pd.read_parquet('./aapl_sample.parquet')
print(df.head())
print(f"Total rows: {len(df)}")
```

---

## 🔧 Common Tasks

### Add a New Universe

1. Edit `config/universes.yaml`:

```yaml
phase_2:
  - name: "us_nasdaq100"
    source_type: "wikipedia"
    url: "https://en.wikipedia.org/wiki/Nasdaq-100"
    table_index: 2
    symbol_column: "Ticker"
    enabled: true
```

2. Refresh universe:

```bash
python container/cli.py refresh-universe --phase phase_2 --use-local
```

3. Backfill data:

```bash
python container/cli.py backfill \
  --start 2023-01-01 \
  --end 2024-12-31 \
  --phase phase_2 \
  --use-local
```

### Change Schedule

Edit `azure_functions/function_app.py`:

```python
@app.schedule(
    schedule="0 7 * * 1-5",  # Change from 6 AM to 7 AM
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def daily_data_update(timer: func.TimerRequest) -> None:
    # ... existing code
```

Redeploy:
```bash
cd azure_functions
func azure functionapp publish func-finsight-data
```

### Customize Worker Count

For Container Apps:
```bash
az containerapp job update \
  --name job-finsight-backfill \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars "MAX_WORKERS=30"
```

For Azure Functions:
```bash
az functionapp config appsettings set \
  --name func-finsight-data \
  --resource-group $RESOURCE_GROUP \
  --settings MAX_WORKERS=15
```

---

## 🐛 Troubleshooting

### Issue: "Import Error" when running locally

**Solution**: Ensure you're in the project root and using the virtual environment:
```bash
cd /path/to/quantx-data-builder
source .venv/bin/activate
export PYTHONPATH=$PWD
python container/cli.py --help
```

### Issue: "Authentication Failed" with Azure Storage

**Solution**: For local development, use connection string:
```bash
export USE_MANAGED_IDENTITY=false
export AZURE_STORAGE_CONNECTION_STRING="your_connection_string"
```

### Issue: Container Apps Job Fails

**Solution**: Check logs:
```bash
# List executions
az containerapp job execution list \
  --name job-finsight-backfill \
  --resource-group $RESOURCE_GROUP

# Get specific execution logs
az containerapp job logs show \
  --name job-finsight-backfill \
  --resource-group $RESOURCE_GROUP \
  --execution <execution-name>
```

### Issue: Functions not triggering

**Solution**: Verify timer configuration and check Application Insights:
```bash
# Check function status
az functionapp function show \
  --name func-finsight-data \
  --resource-group $RESOURCE_GROUP \
  --function-name daily_data_update

# View logs in Application Insights
az monitor app-insights query \
  --app <app-insights-name> \
  --analytics-query "traces | where message contains 'Daily update' | order by timestamp desc"
```

---

## 📚 Next Steps

1. **Expand Coverage**: Enable more phases in `config/universes.yaml`
2. **Customize Storage**: Adjust partitioning in `src/core/data_operations.py`
3. **Add Monitoring**: Set up alerts for failed executions
4. **Optimize Costs**: Tune worker counts and schedules based on usage

---

## 🆘 Getting Help

- **Documentation**: See [README.md](./README.md)
- **Issues**: [GitHub Issues](https://github.com/ycti-com/quantx-data-builder/issues)
- **Examples**: Check `tests/` directory for usage examples

---

**Happy data building! 🚀**
