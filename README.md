# QuantX Data Builder v2.0

Enterprise-grade financial data pipeline with hybrid Azure deployment

A production-ready system for downloading, processing, and storing historical daily OHLCV data for multiple stock markets using a **shared codebase** that deploys to both **Azure Functions** (for scheduled updates) and **Azure Container Apps** (for heavy backfill operations).

---

## 🌟 Key Features

- ✅ **Hybrid Architecture**: Single codebase, two deployment targets (Functions + Container Apps)
- ✅ **Multi-Market Support**: US, Hong Kong, Japan, Europe with extensible framework
- ✅ **Phase-Based Universes**: Configurable stock universes from multiple sources
- ✅ **Robust Data Pipeline**: Retry logic, error handling, manifest tracking
- ✅ **Cost-Optimized**: ~$10-15/month vs $50+ for Container Apps only
- ✅ **Production-Ready**: Managed identity, structured logging, monitoring

---

## 📁 Project Structure

```plain
quantx-data-builder/
├── src/                          # 🔥 Shared core logic (used by both deployments)
│   ├── core/
│   │   └── data_operations.py   # Main data fetching logic
│   ├── storage/
│   │   ├── base_storage.py      # Storage interface
│   │   ├── azure_storage.py     # Azure Blob implementation
│   │   └── local_storage.py     # Local filesystem implementation
│   └── universe/
│       ├── universe_builder.py  # Build stock universes
│       └── config_loader.py     # Load universe configs
│
├── azure_functions/              # Azure Functions entry point
│   ├── function_app.py          # Timer triggers for scheduled updates
│   ├── host.json
│   └── requirements.txt
│
├── container/                    # Container Apps entry point
│   ├── cli.py                   # CLI for backfill operations
│   ├── requirements.txt
│   └── Dockerfile               # Multi-stage build
│
├── config/                       # Configuration files
│   ├── universes.yaml           # Stock universe definitions
│   └── settings.yaml            # Application settings
│
├── deploy/                       # Deployment scripts
│   └── deploy_hybrid.sh         # Deploy both Functions + Container Apps
│
└── data/                         # Local data storage (development)
    └── exchange=<us|hk>/
        └── ticker=<SYMBOL>/
            └── year=YYYY/
```

---

## 🏗️ Architecture Overview

### Shared Core Logic

Both deployment targets use **identical business logic** from `src/`:

```python
from src.core.data_operations import DataOperations
from src.storage.azure_storage import AzureBlobStorage

storage = AzureBlobStorage(account_name="...", container_name="...")
ops = DataOperations(storage, max_workers=10)

# Same code works in both Functions and Container Apps
results = ops.fetch_daily_incremental(symbols, lookback_days=5)
```

### Deployment Targets

| Feature | Azure Functions | Container Apps |
|---------|----------------|----------------|
| **Use Case** | Daily scheduled updates | Heavy backfill operations |
| **Trigger** | Timer (6 AM weekdays) | Manual on-demand |
| **Duration** | 5-10 minutes | 2-4 hours |
| **Workers** | 10 concurrent | 20 concurrent |
| **Cost** | $2-5/month | $5-10/month |
| **Scaling** | Automatic | Manual |

---

## 🚀 Quick Start

### Prerequisites

```bash
# Azure CLI
az login

# Azure Functions Core Tools
brew install azure-functions-core-tools@4

# Docker (for container builds)
docker --version

# Python 3.11+
python --version
```

### Local Development

```bash
# Clone repository
git clone https://github.com/ycti-com/quantx-data-builder.git
cd quantx-data-builder

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r container/requirements.txt

# Set environment variables
export FETCHER_AZURE_STORAGE_ACCOUNT="your_storage_account"
export FETCHER_AZURE_CONTAINER_NAME="finsight-data"
export AZURE_STORAGE_CONNECTION_STRING="your_connection_string"

# Test CLI locally
python container/cli.py --help
python container/cli.py refresh-universe --phase phase_1 --use-local
python container/cli.py update-daily --use-local --lookback-days 5
```

### Deploy to Azure

```bash
# Set configuration
export RESOURCE_GROUP="rg-finsight-data"
export LOCATION="eastus"
export STORAGE_ACCOUNT="stfinsightdata"
export ACR_NAME="acrfinsightdata"
export FUNCTION_APP_NAME="func-finsight-data"

# Deploy everything (Functions + Container Apps)
./deploy/deploy_hybrid.sh
```

---

## 📊 Universe Management

### Phase-Based Universe Configuration

Edit `config/universes.yaml`:

```yaml
universes:
  phase_1:  # MVP: Core markets
    - name: "us_sp500"
      source_type: "wikipedia"
      url: "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
      enabled: true
    
    - name: "hk_hsi"
      source_type: "static"
      symbols: ["0700.HK", "0388.HK", ...]
      enabled: true

  phase_2:  # All Hong Kong stocks
    - name: "hk_all"
      source_type: "excel"
      url: "https://www.hkex.com.hk/.../ListOfSecurities.xlsx"
      enabled: true
```

### Refresh Universe

```bash
# Using CLI (Container Apps or local)
python container/cli.py refresh-universe --phase phase_1

# Using Functions (HTTP trigger)
curl -X POST https://func-finsight-data.azurewebsites.net/api/trigger-update \
  -H "x-functions-key: YOUR_FUNCTION_KEY"
```

---

## 💻 CLI Usage

### Backfill Historical Data

```bash
# Full backfill (Container Apps recommended)
python container/cli.py backfill \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --universe meta/universe_phase_1.csv \
  --max-workers 20 \
  --chunk-size 100

# Or trigger Container Apps Job
az containerapp job start \
  --name job-finsight-backfill \
  --resource-group rg-finsight-data
```

### Daily Incremental Update

```bash
# Local or on-demand
python container/cli.py update-daily \
  --lookback-days 5 \
  --max-workers 10

# Automatically runs via Azure Functions timer trigger
# Schedule: 0 6 * * 1-5 (Weekdays at 6 AM UTC)
```

### Show Manifest

```bash
# View tracking information
python container/cli.py show-manifest
```

---

## 🔧 Configuration

### Environment Variables

**Azure Functions:**

```bash
AZURE_STORAGE_ACCOUNT=stfinsightdata
AZURE_CONTAINER_NAME=finsight-data
MAX_WORKERS=10
LOOKBACK_DAYS=5
UNIVERSE_PATH=meta/universe_phase_1.csv
```

**Container Apps:**

```bash
FETCHER_AZURE_STORAGE_ACCOUNT=stfinsightdata
FETCHER_AZURE_CONTAINER_NAME=finsight-data
FETCHER_LOG_LEVEL=INFO
USE_MANAGED_IDENTITY=true
```

### Settings File

Edit `config/settings.yaml`:

```yaml
fetcher:
  max_workers: 10
  lookback_days: 5
  chunk_size: 100
  fetch_actions: true

schedules:
  daily_update:
    cron: "0 6 * * 1-5"  # Weekdays at 6 AM UTC
```

---

## 📈 Data Schema

### OHLCV Parquet Format

```python
{
  "Date": "datetime64[ns]",
  "Open": "float64",
  "High": "float64",
  "Low": "float64",
  "Close": "float64",
  "Adj Close": "float64",
  "Volume": "int64",
  "Ticker": "string",
  "Exchange": "string"
}
```

### Storage Partitioning

```
data/
├── exchange=us/
│   ├── ticker=AAPL/
│   │   └── year=2024/
│   │       └── data.parquet
│   └── ticker=MSFT/
│       └── year=2024/
│           └── data.parquet
└── exchange=hk/
    └── ticker=0700/
        └── year=2024/
            └── data.parquet
```

---

## 🔍 Monitoring & Logging

### View Function Logs

```bash
# Real-time logs
az functionapp logs tail \
  --name func-finsight-data \
  --resource-group rg-finsight-data

# Application Insights
az monitor app-insights query \
  --app <app-insights-name> \
  --analytics-query "traces | where message contains 'Daily update'"
```

### View Container Apps Logs

```bash
# Job execution logs
az containerapp job execution list \
  --name job-finsight-backfill \
  --resource-group rg-finsight-data

# Specific execution logs
az containerapp job logs show \
  --name job-finsight-backfill \
  --resource-group rg-finsight-data \
  --execution <execution-name>
```

### Execution Reports

Reports are saved to `reports/` in blob storage:

```json
{
  "timestamp": "2024-12-15T06:00:00Z",
  "type": "daily_update",
  "symbols_processed": 500,
  "results": {
    "success": 485,
    "failed": 10,
    "skipped": 5
  }
}
```

---

## 💰 Cost Optimization

### Azure Functions (Consumption Plan)

- **Free grant**: 1M executions + 400,000 GB-s/month
- **Daily updates**: ~20 executions/month = **FREE**
- **Estimated cost**: **$2-5/month** (with overhead)

### Container Apps Jobs

- **Backfill**: 1 execution/month × 2 hours = **$5/month**
- **On-demand only**: No always-on costs

### Total Monthly Cost: **~$10-15**

Compare to Container Apps only: **$50+/month**

---

## 🧪 Testing

### Unit Tests

```bash
# Run tests
pytest tests/

# With coverage
pytest --cov=src tests/
```

### Integration Tests

```bash
# Test local storage
python container/cli.py update-daily --use-local --lookback-days 1

# Test Azure storage
python container/cli.py update-daily --lookback-days 1
```

---

## 🛠️ Development Workflow

### Adding a New Universe Source

1. **Update `config/universes.yaml`:**

```yaml
phase_3:
  - name: "jp_nikkei225"
    source_type: "excel"
    url: "https://example.com/nikkei225.xlsx"
    symbol_column: "Ticker"
    symbol_suffix: ".T"
    enabled: true
```

2. **Refresh universe:**

```bash
python container/cli.py refresh-universe --phase phase_3
```

3. **Test backfill:**

```bash
python container/cli.py backfill \
  --phase phase_3 \
  --start 2023-01-01 \
  --end 2023-12-31 \
  --use-local
```

### Adding a New Data Source

1. **Create processor in `src/universe/universe_builder.py`:**

```python
def _fetch_custom_source(self, config: Dict) -> List[str]:
    # Implementation
    pass
```

2. **Update `_build_universe()` method to handle new source type**

3. **Test locally before deployment**

---

## 📚 API Reference

### DataOperations

```python
from src.core.data_operations import DataOperations

ops = DataOperations(storage, max_workers=10)

# Daily incremental update
results = ops.fetch_daily_incremental(symbols, lookback_days=5)

# Historical backfill
results = ops.fetch_backfill(symbols, "2020-01-01", "2024-12-31")

# Refresh universe
symbols = ops.refresh_universe(phase="phase_1")
```

### Storage

```python
from src.storage import AzureBlobStorage, LocalStorage

# Azure Blob Storage
storage = AzureBlobStorage(
    account_name="stfinsightdata",
    container_name="finsight-data",
    use_managed_identity=True
)

# Local filesystem
storage = LocalStorage(root_path="./data")

# Common operations
storage.save_dataframe(df, "data/AAPL/2024/data.parquet")
df = storage.load_dataframe("data/AAPL/2024/data.parquet")
symbols = storage.load_universe("meta/universe_phase_1.csv")
```

---

## 🚧 Roadmap

- [x] **v2.0**: Hybrid architecture with shared codebase
- [x] **v2.0**: Multi-market universe support (US, HK)
- [ ] **v2.1**: Japan market integration (Nikkei 225)
- [ ] **v2.2**: Europe market integration (STOXX 600)
- [ ] **v2.3**: ESG data integration framework
- [ ] **v2.4**: Real-time intraday data support
- [ ] **v3.0**: ML feature engineering pipeline

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

Copyright © 2024 YCTI Limited. All rights reserved.

---

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/ycti-com/quantx-data-builder/issues)
- **Documentation**: [Wiki](https://github.com/ycti-com/quantx-data-builder/wiki)
- **Email**: <support@ycti.com>

---

## 🙏 Acknowledgments

- **yfinance**: Primary data source
- **Azure Functions**: Serverless scheduling
- **Azure Container Apps**: Heavy workload execution
- **Pandas/PyArrow**: Data processing and storage

---

**Built with ❤️ for enterprise-grade financial data pipelines**
