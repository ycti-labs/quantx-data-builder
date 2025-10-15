---
description: QuantX AI Stock Data Fetcher - Expert mode for building a containerized financial data pipeline
tools: ['edit', 'runNotebooks', 'search', 'new', 'runCommands', 'runTasks', 'Azure MCP/*', 'pylance mcp server/*', 'usages', 'vscodeAPI', 'problems', 'changes', 'testFailure', 'openSimpleBrowser', 'fetch', 'githubRepo', 'ms-azuretools.vscode-azureresourcegroups/azureActivityLog', 'ms-python.python/getPythonEnvironmentInfo', 'ms-python.python/getPythonExecutableCommand', 'ms-python.python/installPythonPackage', 'ms-python.python/configurePythonEnvironment', 'extensions', 'todos', 'runTests']
model: Claude Sonnet 4.5
---

# QuantX Financial Data Builder — Copilot Chat Mode

Enterprise-grade financial data pipeline development with Python, Azure Container Apps, and production-ready CLI applications. Focus on building a robust, containerized Python worker that downloads historical daily OHLCV data for various stock markets, writes partitioned Parquet files, and maintains manifest tracking for incremental updates.

## Mission Statement

Build a reliable batch stock data ingestion system that runs as Azure Container Apps Scheduled Jobs, featuring:
- Universal support for multiple markets (US, HK, JP, EU)
- Historical backfill with resumable operations
- Automated daily incremental updates  
- Idempotent appends to existing Parquet files
- Comprehensive error handling and logging
- ESG data integration framework for future expansion

## Market Universe Evolution

Multiple market-specific universes, following the industry standard. The market-specific logic is encapsulated in separate builder classes (US, HK, JP, EU, etc.), allowing easy extension to new markets.
Phased approach to gradually expand coverage:

### Industry Standards Compliance

**Complete Modular Universe Architecture**
- Separated core abstractions from market-specific builders
- Official HKEX data integration with live Excel download
- Industry-standard universe definitions (S&P indices, HKEX comprehensive)
- Enterprise-grade error handling with fallback mechanisms

**Data Quality & Coverage**
- **HKEX**: Official securities list from `https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx`
- **US Markets**: ETF holdings extraction from SPDR/iShares
- **Fallback Sources**: Wikipedia, static lists for resilience
- **Official sources**: Direct from HKEX Excel download, not static lists
- **Real-time accuracy**: Always current with market additions/delistings
- **Industry standard**: S&P indices from official ETF constituent data

### 🌍 Supported Universes

**US Markets:**
- `us_sp500`: S&P 500 (from SPY ETF holdings)
- `us_sp400`: S&P 400 Mid-Cap (from MDY ETF holdings)
- `us_sp600`: S&P 600 Small-Cap (from SLY ETF holdings)
- `us_sp1500`: S&P 1500 Composite (combined)

**Hong Kong Markets:**
- `hk_hsi`: Hang Seng Index (~80 stocks)
- `hk_hscei`: Hang Seng China Enterprises Index (~50 stocks)
- `hk_all`: **Complete HKEX universe (~2000+ stocks from official source)**

**Future Markets:**
- Japan: Nikkei 225 via Japanese ETFs
- Europe: STOXX 600 via European ETFs

### Phase Definitions

- **v1:** HSI + S&P 500
- **v2:** v1 + All Hong Kong Stocks
- **v3:** v2 + S&P (500/400/600)
- **v4:** v3 + Nikkei 225 (Future)

### Downloadable sources for Market Universes
- [HKEX Securities List](https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx)
- [S&P 500 ETF Holdings](https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx)


## Architecture & Technology Stack

### Core Technologies
- **Python 3.13+** with type hints and mypy compliance
- **Typer** for CLI interface with rich help and structured commands
- **Pydantic** for configuration management and data validation
- **Tenacity** for retry logic and exponential backoff
- **Structlog** for JSON structured logging with correlation IDs
- **Pandas/PyArrow** for data processing and Parquet I/O
- **Azure Storage Blob** + **Azure Identity** for cloud storage
- **Pytest** for comprehensive testing

### Data Sources & Extensibility
- **Primary:** yfinance (robust, free, comprehensive)
- **Future:** CRSP/WRDS/HKEX direct APIs
- **ESG:** Framework prepared for professor-provided ESG data integration

### OHLCV Data Contract
```json
{
  "Date": "datetime64[ns] (UTC-naive)",
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

### Storage Strategy
- **Partitioning:** `/data/exchange=<us|hk>/ticker=<SYMBOL>/year=YYYY/part-*.parquet`
- **Compression:** ZSTD for optimal size/speed balance
- **Append Logic:** New daily data appended with automatic deduplication
- **Actions Data:** Optional dividends and splits storage
- **ESG Path:** Reserved directory structure for future ESG data

### Manifest Tracking System
- **File Format:** `symbols_manifest.(csv|parquet)`
- **Schema:** `[ticker: str, exchange: str, last_date: datetime, backfill_complete: bool]`
- **Purpose:** Enable incremental updates and resume capability after failures

## CLI Interface & Commands

### Historical Backfill
```bash
python -m fetcher.cli backfill \
  --universe meta/universe_v1.csv \
  --out-root /data \
  --manifest meta/symbols_manifest.csv \
  --start 2014-01-01 \
  --end 2024-12-31 \
  --max-workers 10 \
  --fetch-actions \
  --log-level INFO
```

### Daily Incremental Update
```bash
python -m fetcher.cli update-daily \
  --universe meta/universe_v1.csv \
  --out-root /data \
  --manifest meta/symbols_manifest.csv \
  --max-workers 5 \
  --fetch-actions \
  --log-level INFO
```

### ESG Data Import (Future)
```bash
python -m fetcher.cli import-esg \
  --esg-data-path /imports/esg_data.csv \
  --out-root /data \
  --manifest meta/symbols_manifest.csv \
  --log-level INFO
```

### CLI Behavior Patterns
- **Resume Logic:** Check manifest for last_date, fetch incrementally from that point
- **Append Operations:** Add new data with automatic deduplication
- **Rate Limiting:** Respect provider limits with polite delays between requests
- **Error Handling:** Exponential backoff with structured logging and correlation IDs

## Azure Deployment

**Use Azure Functions for:**
- Daily scheduled updates (cheap, automatic scaling)
- Weekly universe refresh
- Trigger-based operations

**Use Container Apps Jobs for:**
- Heavy backfill operations (2-4 hours runtime)
- High parallelism needs (10-20 workers)

## Proposed Architecture

Shared Core Logic -> Two Deployment Targets:

1. **Azure Functions:** (daily updates, universe refresh) - lightweight, scheduled
2. **Container Apps Jobs:** (heavy backfill) - containerized, manual triggers

### Container Configuration
- **Base Image:** `python:3.13-slim` for minimal attack surface
- **Registry:** Azure Container Registry (ACR) with managed identity authentication
- **Build Strategy:** Multi-stage build for optimal image size and security
- **User Security:** Non-root user execution with minimal privileges

### Scheduling Strategy
- **Daily Updates:** `0 6 * * 1-5` (6 AM weekdays for T-1 data)
- **Weekly Backfill:** `0 2 * * 6` (2 AM Saturdays for catch-up operations)
- **Manual Triggers:** On-demand execution for ad-hoc data requests

## Implementation Plan

1. Shared core module structure

```plain
quantx-data-builder/
├── src/                          # Shared business logic
│   ├── config_loader.py
│   ├── data_fetcher.py          # Core fetching logic
│   ├── universe_builder.py
│   ├── processors/
│   └── storage/
├── azure_functions/              # Functions-specific code
│   ├── function_app.py
│   ├── host.json
│   └── requirements.txt
├── container/                    # Container-specific code
│   ├── Dockerfile
│   ├── cli.py                   # CLI entry point
│   └── requirements.txt
└── deploy/
    ├── deploy_functions.sh
    └── deploy_container.sh
```

2. Benefits of Shared Codebase

- Single codebase for all business logic
- Shared dependencies and configurations
- Easy testing - test once, deploy twice
- Cost optimized - right tool for each job
- No code duplication - DRY principle

3. How it works
- Azure Functions imports src/ and wraps in function triggers
- Container Apps imports src/ and wraps in CLI commands
- Both use the same DataFetcher, UniverseBuilder, and Storage modules

## Success Criteria & Deliverables

### Functional Requirements ✅
- Complete 10-year historical backfill for entire market universe
- Automated daily incremental updates via Azure Container Apps scheduler
- Seamless append operations to existing Parquet files with deduplication
- Schema compliance and comprehensive data quality validation
- Manifest accuracy enabling recovery and incremental processing
- ESG framework readiness for future professor-provided data integration

### Non-Functional Requirements ✅
- Container startup under 30 seconds for responsive job execution
- Graceful handling of provider outages with exponential backoff
- Comprehensive structured logging for operational debugging
- Zero hardcoded secrets or credentials (Azure Managed Identity only)
- Mypy type checking and pytest test suite passing in CI/CD
- Efficient append operations minimizing file I/O overhead
- Reliable daily scheduler execution with error recovery
- ESG data integration pathway architecturally prepared

## References & Documentation

### Azure Container Apps Best Practices
- [Jobs in Azure Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/jobs?tabs=azure-cli)
- [Manual Job Creation](https://learn.microsoft.com/en-us/azure/container-apps/jobs-get-started-cli?pivots=container-apps-job-manual#create-and-run-a-scheduled-job)
- [Scheduled Job Creation](https://learn.microsoft.com/en-us/azure/container-apps/jobs-get-started-cli?pivots=container-apps-job-scheduled#create-and-run-a-scheduled-job)

### Infrastructure Templates & Samples
- [Python Azure Container Apps Jobs](https://github.com/Azure-Samples/container-apps-jobs)


**Focus:** Build production-ready, maintainable code that scales from MVP to enterprise-grade data pipeline, with robust daily update capabilities and extensibility for future ESG data integration.
