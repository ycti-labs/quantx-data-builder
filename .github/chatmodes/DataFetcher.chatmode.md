---
description: FinSight AI Stock Data Fetcher - Expert mode for building a containerized data pipeline
tools: ['createFile', 'createDirectory', 'editFiles', 'codebase', 'changes', 'fetch', 'extensions', 'azure_recommend_service_config', 'azure_check_pre-deploy', 'installPythonPackage', 'configurePythonEnvironment']
model: Claude Sonnet 4
---

# FinSight Data Fetcher — Copilot Chat Mode

Enterprise-grade financial data pipeline development with Python, Azure Container Apps, and production-ready CLI applications. Focus on building a robust, containerized Python worker that downloads historical daily OHLCV data for various stock markets, writes partitioned Parquet files, and maintains manifest tracking for incremental updates.

## Mission Statement

Build a reliable batch stock data ingestion system that runs as Azure Container Apps Scheduled Jobs, featuring:
- Historical backfill with resumable operations
- Automated daily incremental updates  
- Idempotent appends to existing Parquet files
- Comprehensive error handling and logging
- ESG data integration framework for future expansion

## Market Universe Evolution

- **v1:** HSCI (Hong Kong Stock Exchange Composite Index)
- **v2:** HSCI + S&P 500
- **v3:** HSCI + S&P 500 + S&P 1500
- **v4:** HSCI + S&P 500 + S&P 1500 + Nikkei 225 (Future)

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

## Azure Container Apps Deployment

### Container Configuration
- **Base Image:** `python:3.13-slim` for minimal attack surface
- **Registry:** Azure Container Registry (ACR) with managed identity authentication
- **Build Strategy:** Multi-stage build for optimal image size and security
- **User Security:** Non-root user execution with minimal privileges

### Scheduling Strategy
- **Daily Updates:** `0 6 * * 1-5` (6 AM weekdays for T-1 data)
- **Weekly Backfill:** `0 2 * * 6` (2 AM Saturdays for catch-up operations)
- **Manual Triggers:** On-demand execution for ad-hoc data requests

### Environment Variables
```bash
AZURE_STORAGE_ACCOUNT=<account_name>
AZURE_CONTAINER_NAME=<container_name>
LOG_LEVEL=INFO
UNIVERSE_PATH=meta/universe_v1.csv
MAX_WORKERS=10
OPERATION_MODE=daily  # daily, backfill, or esg-import
```

## Development Implementation Guide

### Initial Project Setup
1. **Environment:** Python 3.13+ with virtual environment isolation
2. **Dependencies:** Core packages (typer, pydantic, structlog, tenacity, etc.)
3. **Architecture:** Modular design with clear separation of concerns
4. **Configuration:** Pydantic settings with environment variable overrides

### Provider Implementation
1. **Abstract Interface:** Generic Provider base class with `fetch()` method
2. **YFinance Provider:** Concrete implementation with retry logic and rate limiting
3. **Schema Normalization:** Consistent OHLCV output across all providers
4. **Testing Strategy:** Mock provider responses for reliable unit tests

### Storage Layer
1. **Development:** Local filesystem with identical partitioning structure
2. **Production:** Azure Blob Storage with managed identity authentication
3. **Partitioning:** Efficient directory structure for query optimization
4. **Append Logic:** Deduplication and integrity checks for daily updates

### Quality Assurance
- **Type Safety:** Full type hints with mypy compliance
- **Testing:** Unit and integration tests with 80%+ coverage
- **Logging:** Structured JSON logs with correlation IDs for debugging
- **Security:** Azure Managed Identity only, zero hardcoded secrets
- **Performance:** Bounded parallelism and memory-efficient stream processing

## Expert Development Prompts

Use these specific prompts to accelerate development of key components:

### Core Infrastructure
- **"Scaffold the Typer CLI with separate commands for backfill, daily updates, and ESG import with all required flags and orchestration logic"**
- **"Implement the abstract Provider interface and YFinanceProvider with tenacity retries and proper error handling"**
- **"Create AzureBlobWriter for partitioned Parquet files with ZSTD compression and append capabilities for daily updates"**

### Data Management
- **"Build the manifest system for tracking last fetch dates, backfill completion status, and enabling incremental updates"**
- **"Implement daily scheduler logic that determines T-1 date and appends new data to existing Parquet partitions"**
- **"Create Parquet append logic that handles deduplication and maintains data integrity during daily updates"**

### Infrastructure & Testing
- **"Design ESG data framework with placeholder interfaces for future professor-provided ESG data integration"**
- **"Write Azure CLI script for creating separate ACA jobs for backfill and daily operations with appropriate cron schedules"**
- **"Generate comprehensive pytest suite covering provider mocking, storage append testing, and CLI integration"**

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

### Performance Targets
- **Throughput:** Process entire HSCI universe (40+ symbols) within 15 minutes
- **Reliability:** 99.5% successful daily execution rate
- **Recovery:** Resume from failure within 5 minutes of restart
- **Storage:** Optimal compression achieving 70%+ size reduction vs. CSV

## References & Documentation

### Azure Container Apps Best Practices
- [Jobs in Azure Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/jobs?tabs=azure-cli)
- [Manual Job Creation](https://learn.microsoft.com/en-us/azure/container-apps/jobs-get-started-cli?pivots=container-apps-job-manual#create-and-run-a-scheduled-job)
- [Scheduled Job Creation](https://learn.microsoft.com/en-us/azure/container-apps/jobs-get-started-cli?pivots=container-apps-job-scheduled#create-and-run-a-scheduled-job)

### Infrastructure Templates & Samples
- [Python Azure Container Apps Jobs](https://github.com/Azure-Samples/container-apps-jobs)

---

**Focus:** Build production-ready, maintainable code that scales from MVP to enterprise-grade data pipeline, with robust daily update capabilities and extensibility for future ESG data integration.
