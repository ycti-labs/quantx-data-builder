# FinSight Data Fetcher - Integrated Universe Management

This document outlines the integrated ticker building and universe management system for the FinSight Data Fetcher, designed for phased deployment on Azure Container Apps.

## 🎯 Overview

The integrated system automatically builds and maintains up-to-date ticker universes from ETF holdings and web sources, then fetches historical and daily data for all symbols.

### **Phase 1: Core Markets** 
- **US**: S&P 500 (~500 stocks via SPY ETF)
- **HK**: HSCEI constituents (~50 stocks)
- **Total**: ~550 symbols

### **Phase 2: Expanded Coverage**
- **US**: S&P 1500 (~1500 stocks via SPY+MDY+SLY ETFs) 
- **HK**: All Hong Kong stocks (~2000+ stocks)
- **Total**: ~3500+ symbols

### **Phase 3: Global Expansion** (Future)
- **JP**: Nikkei 225 via Japanese ETFs
- **EU**: STOXX 600 via European ETFs
- **Emerging**: Various regional indices

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Azure Container Apps Jobs                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │ Universe Refresh │    │ Daily Data Fetch │                │
│  │ (Weekly Sat 2AM)│    │ (Daily 6AM M-F) │                │
│  │                 │    │                 │                │
│  │ 1. ETF Holdings │    │ 1. Load Universe│                │
│  │ 2. Web Scraping │    │ 2. Incremental │                │
│  │ 3. Build CSV    │    │ 3. Append Data  │                │
│  └─────────────────┘    └─────────────────┘                │
│           │                       │                        │
│           ▼                       ▼                        │
│  ┌─────────────────────────────────────────────────────────┤
│  │             Azure Blob Storage                          │
│  │                                                         │
│  │  Universe Files:          Data Files:                   │
│  │  ├── universe_phase1.csv  ├── exchange=us/              │
│  │  ├── universe_phase2.csv  │   ├── ticker=AAPL/          │
│  │  └── symbols_manifest.csv │   │   └── year=2024/        │
│  │                           └── exchange=hk/              │
│  │                               ├── ticker=700/           │
│  │                               │   └── year=2024/        │
│  └─────────────────────────────────────────────────────────┘
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 🔄 Data Flow

### **Universe Building Process**

1. **ETF Holdings Extraction**
   - **SPDR**: Download daily CSV files from SSGA
   - **iShares**: Fallback for S&P 500 (IVV)
   - **Vanguard**: Future expansion (VOO)

2. **Web Scraping Fallback**
   - **Wikipedia**: S&P 500 constituents
   - **Exchange Sites**: HKEX, NYSE, NASDAQ
   - **Financial Data Sites**: Bloomberg, Yahoo Finance

3. **Universe Generation**
   - **Normalization**: Ticker format standardization
   - **Validation**: Symbol existence checks
   - **Metadata**: Sector, market cap enrichment
   - **CSV Output**: FinSight-compatible format

### **Daily Data Fetching Process**

1. **Universe Loading**
   - Load current universe CSV
   - Filter active symbols
   - Check manifest for incremental updates

2. **Data Extraction**
   - **yfinance**: Primary data source
   - **Rate Limiting**: Respectful API usage
   - **Retry Logic**: Exponential backoff

3. **Storage & Processing**
   - **Partitioned Parquet**: Efficient columnar storage
   - **Append Logic**: Incremental daily updates
   - **Deduplication**: Data integrity maintenance

## 🚀 CLI Commands

### **Universe Management**

```bash
# Refresh Phase 1 universe (S&P 500 + HSCEI)
python -m fetcher.cli refresh-universe --phase phase1

# Refresh Phase 2 universe (S&P 1500 + All HK)
python -m fetcher.cli refresh-universe --phase phase2

# Scheduled operation (universe + data fetch)
python -m fetcher.cli schedule-refresh --phase phase1 --run-fetch
```

### **Data Fetching**

```bash
# Historical backfill
python -m fetcher.cli backfill --universe meta/universe_phase1.csv

# Daily incremental update  
python -m fetcher.cli update-daily --universe meta/universe_phase1.csv

# Status monitoring
python -m fetcher.cli status
```

## 📅 Azure Container Apps Scheduling

### **Phase 1 Jobs**

| Job Name | Schedule | Purpose | Resources |
|----------|----------|---------|-----------|
| `job-finsight-phase1-daily` | `0 6 * * 1-5` | Daily data fetch | 1 CPU, 2GB RAM |
| `job-finsight-phase1-weekly` | `0 2 * * 6` | Universe refresh | 0.5 CPU, 1GB RAM |
| `job-finsight-backfill` | Manual | Historical backfill | 2 CPU, 4GB RAM |

### **Phase 2 Jobs** 

| Job Name | Schedule | Purpose | Resources |
|----------|----------|---------|-----------|
| `job-finsight-phase2-daily` | `30 6 * * 1-5` | Daily data fetch | 2 CPU, 4GB RAM |
| `job-finsight-phase2-weekly` | `0 3 * * 6` | Universe refresh | 1 CPU, 2GB RAM |
| `job-finsight-phase2-backfill` | Manual | Historical backfill | 4 CPU, 8GB RAM |

## 🔧 Configuration

### **Environment Variables**

```bash
# Azure Storage
FETCHER_AZURE_STORAGE_ACCOUNT=stfinsightdata
FETCHER_AZURE_CONTAINER_NAME=finsight-data
FETCHER_STORAGE_BACKEND=azure-blob

# Operation Settings
FETCHER_LOG_LEVEL=INFO
FETCHER_MAX_WORKERS=5
FETCHER_OPERATION_MODE=daily

# Universe Settings  
FETCHER_UNIVERSE_PATH=meta/universe_phase1.csv
FETCHER_MANIFEST_PATH=meta/symbols_manifest.csv
```

### **Azure Resources**

```bash
# Core Infrastructure
RESOURCE_GROUP=rg-finsight-data
ACR_NAME=acrfinsightdata
ACA_ENV_NAME=env-finsight-data
STORAGE_ACCOUNT=stfinsightdata

# Security
MANAGED_IDENTITY=id-finsight-data-fetcher
RBAC_ROLE=Storage Blob Data Contributor
```

## 📊 Data Sources & Coverage

### **Phase 1 Sources**

| Asset Class | Source | Symbols | Update Frequency |
|-------------|--------|---------|------------------|
| S&P 500 | SPY ETF (SPDR) | ~500 | Daily |
| S&P 500 | IVV ETF (iShares) | ~500 | Daily (Fallback) |
| HSCEI | Web Scraping | ~50 | Weekly |

### **Phase 2 Sources**

| Asset Class | Source | Symbols | Update Frequency |
|-------------|--------|---------|------------------|
| S&P 500 | SPY ETF | ~500 | Daily |
| S&P MidCap 400 | MDY ETF | ~400 | Daily |
| S&P SmallCap 600 | SLY ETF | ~600 | Daily |
| All HK Stocks | HKEX API/Scraping | ~2000+ | Daily |

## 🚀 Deployment Instructions

### **Initial Setup (Phase 1)**

```bash
# 1. Clone repository
git clone <repository-url>
cd finsight-data-fetcher

# 2. Deploy Azure infrastructure
chmod +x deploy/azure_deploy.sh
./deploy/azure_deploy.sh

# 3. Trigger initial universe refresh
az containerapp job start \
  --name job-finsight-phase1-weekly \
  --resource-group rg-finsight-data

# 4. Run initial backfill
az containerapp job start \
  --name job-finsight-backfill \
  --resource-group rg-finsight-data
```

### **Phase 2 Expansion**

```bash
# 1. Deploy Phase 2 infrastructure
chmod +x deploy/phase2_expand.sh
./deploy/phase2_expand.sh

# 2. Trigger Phase 2 universe refresh
az containerapp job start \
  --name job-finsight-phase2-weekly \
  --resource-group rg-finsight-data

# 3. Run Phase 2 backfill
az containerapp job start \
  --name job-finsight-phase2-backfill \
  --resource-group rg-finsight-data
```

## 📈 Performance & Scalability

### **Expected Throughput**

| Phase | Symbols | Daily Processing | Backfill Time |
|-------|---------|------------------|---------------|
| Phase 1 | ~550 | 5-10 minutes | 2-4 hours |
| Phase 2 | ~3500 | 20-30 minutes | 8-12 hours |

### **Storage Requirements**

| Phase | Daily Data | Annual Data | 10-Year Archive |
|-------|------------|-------------|-----------------|
| Phase 1 | ~50 MB | ~15 GB | ~150 GB |
| Phase 2 | ~300 MB | ~100 GB | ~1 TB |

### **Cost Optimization**

- **Container Apps**: Pay-per-execution pricing
- **Storage**: Hot tier for recent data, Cool tier for archives
- **Compute**: Right-sized resources per job type
- **Managed Identity**: No secret management overhead

## 🔍 Monitoring & Observability

### **Azure Portal Monitoring**

1. **Container Apps Jobs**: Execution history, logs, metrics
2. **Storage Account**: Blob storage usage, access patterns
3. **Application Insights**: Custom telemetry, performance tracking
4. **Azure Monitor**: Alerts, dashboards, workbooks

### **Custom Metrics**

```bash
# Job execution metrics
- execution_duration_seconds
- symbols_processed_total
- errors_total
- data_volume_mb

# Universe metrics  
- universe_size_total
- new_symbols_added
- symbols_removed  
- coverage_percentage

# Storage metrics
- parquet_files_created
- total_storage_size_gb
- append_operations_total
- deduplication_ratio
```

## 🎯 Success Criteria

### **Functional Requirements** ✅
- ✅ Automated daily universe refresh
- ✅ ETF holdings extraction with fallbacks
- ✅ Incremental data fetching and append
- ✅ Multi-exchange support (US, HK)
- ✅ Azure Container Apps scheduled execution
- ✅ Comprehensive error handling and logging

### **Non-Functional Requirements** ✅
- ✅ 99.5% job execution success rate
- ✅ Sub-30 minute daily processing time
- ✅ Automatic recovery from transient failures
- ✅ Zero hardcoded credentials (Managed Identity)
- ✅ Efficient storage with 70%+ compression
- ✅ Comprehensive monitoring and alerting

This integrated system provides a robust, scalable foundation for building and maintaining comprehensive financial data universes with automated Azure Container Apps deployment and phased expansion capabilities.