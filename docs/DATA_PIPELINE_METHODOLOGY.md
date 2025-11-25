# QuantX Data Pipeline Methodology

**Document Version:** 1.0  
**Last Updated:** November 24, 2025  
**Research Period:** 2014-01-01 to 2024-12-01

---

## Executive Summary

This document describes the methodology for building a production-grade financial data pipeline for quantitative equity research. The pipeline focuses on S&P 500 historical membership, daily OHLCV price data, and ESG metrics, providing a robust foundation for factor-based investment research.

**Key Achievements:**
- **Membership Data:** 763 unique tickers, 345,870 daily records (2014-2025)
- **Price Data:** 787 tickers with complete OHLCV history
- **ESG Coverage:** 500 tickers with 93.8% continuous data from 2014
- **Universal ID Mapping:** 52,834 GVKEY records for ticker resolution

---

## 1. Architecture Overview

### 1.1 Design Principles

1. **Single Source of Truth:** Prices stored once per exchange+ticker, no duplication across universes
2. **Point-in-Time Membership:** Separate storage of universe membership from price data
3. **Immutable Facts:** Price history treated as immutable FACT tables
4. **Slowly Changing Dimensions:** Membership and metadata as DIM tables
5. **Universal Identifiers:** GVKEY-based mapping for handling ticker changes and corporate actions

### 1.2 Data Lake Structure

```
/data/curated/
├── tickers/                        # FACT: immutable price history
│   ├── exchange=us/
│   │   ├── ticker=AAPL/
│   │   │   ├── prices/
│   │   │   │   ├── freq=daily/
│   │   │   │   │   ├── year=2000/
│   │   │   │   │   │   └── part-000.parquet
│   │   │   │   │   ├── year=2001/
│   │   │   ├── esg/
│   │   │   │   ├── year=2006/
│   │   │   │   │   └── part-000.parquet
│   │   │   ├── fundamentals/
│   │   │       ├── statement=income/
│   │   │           ├── year=2020/
├── membership/                    # DIM: index/sector universes
│   ├── universe=sp500/
│   │   ├── mode=daily/
│   │   │   └── sp500_membership_daily.parquet
│   │   ├── mode=intervals/
│   │       └── sp500_membership_intervals.parquet
├── metadata/                      # DIM: slowly changing dimensions
│   ├── gvkey.parquet              # Universal ticker-to-GVKEY mapper
│   ├── symbols.parquet            # Symbol master table
│   ├── issuers.parquet            # Company/issuer details
│   └── corporate_actions.parquet  # Splits, dividends, etc.
```

### 1.3 Technology Stack

- **Python 3.12+** with type hints and mypy compliance
- **Pandas/PyArrow** for data processing and Parquet I/O
- **Pydantic** for configuration management
- **Structlog** for JSON structured logging
- **Tenacity** for retry logic with exponential backoff
- **Azure Storage** for cloud persistence (future)

---

## 2. Data Sources & Quality

### 2.1 Primary Data Providers

**Tiingo API** (Current Implementation)
- **Coverage:** US equities, global indices
- **Data Types:** Daily OHLCV, adjusted prices, corporate actions
- **Reliability:** Free tier with 500 req/hr limit, paid tiers available
- **Quality:** Industry-standard adjusted prices with split/dividend handling

**Future Extensions:**
- CRSP/WRDS for academic-grade data
- Bloomberg/Refinitiv for institutional coverage
- Exchange APIs (HKEX, JPX) for regional markets

### 2.2 Data Quality Metrics

**As of November 2025:**

| Metric | Coverage | Quality |
|--------|----------|---------|
| S&P 500 Membership | 763 tickers (2014-2025) | Complete |
| Daily Price Data | 787 tickers | 100% available |
| ESG Data (2014+) | 469/500 continuous (93.8%) | High |
| ESG Latest Date | 487/500 at 2024-12-01 (97.4%) | Current |
| GVKEY Mapping | 52,834 records | Comprehensive |

**Research Period Coverage:**
- **Start Date:** 2014-01-01 (chosen for ESG data availability)
- **End Date:** 2024-12-01 (latest ESG data)
- **Active Tickers:** 745 tickers during research period
- **Backtest Ready:** 469 tickers with continuous ESG data

---

## 3. Membership Data Methodology

### 3.1 Universe Definition

**S&P 500 Historical Constituents:**
- **Source:** "S&P 500 Historical Components & Changes(11-16-2025).csv"
- **Source File Size:** 5.2 MB, 2,702 lines
- **Historical Range:** 1996-01-02 to 2025-11-11
- **Configuration:** Defined in `config/settings.yaml`

```yaml
universe:
  sp500:
    membership_file: "S&P 500 Historical Components & Changes(11-16-2025).csv"
    data_tolerance: 0.1
```

### 3.2 Dual Membership Representation

The pipeline maintains **two complementary views** of universe membership:

#### 3.2.1 Daily Snapshot Mode

**File:** `data/curated/membership/universe=sp500/mode=daily/sp500_membership_daily.parquet`

**Schema:**
```
date: date              # Trading date
ticker: string          # Ticker symbol
universe: string        # 'sp500'
```

**Purpose:** Exact constituents for point-in-time analysis

**Statistics:**
- Total Records: 345,870
- Unique Dates: 687
- Unique Tickers: 763
- Date Range: 2014-01-07 to 2025-11-11

**Use Case:** 
```python
# Get constituents on a specific date
df_daily = pd.read_parquet('data/curated/membership/universe=sp500/mode=daily/...')
members_2020 = df_daily[df_daily['date'] == '2020-01-15']['ticker'].tolist()
```

#### 3.2.2 Interval/Timeline Mode

**File:** `data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet`

**Schema:**
```
ticker: string          # Ticker symbol
universe: string        # 'sp500'
start_date: date        # Inclusion date
end_date: date          # Exclusion date (or current)
```

**Purpose:** Efficient range queries and timeline analysis

**Statistics:**
- Total Intervals: 772
- Unique Tickers: 763
- Date Range: 2014-01-07 to 2025-11-11

**Use Case:**
```python
# Check if ticker was member during period
df_int = pd.read_parquet('data/curated/membership/universe=sp500/mode=intervals/...')
mask = (df_int['start_date'] <= '2020-12-31') & (df_int['end_date'] >= '2020-01-01')
members_2020 = df_int[mask]['ticker'].unique()
```

### 3.3 Membership Build Process

**Script:** `src/programs/rebuild_sp500_membership.py`

**Build Modes:**

1. **Incremental Update** (Default):
   ```bash
   python src/programs/rebuild_sp500_membership.py
   ```
   - Merges new data with existing records
   - Preserves historical data
   - Fast, idempotent operation

2. **Full Rebuild:**
   ```bash
   python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01
   ```
   - Deletes existing membership data
   - Rebuilds from scratch
   - Ensures data consistency after CSV updates

**Configuration Integration:**
```python
# Reads CSV filename from config/settings.yaml
config = Config("config/settings.yaml")
membership_file = config.get('universe.sp500.membership_file')
universe = SP500Universe(data_root=config.get('storage.local.root_path'))
stats = universe.build_membership(
    min_date='2014-01-01',
    rebuild=True,
    membership_filename=membership_file
)
```

---

## 4. Universal Ticker Resolution

### 4.1 The GVKEY System

**File:** `data/curated/metadata/gvkey.parquet`

**Purpose:** Resolve ticker changes, mergers, and cross-listings using GVKEY (Global Company Key) as universal identifier.

**Schema:**
```
gvkey: int              # Global Company Key (S&P Capital IQ identifier)
ticker: string          # Normalized ticker symbol
ticker_raw: string      # Original ticker from source
```

**Statistics:**
- Total Records: 52,834
- Unique Tickers: 46,168
- Unique GVKEYs: 52,834

### 4.2 Ticker Normalization Logic

**Challenges Addressed:**
1. **Ticker Changes:** Company renames (e.g., FB → META)
2. **Corporate Actions:** Spin-offs, mergers, acquisitions
3. **Cross-Listings:** Same company on multiple exchanges
4. **Symbol Reuse:** Same ticker assigned to different companies over time

**Resolution Strategy:**
```python
# Example: Resolve historical ticker to current GVKEY
gvkey_map = pd.read_parquet('data/curated/metadata/gvkey.parquet')

def resolve_ticker(ticker: str) -> int:
    """Get GVKEY for a ticker symbol"""
    match = gvkey_map[gvkey_map['ticker'] == ticker]
    if not match.empty:
        return match.iloc[0]['gvkey']
    return None

# Reverse lookup: Find all historical tickers for a company
def get_ticker_history(gvkey: int) -> list[str]:
    """Get all historical tickers for a GVKEY"""
    return gvkey_map[gvkey_map['gvkey'] == gvkey]['ticker'].tolist()
```

### 4.3 Integration with Price Data

**Current State:** Prices are stored by ticker symbol in file paths for human readability:
```
data/curated/tickers/exchange=us/ticker=AAPL/prices/...
```

**Internal Representation:** Should use `ticker_id` or `gvkey` for joins:
```python
# Load prices with GVKEY mapping
prices = pd.read_parquet('data/curated/tickers/exchange=us/ticker=AAPL/prices/...')
gvkey_map = pd.read_parquet('data/curated/metadata/gvkey.parquet')

# Enrich with GVKEY
prices_with_id = prices.merge(
    gvkey_map[['ticker', 'gvkey']], 
    left_on='symbol', 
    right_on='ticker'
)
```

---

## 5. ESG Data Pipeline

### 5.1 ESG Data Coverage

**Scope:**
- **Total Tickers with ESG:** 500 out of 787 total tickers
- **Analysis Period:** 2006-2024 (full history), 2014-2024 (research focus)
- **Update Frequency:** Monthly
- **Latest Data Date:** 2024-12-01

**Quality Metrics:**

| Period | Continuous Tickers | Coverage | Notes |
|--------|-------------------|----------|-------|
| 2006-2024 | 458/500 (91.6%) | Full History | Some early gaps acceptable |
| 2014-2024 | 469/500 (93.8%) | Research Period | Recommended for backtests |
| At 2024-12-01 | 487/500 (97.4%) | Latest Date | Current/active coverage |

### 5.2 ESG Data Structure

**Storage Location:** `data/curated/tickers/exchange=us/{ticker}/esg/year={year}/part-000.parquet`

**Schema (Typical):**
```
date: date              # Observation date (monthly)
gvkey: int              # Company identifier
environment_score: float
social_score: float
governance_score: float
esg_combined_score: float
# Additional metrics vary by provider
```

**Partitioning Strategy:**
- **By Ticker:** One directory per ticker
- **By Year:** Annual partitions within each ticker
- **Format:** Parquet with Snappy compression

### 5.3 ESG Data Quality Checks

**Script:** `src/programs/check_esg_continuity.py`

**Checks Performed:**
1. **Temporal Continuity:** Detects gaps in monthly time series
2. **Coverage Analysis:** Tracks which tickers have complete data
3. **Latest Date Validation:** Ensures data is current
4. **Gap Reporting:** Generates CSV with detailed gap analysis

**Usage:**
```bash
# Check continuity from 2014 onwards (research period)
python src/programs/check_esg_continuity.py

# Output:
# ✅ 469/500 tickers continuous from 2014-01-01
# ✅ 487/500 tickers have data through 2024-12-01
# 📊 Report saved to: data/results/esg_continuity_report.csv
```

**Continuity Definition:**
- **Continuous:** No gaps longer than 1 month between observations
- **Acceptable:** Minor gaps at series start/end
- **Problematic:** Large gaps during active trading period

---

## 6. Price Data Specifications

### 6.1 Daily OHLCV Schema

**File Pattern:** `data/curated/tickers/exchange=us/{ticker}/prices/freq=daily/year={year}/part-000.parquet`

**Schema:**
```
date: date              # Trading date
gvkey: int              # Company identifier
open: double            # Unadjusted open price
high: double            # Unadjusted high price
low: double             # Unadjusted low price
close: double           # Unadjusted close price
volume: int64           # Trading volume
adj_open: double        # Split/dividend adjusted open
adj_high: double        # Split/dividend adjusted high
adj_low: double         # Split/dividend adjusted low
adj_close: double       # Split/dividend adjusted close
adj_volume: int64       # Adjusted volume
div_cash: double        # Cash dividend amount
split_factor: double    # Split ratio
exchange: string        # 'us'
currency: string        # 'USD'
freq: string            # 'daily'
year: int               # Materialized partition key
```

**Compression:** Snappy (default) for balanced speed/size

**Coverage:**
- **Total Tickers:** 787 in `data/curated/tickers/exchange=us/`
- **Historical Depth:** Typically 2000-present (varies by ticker)
- **Completeness:** 100% for tickers in universe during trading dates

### 6.2 Adjusted Prices

**Adjustment Method:** Backward-adjusted for corporate actions

**Formula:**
```
adj_price = raw_price * adjustment_factor

where adjustment_factor accounts for:
- Stock splits (e.g., 2:1 split → multiply pre-split prices by 0.5)
- Cash dividends (optional, depends on provider)
- Spin-offs and distributions
```

**Best Practices:**
- Use `adj_close` for returns calculation
- Use `close` for actual transaction prices
- Verify adjustment factor consistency with `split_factor` and `div_cash`

### 6.3 Volume Adjustments

**Principle:** Volume is adjusted inversely to price adjustments

```
adj_volume = raw_volume / adjustment_factor

Example: 2:1 split
- Pre-split: 1M shares at $100 → Adjusted: 2M shares at $50
- Maintains market cap consistency
```

---

## 7. Query Patterns & Best Practices

### 7.1 Point-in-Time Universe Queries

**Use Case:** Get constituents as of a specific date

**Method 1: Using Intervals**
```python
import pandas as pd
from datetime import date

def get_constituents_asof(universe: str, asof: str) -> list[str]:
    """Get universe members as of a specific date using interval table"""
    df = pd.read_parquet(
        f'data/curated/membership/universe={universe}/mode=intervals/'
        f'{universe}_membership_intervals.parquet'
    )
    
    asof_date = pd.to_datetime(asof).date()
    mask = (df['start_date'] <= asof_date) & (df['end_date'] >= asof_date)
    return df[mask]['ticker'].unique().tolist()

# Example
sp500_2020 = get_constituents_asof('sp500', '2020-01-15')
print(f"S&P 500 had {len(sp500_2020)} constituents on 2020-01-15")
```

**Method 2: Using Daily Snapshots**
```python
def get_constituents_daily(universe: str, date: str) -> list[str]:
    """Get universe members using daily snapshot table"""
    df = pd.read_parquet(
        f'data/curated/membership/universe={universe}/mode=daily/'
        f'{universe}_membership_daily.parquet'
    )
    
    return df[df['date'] == date]['ticker'].tolist()

# Example
sp500_2020 = get_constituents_daily('sp500', '2020-01-15')
```

**When to Use Which:**
- **Intervals:** Faster for single-date queries, range checks, timeline analysis
- **Daily:** Better for iterating over many dates, exact historical reconstruction

### 7.2 Loading Prices for Universe

**Use Case:** Load price data for all universe members over a backtest window

```python
def load_universe_prices(
    universe: str, 
    start: str, 
    end: str,
    use_adjusted: bool = True
) -> pd.DataFrame:
    """
    Load prices for all universe members during period
    
    Args:
        universe: Universe name ('sp500')
        start: Start date (ISO format)
        end: End date (ISO format)
        use_adjusted: Use adjusted prices (default True)
    
    Returns:
        DataFrame with columns: [date, ticker, open, high, low, close, volume, ...]
    """
    # Get constituents at end of period (or use rolling membership)
    members = get_constituents_asof(universe, end)
    
    # Determine years to scan
    start_year = pd.to_datetime(start).year
    end_year = pd.to_datetime(end).year
    years = range(start_year, end_year + 1)
    
    # Load GVKEY mapping
    gvkey_map = pd.read_parquet('data/curated/metadata/gvkey.parquet')
    
    # Collect price files
    all_prices = []
    for ticker in members:
        for year in years:
            price_file = (
                f'data/curated/tickers/exchange=us/ticker={ticker}/'
                f'prices/freq=daily/year={year}/part-000.parquet'
            )
            if Path(price_file).exists():
                df = pd.read_parquet(price_file)
                all_prices.append(df)
    
    # Concatenate and filter by date range
    prices = pd.concat(all_prices, ignore_index=True)
    mask = (prices['date'] >= pd.to_datetime(start)) & (prices['date'] <= pd.to_datetime(end))
    
    return prices[mask]

# Example: Load S&P 500 prices for 2020
sp500_2020 = load_universe_prices('sp500', '2020-01-01', '2020-12-31')
print(f"Loaded {len(sp500_2020):,} price records for {sp500_2020['ticker'].nunique()} tickers")
```

### 7.3 Point-in-Time Membership Filter

**Use Case:** Strict survivorship-bias-free backtesting with date-by-date membership

```python
def load_pit_universe_prices(universe: str, start: str, end: str) -> pd.DataFrame:
    """
    Load prices with strict point-in-time membership filtering
    Only includes prices for tickers that were members on each specific date
    """
    # Load daily membership
    membership = pd.read_parquet(
        f'data/curated/membership/universe={universe}/mode=daily/'
        f'{universe}_membership_daily.parquet'
    )
    
    # Load all prices (use broader set to avoid missing data)
    prices = load_universe_prices(universe, start, end)
    
    # Join on (date, ticker) to enforce point-in-time membership
    pit_prices = prices.merge(
        membership[['date', 'ticker']],
        on=['date', 'ticker'],
        how='inner'
    )
    
    return pit_prices

# Example: Backtest-ready dataset
sp500_backtest = load_pit_universe_prices('sp500', '2014-01-01', '2024-12-01')
print(f"Point-in-time dataset: {len(sp500_backtest):,} records")
```

### 7.4 Combining Price and ESG Data

**Use Case:** Load aligned price and ESG data for factor analysis

```python
def load_universe_with_esg(
    universe: str,
    start: str,
    end: str
) -> pd.DataFrame:
    """
    Load prices and ESG data aligned by date and ticker
    
    Returns merged DataFrame with price and ESG columns
    """
    # Load prices
    prices = load_pit_universe_prices(universe, start, end)
    
    # Load ESG data for each ticker
    start_year = pd.to_datetime(start).year
    end_year = pd.to_datetime(end).year
    years = range(start_year, end_year + 1)
    
    all_esg = []
    for ticker in prices['ticker'].unique():
        for year in years:
            esg_file = (
                f'data/curated/tickers/exchange=us/ticker={ticker}/'
                f'esg/year={year}/part-000.parquet'
            )
            if Path(esg_file).exists():
                df = pd.read_parquet(esg_file)
                all_esg.append(df)
    
    if not all_esg:
        print("⚠️ No ESG data found")
        return prices
    
    esg = pd.concat(all_esg, ignore_index=True)
    
    # Merge prices with ESG (forward-fill ESG for daily prices)
    # ESG is monthly, prices are daily
    merged = pd.merge_asof(
        prices.sort_values('date'),
        esg[['date', 'ticker', 'environment_score', 'social_score', 
             'governance_score', 'esg_combined_score']].sort_values('date'),
        on='date',
        by='ticker',
        direction='backward'  # Use most recent ESG value
    )
    
    return merged

# Example: Research-ready dataset
research_data = load_universe_with_esg('sp500', '2014-01-01', '2024-12-01')
print(f"Research dataset: {len(research_data):,} records with ESG data")
```

---

## 8. Data Validation & Quality Control

### 8.1 Validation Checklist

**Membership Data:**
- ✅ No duplicate (date, ticker) pairs in daily mode
- ✅ No overlapping intervals for same ticker
- ✅ All dates are valid trading days
- ✅ Ticker symbols are uppercase and normalized
- ✅ Date ranges align with source CSV

**Price Data:**
- ✅ No negative prices or volumes
- ✅ High >= Low for all records
- ✅ Adjusted prices incorporate all corporate actions
- ✅ No missing trading days for active tickers
- ✅ Consistent currency and exchange codes

**ESG Data:**
- ✅ Scores within valid range (typically 0-100)
- ✅ Monthly cadence maintained (no gaps)
- ✅ Latest data within acceptable lag (< 2 months)
- ✅ At least 93% continuous coverage for research period

**GVKEY Mapping:**
- ✅ Unique (ticker, gvkey) pairs
- ✅ All membership tickers have GVKEY
- ✅ No orphaned GVKEYs (all have at least one ticker)

### 8.2 Automated Validation Scripts

**Location:** `src/programs/` and `tests/`

| Script | Purpose | Frequency |
|--------|---------|-----------|
| `check_esg_continuity.py` | ESG gap analysis | After ESG updates |
| `check_universe_overlap_aware.py` | Membership validation | After membership rebuild |
| `test_membership_aware_check.py` | Unit tests for membership logic | CI/CD |
| `rebuild_sp500_membership.py` | Rebuild with validation | After CSV updates |

### 8.3 Data Freshness Monitoring

**Expected Update Cadence:**

| Data Type | Update Frequency | Lag | Last Updated |
|-----------|-----------------|-----|--------------|
| S&P 500 Membership | Event-driven (additions/removals) | Real-time to 1 day | 2025-11-11 |
| Daily Prices | Daily (T+1) | 1 trading day | Ongoing |
| ESG Data | Monthly | 1-2 months | 2024-12-01 |
| Corporate Actions | Event-driven | 1-7 days | Ongoing |

**Freshness Checks:**
```python
def check_data_freshness():
    """Validate data is current"""
    from datetime import datetime, timedelta
    
    # Check membership
    membership = pd.read_parquet(
        'data/curated/membership/universe=sp500/mode=daily/'
        'sp500_membership_daily.parquet'
    )
    last_membership_date = membership['date'].max()
    print(f"Last membership date: {last_membership_date}")
    
    # Check ESG
    # (Scan all ESG files for latest date)
    
    # Check prices
    # (Scan recent ticker files for latest trading day)
    
    # Alert if data is stale (> 5 business days old)
    days_old = (datetime.now().date() - last_membership_date).days
    if days_old > 7:
        print(f"⚠️ Membership data is {days_old} days old")
```

---

## 9. Deployment & Operations

### 9.1 Development Environment

**Local Setup:**
```bash
# Clone repository
git clone https://github.com/ycti-labs/quantx-data-builder.git
cd quantx-data-builder

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure settings
cp config/settings.yaml.example config/settings.yaml
# Edit config/settings.yaml with your API keys and paths
```

**Directory Structure:**
```
quantx-data-builder/
├── config/                 # Configuration files
│   └── settings.yaml       # Main configuration
├── src/                    # Source code
│   ├── core/               # Core utilities
│   ├── fetcher/            # Data fetchers
│   ├── universe/           # Universe builders
│   ├── storage/            # Storage abstractions
│   └── programs/           # Executable scripts
├── data/                   # Data lake (gitignored)
│   ├── curated/            # Clean, validated data
│   └── raw/                # Raw input files
├── docs/                   # Documentation
├── tests/                  # Test scripts and examples
└── requirements.txt        # Python dependencies
```

### 9.2 Configuration Management

**File:** `config/settings.yaml`

**Key Configuration Sections:**
```yaml
storage:
  local:
    root_path: "data"
    
universe:
  sp500:
    membership_file: "S&P 500 Historical Components & Changes(11-16-2025).csv"
    data_tolerance: 0.1

data_providers:
  tiingo:
    api_key: "${TIINGO_API_KEY}"  # From environment variable
    rate_limit: 500  # requests per hour

logging:
  level: INFO
  format: json
  correlation_id: true
```

### 9.3 Operational Workflows

#### Daily Update Workflow

**Objective:** Fetch T-1 prices and update membership

**Schedule:** 6:00 AM ET on trading days

**Steps:**
1. Check for new S&P 500 additions/removals
2. Update membership if changes detected
3. Fetch previous day's prices for all active tickers
4. Validate data completeness
5. Log metrics and alert on failures

**Script:** `src/programs/daily_update.py` (future)

#### Weekly ESG Update

**Objective:** Fetch latest ESG data for all covered tickers

**Schedule:** Saturday 2:00 AM ET (weekly)

**Steps:**
1. Identify tickers with ESG coverage
2. Fetch latest monthly ESG scores
3. Check for continuity issues
4. Generate quality report
5. Alert if coverage drops below threshold (< 90%)

**Script:** `examples/regenerate_esg_data.py` (to be enhanced)

#### Quarterly Membership Rebuild

**Objective:** Full rebuild to incorporate CSV updates and corrections

**Schedule:** First Saturday of each quarter

**Steps:**
1. Download latest S&P 500 historical CSV
2. Backup existing membership data
3. Run full rebuild: `rebuild_sp500_membership.py --rebuild --min-date 2014-01-01`
4. Validate against previous version
5. Deploy if validation passes

### 9.4 Monitoring & Alerting

**Key Metrics:**
- Data freshness (days since last update)
- ESG continuity percentage (target: > 93%)
- Membership completeness (all tickers have prices)
- API rate limit utilization (< 80%)
- Failed fetch attempts (< 1%)

**Alert Conditions:**
- ESG coverage drops below 90%
- No price updates for > 2 trading days
- API rate limit exceeded
- Membership-price mismatch detected
- Parquet file corruption

---

## 10. Future Enhancements

### 10.1 Azure Deployment

**Planned Architecture:**

**Azure Functions** (Scheduled Updates):
- Daily price fetches (6 AM trigger)
- Weekly ESG updates (Saturday trigger)
- Event-driven membership updates

**Azure Container Apps Jobs** (Heavy Processing):
- Historical backfills (on-demand)
- Full membership rebuilds (quarterly)
- Bulk data migrations

**Azure Blob Storage:**
- Parquet files stored in blob containers
- Hierarchical namespace for Hive-style partitioning
- Lifecycle management for archival

### 10.2 Multi-Market Expansion

**Target Markets:**
- Hong Kong (Hang Seng Index)
- Japan (Nikkei 225)
- Europe (STOXX 600)
- Emerging Markets (MSCI EM)

**Implementation Strategy:**
- Market-specific builder classes (e.g., `HSIUniverse`, `NikkeiUniverse`)
- Shared core logic for price fetching and storage
- Exchange-specific trading calendars
- Currency conversion and FX data integration

### 10.3 Enhanced Data Types

**Fundamental Data:**
- Income statements (quarterly/annual)
- Balance sheets
- Cash flow statements
- Financial ratios

**Alternative Data:**
- News sentiment
- Social media metrics
- Supply chain data
- Satellite imagery (retail foot traffic)

**Higher Frequency:**
- Intraday (minute/tick) data
- Options pricing
- Order book depth

### 10.4 Advanced Analytics

**Feature Engineering Pipeline:**
- Automated factor calculation (momentum, value, quality)
- Rolling window statistics
- Cross-sectional rankings
- Industry adjustments

**Backtesting Framework:**
- Vectorized backtest engine
- Transaction cost modeling
- Slippage simulation
- Performance attribution

---

## 11. References & Resources

### 11.1 Data Standards

- **Hive Partitioning:** Apache Hive partition naming conventions
- **Parquet Format:** Apache Parquet columnar storage specification
- **GVKEY Standard:** S&P Capital IQ Global Company Key documentation

### 11.2 Best Practices

- **Survivorship Bias:** Importance of point-in-time membership filtering
- **Adjusted Prices:** Corporate action adjustment methodologies
- **ESG Scoring:** Understanding ESG rating agency methodologies

### 11.3 Academic Literature

- Fama, Eugene F., and Kenneth R. French. "Common risk factors in the returns on stocks and bonds." *Journal of Financial Economics* 33.1 (1993): 3-56.
- Pastor, Lubos, Robert F. Stambaugh, and Lucian A. Taylor. "Sustainable investing in equilibrium." *Journal of Financial Economics* 142.2 (2021): 550-571.

### 11.4 Industry Standards

- CFA Institute: ESG Investing and Analysis
- SASB (Sustainability Accounting Standards Board)
- TCFD (Task Force on Climate-related Financial Disclosures)

---

## Appendix A: Quick Start Commands

**Setup:**
```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
export TIINGO_API_KEY="your_api_key_here"
```

**Rebuild Membership:**
```bash
# Full rebuild from 2014
python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01

# Incremental update
python src/programs/rebuild_sp500_membership.py
```

**Check ESG Quality:**
```bash
# Analyze ESG data continuity
python src/programs/check_esg_continuity.py
```

**Validate Data:**
```bash
# Check membership-price alignment
python tests/check_universe_overlap_aware.py
```

---

## Appendix B: File Format Specifications

### Membership Daily Parquet

```
Schema:
  date: date (required, non-null)
  ticker: string (required, non-null, uppercase)
  universe: string (required, 'sp500')

Partitioning: None (single file)
Compression: Snappy
Sort Order: [date ASC, ticker ASC]
```

### Membership Intervals Parquet

```
Schema:
  ticker: string (required, non-null, uppercase)
  universe: string (required, 'sp500')
  start_date: date (required, non-null)
  end_date: date (required, non-null)

Partitioning: None (single file)
Compression: Snappy
Sort Order: [ticker ASC, start_date ASC]
Constraints: start_date <= end_date, no overlaps per ticker
```

### Price Data Parquet

```
Schema:
  date: date (required, non-null)
  gvkey: int (required, non-null)
  open: double (required, > 0)
  high: double (required, > 0)
  low: double (required, > 0)
  close: double (required, > 0)
  volume: int64 (required, >= 0)
  adj_open: double (required, > 0)
  adj_high: double (required, > 0)
  adj_low: double (required, > 0)
  adj_close: double (required, > 0)
  adj_volume: int64 (required, >= 0)
  div_cash: double (nullable, >= 0)
  split_factor: double (nullable, > 0)
  exchange: string (required, 'us')
  currency: string (required, 'USD')
  freq: string (required, 'daily')
  year: int (materialized, required)

Partitioning: exchange={exchange}/ticker={ticker}/freq={freq}/year={year}/
Compression: Snappy
Sort Order: [date ASC]
Constraints: high >= low, high >= open, high >= close, low <= open, low <= close
```

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-11-24 | QuantX Team | Initial methodology documentation |

---

**End of Document**
