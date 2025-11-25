# Market Data Managers

**Comprehensive data managers for price, fundamental, and ESG data** with unified interfaces for fetching, storing, and querying financial market data.

## Overview

The Market module provides three specialized data managers:

| Manager | Data Type | Source | Storage |
|---------|-----------|--------|---------|
| **PriceManager** | OHLCV prices | Tiingo API | `data/curated/prices/` |
| **FundamentalManager** | Financial statements | Tiingo API | `data/curated/fundamentals/` |
| **ESGManager** | ESG scores | Local Excel/CSV + GVKEY mapping | `data/curated/esg/` |

## Quick Start

### PriceManager - Market Prices

```python
from market import PriceManager
from universe import SP500Universe
from tiingo import TiingoClient

# Initialize
universe = SP500Universe(data_root="./data")
tiingo = TiingoClient(config={'api_key': 'YOUR_API_KEY'})
price_mgr = PriceManager(tiingo, universe)

# Fetch daily prices
df = price_mgr.fetch_eod('AAPL', 'daily', '2020-01-01', '2024-12-31')

# Load saved data
df = price_mgr.load_price_data('AAPL', 'daily', '2023-01-01', '2023-12-31')
```

### ESGManager - ESG Scores

```python
from market import ESGManager
from universe import SP500Universe

# Initialize (no API key needed!)
universe = SP500Universe(data_root="./data")
esg_mgr = ESGManager(universe)

# Get ESG data for a company
df = esg_mgr.get_esg_data(symbol='AAPL', start_year=2020)
print(df[['ticker', 'year', 'esg_score', 'env_score', 'soc_score', 'gov_score']])

# Get data for multiple companies
tech_stocks = ['AAPL', 'MSFT', 'GOOGL', 'META']
data = esg_mgr.get_multiple_esg_data(tech_stocks, start_year=2023)

# Export to Parquet
results = esg_mgr.export_to_parquet(['AAPL', 'MSFT'], start_year=2020)
```

## ESGManager Features

✅ **No API required**: Loads data from local Excel/CSV files  
✅ **GVKEY mapping**: Automatic conversion from GVKEY to ticker symbols  
✅ **Comprehensive scores**: ESG composite + Environmental, Social, Governance pillars  
✅ **Year-based queries**: Filter by year range  
✅ **Parquet storage**: Hive-style partitioned files (ticker=SYMBOL/year=YYYY)  
✅ **Coverage reports**: Analyze data availability by year  
✅ **Batch operations**: Process multiple symbols efficiently  

## Data Sources

# Check specific universe
if config.is_universe_enabled("SP500"):
    sp500_config = config.get_universe_config("SP500")
    print(f"Start date: {sp500_config.start_date}")
```

## Output Format

Data is saved in **Hive-partitioned Parquet** format:

```plain
data/curated/prices/
└── exchange=us/
    └── ticker=AAPL/
        └── freq=daily/
            └── adj=true/
                └── year=2024/
                    └── part-000.parquet
```

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | Trading date |
| `ticker_id` | int | Stable internal ID (hash-based) |
| `exchange` | string | Exchange code (us, hk, jp) |
| `symbol` | string | Ticker symbol (AAPL, MSFT) |
| `open` | double | Opening price |
| `high` | double | High price |
| `low` | double | Low price |
| `close` | double | Closing price |
| `volume` | int64 | Trading volume |
| `dividend` | double | Dividend amount |
| `split_ratio` | double | Stock split ratio |
| `currency` | string | Currency (USD, HKD) |
| `freq` | string | Frequency (daily) |
| `adj` | boolean | Adjusted prices flag |
| `year` | int | Year (partition column) |

## Adding New Universes

### 1. Create Fetcher Class

```python
# src/fetcher/hkex_fetcher.py
from .base_fetcher import BaseFetcher

class HKEXFetcher(BaseFetcher):
    def __init__(self, storage, start_date, max_workers=10, fetch_actions=True):
        super().__init__(
            storage=storage,
            universe_name="HKEX",
            start_date=start_date,
            max_workers=max_workers,
            fetch_actions=fetch_actions
        )
    
    def get_universe_symbols(self):
        # Load symbols from data source
        return ["0005.HK", "0700.HK", "9988.HK"]
    
    def get_metadata_path(self):
        return "data/curated/membership/universe=hkex/"
```

### 2. Register in Factory

```python
# src/fetcher/fetcher_factory.py
FETCHER_MAP = {
    "SP500": SP500Fetcher,
    "NASDAQ100": Nasdaq100Fetcher,
    "HKEX": HKEXFetcher,  # ← Add here
}
```

### 3. Add to Configuration

```yaml
# config/settings.yaml
universes:
  HKEX:
    - enable: true
    - start_date: "2010-01-01"
```

## Architecture

```plain
FetcherConfig (settings.yaml)
    ↓
FetcherFactory
    ↓
BaseFetcher (abstract)
    ├── SP500Fetcher
    ├── Nasdaq100Fetcher
    └── Russell2000Fetcher
```

### Key Classes

- **`FetcherConfig`**: Loads and validates settings.yaml
- **`FetcherFactory`**: Creates appropriate fetchers based on config
- **`BaseFetcher`**: Abstract base with common fetch logic
- **`SP500Fetcher`** / `Nasdaq100Fetcher` / etc.: Universe-specific implementations

## Performance

- **Parallel downloads**: 10 concurrent workers (configurable)
- **Batch processing**: 100 symbols per chunk
- **Automatic retry**: 3 attempts with exponential backoff (2s → 4s → 8s)
- **Deduplication**: Automatic on append, keeps most recent data
- **Partitioning**: Year-based for efficient queries

## Error Handling

```python
{
    "success": 495,      # Successfully fetched
    "failed": 5,         # Failed after retries
    "skipped": 0,        # No data available
    "errors": [          # Detailed error list
        {
            "symbol": "XYZ",
            "error": "No data returned",
            "timestamp": "2024-11-12T10:30:00"
        }
    ]
}
```

## Logging

Structured logging with universe context:

```
INFO - ✅ [SP500] Fetched 1000 rows for AAPL
WARNING - ⚠️  [SP500] No data for XYZ
ERROR - ❌ [SP500] Failed to fetch ABC: timeout
```

## Dependencies

```txt
yfinance>=0.2.0       # Market data source
pandas>=2.0.0         # Data manipulation
pyarrow>=10.0.0       # Parquet I/O
tenacity>=8.0.0       # Retry logic
pyyaml>=6.0.0         # Configuration loader
```

Install:

```bash
pip install -r requirements.txt
```

## Testing

Run examples:

```bash
python examples/fetcher_usage.py
```

Run tests:

```bash
pytest tests/test_fetcher.py -v
```

## Documentation

📚 **Full documentation**: [docs/DATA_FETCHER.md](../docs/DATA_FETCHER.md)

## Integration

### Azure Functions (Daily Updates)

```python
@app.schedule(schedule="0 6 * * 1-5", ...)
def daily_update(timer):
    config = FetcherConfig()
    factory = FetcherFactory(config, AzureBlobStorage(...))
    fetchers = factory.create_all_enabled_fetchers()
    
    for name, fetcher in fetchers.items():
        fetcher.fetch_daily_incremental()
```

### Container Apps (Backfills)

```bash
python -m container.cli backfill --universe SP500
```

## License

MIT License - QuantX Data Team

---

**Status**: ✅ Production-ready  
**Last Updated**: November 12, 2025
