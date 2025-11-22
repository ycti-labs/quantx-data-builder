# Data Fetcher Module

**Configuration-driven, universe-specific data fetchers** for downloading historical stock market data.

## Quick Start

```python
from src.fetcher import FetcherConfig, FetcherFactory
from src.storage import LocalStorage

# 1. Load configuration
config = FetcherConfig("config/settings.yaml")

# 2. Create storage
storage = LocalStorage(root_path="./data")

# 3. Create factory
factory = FetcherFactory(config, storage)

# 4. Create fetcher for enabled universe
fetcher = factory.create_fetcher("SP500")

# 5. Fetch data
results = fetcher.fetch_daily_incremental(lookback_days=5)
print(f"✅ Fetched {results['success']} symbols")
```

## Features

- ✅ **Configuration-driven**: All settings in `config/settings.yaml`
- ✅ **Universe-specific**: Dedicated fetchers for each market (SP500, NASDAQ100, etc.)
- ✅ **Parallel fetching**: ThreadPoolExecutor for concurrent downloads
- ✅ **Automatic retry**: Exponential backoff with tenacity
- ✅ **Data deduplication**: Automatic on append operations
- ✅ **Partitioned storage**: Hive-style year-based partitioning
- ✅ **Stable IDs**: Hash-based ticker_id for robust joins
- ✅ **Extensible**: Easy to add new universes

## Configuration

Edit `config/settings.yaml` to enable/disable universes:

```yaml
universes:
  SP500:
    - enable: true          # ✅ Fetch S&P 500 data
    - start_date: "2000-01-01"
  
  NASDAQ100:
    - enable: false         # ❌ Skip NASDAQ-100
    - start_date: "2000-01-01"

fetcher:
  max_workers: 10           # Concurrent downloads
  lookback_days: 5          # For incremental updates
  chunk_size: 100           # Symbols per batch
  fetch_actions: true       # Include dividends/splits
```

## Available Fetchers

| Universe | Fetcher Class | Status | Symbol Count |
|----------|---------------|--------|--------------|
| S&P 500 | `SP500Fetcher` | ✅ Ready | ~500 |
| NASDAQ-100 | `Nasdaq100Fetcher` | ✅ Ready | ~100 |
| Russell 2000 | `Russell2000Fetcher` | ⚠️ Requires paid data | ~2000 |

## Usage Examples

### Daily Incremental Update

```python
# Fetch last 5 days for all enabled universes
fetchers = factory.create_all_enabled_fetchers()

for name, fetcher in fetchers.items():
    results = fetcher.fetch_daily_incremental(lookback_days=5)
    print(f"{name}: {results['success']} success, {results['failed']} failed")
```

### Historical Backfill

```python
# Backfill from configured start_date to today
sp500_fetcher = factory.create_fetcher("SP500")
results = sp500_fetcher.fetch_backfill(
    end_date="2024-12-31",
    chunk_size=100
)
```

### Check Configuration

```python
config = FetcherConfig("config/settings.yaml")

# Get enabled universes
enabled = config.get_enabled_universes()
print([u.name for u in enabled])  # ['SP500']

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
