# Data Fetcher System Documentation

## Overview

The QuantX Data Fetcher system provides a **configuration-driven, universe-specific** approach to downloading historical stock market data. Each market universe (SP500, NASDAQ100, etc.) has its own dedicated fetcher that reads configuration from `config/settings.yaml`.

## Architecture

### Core Components

```
src/fetcher/
├── __init__.py              # Module exports
├── config_loader.py         # YAML configuration loader
├── base_fetcher.py          # Abstract base class for all fetchers
├── sp500_fetcher.py         # S&P 500 fetcher implementation
├── nasdaq100_fetcher.py     # NASDAQ-100 fetcher implementation
├── russell2000_fetcher.py   # Russell 2000 fetcher implementation
└── fetcher_factory.py       # Factory to create fetchers from config
```

### Design Patterns

1. **Abstract Factory Pattern**: `FetcherFactory` creates appropriate fetchers based on configuration
2. **Template Method Pattern**: `BaseFetcher` defines common fetch logic, subclasses implement universe-specific details
3. **Configuration-Driven**: All settings externalized to `config/settings.yaml`

## Configuration

### settings.yaml Structure

```yaml
universes:
  SP500:
    - enable: true
    - start_date: "2000-01-01"
  NASDAQ100:
    - enable: false
    - start_date: "2000-01-01"
  RUSSELL2000:
    - enable: false
    - start_date: "2000-01-01"

storage:
  compression: "snappy"
  azure:
    account_name: "stfinsightdata"
    container_name: "finsight-data"
    use_managed_identity: true
  local:
    root_path: "./data"

fetcher:
  max_workers: 10
  lookback_days: 5
  chunk_size: 100
  fetch_actions: true
  retry:
    max_attempts: 3
    exponential_base: 2
    min_wait: 2
    max_wait: 10

logging:
  level: "INFO"
  format: "json"

schedules:
  daily_update:
    cron: "0 6 * * 1-5"
    description: "Daily incremental data update"
  weekly_universe_refresh:
    cron: "0 2 * * 6"
    description: "Weekly universe refresh"
```

### Configuration Sections

#### 1. Universes

Controls which market universes are active and their backfill start dates:

- **enable**: `true` to activate fetcher, `false` to disable
- **start_date**: Historical backfill start date (YYYY-MM-DD format)

#### 2. Storage

Configures where data is stored:

- **compression**: Parquet compression codec (`snappy`, `gzip`, `zstd`)
- **azure**: Azure Blob Storage settings (production)
- **local**: Local filesystem settings (development)

#### 3. Fetcher

Controls data fetching behavior:

- **max_workers**: Concurrent threads for parallel downloads (10-20 recommended)
- **lookback_days**: Days to look back for incremental updates (5 covers weekends)
- **chunk_size**: Symbols per batch for backfills (100 recommended)
- **fetch_actions**: Include corporate actions (dividends, splits)
- **retry**: Exponential backoff retry configuration

## Usage

### Basic Usage - Single Universe

```python
from src.market_data import FetcherConfig, FetcherFactory
from src.storage import LocalStorage

# Load configuration
config = FetcherConfig("config/settings.yaml")

# Create storage
storage = LocalStorage(root_path="./data")

# Create factory
factory = FetcherFactory(config, storage)

# Create SP500 fetcher
sp500_fetcher = factory.create_fetcher("SP500")

# Fetch incremental data (last 5 days)
results = sp500_fetcher.fetch_daily_incremental(lookback_days=5)

print(f"Success: {results['success']}, Failed: {results['failed']}")
```

### Multi-Universe Fetching

```python
# Create all enabled fetchers
fetchers = factory.create_all_enabled_fetchers()

# Fetch data for all universes
for name, fetcher in fetchers.items():
    print(f"Fetching {name}...")
    results = fetcher.fetch_daily_incremental()
    print(f"  ✅ {results['success']} symbols fetched")
```

### Historical Backfill

```python
# Create fetcher
sp500_fetcher = factory.create_fetcher("SP500")

# Run backfill from configured start_date to today
results = sp500_fetcher.fetch_backfill(
    end_date="2024-12-31",
    chunk_size=100
)

print(f"Backfill complete: {results['success']} symbols, {results['failed']} failures")
```

### Configuration Inspection

```python
config = FetcherConfig("config/settings.yaml")

# Check universe status
enabled = config.get_enabled_universes()
print(f"Enabled universes: {[u.name for u in enabled]}")

# Check specific universe
sp500_config = config.get_universe_config("SP500")
if sp500_config and sp500_config.enabled:
    print(f"SP500 start date: {sp500_config.start_date}")
```

## Data Schema

### Output Structure

Fetched data is saved in Hive-partitioned Parquet format:

```
data/curated/prices/
└── exchange=us/
    └── ticker=AAPL/
        └── freq=daily/
            └── adj=true/
                └── year=2024/
                    └── part-000.parquet
```

### Parquet Schema

Each Parquet file contains:

```python
{
    'date': date,              # Trading date
    'ticker_id': int,          # Stable internal ID (hash-based)
    'exchange': string,        # Exchange code (us, hk, jp, uk)
    'symbol': string,          # Ticker symbol (AAPL, MSFT)
    'open': double,            # Opening price
    'high': double,            # High price
    'low': double,             # Low price
    'close': double,           # Closing price
    'volume': int64,           # Trading volume
    'dividend': double,        # Dividend amount (if any)
    'split_ratio': double,     # Stock split ratio (if any)
    'currency': string,        # Currency code (USD, HKD)
    'freq': string,            # Frequency (daily)
    'adj': boolean,            # Adjusted prices flag
    'year': int                # Year (for partitioning)
}
```

### ticker_id Generation

Each symbol gets a **stable integer ID** based on exchange + symbol:

```python
def _generate_ticker_id(exchange: str, symbol: str) -> int:
    """Generate stable ticker_id from exchange + symbol"""
    composite_key = f"{exchange}:{symbol}"
    return hash(composite_key) & 0x7FFFFFFF  # 32-bit positive int
```

**Why ticker_id?**
- Handles ticker symbol changes (FB → META)
- Enables cross-listings (US:BABA, HK:9988.HK share same issuer)
- Faster integer joins vs. string joins in analytics
- Prevents ticker symbol reuse conflicts

## Fetcher Classes

### BaseFetcher

Abstract base class providing:
- ✅ Parallel fetching with ThreadPoolExecutor
- ✅ Exponential backoff retry logic with tenacity
- ✅ Automatic data deduplication on append
- ✅ Year-based partitioning
- ✅ ticker_id generation
- ✅ Comprehensive error handling and logging

**Methods:**
- `fetch_daily_incremental(lookback_days)`: Fetch last N days
- `fetch_backfill(end_date, chunk_size)`: Historical backfill
- `get_universe_symbols()`: **Abstract** - must implement
- `get_metadata_path()`: **Abstract** - must implement

### SP500Fetcher

Fetches S&P 500 constituents.

**Symbol Sources (priority order):**
1. Curated membership intervals: `data/curated/membership/universe=sp500/mode=intervals/`
2. Raw CSV: `data/raw/sp500.csv`
3. Hardcoded fallback: `["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]`

### Nasdaq100Fetcher

Fetches NASDAQ-100 constituents.

**Symbol Sources (priority order):**
1. Curated membership intervals: `data/curated/membership/universe=nasdaq100/mode=intervals/`
2. Raw CSV: `data/raw/nasdaq100.csv`
3. Hardcoded fallback: Top 15 tech stocks

### Russell2000Fetcher

Fetches Russell 2000 constituents.

**Note:** Russell 2000 membership requires paid data source (FTSE Russell). Returns empty list by default.

**Symbol Sources (priority order):**
1. Curated membership intervals: `data/curated/membership/universe=russell2000/mode=intervals/`
2. Raw CSV: `data/raw/russell2000.csv`
3. Empty list (no free data source)

## Adding New Universes

### Step 1: Create Fetcher Class

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
    
    def get_universe_symbols(self) -> List[str]:
        # Read from HKEX data source
        # Format: ["0005.HK", "0700.HK", "9988.HK"]
        pass
    
    def get_metadata_path(self) -> str:
        return "data/curated/membership/universe=hkex/"
```

### Step 2: Register in Factory

```python
# src/fetcher/fetcher_factory.py
from .hkex_fetcher import HKEXFetcher

class FetcherFactory:
    FETCHER_MAP = {
        "SP500": SP500Fetcher,
        "NASDAQ100": Nasdaq100Fetcher,
        "RUSSELL2000": Russell2000Fetcher,
        "HKEX": HKEXFetcher,  # Add new universe
    }
```

### Step 3: Add to Configuration

```yaml
# config/settings.yaml
universes:
  SP500:
    - enable: true
    - start_date: "2000-01-01"
  HKEX:
    - enable: true
    - start_date: "2010-01-01"  # Hong Kong data start
```

### Step 4: Export in __init__.py

```python
# src/fetcher/__init__.py
from .hkex_fetcher import HKEXFetcher

__all__ = [
    # ... existing exports
    "HKEXFetcher",
]
```

## Error Handling

### Retry Logic

Fetchers use **tenacity** for exponential backoff:

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def _fetch_symbol(self, symbol, start_date, end_date):
    # Fetch logic with automatic retry on failure
    pass
```

- **Max attempts**: 3
- **Wait times**: 2s → 4s → 8s (exponential)

### Error Reporting

Failed symbols are tracked in results:

```python
{
    "success": 495,
    "failed": 5,
    "skipped": 0,
    "errors": [
        {
            "symbol": "XYZ",
            "error": "No data returned from yfinance",
            "timestamp": "2024-11-12T10:30:00"
        }
    ]
}
```

### Logging

Structured logging at multiple levels:

```python
self.logger.info(f"✅ [{self.universe_name}] Fetched 1000 rows for AAPL")
self.logger.warning(f"⚠️  [{self.universe_name}] No data for XYZ")
self.logger.error(f"❌ [{self.universe_name}] Failed to fetch ABC: timeout")
```

## Performance Optimization

### Parallel Fetching

- **ThreadPoolExecutor**: Downloads multiple symbols concurrently
- **max_workers=10**: Good balance for yfinance rate limits
- **chunk_size=100**: Prevents memory issues on large universes

### Data Deduplication

Automatic deduplication on append:

```python
year_df = pd.concat([existing_df, new_df]).drop_duplicates(
    subset=['date', 'ticker_id'], 
    keep='last'
)
```

### Partitioning Strategy

Year-based partitioning balances:
- ✅ Query performance (partition pruning)
- ✅ File management (not too many small files)
- ✅ Incremental updates (append to current year)

## Integration Points

### Azure Functions

```python
# azure_functions/function_app.py
from src.market_data import FetcherConfig, FetcherFactory
from src.storage import AzureBlobStorage

@app.schedule(schedule="0 6 * * 1-5", ...)
def daily_update(timer):
    config = FetcherConfig()
    storage = AzureBlobStorage(...)
    factory = FetcherFactory(config, storage)
    
    fetchers = factory.create_all_enabled_fetchers()
    for name, fetcher in fetchers.items():
        results = fetcher.fetch_daily_incremental()
        # Log results
```

### Container Apps CLI

```python
# container/cli.py
import typer
from src.market_data import FetcherConfig, FetcherFactory

app = typer.Typer()

@app.command()
def backfill(universe: str):
    config = FetcherConfig()
    factory = FetcherFactory(config, storage)
    fetcher = factory.create_fetcher(universe)
    results = fetcher.fetch_backfill()
    typer.echo(f"Backfill complete: {results['success']} symbols")
```

## Testing

### Unit Tests

```python
def test_config_loader():
    config = FetcherConfig("config/settings.yaml")
    assert config.get_universe_config("SP500").enabled == True
    assert config.fetcher.max_workers == 10

def test_sp500_fetcher():
    fetcher = SP500Fetcher(storage, start_date="2000-01-01")
    symbols = fetcher.get_universe_symbols()
    assert len(symbols) > 0
```

### Integration Tests

```python
def test_fetch_incremental():
    config = FetcherConfig()
    factory = FetcherFactory(config, LocalStorage())
    fetcher = factory.create_fetcher("SP500")
    results = fetcher.fetch_daily_incremental(lookback_days=5)
    assert results['success'] + results['failed'] + results['skipped'] > 0
```

## Troubleshooting

### Issue: No symbols loaded

**Cause**: Missing membership data files

**Solution**:
```bash
# Run universe builder first
python -m src.universe.sp500
```

### Issue: yfinance rate limiting

**Cause**: Too many concurrent requests

**Solution**: Reduce `max_workers` in settings.yaml:
```yaml
fetcher:
  max_workers: 5  # Reduce from 10
```

### Issue: Import errors

**Cause**: Missing dependencies

**Solution**:
```bash
pip install -r requirements.txt
```

### Issue: ticker_id collisions

**Cause**: Hash collisions (extremely rare with 32-bit hash)

**Solution**: Switch to UUID-based IDs or auto-increment from database.

## Future Enhancements

### Planned Features

1. **Intraday Data**: Minute/tick-level fetching
2. **Alternative Data Sources**: CRSP, Quandl, AlphaVantage
3. **Real-time Streaming**: WebSocket support for live prices
4. **Smart Retry**: Adaptive retry based on error type
5. **Metadata Enrichment**: Sector, industry, market cap
6. **Corporate Actions Table**: Separate Parquet for dividends/splits
7. **Data Quality Checks**: Automated validation and alerts

### Extensibility

The fetcher system is designed for easy extension:
- ✅ Add new universes with minimal code
- ✅ Plug in alternative data sources
- ✅ Switch storage backends (Azure → AWS S3)
- ✅ Custom partitioning strategies
- ✅ Universe-specific transformations

## References

- **yfinance**: https://github.com/ranaroussi/yfinance
- **Tenacity**: https://tenacity.readthedocs.io/
- **PyArrow**: https://arrow.apache.org/docs/python/
- **Hive Partitioning**: https://arrow.apache.org/docs/python/parquet.html#partitioned-datasets

---

**Status**: ✅ Production-ready
**Last Updated**: November 12, 2025
**Maintained By**: QuantX Data Team
