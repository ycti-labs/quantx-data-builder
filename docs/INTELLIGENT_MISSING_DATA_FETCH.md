# Intelligent Missing Data Fetching

## Overview

The `PriceDataManager` now includes intelligent missing data detection and fetching. Instead of re-fetching entire datasets, it:

1. **Checks existing data** - Determines what data already exists
2. **Identifies gaps** - Finds missing portions at start/end of required period
3. **Fetches only what's needed** - Downloads only missing data to save time and API calls

## New Methods

### 1. `get_existing_date_range()`

Get the date range of existing data for a symbol.

```python
builder = PriceDataManager(api_key=api_key)

# Check what data exists for AAPL
date_range = builder.get_existing_date_range("AAPL", exchange="us")

if date_range:
    min_date, max_date = date_range
    print(f"AAPL data: {min_date} to {max_date}")
else:
    print("No data exists for AAPL")
```

**Returns:** `Tuple[date, date]` or `None` if no data exists

### 2. `check_missing_data()`

Check what data is missing for a symbol during a required period.

```python
builder = PriceDataManager(api_key=api_key)

# Check if AAPL has complete data for 2020-2024
check = builder.check_missing_data(
    symbol="AAPL",
    required_start="2020-01-01",
    required_end="2024-12-31",
    tolerance_days=2  # Ignore gaps ≤ 2 days
)

print(f"Status: {check['status']}")  # 'complete', 'partial', or 'missing'
print(f"Actual range: {check['actual_start']} to {check['actual_end']}")
print(f"Missing at start: {check['missing_start_days']} days")
print(f"Missing at end: {check['missing_end_days']} days")

if check['fetch_start']:
    print(f"Need to fetch from: {check['fetch_start']}")
if check['fetch_end']:
    print(f"Need to fetch until: {check['fetch_end']}")
```

**Returns:** Dictionary with:
- `status`: 'complete', 'partial', or 'missing'
- `actual_start`: Existing data start date (or None)
- `actual_end`: Existing data end date (or None)
- `missing_start_days`: Days missing at start
- `missing_end_days`: Days missing at end
- `fetch_start`: Recommended fetch start date (or None)
- `fetch_end`: Recommended fetch end date (or None)

### 3. `fetch_missing_data()`

Intelligently fetch only missing data for a symbol.

```python
builder = PriceDataManager(api_key=api_key)

# Fetch only what's missing for TSLA
df, paths = builder.fetch_missing_data(
    symbol="TSLA",
    required_start="2020-01-01",
    required_end="2024-12-31",
    tolerance_days=2
)

if df.empty:
    print("✅ Already have complete data")
else:
    print(f"📥 Fetched {len(df)} new rows")
    print(f"   Saved to {len(paths)} files")
```

**Logic:**
- **Complete data** → Skip fetching, return empty DataFrame
- **No data** → Fetch entire period
- **Partial data** → Fetch only missing start/end portions

**Parameters:**
- `force=True` → Force fetch entire period regardless of existing data

### 4. `fetch_universe_missing_data()`

Fetch missing data for an entire universe (e.g., S&P 500).

```python
builder = PriceDataManager(api_key=api_key)

# Fetch missing data for all S&P 500 historical members
results = builder.fetch_universe_missing_data(
    universe="sp500",
    start_date="2014-01-01",
    end_date="2024-12-31",
    tolerance_days=2
)

# Print summary
complete = sum(1 for r in results.values() if r['status'] == 'complete')
fetched = sum(1 for r in results.values() if r['status'] == 'fetched')
errors = sum(1 for r in results.values() if r['status'] == 'error')

print(f"Complete: {complete}")
print(f"Fetched: {fetched}")
print(f"Errors: {errors}")
```

**Returns:** Dictionary mapping `symbol → result` with:
- `status`: 'complete', 'fetched', or 'error'
- `message`: Status message
- `fetched_rows`: Number of rows fetched
- `saved_paths`: List of saved file paths

## Usage Examples

### Example 1: Single Ticker

```python
from src.fetcher.price_data_builder import PriceDataManager
from src.fetcher.config_loader import FetcherConfig

# Load config
config = FetcherConfig("config/settings.yaml")
builder = PriceDataManager(api_key=config.fetcher.tiingo.api_key)

# Check and fetch missing data for AAPL
print("Checking AAPL...")
check = builder.check_missing_data("AAPL", "2020-01-01", "2024-12-31")

if check['status'] == 'complete':
    print("✅ Already have complete data")
elif check['status'] == 'partial':
    print(f"⚠️  Missing {check['missing_start_days']}d at start, {check['missing_end_days']}d at end")
    df, paths = builder.fetch_missing_data("AAPL", "2020-01-01", "2024-12-31")
    print(f"📥 Fetched {len(df)} rows")
else:
    print("❌ No data exists - fetching entire period")
    df, paths = builder.fetch_missing_data("AAPL", "2020-01-01", "2024-12-31")
    print(f"📥 Fetched {len(df)} rows")
```

### Example 2: Universe Data Refresh

```python
# Refresh entire S&P 500 database
results = builder.fetch_universe_missing_data(
    universe="sp500",
    start_date="2014-01-01",
    end_date="2024-12-31",
    tolerance_days=2
)

# Show what was fetched
fetched_tickers = [s for s, r in results.items() if r['status'] == 'fetched']
print(f"Fetched new data for {len(fetched_tickers)} tickers:")
for ticker in fetched_tickers[:10]:
    rows = results[ticker]['fetched_rows']
    print(f"  • {ticker}: {rows} rows")
```

### Example 3: Daily Update Script

```python
from datetime import datetime, timedelta

# Get today's date
today = datetime.now().strftime('%Y-%m-%d')

# Fetch last 7 days for all current S&P 500 members
current_members = builder.get_current_members("sp500")

for symbol in current_members:
    try:
        df, paths = builder.fetch_missing_data(
            symbol=symbol,
            required_start=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            required_end=today,
            tolerance_days=1  # Strict for daily updates
        )
        if not df.empty:
            print(f"✅ {symbol}: Updated with {len(df)} rows")
    except Exception as e:
        print(f"❌ {symbol}: {e}")
```

## Benefits

### 1. Save API Calls

**Before:**
```python
# Always fetches entire period (2000 days)
df = builder.fetch_eod("AAPL", "2019-01-01", "2024-12-31")
```

**After:**
```python
# Only fetches what's missing (maybe 0-10 days)
df, paths = builder.fetch_missing_data("AAPL", "2019-01-01", "2024-12-31")
```

### 2. Faster Updates

For a universe of 600 tickers:
- **Without check**: Re-fetch all 600 tickers (even if complete) → ~30-60 minutes
- **With check**: Only fetch 10-20 incomplete tickers → ~1-2 minutes

### 3. Tolerance for Alignment Issues

Weekend/holiday alignment issues are ignored:
```python
# TSLA joined S&P 500 on Monday 2020-12-21
# Data starts 2020-12-22 (1 day gap)
# With tolerance_days=2, this is treated as COMPLETE ✅
```

### 4. Point-in-Time Membership

Uses membership intervals to fetch only data during S&P 500 membership:
```python
# TSLA joined S&P 500 on 2020-12-21
# Only fetches from 2020-12-21 onwards, not from 2014 ✅
```

## Configuration

### Tolerance Days

Control how strict the completeness check is:

```python
# Strict (exact day alignment required)
check = builder.check_missing_data(..., tolerance_days=0)

# Standard (ignore 1-2 day gaps from weekends/holidays)
check = builder.check_missing_data(..., tolerance_days=2)  # DEFAULT

# Lenient (useful for international markets)
check = builder.check_missing_data(..., tolerance_days=5)
```

### Force Refetch

Override the check and fetch entire period:

```python
# Force complete refetch regardless of existing data
df, paths = builder.fetch_missing_data(
    symbol="AAPL",
    required_start="2020-01-01",
    required_end="2024-12-31",
    force=True  # Ignores existing data
)
```

## Integration with Existing Code

The new methods integrate seamlessly with existing `PriceDataManager` functionality:

```python
# Old way (still works)
df = builder.fetch_eod("AAPL", "2020-01-01", "2024-12-31")
paths = builder.save_price_data(df, "AAPL")

# New way (smarter)
df, paths = builder.fetch_missing_data("AAPL", "2020-01-01", "2024-12-31")
# Automatically checks, fetches only what's missing, and saves
```

## Command Line Usage

Use the example script to fetch missing data:

```bash
# Interactive mode - check single ticker and universe
python examples/fetch_universe_missing_data.py

# Or import in your own scripts
from src.fetcher.price_data_builder import PriceDataManager
```

## Performance

### Benchmark: S&P 500 Historical Data (2014-2024)

**Initial build (no data exists):**
- All 748 tickers need fetching
- Time: ~30-60 minutes (depends on API rate limits)

**Daily update (most data complete):**
- ~10-20 tickers need updates (new additions, recent data)
- Time: ~1-2 minutes ⚡

**Weekly catch-up (some partial data):**
- ~50-100 tickers need gap filling
- Time: ~5-10 minutes

## Summary

The intelligent missing data fetching provides:

- ✅ **Automatic gap detection** - Knows what's missing
- ✅ **Selective fetching** - Only downloads what's needed
- ✅ **Tolerance handling** - Ignores minor alignment issues
- ✅ **Universe support** - Works for entire S&P 500, NASDAQ, etc.
- ✅ **Point-in-time aware** - Respects membership periods
- ✅ **Progress tracking** - Shows what's complete/fetched/error
- ✅ **Backward compatible** - Existing code still works

**Result:** Fast, efficient, and intelligent data updates that save API calls and time.
