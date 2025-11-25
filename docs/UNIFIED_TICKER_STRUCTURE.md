# Unified Ticker Data Structure

**Implementation Date:** November 23, 2025  
**Status:** ✅ Complete

## Overview

All ticker data (prices, ESG, fundamentals) now organized in a unified hierarchical structure with exchange and ticker as primary partition keys. This creates a **single source of truth** per ticker with consistent organization across all data types.

## Architecture

### Unified Structure

```
data/curated/tickers/
└── exchange={EXCHANGE}/          # Exchange partition (us, hk, jp, eu)
    └── ticker={SYMBOL}/          # Ticker partition (AAPL, 0700.HK, etc.)
        ├── prices/               # Price data (OHLCV)
        │   ├── freq=daily/
        │   │   └── year={YEAR}/
        │   │       └── part-000.parquet
        │   └── freq=weekly/
        │       └── year={YEAR}/
        │           └── part-000.parquet
        │
        ├── esg/                  # ESG scores (monthly)
        │   └── year={YEAR}/
        │       └── part-000.parquet
        │
        └── fundamentals/         # Financial statements
            ├── statement=income/
            │   └── year={YEAR}/
            │       └── part-000.parquet
            ├── statement=balance/
            │   └── year={YEAR}/
            │       └── part-000.parquet
            └── statement=cashflow/
                └── year={YEAR}/
                    └── part-000.parquet
```

### Example: Apple (AAPL)

```
data/curated/tickers/exchange=us/ticker=AAPL/
├── prices/
│   ├── freq=daily/
│   │   ├── year=2023/part-000.parquet     (Daily OHLCV)
│   │   └── year=2024/part-000.parquet
│   └── freq=weekly/
│       ├── year=2023/part-000.parquet     (Weekly OHLCV)
│       └── year=2024/part-000.parquet
│
├── esg/
│   ├── year=2023/part-000.parquet         (12 monthly records)
│   └── year=2024/part-000.parquet         (12 monthly records)
│
└── fundamentals/
    ├── statement=income/
    │   ├── year=2023/part-000.parquet     (Quarterly reports)
    │   └── year=2024/part-000.parquet
    ├── statement=balance/
    │   ├── year=2023/part-000.parquet
    │   └── year=2024/part-000.parquet
    └── statement=cashflow/
        ├── year=2023/part-000.parquet
        └── year=2024/part-000.parquet
```

## Benefits

### 1. Single Source of Truth
All data for a ticker is co-located in one directory tree. No need to search multiple locations.

**Example:**
```bash
# All AAPL data in one place
ls data/curated/tickers/exchange=us/ticker=AAPL/
# Output: prices/  esg/  fundamentals/
```

### 2. Logical Organization
Data naturally grouped by ticker, then by type. Mirrors how analysts think about data.

### 3. Consistent Pattern
Same organizational structure across all data types:
- Prices: `tickers/exchange=us/ticker=AAPL/prices/freq=daily/year=2024/`
- ESG: `tickers/exchange=us/ticker=AAPL/esg/year=2024/`
- Fundamentals: `tickers/exchange=us/ticker=AAPL/fundamentals/statement=income/year=2024/`

### 4. Exchange Support
First-class support for multiple exchanges (US, HK, JP, EU):
```
tickers/exchange=us/ticker=AAPL/       # Apple (US)
tickers/exchange=hk/ticker=0700.HK/    # Tencent (Hong Kong)
tickers/exchange=jp/ticker=7203.T/     # Toyota (Tokyo)
```

### 5. Easier Navigation
Browse all data for a ticker in one location:
```bash
find data/curated/tickers/exchange=us/ticker=AAPL -name "*.parquet"
```

### 6. Better Analytics
Co-located data enables efficient joins and multi-dataset analysis without cross-directory searches.

### 7. Simpler Operations
- **Backup:** Copy entire ticker directory
- **Delete:** Remove entire ticker directory
- **Migration:** Move ticker directory between environments
- **Permissions:** Set access controls per ticker

## Implementation

### ESGManager

**Updated Methods:**
```python
# Save with exchange parameter
esg_manager.save_esg_data(df, ticker="AAPL", exchange="us")
# → Saves to: tickers/exchange=us/ticker=AAPL/esg/year=2024/

# Load with exchange parameter
df = esg_manager.load_esg_data(ticker="AAPL", exchange="us", start_year=2023)
# → Reads from: tickers/exchange=us/ticker=AAPL/esg/year=*/
```

**Path Template:**
```
data/curated/tickers/exchange={exchange}/ticker={ticker}/esg/year={year}/part-000.parquet
```

### FundamentalManager

**Updated Methods:**
```python
# Save (exchange from universe)
fund_manager.save_fundamental_data(df, symbol="AAPL", statement_type="income")
# → Saves to: tickers/exchange=us/ticker=AAPL/fundamentals/statement=income/year=2024/

# Load (exchange from universe)
df = fund_manager.read_fundamental_data(symbol="AAPL", statement_type="income")
# → Reads from: tickers/exchange=us/ticker=AAPL/fundamentals/statement=income/year=*/
```

**Path Template:**
```
data/curated/tickers/exchange={exchange}/ticker={symbol}/fundamentals/statement={type}/year={year}/part-000.parquet
```

### PriceManager (Already Unified)

**Existing Methods:**
```python
# Already using unified structure
price_manager.save_prices(df, ticker="AAPL", freq="daily")
# → Saves to: tickers/exchange=us/ticker=AAPL/prices/freq=daily/year=2024/

# Load prices
df = price_manager.load_prices(ticker="AAPL", freq="daily", start_year=2023)
# → Reads from: tickers/exchange=us/ticker=AAPL/prices/freq=daily/year=*/
```

**Path Template:**
```
data/curated/tickers/exchange={exchange}/ticker={ticker}/prices/freq={freq}/year={year}/part-000.parquet
```

## Migration

### Before (Inconsistent)
```
data/
├── curated/
│   ├── tickers/exchange=us/ticker=AAPL/prices/     ✓ Unified
│   └── esg/ticker=AAPL/                            ✗ Separate location
└── fundamentals/exchange=us/ticker=AAPL/           ✗ Separate location
```

### After (Unified)
```
data/curated/tickers/
└── exchange=us/
    └── ticker=AAPL/
        ├── prices/       ✓ OHLCV data
        ├── esg/          ✓ ESG scores
        └── fundamentals/ ✓ Financial statements
```

### Migration Steps

**1. ESG Data Migration:**
```python
# Migrated from: data/curated/esg/ticker=AAPL/
# To: data/curated/tickers/exchange=us/ticker=AAPL/esg/
# Status: ✅ Complete (8 tickers, 85 year files)
```

**2. Fundamental Data Migration:**
```python
# Migrated from: data/fundamentals/exchange=us/ticker=AAPL/
# To: data/curated/tickers/exchange=us/ticker=AAPL/fundamentals/
# Status: ✅ Complete (no existing data to migrate)
```

**3. Old Directory Cleanup:**
```bash
# Removed after migration:
rm -rf data/curated/esg/
rm -rf data/fundamentals/
```

## Query Patterns

### Load All Data Types for a Ticker

```python
from pathlib import Path
import pandas as pd

ticker = "AAPL"
exchange = "us"
base_path = Path(f"data/curated/tickers/exchange={exchange}/ticker={ticker}")

# Load prices
prices = pd.read_parquet(base_path / "prices/freq=daily/year=2024/part-000.parquet")

# Load ESG
esg = pd.read_parquet(base_path / "esg/year=2024/part-000.parquet")

# Load fundamentals
income = pd.read_parquet(base_path / "fundamentals/statement=income/year=2024/part-000.parquet")
```

### Join Prices with ESG

```python
# Co-located data enables efficient joins
prices = price_manager.load_prices("AAPL", freq="daily", start_year=2024)
esg = esg_manager.load_esg_data("AAPL", exchange="us", start_year=2024)

# Join on date
combined = prices.merge(esg, on=['ticker', 'date'], how='left')
```

### Backup a Single Ticker

```bash
# Backup all AAPL data
tar -czf aapl_backup.tar.gz data/curated/tickers/exchange=us/ticker=AAPL/

# Restore
tar -xzf aapl_backup.tar.gz
```

### List All Data Types for a Ticker

```python
import os
from pathlib import Path

ticker_path = Path("data/curated/tickers/exchange=us/ticker=AAPL")
data_types = [d.name for d in ticker_path.iterdir() if d.is_dir()]
print(f"AAPL data types: {data_types}")
# Output: ['prices', 'esg', 'fundamentals']
```

## Statistics

### Current State (Post-Migration)

```
Tickers with prices:       725
Tickers with ESG:            8
Tickers with fundamentals:   0
Total unique tickers:      725

Total parquet files:
  - Prices:       14,699 files
  - ESG:              85 files
  - Fundamentals:      0 files
```

### Storage Distribution

```
data/curated/tickers/exchange=us/
├── ticker=AAPL/
│   ├── prices/         ~15 MB   (5 years daily + weekly)
│   ├── esg/            ~180 KB  (20 years monthly)
│   └── fundamentals/   ~0 KB    (not yet populated)
├── ticker=MSFT/
│   ├── prices/         ~15 MB
│   ├── esg/            ~90 KB
│   └── fundamentals/   ~0 KB
...
```

## Best Practices

### 1. Always Use Exchange Parameter
```python
# Good: Explicit exchange
esg_manager.save_esg_data(df, "AAPL", exchange="us")

# Also good: Uses default 'us'
esg_manager.save_esg_data(df, "AAPL")
```

### 2. Co-locate Related Data
```python
# Load all data for a ticker for analysis
prices = price_manager.load_prices("AAPL", freq="daily", start_year=2024)
esg = esg_manager.load_esg_data("AAPL", exchange="us", start_year=2024)
fundamentals = fund_manager.read_fundamental_data("AAPL", start_date="2024-01-01")
```

### 3. Consistent Naming
- Exchange codes: lowercase (`us`, `hk`, `jp`, `eu`)
- Ticker symbols: uppercase (`AAPL`, `MSFT`, `0700.HK`)
- Data types: lowercase (`prices`, `esg`, `fundamentals`)

### 4. Partition Pruning
Use year-based filtering to reduce data scans:
```python
# Good: Only reads 2024 files
df = esg_manager.load_esg_data("AAPL", start_year=2024, end_year=2024)

# Less efficient: Reads all files then filters
df = esg_manager.load_esg_data("AAPL")
df = df[df['year'] == 2024]
```

## Future Extensions

### Multi-Exchange Support
```python
# Hong Kong stock
esg_manager.load_esg_data("0700.HK", exchange="hk")

# Japanese stock
esg_manager.load_esg_data("7203.T", exchange="jp")

# Cross-listings (same company, different exchanges)
aapl_us = price_manager.load_prices("AAPL", exchange="us")
aapl_eu = price_manager.load_prices("AAPL", exchange="eu")
```

### Additional Data Types
Future additions can follow the same pattern:
```
tickers/exchange=us/ticker=AAPL/
├── prices/
├── esg/
├── fundamentals/
├── news/             # News sentiment (planned)
├── options/          # Options chains (planned)
└── ownership/        # Institutional ownership (planned)
```

## Documentation

**Updated Files:**
- `src/market/esg_manager.py` - Unified path structure
- `src/market/fundamental_manager.py` - Unified path structure
- `docs/ESG_STORAGE_OPTIMIZATION.md` - Storage optimization details
- `docs/UNIFIED_TICKER_STRUCTURE.md` - This document

**Related Documents:**
- [ARCHITECTURE_REFACTORING.md](./ARCHITECTURE_REFACTORING.md) - Overall architecture
- [PARQUET_SAVE_IMPLEMENTATION.md](./PARQUET_SAVE_IMPLEMENTATION.md) - Parquet patterns
- [ESG_PANEL_METHODOLOGY.md](./ESG_PANEL_METHODOLOGY.md) - ESG data methodology

## Conclusion

The unified ticker structure provides:

✅ **Single source of truth** - All ticker data co-located  
✅ **Consistent organization** - Same pattern across all data types  
✅ **Exchange support** - Ready for multi-market expansion  
✅ **Efficient analytics** - Co-located data for fast joins  
✅ **Simpler operations** - Backup, delete, migrate by ticker  
✅ **Backward compatible** - All existing code works  

This architectural improvement aligns with the principle of "storing price data once per ticker and managing universe membership separately" and extends it to all data types, creating a more maintainable and scalable data pipeline.

---

**Implementation Status:** ✅ Complete  
**Migration Status:** ✅ Complete  
**Testing Status:** ✅ All tests passing  
**Documentation Status:** ✅ Complete
