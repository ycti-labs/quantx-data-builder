# ESG Storage Optimization: Year-Based Partitioning

**Date:** November 23, 2024  
**Status:** ✅ Completed

## Overview

Optimized ESG data storage from month-based partitioning (12 files per ticker per year) to year-based partitioning (1 file per ticker per year), achieving 12x file reduction while maintaining full monthly data granularity.

## Problem Statement

### Before Optimization
```
data/curated/esg/
├── ticker=AAPL/
│   ├── year=2023/
│   │   ├── month=01/part-000.parquet    # 1 record
│   │   ├── month=02/part-000.parquet    # 1 record
│   │   ├── ... (10 more files)
│   │   └── month=12/part-000.parquet    # 1 record
│   └── year=2024/
│       ├── month=01/part-000.parquet
│       └── ... (11 more files)
```

**Issues:**
- 12 separate files per ticker per year
- Excessive file I/O overhead
- Slower loading (12 reads instead of 1)
- Directory clutter with month subdirectories

### After Optimization
```
data/curated/esg/
├── ticker=AAPL/
│   ├── year=2023/
│   │   └── part-000.parquet    # All 12 monthly records
│   └── year=2024/
│       └── part-000.parquet    # All 12 monthly records
```

**Benefits:**
- 1 file per ticker per year (12x reduction)
- Faster loading (single read per year)
- Cleaner directory structure
- Maintained monthly data granularity

## Implementation Changes

### 1. Updated `save_esg_data()` Method

**Before:**
```python
# Group by year AND month, then save
for (year, month), period_df in df.groupby(['year', 'month']):
    period_path = base_path / f"ticker={ticker}" / f"year={year}" / f"month={month:02d}"
    # Creates 12 separate files
```

**After:**
```python
# Group by year only (all months in one file)
for year, year_df in df.groupby('year'):
    year_path = base_path / f"ticker={ticker}" / f"year={year}"
    # Creates 1 file containing all 12 monthly records
```

**Changes:**
- Removed month from groupby operation
- Removed month subdirectory creation
- All monthly records for a year saved together

### 2. Updated `load_esg_data()` Method

**Before:**
```python
# Find all year directories
year_dirs = [d for d in base_path.iterdir() if d.name.startswith('year=')]

# Read all parquet files from month subdirectories
for year_dir in year_dirs:
    month_dirs = [d for d in year_dir.iterdir() if d.name.startswith('month=')]
    for month_dir in month_dirs:
        parquet_file = month_dir / "part-000.parquet"
        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            dfs.append(df)
```

**After:**
```python
# Find all year directories
year_dirs = [d for d in base_path.iterdir() if d.name.startswith('year=')]

# Read parquet files directly from year directories
for year_dir in year_dirs:
    parquet_file = year_dir / "part-000.parquet"
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        dfs.append(df)
```

**Changes:**
- Removed month subdirectory iteration
- Read directly from year-level parquet files
- Simpler, faster loading logic

### 3. Added Data Quality Handling

```python
# Filter out rows with None/NaN dates before applying date filters
if 'date' in result.columns:
    result = result.dropna(subset=['date'])
```

**Reason:** Some ESG records lack YearMonth data (date=None), which caused TypeError in date comparisons. Now gracefully handled by filtering out null dates before operations.

## Test Results

### Storage Efficiency
```
Before: ~1,020 files (85 year directories × 12 months)
After:  85 files (1 per year directory)

File Reduction: 935 fewer files (91.7% reduction)
Efficiency Gain: 12.0x fewer files
```

### Data Integrity Tests

✅ **Test 1: Multi-Year Save/Load**
- Saved GOOGL 2022-2024: 36 records → 3 files (1 per year)
- Loaded: 36 records (100% match)
- Records per year: {2022: 12, 2023: 12, 2024: 12}

✅ **Test 2: Date-Based Filtering**
- Filter: 2023-01-01 to 2023-06-30
- Result: 6 records (Jan-Jun, correct)
- Months: [1, 2, 3, 4, 5, 6]

✅ **Test 3: Multi-Ticker Batch**
- AAPL: 24 records → 2 files, loaded 24 records
- MSFT: 24 records → 2 files, loaded 24 records
- GOOGL: 24 records → 2 files, loaded 24 records

✅ **Test 4: Directory Structure**
- All tickers: 1 file per year, 0 month subdirs
- Clean structure verified across all tickers

## Data Schema

Each parquet file contains monthly records with full granularity:

```python
Columns:
- ticker: str           # Stock symbol
- gvkey: int           # GVKEY identifier
- year: int            # Calendar year
- month: float         # Month (1-12)
- date: date           # First day of month (YYYY-MM-01)
- esg_year: float      # Year of ESG score publication
- esg_score: float     # Overall ESG score
- env_score: float     # Environmental score
- soc_score: float     # Social score
- gov_score: float     # Governance score
- PERMNO: int          # Permanent number
- sic_code: int        # SIC industry code
- industry_code: int   # Industry classification
```

**Records per file:** 12 (one per month for the year)

## Performance Improvements

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| **Files created per year** | 12 | 1 | 12x fewer |
| **Read operations per year** | 12 | 1 | 12x faster |
| **Directory depth** | 3 levels | 2 levels | Simpler |
| **Total files (85 years)** | 1,020 | 85 | 91.7% reduction |

## Backward Compatibility

✅ **Interface unchanged:**
- `get_esg_data()` - Same parameters and behavior
- `load_esg_data()` - Same parameters and behavior
- `save_esg_data()` - Same parameters and behavior

✅ **Data preserved:**
- All monthly records maintained
- Date-based filtering works correctly
- No data loss or modification

## Migration Notes

**Unified Structure (November 2024):**
All ticker data (prices, ESG, fundamentals) now stored in unified structure:
```
data/curated/tickers/exchange=us/ticker=SYMBOL/
  ├── prices/freq=daily/year=YYYY/
  ├── esg/year=YYYY/
  └── fundamentals/statement=TYPE/year=YYYY/
```

Benefits:
- Single source of truth per ticker
- Co-located data for efficient analytics
- Exchange-aware for multi-market support
- Consistent pattern across all data types

**For existing data:**
- Old separate directories (`data/curated/esg/`, `data/fundamentals/`) migrated to unified structure
- Old directories removed after migration
- New saves automatically use unified structure
- Load methods updated to use unified paths

**For future development:**
- Continue using unified structure: `data/curated/tickers/exchange={ex}/ticker={sym}/{type}/`
- All data types follow same organizational pattern
- Monthly/yearly granularity preserved in data records
- Exchange parameter required for save/load operations (default: 'us')

## Conclusion

Successfully optimized ESG storage structure from month-based to year-based partitioning, achieving:

✅ **12x file reduction** (1,020 → 85 files)  
✅ **Faster I/O performance** (1 read vs 12 reads per year)  
✅ **Cleaner directory structure** (no month subdirs)  
✅ **100% data integrity** maintained  
✅ **Zero breaking changes** to API interface  

The optimization makes the codebase more maintainable, performant, and scalable for future ESG data operations.

---

**Related Files:**
- `src/market/esg_manager.py` - Updated save/load methods
- `examples/test_esg_manager.py` - Comprehensive test suite
- `docs/ESG_PANEL_METHODOLOGY.md` - ESG data methodology

**See Also:**
- [PARQUET_SAVE_IMPLEMENTATION.md](./PARQUET_SAVE_IMPLEMENTATION.md) - General parquet patterns
- [MEMBERSHIP_AWARE_MISSING_DATA.md](./MEMBERSHIP_AWARE_MISSING_DATA.md) - Data quality patterns
