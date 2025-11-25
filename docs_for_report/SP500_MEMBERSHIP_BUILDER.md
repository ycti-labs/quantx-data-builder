# S&P 500 Membership Builder Guide

**Document Version:** 1.0  
**Last Updated:** November 24, 2025  
**Quick Reference for:** Building and maintaining S&P 500 historical membership data

---

## Overview

The S&P 500 Membership Builder transforms historical constituent data from CSV files into a dual-format data structure optimized for point-in-time queries and survivorship-bias-free backtesting.

**Key Features:**
- ✅ Dual representation: Daily snapshots + Interval timelines
- ✅ Point-in-time membership tracking (2014-2025)
- ✅ Configuration-driven CSV source management
- ✅ Incremental updates or full rebuilds
- ✅ Automatic validation and statistics reporting

---

## Current Data Status

**As of November 24, 2025:**

| Metric | Value |
|--------|-------|
| Unique Tickers | 763 |
| Total Intervals | 772 |
| Daily Records | 345,870 |
| Date Range | 2014-01-07 to 2025-11-11 |
| Unique Trading Dates | 687 |
| Active in Research Period (2014-2024) | 745 tickers |

**Source Data:**
- **File:** "S&P 500 Historical Components & Changes(11-16-2025).csv"
- **Location:** `data/raw/`
- **Size:** 5.2 MB, 2,702 lines
- **Coverage:** 1996-01-02 to 2025-11-11

---

## Data Structure

### 1. Daily Snapshot Mode

**Purpose:** Exact point-in-time constituent lists for each trading day

**File:** `data/curated/membership/universe=sp500/mode=daily/sp500_membership_daily.parquet`

**Schema:**
```
date: date              # Trading date
ticker: string          # Ticker symbol (uppercase)
universe: string        # Always 'sp500'
```

**Use Case:**
```python
import pandas as pd

# Get S&P 500 constituents on a specific date
daily = pd.read_parquet('data/curated/membership/universe=sp500/mode=daily/sp500_membership_daily.parquet')
members = daily[daily['date'] == '2020-01-15']['ticker'].tolist()
print(f"S&P 500 had {len(members)} constituents on 2020-01-15")
```

**Benefits:**
- Perfect for iterating over many dates
- Exact historical reconstruction
- Simple date-based filtering
- Ideal for daily rebalancing strategies

### 2. Interval/Timeline Mode

**Purpose:** Efficient range queries and membership period analysis

**File:** `data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet`

**Schema:**
```
ticker: string          # Ticker symbol (uppercase)
universe: string        # Always 'sp500'
start_date: date        # First day as constituent
end_date: date          # Last day as constituent (inclusive)
```

**Use Case:**
```python
import pandas as pd
from datetime import date

# Check which tickers were members during a period
intervals = pd.read_parquet('data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet')

# Get all tickers active during 2020
start = date(2020, 1, 1)
end = date(2020, 12, 31)
mask = (intervals['start_date'] <= end) & (intervals['end_date'] >= start)
members_2020 = intervals[mask]['ticker'].unique().tolist()
print(f"{len(members_2020)} tickers were in S&P 500 at some point during 2020")
```

**Benefits:**
- Faster for single-date queries
- Efficient range checks
- Shows membership tenure
- Compact storage (772 rows vs 345K)

---

## Configuration

### Settings File

**Location:** `config/settings.yaml`

**Relevant Section:**
```yaml
storage:
  local:
    root_path: "data"

universe:
  sp500:
    membership_file: "S&P 500 Historical Components & Changes(11-16-2025).csv"
    data_tolerance: 0.1
```

**Key Parameters:**
- `membership_file`: CSV filename in `data/raw/` directory
- `data_tolerance`: Acceptable data gap tolerance (10% = 0.1)
- `root_path`: Base directory for all data storage

### CSV Format Requirements

**Expected Columns:**
- `date`: Addition/removal date (ISO format: YYYY-MM-DD)
- `tickers`: Comma-separated list of ticker symbols
- `action`: Either "add" or "remove"

**Example CSV Rows:**
```csv
date,tickers,action
2014-01-02,AAPL,add
2014-01-02,GOOGL,add
2014-03-15,XYZ,remove
```

---

## Usage

### Building Membership Data

**Script Location:** `src/programs/rebuild_sp500_membership.py`

#### Option 1: Full Rebuild (Recommended)

Use this when:
- Starting fresh
- CSV file has been updated
- Data consistency issues detected
- Want to change date range

```bash
# Navigate to project root
cd /Users/frank/Projects/QuantX/quantx-data-builder

# Activate virtual environment
source .venv/bin/activate

# Full rebuild from 2014 onwards
python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01
```

**What Happens:**
1. Prompts for confirmation (type "yes" to proceed)
2. Deletes existing membership data in `data/curated/membership/universe=sp500/`
3. Reads CSV file specified in `config/settings.yaml`
4. Processes all additions/removals from min-date forward
5. Generates both daily and interval representations
6. Validates data and reports statistics

**Expected Output:**
```
================================================================================
S&P 500 Membership Data Builder
================================================================================

Mode: REBUILD FROM SCRATCH
Min Date: 2014-01-01

⚠️  WARNING: This will DELETE existing membership data!
Continue? (yes/no): yes

================================================================================

✅ Success!
   First date:      2014-01-07
   Last date:       2025-11-11
   Unique tickers:  763
   Daily rows:      345,870
   Interval rows:   772
   
   Source file:     S&P 500 Historical Components & Changes(11-16-2025).csv

✅ Membership data is ready!

💡 Tip: Run check_universe_overlap_aware.py to validate data completeness
```

#### Option 2: Incremental Update

Use this when:
- CSV has minor additions
- Adding recent data only
- Want to preserve existing data

```bash
python src/programs/rebuild_sp500_membership.py
```

**What Happens:**
1. Reads existing membership data
2. Merges new records from CSV
3. Deduplicates and sorts
4. Updates both representations

**Note:** Incremental updates assume CSV is append-only. For corrections to historical data, use full rebuild.

### Command-Line Options

```bash
python src/programs/rebuild_sp500_membership.py [OPTIONS]

Options:
  --rebuild              Delete existing data and rebuild from scratch
                        (default: incremental update)
  
  --min-date YYYY-MM-DD  Minimum date to include in build
                        (default: 2000-01-01)

Examples:
  # Rebuild from 2014 (research period)
  python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01
  
  # Rebuild from 2000 (full history)
  python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2000-01-01
  
  # Incremental update (no flags)
  python src/programs/rebuild_sp500_membership.py
```

---

## Implementation Details

### How It Works

**Step-by-Step Process:**

1. **Load Configuration**
   ```python
   from core.config import Config
   
   config = Config("config/settings.yaml")
   membership_file = config.get('universe.sp500.membership_file')
   ```

2. **Initialize Universe Builder**
   ```python
   from universe import SP500Universe
   
   universe = SP500Universe(
       data_root=config.get('storage.local.root_path')
   )
   ```

3. **Build Membership**
   ```python
   stats = universe.build_membership(
       min_date='2014-01-01',
       rebuild=True,  # or False for incremental
       membership_filename=membership_file
   )
   ```

4. **Generate Dual Representations**
   - **Daily Mode:** Expands intervals to daily records for each trading day
   - **Interval Mode:** Consolidates consecutive membership into start/end dates

5. **Save to Parquet**
   - Daily: `data/curated/membership/universe=sp500/mode=daily/`
   - Intervals: `data/curated/membership/universe=sp500/mode=intervals/`

6. **Validate and Report**
   - Statistics: ticker count, date range, row counts
   - Consistency checks: no overlaps, no duplicates
   - Data quality validation

### Key Classes

**SP500Universe** (`src/universe/sp500_universe.py`)
- Manages S&P 500 membership lifecycle
- Handles CSV parsing and date normalization
- Generates both daily and interval representations
- Performs data validation

**Config** (`src/core/config.py`)
- Loads `config/settings.yaml`
- Provides typed configuration access
- Supports environment variable substitution

---

## Data Quality Checks

### Automatic Validations

During build, the system automatically checks:

- ✅ **No Duplicates:** Each (date, ticker) pair appears only once in daily mode
- ✅ **No Overlaps:** Same ticker cannot have overlapping intervals
- ✅ **Valid Dates:** All dates are valid ISO format and chronological
- ✅ **Ticker Format:** All tickers are uppercase and normalized
- ✅ **Consistency:** Daily and interval modes represent same membership

### Manual Validation Scripts

**Check Universe Overlap** (`tests/check_universe_overlap_aware.py`)
```bash
python tests/check_universe_overlap_aware.py
```
- Validates membership-price data alignment
- Identifies tickers with membership but no price data
- Reports coverage statistics

**Check Multiple Intervals** (`tests/check_multiple_intervals.py`)
```bash
python tests/check_multiple_intervals.py
```
- Identifies tickers with multiple membership periods
- Useful for understanding churn and re-additions
- Example: Companies removed and later re-added to S&P 500

---

## Common Use Cases

### 1. Point-in-Time Backtest

**Scenario:** Run a backtest without survivorship bias

```python
import pandas as pd

def get_backtest_universe(date: str) -> list[str]:
    """Get exact S&P 500 constituents on a specific date"""
    intervals = pd.read_parquet(
        'data/curated/membership/universe=sp500/mode=intervals/'
        'sp500_membership_intervals.parquet'
    )
    
    date = pd.to_datetime(date).date()
    mask = (intervals['start_date'] <= date) & (intervals['end_date'] >= date)
    return intervals[mask]['ticker'].tolist()

# Get constituents for January 2020
tickers = get_backtest_universe('2020-01-15')
print(f"Backtest universe: {len(tickers)} tickers")
```

### 2. Membership Tenure Analysis

**Scenario:** Find longest-tenured S&P 500 members

```python
import pandas as pd

intervals = pd.read_parquet(
    'data/curated/membership/universe=sp500/mode=intervals/'
    'sp500_membership_intervals.parquet'
)

# Calculate tenure
intervals['days'] = (intervals['end_date'] - intervals['start_date']).dt.days

# Find longest continuous memberships
longest = intervals.nlargest(10, 'days')[['ticker', 'start_date', 'end_date', 'days']]
print("Longest S&P 500 Memberships:")
print(longest)
```

### 3. Rebalancing Event Detection

**Scenario:** Identify all additions/removals in a specific month

```python
import pandas as pd

daily = pd.read_parquet(
    'data/curated/membership/universe=sp500/mode=daily/'
    'sp500_membership_daily.parquet'
)

# Get changes in September 2024
sep_start = daily[daily['date'] == '2024-09-01']['ticker'].tolist()
sep_end = daily[daily['date'] == '2024-09-30']['ticker'].tolist()

additions = set(sep_end) - set(sep_start)
removals = set(sep_start) - set(sep_end)

print(f"September 2024 Changes:")
print(f"  Additions: {additions}")
print(f"  Removals: {removals}")
```

### 4. Historical Coverage Report

**Scenario:** Generate coverage report for research period

```python
import pandas as pd
from datetime import date

intervals = pd.read_parquet(
    'data/curated/membership/universe=sp500/mode=intervals/'
    'sp500_membership_intervals.parquet'
)

# Research period
start = date(2014, 1, 1)
end = date(2024, 12, 1)

# Tickers active during period
mask = (intervals['start_date'] <= end) & (intervals['end_date'] >= start)
active = intervals[mask]

print(f"S&P 500 Research Period Coverage (2014-2024):")
print(f"  Total tickers: {active['ticker'].nunique()}")
print(f"  Total intervals: {len(active)}")
print(f"  Average tenure: {active['days'].mean():.0f} days")
```

---

## Troubleshooting

### Issue: "CSV file not found"

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: 'data/raw/S&P 500 Historical Components & Changes(11-16-2025).csv'
```

**Solution:**
1. Verify CSV file exists in `data/raw/` directory
2. Check filename matches exactly in `config/settings.yaml`
3. Ensure no extra spaces or special characters

### Issue: "Membership data seems incomplete"

**Symptoms:**
- Fewer tickers than expected
- Date range shorter than CSV coverage
- Missing recent additions

**Solution:**
1. Check `--min-date` parameter (may be filtering too aggressively)
2. Verify CSV file is the latest version
3. Run full rebuild: `--rebuild --min-date 2000-01-01`

### Issue: "Data validation errors"

**Error:**
```
ValidationError: Overlapping intervals detected for ticker XYZ
```

**Solution:**
1. Check source CSV for duplicate entries
2. Verify date formats are consistent (YYYY-MM-DD)
3. Ensure "add" and "remove" actions are properly paired
4. Run full rebuild to reset state

### Issue: "Slow query performance"

**Symptoms:**
- Point-in-time queries taking > 1 second
- Large memory usage

**Solution:**
- Use **interval mode** for single-date queries (much faster)
- Use **daily mode** only when iterating over many dates
- Consider adding date index if using pandas extensively

---

## Best Practices

### 1. Always Use Configuration

❌ **Don't hardcode:**
```python
df = pd.read_csv('data/raw/some_file.csv')
```

✅ **Do use config:**
```python
from core.config import Config
config = Config('config/settings.yaml')
filename = config.get('universe.sp500.membership_file')
df = pd.read_csv(f'data/raw/{filename}')
```

### 2. Choose Right Representation

**Use Interval Mode When:**
- Checking if ticker was member on specific date
- Performing range queries (was member during period?)
- Analyzing tenure and churn
- Minimizing memory usage

**Use Daily Mode When:**
- Iterating over many dates sequentially
- Daily rebalancing simulations
- Need exact historical membership on every date
- Joining with daily price data

### 3. Validate After Rebuild

Always run validation after rebuilding:

```bash
# 1. Rebuild membership
python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01

# 2. Validate coverage
python tests/check_universe_overlap_aware.py

# 3. Check ESG alignment (if applicable)
python src/programs/check_esg_continuity.py
```

### 4. Backup Before Rebuild

For production systems:

```bash
# Backup existing data
cp -r data/curated/membership data/curated/membership_backup_$(date +%Y%m%d)

# Rebuild
python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01

# If issues, restore
# mv data/curated/membership_backup_YYYYMMDD data/curated/membership
```

### 5. Document CSV Updates

Maintain a changelog when updating source CSV:

```markdown
## Membership CSV Change Log

### 2025-11-16
- File: "S&P 500 Historical Components & Changes(11-16-2025).csv"
- Changes: Added Q4 2025 rebalancing events
- Impact: 5 new tickers, 3 removals
- Rebuild required: Yes
```

---

## Integration with Other Data

### Linking to Price Data

```python
import pandas as pd

# Get S&P 500 members on a date
intervals = pd.read_parquet('data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet')
date = pd.to_datetime('2020-01-15').date()
mask = (intervals['start_date'] <= date) & (intervals['end_date'] >= date)
members = intervals[mask]['ticker'].tolist()

# Load prices for those members
all_prices = []
for ticker in members:
    price_file = f'data/curated/tickers/exchange=us/ticker={ticker}/prices/freq=daily/year=2020/part-000.parquet'
    if Path(price_file).exists():
        df = pd.read_parquet(price_file)
        df = df[df['date'] == '2020-01-15']
        all_prices.append(df)

prices = pd.concat(all_prices, ignore_index=True)
print(f"Loaded prices for {len(prices)} tickers")
```

### Linking to ESG Data

```python
import pandas as pd

# Get members with ESG data
intervals = pd.read_parquet('data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet')
members = intervals['ticker'].unique()

# Check which have ESG data
esg_tickers = []
for ticker in members:
    esg_path = f'data/curated/tickers/exchange=us/ticker={ticker}/esg/'
    if Path(esg_path).exists():
        esg_tickers.append(ticker)

print(f"S&P 500 members with ESG data: {len(esg_tickers)}/{len(members)}")
```

### Linking to GVKEY Mapper

```python
import pandas as pd

# Load membership and GVKEY mapper
intervals = pd.read_parquet('data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet')
gvkey = pd.read_parquet('data/curated/metadata/gvkey.parquet')

# Enrich membership with GVKEY
enriched = intervals.merge(gvkey[['ticker', 'gvkey']], on='ticker', how='left')

# Check coverage
missing_gvkey = enriched[enriched['gvkey'].isna()]
print(f"Tickers without GVKEY: {len(missing_gvkey)}/{len(enriched)}")
```

---

## Quick Reference

### File Locations

| Data Type | Path |
|-----------|------|
| Daily Parquet | `data/curated/membership/universe=sp500/mode=daily/sp500_membership_daily.parquet` |
| Intervals Parquet | `data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet` |
| Source CSV | `data/raw/S&P 500 Historical Components & Changes(11-16-2025).csv` |
| Config | `config/settings.yaml` |
| Builder Script | `src/programs/rebuild_sp500_membership.py` |

### Common Commands

```bash
# Full rebuild from 2014 (research period)
python src/programs/rebuild_sp500_membership.py --rebuild --min-date 2014-01-01

# Incremental update
python src/programs/rebuild_sp500_membership.py

# Validate coverage
python tests/check_universe_overlap_aware.py

# Check for multiple membership periods
python tests/check_multiple_intervals.py
```

### Key Statistics

| Metric | Current Value |
|--------|--------------|
| Unique Tickers | 763 |
| Daily Records | 345,870 |
| Intervals | 772 |
| Date Range | 2014-01-07 to 2025-11-11 |
| Research Period Tickers | 745 |

---

## FAQ

**Q: How often should I rebuild membership data?**  
A: Quarterly or when CSV file is updated. Daily incremental updates can be done for recent additions.

**Q: Why do I have more intervals (772) than tickers (763)?**  
A: Some companies were removed and later re-added to the S&P 500, creating multiple intervals per ticker.

**Q: Can I change the date range after initial build?**  
A: Yes, run full rebuild with different `--min-date`. All data is reprocessed.

**Q: What happens if I run incremental update twice?**  
A: Idempotent operation. Duplicates are automatically removed.

**Q: Should I use daily or interval mode for my backtest?**  
A: For single-date queries, use intervals (faster). For daily iteration, use daily mode.

**Q: How do I handle ticker changes (e.g., FB → META)?**  
A: Use GVKEY mapper (`data/curated/metadata/gvkey.parquet`) to track company identity across ticker changes.

**Q: Why does my research period start on 2014-01-07, not 2014-01-01?**  
A: 2014-01-01 was a holiday. First trading day of 2014 was January 7th.

---

## Document History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-24 | Initial S&P 500 membership builder documentation |

---

**For comprehensive methodology and integration details, see:** `docs/DATA_PIPELINE_METHODOLOGY.md`

**End of Document**
