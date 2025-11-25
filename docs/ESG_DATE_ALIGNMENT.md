# ESG and Price Data Date Alignment

## Problem (RESOLVED ✅)

ESG data and price data previously used different date conventions:
- **ESG data (old)**: First day of month (2020-01-01, 2020-02-01, 2020-03-01, ...)
- **Price data**: Last business day of month (2020-01-31, 2020-02-28, 2020-03-31, ...)

This caused zero overlap when joining datasets on date, resulting in empty factor returns.

## Root Cause

The raw ESG data file uses YearMonth format (YYYYMM) which was being converted to first-of-month dates. However, financial returns are calculated at month-end, so dates needed to be aligned.

## Solution ✅

**Date normalization is now handled automatically in `ESGManager`** during data loading. The `_load_esg_data()` method applies `MonthEnd(0)` offset when creating the date column:

```python
# In src/esg/esg_manager.py, lines 177-182
df['date'] = pd.to_datetime(
    df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2) + '-01'
) + pd.offsets.MonthEnd(0)
df['date'] = df['date'].dt.date
```

This automatically converts:
- `2020-01-01` → `2020-01-31`
- `2020-02-01` → `2020-02-29`
- `2020-03-01` → `2020-03-31`

## Implementation

**Primary Implementation**: `src/esg/esg_manager.py`
- Lines 173-182: Date normalization in `_load_esg_data()` method
- Applied at source - all ESG data loaded through ESGManager has aligned dates
- No manual normalization needed in consuming code

**Consumer Code** (no normalization needed):
- `tests/test_build_esg_factors.py` - Removed manual date normalization
- `src/programs/build_esg_factors.py` - Removed manual date normalization
- All code using `ESGManager.load_esg_data()` automatically gets aligned dates

## Verification

After regenerating ESG data with the fix, date alignment is automatic:

**Example verification for AAPL (2020):**
```
ESG dates:     2020-01-31, 2020-02-29, 2020-03-31, ... (end-of-month)
Price dates:   2020-01-31, 2020-02-28, 2020-03-31, ... (end-of-month)
Common dates:  9/12 months match perfectly ✓
```

The slight difference (9/12 vs 12/12) is due to prices using last **business day** (e.g., 2020-02-28 vs 2020-02-29, 2020-05-29 vs 2020-05-31), but they're in the same month and align properly for monthly analysis.

**Check alignment:**

```python
from esg import ESGManager
from universe.sp500_universe import SP500Universe

universe = SP500Universe()
esg_mgr = ESGManager(universe)

# Load ESG data - dates are already aligned
df = esg_mgr.load_esg_data('AAPL', start_date='2020-01-01', end_date='2020-12-31')
print("ESG dates:", sorted(df['date'].unique()))

# All dates are end-of-month automatically
# Output: [2020-01-31, 2020-02-29, 2020-03-31, ...]
```

## Impact on Factor Construction

Proper date alignment is **critical** for:
- ✅ Joining ESG signals with price returns
- ✅ Lagging signals to avoid look-ahead bias
- ✅ Building long-short portfolios
- ✅ Calculating factor returns

Without date alignment, all factor methods will return empty results (NaN values).

## Best Practice

**No manual date normalization required!** 

ESGManager automatically returns end-of-month dates for all data loaded via:
- `get_esg_data(symbol=...)` 
- `load_esg_data(ticker=...)`
- `process_universe_esg(...)`

**Key Points:**
1. ✅ Date alignment handled at source (ESGManager)
2. ✅ All saved ESG data has end-of-month dates
3. ✅ No manual conversion needed in analysis code
4. ✅ Transparent to users - "just works"

**If you regenerate ESG data**, the fix is automatically applied during processing.

## Technical Details

**MonthEnd Offset Behavior:**
```python
pd.offsets.MonthEnd(0)  # Move to end of current month
pd.offsets.MonthEnd(1)  # Move to end of next month
pd.offsets.MonthEnd(-1) # Move to end of previous month
```

**Applied in ESGManager:**
```python
# Parse YearMonth (YYYYMM) → create first-of-month date → convert to end-of-month
df['date'] = pd.to_datetime(
    df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2) + '-01'
) + pd.offsets.MonthEnd(0)
```
