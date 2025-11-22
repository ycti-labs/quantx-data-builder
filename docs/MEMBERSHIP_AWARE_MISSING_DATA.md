# Membership-Aware Missing Data Check Implementation

## Overview

Enhanced `check_missing_data()` in `PriceManager` to only consider data as "missing" if it falls within a symbol's membership interval in the universe. This prevents false positives when requesting data for periods before a stock joined the index.

## Changes Made

### 1. Updated `check_missing_data()` Method

**Location**: `src/market/price_manager.py`

**Key Features**:
- Reads membership intervals from `membership_intervals.parquet`
- Intersects required period with actual membership period
- Only checks for missing data during membership
- Returns membership period info in result dict
- Enhanced logging to show membership periods

**New Return Fields**:
- `membership_start`: Date symbol joined universe (or None)
- `membership_end`: Date symbol left universe or latest date (or None)

### 2. New Method `get_membership_interval()` in Universe Class

**Location**: `src/universe/universe.py`

**Purpose**: Load and parse membership intervals for a symbol

**Logic**:
- Reads `{universe}_membership_intervals.parquet` 
- Filters to specific symbol
- Returns earliest start and latest end date (handles multiple intervals)
- Gracefully handles missing files or symbols

**Usage**:
```python
universe = SP500Universe()
interval = universe.get_membership_interval('TSLA')
# Returns: (datetime.date(2020, 12, 21), datetime.date(2099, 12, 31))
```

### 3. Updated `check_missing_data()` in PriceManager

### Case 1: Symbol Joined Recently (e.g., TSLA in 2020)

```python
result = price_mgr.check_missing_data(
    symbol='TSLA',
    required_start='2018-01-01',  # Before membership
    required_end='2024-12-31'
)
# Only checks 2020-12-21 (join date) to 2024-12-31
# Does not report 2018-2020 as "missing"
```

**Log Output**:
```
⚠️  TSLA: Existing data PARTIAL | (2020-12-21 to 2024-12-31) | 
Missing: 0d at start, 5d at end (tolerance: ±2d) | 
Membership: (2020-12-21 to 2099-12-31)
```

### Case 2: Required Period Before Membership

```python
result = price_mgr.check_missing_data(
    symbol='TSLA',
    required_start='2018-01-01',
    required_end='2019-12-31'  # Before TSLA joined SP500
)
# Returns status='complete' because period is outside membership
```

**Log Output**:
```
✅ TSLA: Required period outside membership | 
Membership: (2020-12-21 to 2099-12-31), Required: (2018-01-01 to 2019-12-31)
```

### Case 3: Long-Standing Member (e.g., AAPL)

```python
result = price_mgr.check_missing_data(
    symbol='AAPL',
    required_start='2020-01-01',
    required_end='2024-12-31'
)
# Checks full period since AAPL was member throughout
```

### Case 4: No Membership Data

If membership data is unavailable (file missing or symbol not found):
- Falls back to checking full required period
- Logs warning about missing membership data
- Continues with normal gap detection

## Result Dictionary Structure

```python
{
    'status': 'complete' | 'partial' | 'missing',
    'actual_start': date or None,
    'actual_end': date or None,
    'missing_start_days': int,
    'missing_end_days': int,
    'fetch_start': str or None,
    'fetch_end': str or None,
    'membership_start': date or None,  # NEW
    'membership_end': date or None     # NEW
}
```

## Testing

Run the test script to see all scenarios:

```bash
cd /Users/frank/Projects/QuantX/quantx-data-builder
python examples/test_membership_aware_check.py
```

**Test Cases**:
1. TSLA (joined 2020) - requesting data from 2018
2. AAPL (long-standing member) - requesting data from 2020
3. Unknown symbol - fallback behavior
4. Required period before membership - returns 'complete'

## Benefits

1. **No False Positives**: Don't report pre-membership data as missing
2. **Accurate Gap Analysis**: Only analyzes relevant periods
3. **Efficient Fetching**: Recommendations only cover membership periods
4. **Point-in-Time Accuracy**: Respects historical universe composition
5. **Academic Rigor**: Critical for unbalanced panel studies

## Implementation Details

### Membership Interval File Structure

```
data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet
```

**Columns**:
- `ticker`: Symbol (e.g., 'TSLA')
- `start_date`: Date joined universe
- `end_date`: Date left universe (or 2099-12-31 for current members)
- `gvkey`: GVKEY identifier (optional)

### Multiple Intervals Handling

If a symbol was removed and re-added (multiple intervals):
- Uses earliest start_date and latest end_date
- Covers full span of all membership periods
- Future enhancement: check gaps between intervals

## Integration with Other Methods

### Methods That May Need Updates

1. `fetch_missing_data()` - Already uses `check_missing_data()` ✅
2. `fetch_universe_missing_data()` - Iterates over current members ✅
3. `get_existing_date_range()` - No change needed (reads actual data) ✅

### Consistency with `fetch_eod()`

The `fetch_eod()` method still reports completeness based on requested dates:
- This is correct behavior (user explicitly requested specific dates)
- `check_missing_data()` adds intelligence for automatic data management
- Both methods complement each other

## Future Enhancements

1. **Gap Between Intervals**: Handle symbols removed and re-added with gaps
2. **Partial Membership**: Check each interval separately 
3. **Lookahead Bias Prevention**: Ensure no future-dated membership usage
4. **Performance**: Cache membership intervals for batch operations
5. **Corporate Actions**: Integrate with ticker corrections for name changes

## Dependencies

- **Universe class**: Provides `get_membership_path(mode='intervals')`
- **Parquet files**: Requires `membership_intervals.parquet` to exist
- **Pandas**: For date parsing and interval filtering

## Backward Compatibility

✅ **Fully backward compatible**:
- Existing code continues to work without changes
- New fields in return dict are optional additions
- Gracefully handles missing membership data
- No breaking changes to method signature (tolerance_days was already there)
