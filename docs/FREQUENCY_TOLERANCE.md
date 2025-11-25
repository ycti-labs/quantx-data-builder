# Frequency-Aware Tolerance in Data Validation

## Overview

The QuantX data validation system now supports **automatic tolerance calculation** based on data frequency. This ensures accurate completeness checks for daily, weekly, and monthly price data.

## Why Different Tolerances?

Different data frequencies have different natural alignment characteristics:

### Daily Data (±2 days)
- **Issue**: Weekends and holidays create natural gaps
- **Example**: Required start = Monday 2024-01-01, Data starts = Tuesday 2024-01-02
  - Gap = 1 day → ✅ COMPLETE (within ±2 days tolerance)
- **Use case**: Most stock data, intraday strategies

### Weekly Data (±6 days)
- **Issue**: Weekly data can be on any weekday (Mon-Fri), not just Mondays
- **Example 1**: Required start = Wednesday 2024-01-03, Data starts = Monday 2024-01-01
  - Gap = 2 days → ✅ COMPLETE (within ±6 days tolerance)
- **Example 2**: Required end = Tuesday 2024-12-31, Data ends = Friday 2024-12-27
  - Gap = 4 days → ✅ COMPLETE (within ±6 days tolerance)
- **Rationale**: Maximum gap within same week is ~6 days
- **Use case**: Medium-term trend analysis, reduced noise

### Monthly Data (±3 days)
- **Issue**: Monthly data uses last trading day of month, not calendar month-end
- **Example**: Required = 2024-01-31 (calendar), Data = 2024-01-29 (last trading day)
  - Gap = 2 days → ✅ COMPLETE (within ±3 days tolerance)
- **Rationale**: Last trading day can be 1-3 days before month-end
- **Use case**: Long-term trends, fundamental analysis

## Implementation

### Automatic Tolerance Calculation

```python
from programs.check_missing_data import get_tolerance_for_frequency

# Get appropriate tolerance for frequency
tolerance_daily = get_tolerance_for_frequency('daily')    # Returns: 2
tolerance_weekly = get_tolerance_for_frequency('weekly')  # Returns: 6
tolerance_monthly = get_tolerance_for_frequency('monthly') # Returns: 3
```

### Using with MissingDataChecker

```python
# Auto-calculate tolerance (recommended)
result = checker.check_missing_data(
    symbol='AAPL',
    required_start='2020-01-01',
    required_end='2020-12-31',
    frequency='weekly',      # Specify frequency
    tolerance_days=None      # Auto-calculate (±6 days for weekly)
)

# Or explicitly override
result = checker.check_missing_data(
    symbol='AAPL',
    required_start='2020-01-01',
    required_end='2020-12-31',
    frequency='weekly',
    tolerance_days=10        # Custom tolerance
)
```

### Fetching Different Frequencies

```python
# Fetch daily data (default)
price_mgr.fetch_eod('AAPL', '2020-01-01', '2020-12-31', frequency='daily')

# Fetch weekly data
price_mgr.fetch_eod('AAPL', '2020-01-01', '2020-12-31', frequency='weekly')

# Fetch monthly data
price_mgr.fetch_eod('AAPL', '2020-01-01', '2020-12-31', frequency='monthly')
```

## Tolerance Decision Logic

```python
def get_tolerance_for_frequency(frequency: str) -> int:
    """
    Calculate appropriate tolerance in days based on data frequency
    
    Daily:   ±2 days  (weekends, holidays)
    Weekly:  ±6 days  (any weekday in the week)
    Monthly: ±3 days  (month-end trading day adjustments)
    """
    if frequency == 'daily':
        return 2
    elif frequency == 'weekly':
        return 6
    elif frequency == 'monthly':
        return 3
    else:
        return 2  # Default to daily
```

## Real-World Examples

### Example 1: Weekly Data Alignment

**Scenario**: Fetching weekly S&P 500 data for 2020

```python
# Universe check for 2020
result = checker.check_missing_data(
    symbol='SPY',
    required_start='2020-01-01',  # Wednesday
    required_end='2020-12-31',     # Thursday
    frequency='weekly',
    tolerance_days=None            # Auto: ±6 days
)

# Data might start on Monday 2019-12-30 (prev week)
# Gap = 2 days → Status: COMPLETE ✅
```

### Example 2: Monthly Data Month-End

**Scenario**: Checking monthly data completeness

```python
result = checker.check_missing_data(
    symbol='AAPL',
    required_start='2020-01-01',   # Calendar month start
    required_end='2020-12-31',     # Calendar month end
    frequency='monthly',
    tolerance_days=None            # Auto: ±3 days
)

# Data ends on 2020-12-29 (last trading day, not 12-31)
# Gap = 2 days → Status: COMPLETE ✅
```

### Example 3: Custom Tolerance for Quality Check

**Scenario**: Strict validation requiring exact dates

```python
# Strict check: no tolerance
result = checker.check_missing_data(
    symbol='AAPL',
    required_start='2020-01-01',
    required_end='2020-12-31',
    frequency='daily',
    tolerance_days=0              # No tolerance
)

# Even 1-day gap → Status: PARTIAL ⚠️
```

## Data Storage Structure

All frequencies use the same Hive-style partitioning:

```
data/prices/
├── exchange=us/
    ├── ticker=AAPL/
        ├── freq=daily/
        │   ├── adj=true/
        │       ├── year=2020/
        │           ├── part-000.parquet
        ├── freq=weekly/
        │   ├── adj=true/
        │       ├── year=2020/
        │           ├── part-000.parquet
        ├── freq=monthly/
            ├── adj=true/
                ├── year=2020/
                    ├── part-000.parquet
```

## API Changes

### Updated Methods

1. **check_missing_data()** - Added `frequency` parameter
   ```python
   def check_missing_data(
       self,
       symbol: str,
       required_start: str,
       required_end: str,
       frequency: str = "daily",         # NEW
       tolerance_days: Optional[int] = None,  # Now optional
       handle_gaps: bool = True
   ) -> Dict:
   ```

2. **get_tolerance_for_frequency()** - New utility function
   ```python
   def get_tolerance_for_frequency(frequency: str) -> int:
       """Calculate appropriate tolerance based on frequency"""
   ```

### Backward Compatibility

All existing code continues to work:
- Default `frequency='daily'` maintains existing behavior
- Default `tolerance_days=None` auto-calculates as 2 days for daily
- Explicit tolerance values override auto-calculation

## Testing

Run the test scripts to verify:

```bash
# Test tolerance calculation
python examples/test_frequency_tolerance.py

# Comprehensive multi-frequency demo
python examples/demo_multi_frequency.py
```

## Summary

| Frequency | Tolerance | Rationale | Typical Count |
|-----------|-----------|-----------|---------------|
| Daily     | ±2 days   | Weekends, holidays | ~252/year |
| Weekly    | ±6 days   | Any weekday alignment | ~52/year |
| Monthly   | ±3 days   | Month-end trading day | ~12/year |

**Key Benefits:**
- ✅ Automatic tolerance calculation
- ✅ Frequency-aware validation
- ✅ Reduced false positives for missing data
- ✅ Backward compatible
- ✅ Override capability for custom requirements
