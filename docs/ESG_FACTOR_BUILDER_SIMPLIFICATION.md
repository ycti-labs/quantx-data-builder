# ESGFactorBuilder Simplification Summary

## Overview

Simplified `ESGFactorBuilder` to use only `RiskFreeRateManager` for loading cached risk-free rate data, removing redundant parameters and the builder dependency.

## Changes Made

### 1. Constructor Simplification

**Removed Parameters:**
- `rf_frequency` - No longer needed (FRED data is always annual)
- `rf_is_percent` - No longer needed (FRED data is always percentage)
- `fred_api_key` - Removed (use `RiskFreeRateBuilder` separately for data fetching)

**Before:**
```python
ESGFactorBuilder(
    universe=universe,
    rf_frequency="monthly",      # REMOVED
    rf_is_percent=True,          # REMOVED
    fred_api_key=api_key,        # REMOVED
    rf_rate_type="3month"
)
```

**After:**
```python
ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month"
)
```

### 2. Removed RiskFreeRateBuilder Dependency

**Before:**
- Had both `self.rf_builder` (for fetching) and `self.rf_manager` (for loading)
- Complex logic deciding whether to fetch or load
- Required API key handling

**After:**
- Only `self.rf_manager` (for loading cached data)
- Simple, single-purpose: load from cache
- No API key needed

### 3. Simplified `_load_risk_free_rate()` Method

**Before (70+ lines):**
- Checked cache manually
- Tried builder if available
- Tried manager as fallback
- Complex error handling

**After (18 lines):**
```python
def _load_risk_free_rate(self, returns_df: pd.DataFrame) -> pd.DataFrame:
    """Load risk-free rate data from cache"""
    # Determine date range
    dates = returns_df.index.get_level_values("date")
    start_date = pd.to_datetime(dates.min()).strftime("%Y-%m-%d")
    end_date = pd.to_datetime(dates.max()).strftime("%Y-%m-%d")
    
    # Load using RiskFreeRateManager
    rf_df = self.rf_manager.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type=self.rf_rate_type,
        frequency="monthly"
    )
    
    # Normalize and return
    if "rate" in rf_df.columns:
        rf_df = rf_df.rename(columns={"rate": "RF"})
    rf_df["date"] = pd.to_datetime(rf_df["date"])
    
    return rf_df
```

### 4. Simplified `_to_excess_returns()` Method

**Removed:**
- Auto-detection logic for percentage format
- Conditional RF frequency conversion
- Complex normalization based on parameters

**Standardized:**
- FRED data is ALWAYS annual percentage
- Single conversion: `rf / 100 / 12` (annual % → monthly decimal)
- Clear logging of conversion steps

**Before:**
```python
# Complex logic with rf_frequency and rf_is_percent
if self.rf_is_percent or auto_detected_percent:
    rf_copy["RF"] = rf_copy["RF"] / 100
if self.rf_frequency == "annual":
    rf_copy["RF"] = rf_copy["RF"] / 12
```

**After:**
```python
# Simple, always the same
rf_copy["RF"] = rf_copy["RF"] / 100 / 12
self.logger.info("Converted RF from annual % to monthly decimal (÷100÷12)")
```

## Benefits

1. **Simpler API**: 3 fewer parameters
2. **Clearer Responsibility**: ESGFactorBuilder loads, RiskFreeRateBuilder fetches
3. **No API Key Management**: ESGFactorBuilder doesn't need credentials
4. **Less Code**: ~100 lines removed
5. **Single Source of Truth**: FRED data format is standardized

## Usage Pattern

### Step 1: Pre-cache Risk-Free Rate Data (One-Time Setup)

```python
from market.risk_free_rate_manager import RiskFreeRateBuilder

# Fetch and cache data once
builder = RiskFreeRateBuilder(
    fred_api_key=config.get("fred.api_key"),
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)

builder.build_and_save(
    start_date="2000-01-01",
    end_date="2024-12-31",
    rate_type="3month",
    frequency="monthly"
)
```

### Step 2: Build ESG Factors (Uses Cached Data)

```python
from esg.esg_factor import ESGFactorBuilder

# No API key needed!
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month"
)

factors = builder.build_factors(
    prices_df=prices_df,
    esg_df=esg_df
)
```

## Error Handling

If cached data is missing:

```python
FileNotFoundError: Risk-free rate cache not found: data/.../3month_monthly.parquet
Use RiskFreeRateBuilder to fetch and save data first.
```

**Solution:**
```bash
# Use RiskFreeRateBuilder to fetch data
python -c "
from market.risk_free_rate_manager import RiskFreeRateBuilder
builder = RiskFreeRateBuilder(fred_api_key='your_key', ...)
builder.build_and_save(start_date='2000-01-01', end_date='2024-12-31')
"
```

## Migration Guide

### For Existing Code

**Old code:**
```python
builder = ESGFactorBuilder(
    universe=universe,
    rf_frequency="monthly",
    rf_is_percent=True,
    fred_api_key=config.get("fred.api_key"),
    rf_rate_type="3month"
)
```

**New code:**
```python
# 1. Pre-cache RF data (if not already cached)
from market.risk_free_rate_manager import RiskFreeRateBuilder
rf_builder = RiskFreeRateBuilder(
    fred_api_key=config.get("fred.api_key"),
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)
rf_builder.build_and_save("2000-01-01", "2024-12-31", rate_type="3month")

# 2. Build factors (simpler constructor)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month"
)
```

## Technical Details

### Risk-Free Rate Normalization

**FRED Data Format:**
- Series: DGS3MO (3-Month Treasury)
- Units: Annual percentage points
- Example: 5.25 means 5.25% per year

**Conversion to Monthly Decimal:**
```python
annual_pct = 5.25        # From FRED
monthly_decimal = 5.25 / 100 / 12  # = 0.004375 (0.4375%)
```

**Why This Matters:**
- Stock returns are monthly decimals (e.g., 0.02 = 2%)
- Must match units for excess returns: `r_excess = r_stock - r_rf`
- Incorrect units corrupt Sharpe ratios and factor loadings

### Validation Logging

```
INFO: Risk-free rate (FRED annual %): mean=4.2500%, std=1.5000%
INFO: Converted RF from annual % to monthly decimal (÷100÷12)
INFO: Risk-free rate after normalization: mean=0.003542, std=0.001250
```

## Testing

Run the test program to verify:

```bash
python src/programs/build_esg_factors.py --continuous-esg-only --sector-neutral
```

Expected output:
```
INFO: Loading risk-free rate for 2006-12-31 to 2024-11-30
INFO: Loaded 216 risk-free rate observations
INFO: Risk-free rate (FRED annual %): mean=2.5000%, std=2.1000%
INFO: Converted RF from annual % to monthly decimal (÷100÷12)
INFO: Risk-free rate after normalization: mean=0.002083, std=0.001750
```

## Files Changed

- ✅ `src/esg/esg_factor.py` - Simplified constructor and RF handling
- ✅ `src/market/risk_free_rate_manager.py` - Split into Builder/Manager
- ✅ `docs/RISK_FREE_RATE_REFACTORING.md` - Architecture documentation
- ✅ `tests/demo_risk_free_rate_split.py` - Usage examples

## Breaking Changes

### Constructor Parameters

| Parameter | Status | Alternative |
|-----------|--------|-------------|
| `rf_frequency` | ❌ Removed | Always assumes monthly (FRED is annual, auto-converted) |
| `rf_is_percent` | ❌ Removed | Always assumes percentage (FRED format) |
| `fred_api_key` | ❌ Removed | Use `RiskFreeRateBuilder` separately |
| `rf_rate_type` | ✅ Kept | Still configurable (3month, 1year, etc.) |

### Behavior Changes

1. **No Automatic Fetching**: Previously would fetch from FRED if cache missing (with API key). Now raises `FileNotFoundError` and directs user to use `RiskFreeRateBuilder`.

2. **Standardized Normalization**: Always converts FRED annual % to monthly decimal. No more conditional logic based on parameters.

3. **Clearer Error Messages**: Errors now explicitly mention using `RiskFreeRateBuilder` for data fetching.

## Recommendations

1. **Pre-cache RF Data**: Run `RiskFreeRateBuilder` once to cache 20+ years of data
2. **Periodic Updates**: Re-run monthly to keep cache current
3. **Monitor Cache**: Check for gaps in cached date ranges
4. **Validate Units**: Review logging output to ensure correct normalization

## Future Enhancements

1. **Auto-Update**: Scheduled job to refresh cache
2. **Multiple Rates**: Support loading multiple rate types simultaneously
3. **International**: Support non-US risk-free rates (LIBOR, EURIBOR, etc.)
4. **Forward Curves**: Support term structure analysis
