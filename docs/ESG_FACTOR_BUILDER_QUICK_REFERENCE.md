# Quick Reference: ESGFactorBuilder After Simplification

## Constructor

```python
ESGFactorBuilder(
    universe: Universe,
    quantile: float = 0.2,
    sector_neutral: bool = False,
    lag_signal: int = 1,
    weighting: str = "equal",
    rf_rate_type: str = "3month"
)
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `universe` | Universe | Required | Universe instance for data access |
| `quantile` | float | 0.2 | Quantile for long/short legs (0.2 = top/bottom 20%) |
| `sector_neutral` | bool | False | Rank within sectors instead of cross-sectionally |
| `lag_signal` | int | 1 | Number of periods to lag signal |
| `weighting` | str | "equal" | Portfolio weighting: "equal" or "value" |
| `rf_rate_type` | str | "3month" | Treasury rate: "3month", "1year", "5year", "10year", "30year" |

## Removed Parameters

| Parameter | Why Removed | Migration |
|-----------|-------------|-----------|
| `rf_frequency` | FRED data is always annual | Remove parameter |
| `rf_is_percent` | FRED data is always percentage | Remove parameter |
| `fred_api_key` | Use `RiskFreeRateBuilder` separately | See setup below |

## Setup (One-Time)

### Pre-cache Risk-Free Rate Data

```python
from market.risk_free_rate_manager import RiskFreeRateBuilder
from core.config import Config

config = Config("config/settings.yaml")

# Create builder with API key
builder = RiskFreeRateBuilder(
    fred_api_key=config.get("fred.api_key"),
    data_root="data/curated/references/risk_free_rate/freq=monthly",
    default_rate="3month"
)

# Fetch and save 20 years of data
builder.build_and_save(
    start_date="2004-01-01",
    end_date="2024-12-31",
    rate_type="3month",
    frequency="monthly"
)
```

## Usage (Regular)

```python
from esg.esg_factor import ESGFactorBuilder
from universe import SP500Universe

# Load universe
universe = SP500Universe(data_root="data/curated")

# Create factor builder (no API key needed!)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month"
)

# Build factors
factors = builder.build_factors(
    prices_df=prices_df,
    esg_df=esg_df,
    save=True
)
```

## Common Patterns

### Pattern 1: Equal-Weighted Factors (Academic)

```python
builder = ESGFactorBuilder(
    universe=universe,
    quantile=0.2,           # Top/bottom 20%
    weighting="equal",      # Equal-weighted
    sector_neutral=False    # Cross-sectional
)
```

### Pattern 2: Value-Weighted Factors (Practitioner)

```python
builder = ESGFactorBuilder(
    universe=universe,
    quantile=0.2,
    weighting="value",      # Value-weighted by market cap
    sector_neutral=False
)
```

### Pattern 3: Sector-Neutral Factors

```python
builder = ESGFactorBuilder(
    universe=universe,
    quantile=0.2,
    weighting="equal",
    sector_neutral=True     # Rank within sectors
)

# Need to provide sector map
factors = builder.build_factors(
    prices_df=prices_df,
    esg_df=esg_df,
    sector_map=sector_map   # Series: ticker -> sector
)
```

### Pattern 4: Different Treasury Rate

```python
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="10year"   # Use 10-year instead of 3-month
)
```

## Error Handling

### Missing Cache File

**Error:**
```
FileNotFoundError: Risk-free rate cache not found: 
data/curated/references/risk_free_rate/freq=monthly/3month_monthly.parquet
Use RiskFreeRateBuilder to fetch and save data first.
```

**Solution:**
```python
from market.risk_free_rate_manager import RiskFreeRateBuilder

builder = RiskFreeRateBuilder(
    fred_api_key="your_api_key",
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)
builder.build_and_save("2000-01-01", "2024-12-31", rate_type="3month")
```

### Missing Data in Date Range

**Error:**
```
ValueError: No data in requested date range [2025-01-01, 2025-12-31] in cache.
Use RiskFreeRateBuilder to fetch data for this date range.
```

**Solution:**
```python
# Fetch additional data
builder.build_and_save(
    start_date="2025-01-01",
    end_date="2025-12-31",
    rate_type="3month",
    merge_existing=True  # Merge with existing cache
)
```

## Output

### Factor Returns DataFrame

```python
factors = builder.build_factors(...)
print(factors.head())
```

```
            ESG_factor  E_factor  S_factor  G_factor  ESG_mom_factor
date                                                                
2007-01-31    0.023456  0.018234  0.015678  0.019876      0.012345
2007-02-28   -0.015234  0.002345 -0.008765  0.005432     -0.003456
2007-03-31    0.031234  0.025678  0.018901  0.022345      0.015678
...
```

### Factor Leg Returns (if `save_legs=True`)

```python
legs = builder.load_factor_legs()
print(legs.head())
```

```
            ESG_long  ESG_short  ESG_factor  E_long  E_short  E_factor  ...
date                                                                     
2007-01-31  0.045123   0.021667    0.023456  0.038234  0.020000  0.018234  ...
2007-02-28  0.012345   0.027579   -0.015234  0.022345  0.020000  0.002345  ...
...
```

## Validation

### Check Loaded RF Data

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.INFO)

builder = ESGFactorBuilder(universe=universe)
factors = builder.build_factors(prices_df=prices_df, esg_df=esg_df)
```

**Expected Log Output:**
```
INFO: Loading risk-free rate for 2006-12-31 to 2024-11-30
INFO: Loaded 216 risk-free rate observations
INFO: Risk-free rate (FRED annual %): mean=2.5000%, std=2.1000%
INFO: Converted RF from annual % to monthly decimal (÷100÷12)
INFO: Risk-free rate after normalization: mean=0.002083, std=0.001750
```

### Verify Normalization

RF should be:
- **Before:** Annual percentage (e.g., 2.5% = 2.5)
- **After:** Monthly decimal (e.g., 0.002083 = 0.2083%)

**Validation checks:**
- Mean monthly RF: 0.001 - 0.01 (0.1% - 1%)
- If mean > 0.10 (10% monthly) → ERROR
- If mean < 0 (negative rates) → WARNING

## Tips

1. **Cache Once, Use Many Times**: Pre-cache 20+ years of RF data once, reuse across all analyses
2. **Check Cache Location**: `data/curated/references/risk_free_rate/freq=monthly/`
3. **Update Monthly**: Refresh cache monthly to get latest rates
4. **Match Rate to Horizon**: 3-month for short-term, 10-year for long-term strategies
5. **Monitor Logs**: Check normalization output to catch data issues early

## References

- [Risk-Free Rate Refactoring](RISK_FREE_RATE_REFACTORING.md) - Architecture details
- [ESG Factor Builder Simplification](ESG_FACTOR_BUILDER_SIMPLIFICATION.md) - Full migration guide
