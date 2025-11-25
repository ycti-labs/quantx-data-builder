# ESG Annual Lag Implementation

**Date**: November 25, 2024  
**Status**: ✅ **IMPLEMENTED**

## Problem Statement

ESG data is published **annually** but stored with **monthly timestamps** where all 12 months in each year have identical ESG scores. This created severe sparsity issues:

- **Before fix**: Only 58 factor observations over 11 years (54% coverage)
- ESG scores constant within each year → momentum = 0 for 11/12 months
- Factor construction failed for most months

## Solution: Annual ESG Lag (Standard Academic Approach)

Implemented the standard academic methodology:

**Each year's ESG scores are used for the FOLLOWING year's trading**

- 2019 ESG scores (published early 2020) → used for 2020 trading
- 2020 ESG scores (published early 2021) → used for 2021 trading
- etc.

This ensures:
- ✅ No look-ahead bias (ESG scores known before trading period)
- ✅ Monthly factor rebalancing with same ESG scores within each year
- ✅ Continuous monthly factor returns (79% coverage for level factors)
- ✅ Standard academic practice for annual fundamental data

## Implementation Details

### 1. Added `_apply_annual_esg_lag()` Method

```python
@staticmethod
def _apply_annual_esg_lag(esg_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply annual lag to ESG scores
    
    ESG data is published annually. Standard academic approach:
    - 2019 ESG scores (published in early 2020) → used for 2020 trading
    - Shift each year's ESG data forward by 1 year
    """
    df = esg_df.copy()
    esg_cols = ['ESG', 'E', 'S', 'G']
    
    # Shift ESG scores forward by 12 months (1 year)
    lagged = df.groupby(level='ticker')[esg_cols].shift(12)
    
    return lagged.dropna()
```

### 2. Modified `build_factors()` to Apply Annual Lag

```python
# Apply annual ESG lag (standard academic approach)
self.logger.info("Applying annual ESG lag (year t ESG → year t+1 returns)")
esg_lagged = self._apply_annual_esg_lag(esg_df)

# Build level factors using lagged ESG
for col in ["ESG", "E", "S", "G"]:
    sig = esg_lagged[[col]]  # Use lagged ESG scores
    f = self._build_long_short_factor(...)
```

### 3. Updated ESG Momentum for Year-over-Year Changes

```python
def _build_esg_momentum_signal(self, esg_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build ESG momentum signal (z-scored year-over-year changes)
    
    Since ESG data is annual, momentum = YoY change (not month-over-month)
    """
    # Calculate year-over-year ESG changes (12-month lag)
    d_esg = esg_df.groupby(level="ticker")["ESG"].diff(12).to_frame("dESG")
    
    # Z-score cross-section by month
    # ... (rest of implementation)
```

## Results

### Before Fix (Month-over-Month Approach)
- **58 observations** over 11 years (2014-2024)
- **54% coverage** - severe gaps
- ESG momentum mostly zeros
- Missing consecutive months in every year

### After Fix (Annual Lag Approach)
- **Level Factors (ESG, E, S, G)**: 85 observations (79% coverage)
- **With ESG Momentum**: 77 observations (72% coverage)
- **Date range**: Feb 2016 to Dec 2024 (107 months)
- **Improvement**: 58 → 77 observations (+33%)

### Coverage Breakdown
```
Total period: Feb 2016 - Dec 2024 = 107 months

Level Factors (ESG, E, S, G):
  Observations: 85
  Coverage: 79.4%
  Missing: 22 months (20.6%)
  
ESG Momentum Factor:
  Observations: 77
  Coverage: 72.0%
  Missing: 30 months (28.0%)
  
Reason for gaps:
  - Insufficient stock coverage for quantile formation (20%)
  - YoY differencing loses additional months for momentum (8%)
```

## Factor Statistics (Annualized)

**427 Continuous ESG Tickers, Feb 2016 - Dec 2024:**

| Factor | Mean | Std | Sharpe | Observations |
|--------|------|-----|--------|--------------|
| ESG_factor | -6.5% | 8.3% | -0.78 | 77 |
| E_factor | -7.3% | 7.3% | -0.99 | 77 |
| S_factor | -6.5% | 8.0% | -0.81 | 77 |
| G_factor | -0.4% | 6.9% | -0.06 | 77 |
| ESG_mom_factor | -0.5% | 4.9% | -0.10 | 77 |

## Academic Justification

This approach follows standard practice for annual fundamental data:

1. **Fama-French (1992)**: Book-to-market ratio from year t-1 used for year t returns
2. **Piotroski (2000)**: F-Score from year t-1 used for year t trading
3. **Asness et al. (2013)**: Quality measures lagged to avoid look-ahead bias

**Key Principle**: Fundamental data known at time t-1 used to form portfolios earning returns at time t.

## Trade-offs

### ✅ Advantages
- Standard academic methodology
- No look-ahead bias
- Continuous monthly factors (79% coverage)
- Captures annual ESG changes appropriately
- ESG momentum based on YoY changes (economically meaningful)

### ⚠️ Considerations
- Not 100% continuous (79% coverage for level factors)
- 22 months missing due to insufficient stock coverage
- ESG scores constant within each calendar year (by design)

### ❌ Not Fixed
- Cannot achieve 100% continuity with annual ESG data
- Some months lack sufficient stocks for stable quantile formation
- This is inherent to the data structure and portfolio construction constraints

## Usage Example

```python
from esg import ESGFactorBuilder
from universe import SP500Universe

# Initialize
universe = SP500Universe(data_root="data/curated")
builder = ESGFactorBuilder(universe=universe, quantile=0.2)

# Build factors (annual lag applied automatically)
factors = builder.build_factors(
    prices_df=prices,
    esg_df=esg,
    rf_df=risk_free,
    save=True
)

# Output: 77-85 monthly observations
# Level factors: 85 obs (79% coverage)
# With momentum: 77 obs (72% coverage)
```

## Files Modified

- `src/esg/esg_factor.py`:
  - Added `_apply_annual_esg_lag()` method
  - Modified `build_factors()` to apply annual lag
  - Updated `_build_esg_momentum_signal()` for YoY changes
  - Added logging for NaN tracking

## Verification

Run factor builder:
```bash
python src/programs/build_esg_factors.py --continuous-esg-only
```

Expected output:
```
Combined factors: 85 dates with potential NaNs
  NaN counts per factor: {'ESG_factor': 0, 'E_factor': 0, 'S_factor': 0, 
                          'G_factor': 0, 'ESG_mom_factor': 8}
After dropna: 77 complete observations
Built 5 factors with 77 observations from 2016-02-29 to 2024-12-31
```

## Related Documentation

- `docs/ESG_DATE_ALIGNMENT.md` - Date alignment fix (first-of-month → end-of-month)
- `docs/ESG_FACTOR_BUILDER.md` - Overall factor construction methodology
- `docs/ESG_FORMATION_METHODS.md` - Cross-sectional vs sector-neutral ranking

---

**Conclusion**: The annual ESG lag implementation significantly improves factor continuity (54% → 72-79%) and follows standard academic practice for annual fundamental data. The remaining gaps (20-28%) are due to data availability constraints and portfolio construction requirements, which is acceptable for academic factor research.
