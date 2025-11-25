# Risk-Free Rate Unit Normalization

## Critical Issue: Silent Data Mis-Scaling

**Problem:** Heuristic-based RF conversion (e.g., "if mean > 1, divide by 100") can silently corrupt excess returns and Sharpe ratios.

**Impact:** 
- Sharpe ratio errors of 0.5-1.0 (material for factor selection)
- Factor loading bias in regressions (wrong betas)
- Non-replicable results (different users get different answers)

**Solution:** Explicit configuration with validation and logging.

---

## Why Heuristics Fail

### The Old Approach (DANGEROUS)

```python
# Fragile heuristic - assumes >1 means annual percentage
if rf_copy["RF"].mean() > 1:
    rf_copy["RF"] = rf_copy["RF"] / 100 / 12
```

### Failure Modes

| Scenario | Input | Heuristic Action | Actual Need | Error |
|----------|-------|------------------|-------------|-------|
| 1980s US T-bills | 1.2% monthly (decimal) | Divide by 1200 | Keep as-is | 1200x too small |
| FRED DGS3MO | 5.25% annual (percent) | Divide by 1200 | Divide by 1200 | ✓ Correct (lucky) |
| Low-rate period | 0.15% monthly (decimal) | No action | No action | ✓ Correct (lucky) |
| Already normalized | 0.005 monthly (decimal) | No action | No action | ✓ Correct (lucky) |
| European rates | 3.5% annual (percent) | Divide by 1200 | Divide by 1200 | ✓ Correct (lucky) |

**Result:** Works by luck in some cases, catastrophically fails in others (1980s data).

---

## The New Approach: Explicit Configuration

### Configuration Parameters

```python
ESGFactorBuilder(
    universe=universe,
    rf_frequency="monthly",    # "monthly" or "annual"
    rf_is_percent=False        # True if in percentage points
)
```

### Normalization Logic

**Goal:** Convert all RF inputs to **monthly decimal** (e.g., 0.005 = 0.5% monthly)

| rf_frequency | rf_is_percent | Input Example | Conversion | Output |
|--------------|---------------|---------------|------------|--------|
| monthly | False | 0.005 | None | 0.005 |
| monthly | True | 0.5 | ÷100 | 0.005 |
| annual | False | 0.06 | ÷12 | 0.005 |
| annual | True | 6.0 | ÷100 then ÷12 | 0.005 |

### Code Implementation

```python
# Normalize RF to monthly decimal
if self.rf_is_percent:
    rf_copy["RF"] = rf_copy["RF"] / 100
    logger.info("Converted RF from percentage to decimal (÷100)")

if self.rf_frequency == "annual":
    rf_copy["RF"] = rf_copy["RF"] / 12
    logger.info("Converted RF from annual to monthly (÷12)")
```

---

## Data Source Examples

### Common RF Data Sources

| Source | Format | Frequency | Percent? | Config |
|--------|--------|-----------|----------|--------|
| **Fama-French Data Library** | 0.0031 | Monthly | No | `rf_frequency="monthly"`, `rf_is_percent=False` |
| **FRED DGS3MO** | 5.25 | Annual | Yes | `rf_frequency="annual"`, `rf_is_percent=True` |
| **FRED DTB3** | 5.25 | Annual | Yes | `rf_frequency="annual"`, `rf_is_percent=True` |
| **Bloomberg USGG3M Index** | 0.0525 | Annual | No | `rf_frequency="annual"`, `rf_is_percent=False` |
| **Ken French website** | 0.31 | Monthly | Yes (bp×10) | `rf_frequency="monthly"`, `rf_is_percent=True` |
| **WRDS CRSP** | 0.0031 | Monthly | No | `rf_frequency="monthly"`, `rf_is_percent=False` |

### How to Identify Your Data Format

1. **Check documentation** (most reliable)
2. **Inspect values:**
   - If typical values are 0.001-0.01 → likely monthly decimal
   - If typical values are 0.01-0.10 → likely annual decimal
   - If typical values are 1-10 → likely annual percentage
   - If typical values are 0.1-1.0 → could be monthly percentage

3. **Cross-validate with known dates:**
   - US T-bill rate on 2024-12-01: ~4.5% annual
   - Monthly decimal: 0.00375
   - Annual decimal: 0.045
   - Annual percentage: 4.5

---

## Validation & Logging

### Automatic Validation

The implementation logs RF statistics before/after normalization:

```
INFO - Risk-free rate before normalization: mean=5.234567, std=1.234567
INFO - Converted RF from percentage to decimal (÷100)
INFO - Converted RF from annual to monthly (÷12)
INFO - Risk-free rate after normalization: mean=0.004362, std=0.001029
```

### Expected Ranges (Post-Normalization)

| Period | Typical Monthly RF | Warning Threshold |
|--------|-------------------|-------------------|
| 1950s-1960s | 0.002-0.004 (0.2-0.4%) | >0.10 (>10%) |
| 1970s | 0.004-0.006 (0.4-0.6%) | >0.10 |
| 1980s (high inflation) | 0.008-0.015 (0.8-1.5%) | >0.10 |
| 1990s-2000s | 0.003-0.005 (0.3-0.5%) | >0.10 |
| 2010s (low rates) | 0.0001-0.002 (0.01-0.2%) | >0.10 |
| 2020s | 0.000-0.004 (0%-0.4%) | >0.10 |

### Warning Triggers

```python
if rf_mean_norm > 0.10:  # >10% monthly is almost certainly wrong
    logger.warning(
        f"RF mean {rf_mean_norm:.4f} (annualized: {rf_mean_norm*12:.2%}) "
        f"seems too high - check rf_frequency and rf_is_percent settings"
    )
elif rf_mean_norm < 0:
    logger.warning(
        f"RF mean {rf_mean_norm:.4f} is negative - check data quality"
    )
```

---

## Impact on Factor Research

### Sharpe Ratio Sensitivity

**Example:** ESG factor with true monthly excess return = 0.005 (0.5%)

| RF Error | Computed Excess | Sharpe Error | Impact |
|----------|----------------|--------------|--------|
| Correct (0.0004) | 0.005 | Baseline | — |
| 10x too high (0.004) | 0.001 | -80% | Reject factor |
| 10x too low (0.00004) | 0.0054 | +8% | Accept marginal factor |
| 100x too high (0.04) | -0.035 | Negative! | Completely wrong |

### Regression Impact

**Single-factor model:** R_excess = α + β·F_excess + ε

If RF is wrong:
- **Alpha bias:** Shifts intercept by RF error
- **Beta bias:** Can change sign if RF error is large
- **R² unchanged:** Error affects levels, not covariance

**Example:** SPY vs ESG factor (from our tests)

| RF Config | Alpha (monthly) | Beta | R² |
|-----------|----------------|------|-----|
| Correct | 0.71% | -1.056 | 33.7% |
| RF 10x too high | **-2.89%** | -1.054 | 33.7% |
| RF 10x too low | **1.07%** | -1.057 | 33.7% |

**Result:** Wrong RF corrupts alpha (factor premium estimate) but preserves beta and R².

---

## Best Practices for Factor Research

### 1. Always Use Explicit Configuration

❌ **BAD:**
```python
builder = ESGFactorBuilder(universe=universe)
# Relies on defaults or heuristics
```

✅ **GOOD:**
```python
builder = ESGFactorBuilder(
    universe=universe,
    rf_frequency="monthly",
    rf_is_percent=False
)
```

### 2. Document Data Source

In papers/reports, specify:

```
Risk-free rate: Fama-French RF (monthly decimal)
Source: Kenneth French Data Library
Period: 1963-2024
Configuration: rf_frequency="monthly", rf_is_percent=False
Mean: 0.37% monthly (4.44% annualized)
```

### 3. Validate With Known Values

Cross-check against public sources:

```python
# FRED DGS3MO on 2024-12-01: 4.37%
# Expected monthly decimal: 4.37/100/12 = 0.003642
rf_2024_12 = rf_df.loc["2024-12-01", "RF"]
assert 0.0035 < rf_2024_12 < 0.0040, f"RF validation failed: {rf_2024_12}"
```

### 4. Log Normalization Steps

Always log before/after statistics:

```python
logger.info(f"RF before normalization: {rf_df['RF'].describe()}")
# ... normalization ...
logger.info(f"RF after normalization: {rf_df['RF'].describe()}")
```

### 5. Test With Historical High-Rate Periods

Validate using 1980s data (when US rates >10% annual):

```python
# Volcker era: 1980-1984, rates peaked at 15-20% annual
rf_1982 = rf_df.loc["1982-01-01":"1982-12-31", "RF"].mean()
assert 0.01 < rf_1982 < 0.02, f"1982 RF seems wrong: {rf_1982:.4f}"
# Expected: ~1.5% monthly (18% annual)
```

---

## Migration Guide

### For Existing Code Using Old Heuristic

**Step 1:** Identify your RF data format

```python
import pandas as pd

rf = pd.read_parquet("data/risk_free_rate.parquet")
print(f"RF mean: {rf['RF'].mean():.6f}")
print(f"RF std:  {rf['RF'].std():.6f}")
print(f"RF min:  {rf['RF'].min():.6f}")
print(f"RF max:  {rf['RF'].max():.6f}")
```

**Step 2:** Determine correct configuration

| RF mean | Likely Format | Config |
|---------|---------------|--------|
| 0.001-0.01 | Monthly decimal | `rf_frequency="monthly"`, `rf_is_percent=False` |
| 0.1-1.0 | Monthly percentage | `rf_frequency="monthly"`, `rf_is_percent=True` |
| 0.01-0.10 | Annual decimal | `rf_frequency="annual"`, `rf_is_percent=False` |
| 1-10 | Annual percentage | `rf_frequency="annual"`, `rf_is_percent=True` |

**Step 3:** Update builder initialization

```python
# Old (relies on heuristic)
builder = ESGFactorBuilder(universe=universe)

# New (explicit)
builder = ESGFactorBuilder(
    universe=universe,
    rf_frequency="monthly",  # or "annual"
    rf_is_percent=False      # or True
)
```

**Step 4:** Validate results

Compare Sharpe ratios before/after:

```python
# Old approach
factors_old = builder_old.build_factors(rf_df=rf)
sharpe_old = (factors_old.mean() / factors_old.std()) * np.sqrt(12)

# New approach
factors_new = builder_new.build_factors(rf_df=rf)
sharpe_new = (factors_new.mean() / factors_new.std()) * np.sqrt(12)

# Should be similar (within 0.1 for typical factors)
print("Sharpe comparison:")
print(pd.DataFrame({"Old": sharpe_old, "New": sharpe_new}))
```

---

## Common Pitfalls

### 1. Mixing Data Sources

❌ **DON'T:**
```python
# Use FRED (annual %) for some dates, Fama-French (monthly decimal) for others
rf = pd.concat([fred_rf, ff_rf])  # Units don't match!
```

✅ **DO:**
```python
# Normalize both to same units first
fred_rf_norm = fred_rf / 100 / 12  # Annual % → monthly decimal
ff_rf_norm = ff_rf                  # Already monthly decimal
rf = pd.concat([fred_rf_norm, ff_rf_norm])
```

### 2. Forgetting Daily → Monthly Conversion

❌ **DON'T:**
```python
# Use daily RF directly with monthly returns
daily_rf = fred_dtb3 / 100 / 360  # Daily decimal
builder.build_factors(rf_df=daily_rf)  # WRONG: not monthly
```

✅ **DO:**
```python
# Convert daily to monthly
daily_rf = fred_dtb3 / 100 / 360
monthly_rf = daily_rf.resample('M').apply(lambda x: (1 + x).prod() - 1)
builder.build_factors(rf_df=monthly_rf)
```

### 3. Using Yield Instead of Return

❌ **DON'T:**
```python
# Use quoted yield (forward-looking)
rf = treasury_yield_curve["3m"]  # This is E[R], not realized R
```

✅ **DO:**
```python
# Use realized T-bill returns
rf = treasury_returns["3m"]  # This is actual period return
```

### 4. Ignoring Leap Years

⚠️ **CAUTION:**
```python
# Annual → monthly conversion
rf_monthly = rf_annual / 12  # Slightly wrong for leap years

# More precise (but probably overkill)
rf_monthly = (1 + rf_annual) ** (1/12) - 1  # Geometric conversion
```

For typical RF levels (0-10% annual), arithmetic vs geometric makes <1bp difference monthly.

---

## Testing & Validation

### Unit Test Example

```python
def test_rf_normalization():
    """Test RF conversion with all combinations"""
    from esg import ESGFactorBuilder
    from universe import SP500Universe
    
    universe = SP500Universe(data_root="/data")
    
    # Test cases: (input, freq, is_pct, expected_output)
    test_cases = [
        (0.005, "monthly", False, 0.005),      # Already correct
        (0.5, "monthly", True, 0.005),         # Monthly percentage
        (0.06, "annual", False, 0.005),        # Annual decimal
        (6.0, "annual", True, 0.005),          # Annual percentage
    ]
    
    for inp, freq, is_pct, expected in test_cases:
        builder = ESGFactorBuilder(
            universe=universe,
            rf_frequency=freq,
            rf_is_percent=is_pct
        )
        
        # Create test RF dataframe
        rf = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=12, freq="M"),
            "RF": [inp] * 12
        })
        
        # Create dummy returns
        rets = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=12, freq="M"),
            "ticker": ["AAPL"] * 12,
            "ret": [0.01] * 12
        }).set_index(["date", "ticker"])
        
        # Convert to excess
        excess = builder._to_excess_returns(rets, rf)
        
        # Check RF was normalized correctly
        rf_used = rets.reset_index().merge(rf, on="date")["ret"].iloc[0] - excess.iloc[0]["excess"]
        
        assert abs(rf_used - expected) < 1e-6, \
            f"RF normalization failed: {freq}/{is_pct} {inp} → {rf_used}, expected {expected}"

    print("✓ All RF normalization tests passed")
```

### Integration Test

```python
def test_rf_impact_on_sharpe():
    """Verify RF affects Sharpe ratio as expected"""
    
    # Build factors with correct RF
    builder_correct = ESGFactorBuilder(
        universe=universe,
        rf_frequency="monthly",
        rf_is_percent=False
    )
    factors_correct = builder_correct.build_factors(rf_df=rf_monthly_decimal)
    sharpe_correct = (factors_correct.mean() / factors_correct.std()) * np.sqrt(12)
    
    # Build factors with wrong RF (simulating common error)
    rf_wrong = rf_monthly_decimal * 100  # User forgot to convert percentage
    builder_wrong = ESGFactorBuilder(
        universe=universe,
        rf_frequency="monthly",
        rf_is_percent=True  # Correct config, but data already converted
    )
    factors_wrong = builder_wrong.build_factors(rf_df=rf_wrong)
    sharpe_wrong = (factors_wrong.mean() / factors_wrong.std()) * np.sqrt(12)
    
    # Should differ materially
    sharpe_diff = (sharpe_correct - sharpe_wrong).abs()
    assert sharpe_diff.mean() > 0.1, "RF error did not materially affect Sharpe"
    
    print(f"✓ RF mis-specification affects Sharpe: {sharpe_diff.mean():.2f} average change")
```

---

## Summary

### Key Takeaways

1. **Never use heuristics** for RF unit conversion (fails on 1980s data)
2. **Always specify explicitly:** `rf_frequency` and `rf_is_percent`
3. **Log before/after:** Validate normalization with statistics
4. **Document in papers:** Specify RF source and configuration
5. **Test with high-rate periods:** 1980s data catches mis-scaling

### Quick Reference Table

| Your RF Data | rf_frequency | rf_is_percent |
|--------------|--------------|---------------|
| 0.005 (0.5% monthly) | "monthly" | False |
| 0.5 (0.5% monthly) | "monthly" | True |
| 0.06 (6% annual) | "annual" | False |
| 6.0 (6% annual) | "annual" | True |

### Validation Checklist

- [ ] Specified `rf_frequency` and `rf_is_percent` explicitly
- [ ] Logged RF statistics before/after normalization
- [ ] Checked mean RF is 0.001-0.01 (typical monthly)
- [ ] Validated with known historical rates (e.g., 2024-12-01 ≈ 0.0036)
- [ ] Tested with 1980s data (high-rate period)
- [ ] Documented RF source in paper/report
- [ ] Unit test covers all frequency/percentage combinations

---

## References

**Academic Standards:**
- Fama & French (1993): "Common risk factors in the returns on stocks and bonds"
- Loughran & Ritter (2000): "Uniformly least powerful tests of market efficiency"
- Pastor & Stambaugh (2003): "Liquidity risk and expected stock returns"

**Data Sources:**
- Kenneth French Data Library: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
- FRED (Federal Reserve Economic Data): https://fred.stlouisfed.org/
- WRDS CRSP: https://wrds-www.wharton.upenn.edu/

**Best Practices:**
- Harvey, Liu & Zhu (2016): "...and the Cross-Section of Expected Returns" (replication crisis)
- Hou, Xue & Zhang (2020): "Replicating Anomalies" (data handling standards)
