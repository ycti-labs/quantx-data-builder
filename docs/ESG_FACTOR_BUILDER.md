# ESG Factor Builder - Quick Reference

## Overview

The `ESGFactorBuilder` class provides a robust framework for constructing long-short factor portfolios from ESG signals. It follows the same architectural patterns as `MarketBetaManager` and integrates seamlessly with the QuantX data pipeline.

## Class Structure

```python
from universe import Universe
from esg import ESGFactorBuilder

# Initialize
universe = Universe(name="sp500", data_root="./data", exchange="us")
factor_builder = ESGFactorBuilder(
    universe=universe,
    quantile=0.2,          # Top/bottom 20% for long/short legs
    sector_neutral=False,   # Cross-sectional ranking
    lag_signal=1           # Use t-1 signal for t returns
)
```

## Key Features

### 1. **Multiple Factor Types**
- **ESG Factor**: Composite ESG score
- **E/S/G Factors**: Individual pillar scores
- **ESG Momentum Factor**: Changes in ESG scores (z-scored)

### 2. **Portfolio Construction**
- Cross-sectional or sector-neutral ranking
- Equal-weighted or value-weighted legs
- Configurable quantile cutoffs (default: 20%)
- Proper signal lagging to avoid look-ahead bias

### 3. **Excess Returns**
- Optional risk-free rate adjustment
- Monthly frequency (standard for factor research)
- Long-short portfolio returns

### 4. **Persistence**
- Save/load factor returns
- Parquet format for efficiency
- Summary statistics and diagnostics

## Main Methods

### build_factors()

Build all ESG factors from input data.

```python
factor_df = factor_builder.build_factors(
    prices_df=prices_df,      # MultiIndex [date, ticker], column 'adj_close'
    esg_df=esg_panel,        # MultiIndex [date, ticker], columns ['ESG', 'E', 'S', 'G']
    rf_df=rf_df,             # Optional: Index=date, column 'RF'
    weights_df=None,          # Optional: MultiIndex [date, ticker], column 'weight'
    sector_map=None,          # Optional: Series, index=ticker, value=sector
    save=True                 # Save to parquet
)
```

**Returns**: DataFrame indexed by date with columns:
- `ESG_factor`: Long-short portfolio based on ESG composite
- `E_factor`: Long-short portfolio based on Environmental pillar
- `S_factor`: Long-short portfolio based on Social pillar
- `G_factor`: Long-short portfolio based on Governance pillar
- `ESG_mom_factor`: Long-short portfolio based on ESG momentum

### load_factors()

Load previously saved factor returns.

```python
factor_df = factor_builder.load_factors()
```

### get_factor_summary()

Get summary statistics for factors.

```python
summary = factor_builder.get_factor_summary(factor_df)
```

**Returns**: DataFrame with:
- Mean (annualized)
- Std (annualized)
- Sharpe Ratio
- Min/Max
- Observations

## Data Structure

### Input Requirements

1. **Prices**: MultiIndex [date, ticker]
   ```python
   date        ticker
   2020-01-31  AAPL      145.23
   2020-01-31  MSFT      165.87
   2020-02-29  AAPL      148.56
   ```

2. **ESG Data**: MultiIndex [date, ticker]
   ```python
   date        ticker  ESG    E      S      G
   2020-01-31  AAPL    85.2   87.3   82.1   86.5
   2020-01-31  MSFT    88.7   90.2   87.5   88.3
   ```

3. **Risk-Free Rate** (optional): Index=date
   ```python
   date        RF
   2020-01-31  0.0016  # Monthly rate (decimal)
   2020-02-29  0.0014
   ```

### Output Structure

**File**: `data/curated/factors/esg_factors.parquet`

```python
date        ESG_factor  E_factor  S_factor  G_factor  ESG_mom_factor
2020-02-29  0.0123      0.0098    0.0145    0.0087    0.0112
2020-03-31  -0.0056     -0.0034   -0.0078   -0.0045   -0.0023
```

## Factor Construction Methodology

### Signal Processing

1. **Lagging**: Signals at t-1 used to form portfolios earning returns at t
   - Avoids look-ahead bias
   - Reflects real-world portfolio formation constraints

2. **Ranking**: Cross-sectional or sector-neutral
   - Percentile ranking (0..1)
   - Within-sector ranking if `sector_neutral=True`

3. **Portfolio Formation**:
   - Long: Top quantile (e.g., top 20%)
   - Short: Bottom quantile (e.g., bottom 20%)
   - Middle: Excluded (zero weight)

### Return Calculation

```
Factor Return_t = R_long_t - R_short_t

Where:
- R_long_t: Value-weighted (or equal-weighted) return of long leg
- R_short_t: Value-weighted (or equal-weighted) return of short leg
```

### ESG Momentum Signal

```
ESG_mom_z_t = zscore(ΔESG_t)

Where:
- ΔESG_t = ESG_t - ESG_{t-1}
- zscore: Cross-sectional standardization
```

## Usage Examples

### Example 1: Basic Usage (Equal-Weighted, Cross-Sectional)

```python
from universe import Universe
from esg import ESGFactorBuilder, ESGManager
from market import PriceManager, RiskFreeRateManager

# Initialize
universe = Universe(name="sp500", data_root="./data", exchange="us")
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Load data
esg_mgr = ESGManager(universe)
esg_panel = esg_mgr.load_esg_panel(tickers, start_date='2020-01-01')

price_mgr = PriceManager(universe)
# ... load prices as MultiIndex DataFrame ...

rf_mgr = RiskFreeRateManager(universe)
rf_df = rf_mgr.load_risk_free_rate()

# Build factors
factor_builder = ESGFactorBuilder(universe, quantile=0.2)
factor_df = factor_builder.build_factors(
    prices_df=prices_df,
    esg_df=esg_panel,
    rf_df=rf_df
)

# Get summary
summary = factor_builder.get_factor_summary()
print(summary)
```

### Example 2: Sector-Neutral, Value-Weighted

```python
# Load sector mapping
sector_map = pd.Series({
    'AAPL': 'Technology',
    'MSFT': 'Technology',
    'JPM': 'Financials',
    'JNJ': 'Healthcare',
    # ...
})

# Load market caps for weighting
# weights_df: MultiIndex [date, ticker], column 'weight'

# Build with sector-neutral ranking and value-weighting
factor_builder = ESGFactorBuilder(
    universe,
    quantile=0.2,
    sector_neutral=True
)

factor_df = factor_builder.build_factors(
    prices_df=prices_df,
    esg_df=esg_panel,
    rf_df=rf_df,
    weights_df=weights_df,
    sector_map=sector_map
)
```

### Example 3: Load Saved Factors

```python
factor_builder = ESGFactorBuilder(universe)
factor_df = factor_builder.load_factors()

if factor_df is not None:
    print(f"Loaded {len(factor_df)} months of factor returns")
    print(factor_builder.get_factor_summary())
```

## Demo Program

Run the demo to see the class in action:

```bash
# Basic usage (cross-sectional, 20% quantile)
python tests/demo_esg_factor_class.py

# Sector-neutral with custom quantile
python tests/demo_esg_factor_class.py --sector-neutral --quantile 0.3

# Custom date range
python tests/demo_esg_factor_class.py --start-date 2018-01-01 --end-date 2023-12-31
```

## Integration with Portfolio Analysis

Use the factor returns for:

1. **Factor Regressions**: Explain stock returns using ESG factors
   ```python
   import statsmodels.api as sm
   
   # Regress stock excess returns on ESG factors
   y = stock_excess_returns
   X = sm.add_constant(factor_df)
   model = sm.OLS(y, X).fit()
   print(model.summary())
   ```

2. **Factor-Mimicking Portfolios**: Use factor returns as additional risk factors
   ```python
   # Fama-French + ESG factors
   all_factors = pd.concat([ff_factors, factor_df], axis=1)
   ```

3. **ESG Alpha Attribution**: Decompose portfolio returns
   ```python
   # Portfolio return = β_ESG * ESG_factor + β_E * E_factor + ...
   ```

## Architecture Alignment

The `ESGFactorBuilder` follows the same patterns as other QuantX managers:

- **Universe Integration**: Uses `Universe` class for data root and ticker paths
- **Logging**: Structured logging with correlation IDs
- **Caching**: Internal caching for efficiency
- **Save/Load**: Parquet persistence with datetime handling
- **Type Hints**: Full type annotations for clarity
- **Documentation**: Comprehensive docstrings

## Technical Notes

### Signal Timing
- Signal at t-1 → Portfolio formed → Returns at t
- This ensures no look-ahead bias
- Reflects real-world trading constraints (end-of-month rebalancing)

### Weighting
- **Equal-weighted**: Simple average (default)
- **Value-weighted**: Weighted by market cap or custom weights
- Weights normalized to sum to 1.0 within each leg

### Risk-Free Rate
- If provided: Excess returns = Returns - RF
- If not provided: Use raw returns (less common in factor research)
- RF should be monthly frequency (decimal, not percentage)

### Data Frequency
- Monthly frequency is standard for factor research
- Higher frequency (daily) possible but increases noise
- Lower frequency (quarterly) reduces observations

## Common Issues

### Issue 1: Insufficient Observations
**Symptom**: Few or no factor returns
**Solution**: 
- Check that ESG data and prices overlap in time
- Ensure enough tickers have both ESG and price data
- Verify date alignment (month-end dates)

### Issue 2: High Factor Volatility
**Symptom**: Very large factor returns (>10% monthly)
**Solution**:
- Check for data errors (outliers, missing values)
- Consider wider quantiles (e.g., 0.3 instead of 0.2)
- Use value-weighting to reduce small-cap influence

### Issue 3: Low Factor Correlation with Market
**Symptom**: ESG factors uncorrelated with market returns
**Solution**:
- This is expected! ESG factors capture non-market risk
- Use in multi-factor models alongside market factors
- ESG factors complement rather than replace market factors

## Best Practices

1. **Data Quality**: Use continuous ESG data (no gaps)
2. **Universe**: Use consistent universe throughout analysis
3. **Frequency**: Stick with monthly for comparability
4. **Weighting**: Value-weight for institutional portfolios, equal-weight for research
5. **Sector Neutral**: Use for pure ESG exposure (removes sector tilts)
6. **Testing**: Backtest on out-of-sample data
7. **Documentation**: Save factor construction parameters for reproducibility

## References

- Fama-French Factor Models: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/
- Pastor, Stambaugh & Taylor (2021): "Sustainable Investing in Equilibrium"
- Pedersen, Fitzgibbons & Pomorski (2021): "Responsible Investing: The ESG-Efficient Frontier"

## Next Steps

After building ESG factors:

1. **Analyze Factor Properties**: Summary statistics, time-series plots
2. **Factor Regressions**: Explain stock returns using ESG factors
3. **Portfolio Construction**: Build ESG-tilted portfolios
4. **Performance Attribution**: Decompose portfolio returns
5. **Risk Analysis**: Estimate factor exposures and risk contributions
