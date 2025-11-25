# Market Beta and Alpha Calculator

## Overview

The `MarketBetaManager` calculates market beta (β) and Jensen's alpha (α) using 60-month rolling window OLS regression. This provides insight into a stock's volatility relative to the market and its risk-adjusted performance.

## Market Model

```
R_i,t = α_i + β_i * R_m,t + ε_i,t
```

Where:
- **R_i,t**: Stock return at time t
- **R_m,t**: Market return (SPY) at time t  
- **β_i**: Market beta (sensitivity to market movements)
- **α_i**: Jensen's alpha (excess return after adjusting for market risk)
- **ε_i,t**: Idiosyncratic return (error term)

## Beta Interpretation

| Beta | Interpretation |
|------|----------------|
| β > 1 | Stock is MORE volatile than the market (aggressive) |
| β = 1 | Stock moves WITH the market (neutral) |
| β < 1 | Stock is LESS volatile than the market (defensive) |
| β < 0 | Stock moves INVERSELY to the market (rare) |

## Alpha Interpretation

| Alpha | Interpretation |
|-------|----------------|
| α > 0 | Outperforming market-adjusted expectations |
| α = 0 | Performing as expected given its beta |
| α < 0 | Underperforming market-adjusted expectations |

## Quick Start

### Calculate Beta for Single Ticker

```python
from core.config import Config
from market import MarketBetaManager
from universe import SP500Universe

config = Config("config/settings.yaml")
sp500_universe = SP500Universe(config.get("storage.local.root_path"))

# Initialize with 60-month window (default)
beta_manager = MarketBetaManager(
    universe=sp500_universe,
    window_months=60,        # Rolling window size
    min_observations=36,      # Minimum obs required
    market_ticker="SPY"       # Benchmark ticker
)

# Calculate and save beta
beta_df = beta_manager.calculate_beta("AAPL", save=True)

# Display latest estimate
if beta_df is not None:
    latest = beta_df.iloc[-1]
    print(f"Beta: {latest['beta']:.4f}")
    print(f"Alpha: {latest['alpha']:.4f} ({latest['alpha']*100:.2f}% annualized)")
    print(f"R²: {latest['r_squared']:.4f}")
```

### Calculate Beta for Multiple Tickers

```python
# Calculate for specific tickers
tickers = ["AAPL", "MSFT", "GOOGL", "TSLA"]
results = {}

for ticker in tickers:
    beta_df = beta_manager.calculate_beta(ticker, save=True)
    if beta_df is not None:
        results[ticker] = beta_df

# Calculate for all universe members
results = beta_manager.calculate_universe_betas(
    start_date="2014-01-01",
    end_date="2024-12-31"
)
```

### Load Saved Beta Results

```python
# Load previously calculated beta
beta_df = beta_manager.load_beta("AAPL")

if beta_df is not None:
    print(f"Loaded {len(beta_df)} beta estimates")
    print(f"Period: {beta_df['date'].min()} to {beta_df['date'].max()}")
```

## Output Schema

The beta results DataFrame contains:

| Column | Description |
|--------|-------------|
| `date` | End date of rolling window |
| `beta` | Market beta coefficient |
| `alpha` | Jensen's alpha (annualized) |
| `r_squared` | R² of regression (explanatory power) |
| `std_error_beta` | Standard error of beta estimate |
| `std_error_alpha` | Standard error of alpha estimate |
| `t_stat_beta` | t-statistic for beta |
| `t_stat_alpha` | t-statistic for alpha |
| `p_value_beta` | p-value for beta significance |
| `p_value_alpha` | p-value for alpha significance |
| `observations` | Number of observations in window |
| `correlation` | Correlation between stock and market |

## File Structure

### Input Data

**Ticker Prices (Monthly):**
```
data/curated/tickers/exchange=us/ticker=SYMBOL/prices/freq=monthly/year=*/part-000.parquet
```

**Market Benchmark (SPY Monthly):**
```
data/curated/references/ticker=SPY/prices/freq=monthly/year=*/part-000.parquet
```

### Output Data

**Beta Results:**
```
data/curated/tickers/exchange=us/ticker=SYMBOL/results/betas/market_beta.parquet
```

## Demo Programs

### Single Ticker

```bash
python tests/demo_market_beta.py AAPL
```

Output includes:
- Beta statistics (mean, median, std dev, min, max)
- Alpha statistics (annualized)
- R² and correlation
- Recent 12-month estimates
- Latest interpretation

### Multiple Tickers

```bash
python tests/demo_market_beta.py AAPL MSFT GOOGL TSLA
```

Shows comparison table with latest estimates for all tickers.

### All Continuous ESG Tickers

```bash
python tests/demo_market_beta.py --all-continuous
```

Calculates beta for all 427 tickers with continuous ESG data.

## Configuration Options

### Window Size

```python
# Standard: 60 months (5 years)
beta_manager = MarketBetaManager(universe=sp500_universe, window_months=60)

# Shorter window: 36 months (3 years) - more responsive
beta_manager = MarketBetaManager(universe=sp500_universe, window_months=36)

# Longer window: 120 months (10 years) - more stable
beta_manager = MarketBetaManager(universe=sp500_universe, window_months=120)
```

### Minimum Observations

```python
# Require at least 36 months of data
beta_manager = MarketBetaManager(
    universe=sp500_universe,
    min_observations=36
)
```

### Alternative Benchmark

```python
# Use different market benchmark
beta_manager = MarketBetaManager(
    universe=sp500_universe,
    market_ticker="SPY"  # Default: SPY
)
```

## Statistical Details

### OLS Regression

The manager uses Ordinary Least Squares (OLS) regression to estimate:

```
y = α + β*X + ε
```

Where:
- y = Stock returns
- X = Market returns
- α = Alpha (intercept)
- β = Beta (slope)

### Standard Errors

Standard errors are calculated using:

```
Var(β) = MSE * (X'X)^(-1)
```

Where MSE = Sum of squared residuals / (n - k)

### Significance Tests

- **t-statistic**: Coefficient / Standard Error
- **p-value**: Two-tailed test using t-distribution with (n-2) degrees of freedom
- **Significance levels**:
  - *** p < 0.01 (highly significant)
  - ** p < 0.05 (significant)
  - * p < 0.10 (marginally significant)

### R-Squared

Proportion of variance in stock returns explained by market returns:

```
R² = 1 - (SS_residual / SS_total)
```

Higher R² indicates stronger relationship with market.

## Example Output

```
MARKET BETA CALCULATION: AAPL
================================================================================

Beta Statistics:
  Mean:           1.2327
  Median:         1.2373
  Std Dev:        0.0557
  Min:            1.0835
  Max:            1.3653

Alpha (Annualized):
  Mean:           0.1327 (13.27%)
  Median:         0.1453 (14.53%)

R-Squared:
  Mean:           0.4569
  Median:         0.4429

Latest Estimate:
  Date:           2025-11-28
  Beta:           1.0887  → Moves WITH the market
  Alpha:          0.0422  → Outperforming by 4.22% per year
  R²:             0.4500  → 45.0% variance explained
```

## Best Practices

1. **Use 60-month window** for stable beta estimates (industry standard)
2. **Require minimum 36 observations** to ensure statistical validity
3. **Check p-values** to verify statistical significance
4. **Examine R²** to understand explanatory power
5. **Monitor changes over time** - beta is not constant
6. **Compare with sector** - interpret beta relative to industry peers
7. **Consider fundamentals** - beta alone doesn't capture all risk

## Integration with ESG Analysis

Combine with ESG factors for comprehensive analysis:

```python
from market import MarketBetaManager, ESGFactorBuilder

# Load beta
beta_df = beta_manager.load_beta("AAPL")

# Load ESG factors
esg_manager = ESGFactorBuilder(universe=sp500_universe)
esg_df = esg_manager.load_esg_factor("AAPL")

# Merge on date
combined = pd.merge(beta_df, esg_df, on="date", how="inner")

# Analyze relationship between ESG and beta
correlation = combined[["beta", "esg_composite"]].corr()
```

## Troubleshooting

### No Beta Results

**Issue**: `calculate_beta()` returns None

**Solutions**:
- Check if monthly price data exists for ticker
- Verify SPY benchmark data is available
- Ensure at least 36 months of overlapping data
- Check logs for specific errors

### Insufficient Data

**Issue**: Too few observations for calculation

**Solutions**:
- Lower `min_observations` parameter (minimum: 24)
- Use shorter `window_months` (minimum: 24)
- Check data coverage for ticker

### High Standard Errors

**Issue**: Large standard errors on beta/alpha

**Solutions**:
- Increase window size for more observations
- Check for outliers or data quality issues
- Consider if stock characteristics are changing

## References

- **Beta**: Sharpe, W. F. (1964). Capital asset prices: A theory of market equilibrium under conditions of risk.
- **Jensen's Alpha**: Jensen, M. C. (1968). The performance of mutual funds in the period 1945–1964.
- **Rolling Windows**: Industry standard for time-varying risk estimation

## See Also

- [ESG Factor Builder](ESG_FACTOR_BUILDER.md) - ESG factor construction
- [Risk-Free Rate](RISK_FREE_RATE_IMPLEMENTATION.md) - Treasury rate data
- [Price Manager](../src/market/price_manager.py) - Price data management
