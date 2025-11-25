# Two-Factor OLS Regression: Market + ESG

## Overview

The `two_factor_regression.py` program performs OLS regression to estimate factor exposures (betas) and alpha for stocks using a two-factor model with **Market** and **ESG** factors.

## Regression Model

```
R_i,t - RF_t = α_i + β_market * (R_market,t - RF_t) + β_ESG * ESG_factor_t + ε_i,t
```

Where:
- **R_i,t**: Stock return at time t
- **RF_t**: Risk-free rate at time t (3-month Treasury Bill)
- **R_market,t**: Market return (SPY) at time t
- **ESG_factor_t**: ESG factor return at time t (long-short portfolio)
- **α_i**: Jensen's alpha (excess return not explained by factors)
- **β_market**: Market beta (sensitivity to market movements)
- **β_ESG**: ESG beta (sensitivity to ESG factor)
- **ε_i,t**: Idiosyncratic return (error term)

## Key Features

✅ **Full Sample Regression**: Single beta estimates using all available data
✅ **Rolling Window Regression**: Time-varying beta estimates (default: 36-month window)
✅ **Excess Returns**: Automatically adjusts for risk-free rate with auto-detection
✅ **Statistical Tests**: t-statistics, p-values, R², adjusted R², F-statistic
✅ **Batch Processing**: Run for single ticker, multiple tickers, or entire universe
✅ **Auto-save Results**: Saves to parquet for further analysis

## Quick Start

### 1. Single Stock (Full Sample)

```bash
python src/programs/two_factor_regression.py \
    --ticker AAPL \
    --start-date 2014-01-01 \
    --end-date 2024-12-31
```

**Output:**
```
================================================================================
TWO-FACTOR REGRESSION RESULTS: AAPL
================================================================================

Period: 2024-12-31 00:00:00
Observations: 77

Factor Exposures:
  Alpha (annualized):    0.2805  ( 28.05%)
    t-statistic:           3.17
    p-value:             0.0023  ***

  Market Beta:           1.1863
    t-statistic:           7.63
    p-value:             0.0000  ***

  ESG Beta:              0.1562
    t-statistic:           0.38
    p-value:             0.7049  

Model Fit:
  R²:                    0.4730  (47.30%)
  Adjusted R²:           0.4588  (45.88%)
  F-statistic:            33.21
  F p-value:             0.0000  ***
```

**Interpretation:**
- **Alpha = 28.05%/year**: AAPL outperforms by 28% annually after adjusting for market and ESG risk (highly significant)
- **Market Beta = 1.19**: AAPL is 19% more volatile than the market (moves 1.19% for every 1% market move)
- **ESG Beta = 0.16**: Weak positive exposure to ESG factor (not statistically significant)
- **R² = 47.3%**: Market and ESG factors explain 47% of AAPL's return variance

### 2. Multiple Stocks

```bash
python src/programs/two_factor_regression.py \
    --tickers AAPL MSFT GOOGL TSLA \
    --start-date 2014-01-01 \
    --end-date 2024-12-31
```

### 3. Rolling Window Regression (Time-Varying Betas)

```bash
python src/programs/two_factor_regression.py \
    --ticker AAPL \
    --start-date 2014-01-01 \
    --end-date 2024-12-31 \
    --rolling \
    --window 36
```

**Output:**
```
================================================================================
TWO-FACTOR REGRESSION RESULTS: AAPL
================================================================================

Rolling Window: 42 periods
Period: 2020-03-31 00:00:00 to 2024-12-31 00:00:00

Rolling Statistics:
                 Mean    Median   Std Dev       Min       Max
alpha        0.302767  0.318392  0.064044  0.103359  0.419191
beta_market  1.196193  1.209725  0.089833  1.024438  1.346073
beta_esg     0.092846  0.053275  0.226391 -0.241866  0.619923
r_squared    0.503666  0.514203  0.108583  0.294474  0.651757

Latest Estimate (2024-12-31):
  Alpha:          0.1435  ( 14.35%/year)
  Market Beta:    1.0244
  ESG Beta:      -0.0442
  R²:             0.5485  (54.85%)
```

**Interpretation:**
- **Time-varying betas**: Market beta ranges from 1.02 to 1.35 over rolling windows
- **ESG beta volatility**: ESG exposure fluctuates from -0.24 to +0.62, suggesting time-varying ESG risk
- **Latest estimate**: As of Dec 2024, AAPL has market beta of 1.02 and negative ESG beta (-0.04)

### 4. Universe-Wide Analysis (Continuous ESG Data Only)

```bash
python src/programs/two_factor_regression.py \
    --universe SP500 \
    --continuous-esg-only \
    --start-date 2014-01-01 \
    --end-date 2024-12-31
```

## Command-Line Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--ticker` | str | - | Single stock ticker symbol |
| `--tickers` | list | - | Multiple stock ticker symbols |
| `--universe` | str | SP500 | Universe for batch processing |
| `--continuous-esg-only` | flag | False | Only process tickers with continuous ESG data |
| `--start-date` | str | 2014-01-01 | Start date (YYYY-MM-DD) |
| `--end-date` | str | 2024-12-31 | End date (YYYY-MM-DD) |
| `--esg-factor` | str | ESG_factor | ESG factor column name |
| `--rolling` | flag | False | Use rolling window regression |
| `--window` | int | 60 | Rolling window size in months |
| `--min-obs` | int | 36 | Minimum observations required |
| `--rf-frequency` | str | monthly | Risk-free rate frequency (monthly/annual) |
| `--rf-is-percent` | flag | False | RF is in percentage points (auto-detected) |
| `--save` / `--no-save` | flag | True | Save results to parquet |

## Output Structure

### Saved Files

```
data/results/two_factor_regression/
└── ticker=AAPL/
    └── two_factor_regression.parquet
```

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| ticker | string | Stock ticker symbol |
| date | datetime | End date of regression period/window |
| alpha | float | Annualized Jensen's alpha (intercept × 12) |
| beta_market | float | Market beta coefficient |
| beta_esg | float | ESG beta coefficient |
| alpha_tstat | float | t-statistic for alpha |
| beta_market_tstat | float | t-statistic for market beta |
| beta_esg_tstat | float | t-statistic for ESG beta |
| alpha_pvalue | float | p-value for alpha significance |
| beta_market_pvalue | float | p-value for market beta significance |
| beta_esg_pvalue | float | p-value for ESG beta significance |
| r_squared | float | R² (proportion of variance explained) |
| adj_r_squared | float | Adjusted R² (penalized for # of factors) |
| f_statistic | float | F-statistic for overall model significance |
| f_pvalue | float | p-value for F-statistic |
| observations | int | Number of observations in regression |
| std_error_alpha | float | Standard error of alpha estimate |
| std_error_beta_market | float | Standard error of market beta |
| std_error_beta_esg | float | Standard error of ESG beta |

## Interpretation Guide

### Alpha (α)

- **α > 0**: Stock outperforms after adjusting for market and ESG risk
- **α = 0**: Stock performs as expected given its factor exposures
- **α < 0**: Stock underperforms after adjusting for factors
- **Significance**: p-value < 0.05 indicates genuine outperformance (not luck)

### Market Beta (β_market)

- **β > 1**: Stock is more volatile than the market (aggressive)
- **β = 1**: Stock moves with the market
- **β < 1**: Stock is less volatile than the market (defensive)
- **β < 0**: Stock moves inversely to the market (rare, e.g., gold stocks)

### ESG Beta (β_ESG)

- **β > 0**: Stock has positive exposure to ESG factor (benefits from high ESG scores)
- **β = 0**: Stock is neutral to ESG factor
- **β < 0**: Stock has negative exposure to ESG factor (benefits from low ESG scores)
- **Interpretation**: A high ESG beta means the stock co-moves with the ESG long-short portfolio

### R² and Adjusted R²

- **R²**: Proportion of stock return variance explained by market and ESG factors
- **Adjusted R²**: R² adjusted for number of factors (penalizes overfitting)
- **Good fit**: R² > 0.40 (40% of variance explained)
- **Excellent fit**: R² > 0.70 (70% of variance explained)

### F-statistic

- Tests whether the model (both factors together) explains significant variance
- **High F-stat + low p-value**: Model is statistically significant
- **Low F-stat + high p-value**: Factors don't explain returns (poor model)

## Example Results Interpretation

### MSFT Example

```
Alpha:         0.1398  (13.98%/year)  → p-value: 0.0216  **
Market Beta:   0.9869                  → p-value: 0.0000  ***
ESG Beta:      0.5430                  → p-value: 0.0529  *
R²:            0.5568  (55.68%)
```

**Interpretation:**
1. **Alpha = 13.98%/year**: MSFT outperforms by 14% annually after adjusting for market and ESG risk (p=0.02, significant at 5% level)
2. **Market Beta = 0.99**: MSFT moves almost exactly with the market (neutral volatility)
3. **ESG Beta = 0.54**: MSFT has strong positive ESG exposure (p=0.053, marginally significant at 10% level)
   - This means MSFT benefits when high-ESG stocks outperform low-ESG stocks
   - MSFT's returns move ~54% as much as the ESG long-short portfolio
4. **R² = 55.68%**: Market and ESG factors explain 56% of MSFT's return variance
   - Remaining 44% is idiosyncratic (company-specific) risk

**Investment Implications:**
- MSFT is a market-neutral stock with strong ESG characteristics
- Investors seeking ESG exposure can expect MSFT to capture ~54% of ESG factor returns
- Alpha of 14% suggests MSFT has strong fundamentals beyond just market/ESG exposure

## Statistical Significance Levels

| Symbol | Meaning | p-value |
|--------|---------|---------|
| `***` | Highly significant | p < 0.01 |
| `**` | Significant | p < 0.05 |
| `*` | Marginally significant | p < 0.10 |
| (blank) | Not significant | p ≥ 0.10 |

## Academic References

- **Fama & French (1993)**: "Common risk factors in the returns on stocks and bonds"
  - Introduced multi-factor models for asset pricing
  - Foundation for modern factor regression analysis

- **Carhart (1997)**: "On persistence in mutual fund performance"
  - Added momentum factor to Fama-French 3-factor model
  - Methodology for evaluating factor exposures

- **Pastor, Stambaugh & Taylor (2021)**: "Sustainable investing in equilibrium"
  - ESG as a systematic risk factor in equity returns
  - Theoretical foundation for ESG beta

## Comparison with Single-Factor Model

### Traditional CAPM (1-Factor)
```
R_i - RF = α + β_market * (R_market - RF) + ε
```

### Two-Factor Model (Market + ESG)
```
R_i - RF = α + β_market * (R_market - RF) + β_ESG * ESG_factor + ε
```

**Advantages of Two-Factor Model:**
1. **Better Fit**: Higher R² by including ESG factor
2. **More Accurate Alpha**: Separates ESG exposure from true alpha
3. **ESG Risk Measurement**: Quantifies ESG factor exposure
4. **Modern Portfolio Theory**: Aligns with multi-factor asset pricing

**Example:**
- CAPM might show α = 20%/year for MSFT
- Two-factor model shows α = 14%/year, β_ESG = 0.54
- **Interpretation**: 6% of the "alpha" was actually ESG factor exposure, not true outperformance

## Load and Analyze Results

```python
import pandas as pd

# Load saved results
results = pd.read_parquet('data/results/two_factor_regression/ticker=AAPL/two_factor_regression.parquet')

# Summary statistics
print(results[['alpha', 'beta_market', 'beta_esg', 'r_squared']].describe())

# Plot time-varying betas (rolling window)
import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 1, figsize=(12, 8))

# Market beta over time
axes[0].plot(results['date'], results['beta_market'], label='Market Beta')
axes[0].axhline(1.0, color='red', linestyle='--', alpha=0.5)
axes[0].set_title('AAPL Market Beta Over Time')
axes[0].legend()

# ESG beta over time
axes[1].plot(results['date'], results['beta_esg'], label='ESG Beta', color='green')
axes[1].axhline(0.0, color='red', linestyle='--', alpha=0.5)
axes[1].set_title('AAPL ESG Beta Over Time')
axes[1].legend()

plt.tight_layout()
plt.savefig('aapl_betas_over_time.png')
```

## Troubleshooting

### Issue: "Insufficient data for [TICKER]"
**Cause**: Not enough aligned observations between stock returns, market returns, and ESG factors
**Solution**: 
- Check if ticker has ESG data (use `--continuous-esg-only`)
- Lower `--min-obs` threshold (default: 36 months)
- Verify date range overlaps with ESG data availability (starts 2016-02-29)

### Issue: "Risk-free rate data not found"
**Cause**: Missing RF data parquet file
**Solution**: Run `fetch_risk_free_rate.py` first to download 3-month Treasury data

### Issue: High standard errors / low R²
**Cause**: Stock has high idiosyncratic volatility (not explained by factors)
**Interpretation**: Normal for certain stocks (e.g., small-cap, biotech)
**Solution**: Consider adding more factors (size, value, momentum)

### Issue: ESG beta not significant
**Interpretation**: Stock doesn't have strong ESG characteristics or ESG factor exposure
**Common for**: Tech giants (GOOGL), defensive stocks (PG), financials (JPM)

## Next Steps

After running two-factor regression:

1. **Identify high ESG beta stocks**: Useful for ESG-tilted portfolios
2. **Construct ESG-neutral portfolios**: Select stocks with β_ESG ≈ 0
3. **Alpha decomposition**: Separate true alpha from factor exposures
4. **Risk attribution**: Understand sources of portfolio variance
5. **ESG integration**: Use ESG beta in portfolio optimization

## See Also

- [ESG Factor Builder](ESG_FACTOR_BUILDER.md) - Construct ESG factor returns
- [Market Beta Calculator](MARKET_BETA_CALCULATOR.md) - Single-factor market beta
- [Expected Returns Estimation](EXPECTED_RETURNS_GUIDE.md) - Estimate future returns
- [Risk-Free Rate](RISK_FREE_RATE_IMPLEMENTATION.md) - Treasury rate data
