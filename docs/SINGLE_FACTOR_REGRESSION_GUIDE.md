# Single Factor Regression Analysis - Quick Reference

## Overview

The `single_factor_regression.py` program performs single or multi-factor regression analysis using ESG factors as explanatory variables. It tests whether ESG factors explain stock returns through time-series regression.

## Key Features

✅ **Load ESG factors** from saved parquet files (data/curated/results/esg_factors/)
✅ **Load target returns** for any ticker or custom portfolio
✅ **OLS regression** with statistical significance tests
✅ **Newey-West HAC** standard errors for robustness (corrects for autocorrelation/heteroskedasticity)
✅ **Rolling window analysis** to test coefficient stability over time
✅ **Diagnostic tests** (Durbin-Watson, Breusch-Pagan, Jarque-Bera)
✅ **Visualization** of regression diagnostics and rolling results
✅ **Results saved** to parquet files for further analysis

## Basic Usage

### Single Factor Regression (SPY vs ESG)

```bash
python src/programs/single_factor_regression.py \
  --target SPY \
  --factor ESG_factor \
  --start-date 2016-01-01 \
  --end-date 2024-12-31
```

**Key Results from Example:**
- **Alpha:** 0.71% monthly (8.80% annualized) *** highly significant
- **Beta (ESG_factor):** -1.056 *** (negative exposure to ESG factor)
- **R²:** 33.67% (ESG factor explains ~34% of SPY variance)
- **Interpretation:** SPY has significant negative loading on ESG factor - when ESG factor returns are positive (long high ESG, short low ESG), SPY tends to underperform

### Multi-Factor Regression (PEP vs All ESG Factors)

```bash
python src/programs/single_factor_regression.py \
  --target PEP \
  --factor ESG_factor E_factor S_factor G_factor \
  --start-date 2016-01-01 \
  --end-date 2024-12-31
```

**Key Results from Example:**
- **Alpha:** 0.72% monthly (8.95% annualized) - not significant
- **E_factor Beta:** 0.964 ** (significant positive exposure to Environmental factor)
- **Other factors:** Not significant
- **R²:** 6.74% (low explanatory power)
- **Interpretation:** PEP has positive exposure to Environmental factor specifically, not overall ESG

### Rolling Window Analysis

```bash
python src/programs/single_factor_regression.py \
  --target SPY \
  --factor ESG_factor \
  --rolling-window 36 \
  --start-date 2016-01-01
```

**Results from Example (36-month windows):**
- **Average Beta:** -1.19 (stable negative exposure)
- **Beta Range:** Varies but consistently negative
- **Average R²:** 42.7% (ESG factor explains ~43% on average)
- **Interpretation:** Relationship is stable over time

## Command-Line Arguments

### Required (one of):
- `--target TICKER` - Ticker symbol (e.g., SPY, PEP, AAPL)
- `--portfolio-file PATH` - Custom portfolio returns file (.parquet or .csv)

### Factor Selection:
- `--factor FACTOR [FACTOR ...]` - Factor name(s) from ESG factors file
  - Available: `ESG_factor`, `E_factor`, `S_factor`, `G_factor`, `ESG_mom_factor`
  - Default: `ESG_factor`
  - Can specify multiple for multi-factor regression

### Date Range:
- `--start-date YYYY-MM-DD` - Start date (default: 2016-01-01)
- `--end-date YYYY-MM-DD` - End date (default: 2024-12-31)

### Analysis Options:
- `--rolling-window N` - Rolling window size in months (e.g., 36)
  - Only works with single factor
  - Generates time-series of coefficient estimates
- `--no-hac` - Don't use Newey-West HAC standard errors (use OLS standard errors)
- `--hac-lags N` - Number of lags for HAC (default: 12 months)

## Output Files

All results saved to: `data/curated/results/regression/`

### Generated Files:

1. **regression_{target}.parquet** - Full regression coefficients table
   - Columns: coefficient, std_error, t_statistic, p_value, significance

2. **summary_{target}.parquet** - One-row summary of key statistics
   - All model statistics in single record for easy comparison

3. **diagnostics_{target}.png** - Four-panel diagnostic plot
   - Actual vs Fitted
   - Residuals vs Fitted
   - Residual histogram with normal overlay
   - Q-Q plot

4. **rolling_{target}_{factor}.parquet** - Rolling regression results (if --rolling-window used)
   - Time series of alpha, beta, R², p-values

5. **rolling_{target}_{factor}.png** - Rolling regression visualization (if --rolling-window used)
   - Three panels: Rolling alpha, beta, R²

## Interpretation Guide

### Regression Coefficients

**Alpha (Intercept):**
- Monthly excess return not explained by ESG factors
- Annualized alpha = (1 + monthly_alpha)^12 - 1
- Significance: p < 0.05 suggests genuine skill/mispricing

**Beta (Factor Loading):**
- **Positive beta:** Returns move with factor (long high ESG = profit when factor up)
- **Negative beta:** Returns move against factor (profit when factor down)
- **Magnitude:** Size of exposure (beta=1.0 means 1:1 relationship)

### Model Statistics

**R-squared:**
- % of return variance explained by factors
- 0.33 = 33% of variance explained
- Higher R² = better fit, but not always better model

**F-statistic & p-value:**
- Tests if ALL factors jointly significant
- p < 0.05 = at least one factor is significant

### Diagnostic Tests

**Durbin-Watson (autocorrelation):**
- Tests if errors are correlated over time
- Target: ~2.0 (no autocorrelation)
- < 1.5 or > 2.5: problematic autocorrelation
- Fix: Use HAC standard errors (default)

**Breusch-Pagan (heteroskedasticity):**
- Tests if error variance is constant
- p < 0.05 = heteroskedasticity detected
- Fix: Use HAC standard errors (default)

**Jarque-Bera (normality):**
- Tests if errors are normally distributed
- p < 0.05 = non-normal residuals
- Impact: t-stats/p-values may be unreliable if severe

## Example Use Cases

### 1. Test if ESG Factor Explains Market Returns

```bash
python src/programs/single_factor_regression.py \
  --target SPY \
  --factor ESG_factor \
  --rolling-window 36
```

**Research Question:** Does the ESG factor predict/explain market returns?

### 2. Decompose ESG Effects (E vs S vs G)

```bash
python src/programs/single_factor_regression.py \
  --target AAPL \
  --factor E_factor S_factor G_factor
```

**Research Question:** Which ESG pillar drives stock returns?

### 3. Test ESG Momentum Strategy

```bash
python src/programs/single_factor_regression.py \
  --target XLC \
  --factor ESG_mom_factor \
  --start-date 2018-01-01
```

**Research Question:** Do momentum effects exist in ESG characteristics?

### 4. Custom Portfolio Analysis

```bash
# First, create portfolio_returns.parquet with date index and return column
python src/programs/single_factor_regression.py \
  --portfolio-file data/my_portfolio_returns.parquet \
  --factor ESG_factor E_factor S_factor G_factor
```

**Research Question:** What ESG exposures does my portfolio have?

## Statistical Notes

### Newey-West HAC Standard Errors (Default)

The program uses **Heteroskedasticity and Autocorrelation Consistent (HAC)** standard errors by default (Newey-West, 1987). This corrects for:

1. **Autocorrelation:** Serial correlation in residuals (common in time-series)
2. **Heteroskedasticity:** Non-constant error variance

**Why it matters:** 
- OLS standard errors are **biased** when autocorrelation/heteroskedasticity present
- This leads to **incorrect t-statistics and p-values**
- HAC standard errors fix this → more reliable inference

**Trade-off:**
- HAC standard errors are typically **larger** than OLS
- Results in **more conservative** significance tests
- Better reflects true uncertainty

### Rolling Window Analysis

**Purpose:** Test if factor loadings are stable over time

**Interpretation:**
- **Stable beta:** Relationship is consistent (good for factor investing)
- **Changing beta:** Regime changes, non-stationary relationship
- **High R² variation:** Factor explanatory power changes over time

**Typical window size:**
- 36 months (3 years): Common for equity factors
- 60 months (5 years): Longer-term relationships
- Trade-off: Longer = more stable estimates, but slower to detect changes

## Example Interpretation

### SPY vs ESG_factor Results

```
Alpha:         0.71% monthly (8.80% annualized) **
Beta (ESG):   -1.056 ***
R²:            33.67%
```

**What this means:**

1. **Negative ESG loading:** SPY moves **opposite** to ESG factor
   - When high-ESG stocks outperform low-ESG (factor return > 0), SPY underperforms
   - When low-ESG stocks outperform high-ESG (factor return < 0), SPY outperforms

2. **Large magnitude (|β| > 1):** SPY is **more sensitive** than average stock
   - 1% ESG factor return → -1.06% SPY return (relative to alpha)

3. **Significant alpha:** SPY generates 8.8% annual return beyond ESG exposure
   - This is the "market beta" return (not captured by ESG factor)

4. **Good fit (R² = 34%):** ESG factor explains 1/3 of SPY variance
   - Remaining 66% from other factors (size, value, momentum, idiosyncratic)

### PEP vs E_factor (Environmental) Results

```
Alpha:         0.72% monthly (not significant)
Beta (E):      0.964 **
Beta (S):      0.099 (not significant)
Beta (G):      0.160 (not significant)
```

**What this means:**

1. **Environmental exposure:** PEP has **positive** loading on E factor
   - When environmentally-friendly stocks outperform, PEP tends to outperform

2. **No S or G exposure:** Social and Governance factors don't predict PEP returns
   - Suggests Environmental characteristics are key driver

3. **Low R² (6.7%):** ESG factors explain little of PEP variance
   - Most of PEP returns driven by other factors (market beta, sector, fundamentals)

## Tips & Best Practices

✅ **Use HAC standard errors** (default) for time-series data
✅ **Check diagnostics** before interpreting results
✅ **Run rolling regressions** to verify stability
✅ **Use multiple factors** to avoid omitted variable bias
✅ **Consider economic significance** not just statistical significance
✅ **Compare across stocks** to find common patterns

⚠️ **Caution:**
- Small samples (< 30 obs) → unreliable results
- High multicollinearity → unstable coefficients
- Non-normal residuals → p-values may be wrong
- Spurious correlations → need economic theory

## Next Steps

After running regression:

1. **Examine diagnostic plots** - Look for patterns in residuals
2. **Check coefficient stability** - Run rolling regression
3. **Test other tickers** - Find common patterns
4. **Compare factors** - Which ESG dimension matters most?
5. **Build trading strategies** - Use significant exposures

## References

- **Newey-West (1987):** "A Simple, Positive Semi-Definite, Heteroskedasticity and Autocorrelation Consistent Covariance Matrix"
- **Fama-French:** Factor model methodology
- **Wooldridge:** Econometric Analysis of Cross Section and Panel Data
