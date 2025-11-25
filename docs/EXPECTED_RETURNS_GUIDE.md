# Expected Factor Returns - Quick Guide

## Overview

Expected returns are crucial inputs for portfolio optimization. This guide explains how to estimate expected factor returns using the `estimate_expected_returns.py` program.

## Key Question: How to Get Expected Factor Returns?

**Short Answer:** Use historical data with appropriate statistical methods.

**Methods Available:**
1. **Historical Mean** - Simple average (most common baseline)
2. **EWMA** - Exponentially weighted (more weight on recent data)
3. **James-Stein Shrinkage** - Statistically optimal shrinkage toward grand mean
4. **Bayesian Shrinkage** - Blend sample mean with prior belief

## Quick Start

### Basic Usage (All Methods)

```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method all \
  --lookback 60
```

**Results (ESG Factors, 60-month lookback):**
```
                Historical      EWMA  James-Stein  Bayesian
ESG_factor       -0.0770    -0.0794    -0.0727    -0.0694
E_factor         -0.0838    -0.0831    -0.0784    -0.0741
S_factor         -0.0770    -0.0948    -0.0728    -0.0694
G_factor         -0.0051    -0.0031    -0.0129    -0.0190
ESG_mom_factor   -0.0151    -0.0073    -0.0212    -0.0260
```

**Interpretation:** All ESG factors have **negative expected returns** (-0.5% to -8.4% annualized), suggesting ESG factors have underperformed historically.

### With Confidence Intervals

```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method historical \
  --lookback 60 \
  --bootstrap 1000
```

**Results (Bootstrap CI, 95% confidence):**
```
                   mean  std_error  lower_ci  upper_ci
ESG_factor     -0.0780     0.0408   -0.1603    0.0020
E_factor       -0.0843     0.0336   -0.1512   -0.0197
S_factor       -0.0774     0.0387   -0.1514    0.0001
G_factor       -0.0059     0.0323   -0.0706    0.0570
ESG_mom_factor -0.0144     0.0226   -0.0561    0.0303
```

**Key Insight:** Only **E_factor has significantly negative returns** (CI doesn't include 0). Other factors are not statistically different from zero.

## Estimation Methods Explained

### 1. Historical Mean (Baseline)

**Formula:** μ = (1/T) Σ r_t

**When to Use:**
- ✅ Default baseline method
- ✅ Long sample period (60+ months)
- ✅ Stationary returns (no regime changes)

**Pros:** Simple, unbiased
**Cons:** High estimation error, treats all periods equally

**Example:**
```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method historical \
  --lookback 60
```

### 2. EWMA (Recent Data Focus)

**Formula:** μ_t = λ μ_{t-1} + (1-λ) r_t, where λ = exp(-ln(2)/halflife)

**When to Use:**
- ✅ Believe recent data is more relevant
- ✅ Time-varying expected returns
- ✅ Adapting to market regimes

**Pros:** Adaptive, responds to changes
**Cons:** More volatile, sensitive to recent shocks

**Example:**
```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method ewma \
  --halflife 36  # 3 years
```

**Half-life Guidance:**
- 12 months: Very adaptive (use for high-frequency rebalancing)
- 36 months: Moderate (good balance)
- 60 months: Conservative (similar to 5-year historical mean)

### 3. James-Stein Shrinkage (Statistically Optimal)

**Formula:** μ_JS = (1-λ) μ_sample + λ μ_grand, where λ = (N-2) / [T (μ-μ_grand)' Σ^(-1) (μ-μ_grand)]

**When to Use:**
- ✅ Multiple correlated assets/factors
- ✅ Want to reduce estimation error (MSE)
- ✅ Academic/institutional best practice

**Pros:** Provably better than sample mean (Stein's paradox)
**Cons:** Requires covariance matrix, complex

**Example:**
```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method james_stein \
  --lookback 60
```

**From Example Output:**
- Shrinkage intensity: 0.1677 (16.77% toward grand mean)
- ESG_factor: -0.0770 (historical) → -0.0727 (shrunk)
- Effect: Pulls extreme estimates toward average

### 4. Bayesian Shrinkage (Flexible Prior)

**Formula:** μ_Bayes = (1-w) μ_sample + w μ_prior

**When to Use:**
- ✅ Have prior beliefs (e.g., zero expected return)
- ✅ Want to blend data with judgment
- ✅ Small sample size (prior helps stabilize)

**Pros:** Incorporates external information
**Cons:** Subjective (requires prior specification)

**Example:**
```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method bayesian \
  --prior-strength 0.3  # 30% weight on prior
```

**Prior Strength Guide:**
- 0.0: Pure sample mean (no shrinkage)
- 0.3: Moderate blend (recommended)
- 0.5: Equal weight sample + prior
- 1.0: Pure prior (ignore data)

## Advanced Features

### Rolling Window Analysis

Test stability of expected returns over time:

```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method historical \
  --rolling-window 36
```

**Use Case:** Check if expected returns are time-varying or stable

**Results Interpretation:**
- Stable estimates → Use long-term average
- Trending estimates → Consider EWMA or regime-switching
- High volatility → Large estimation error, be cautious

### Out-of-Sample Validation

Test which method predicts future returns best:

```bash
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method all \
  --validate \
  --train-end 2021-12-31
```

**Example Results:**
```
Forecast Errors:
              mse      mae     rmse
historical  0.00100  0.0290  0.0316
ewma        0.00054  0.0205  0.0232  ← Best
james_stein 0.00099  0.0248  0.0315
```

**Interpretation:** EWMA had lowest forecast error (RMSE=2.32%) for ESG factors.

## Practical Recommendations

### For Factor Investing (ESG Factors)

**Recommendation:** Use **James-Stein or Bayesian shrinkage**

**Rationale:**
1. ESG factors have **high estimation error** (small samples, ~77 months)
2. Factors are **correlated** → shrinkage reduces error
3. Historical means are **negative but insignificant** → shrink toward zero

**Implementation:**
```bash
# Conservative approach (shrink toward zero)
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method james_stein \
  --lookback 60
```

### For Stock Returns (Individual Securities)

**Recommendation:** Use **EWMA** or **Bayesian** with **sector priors**

**Rationale:**
1. Stock returns more **time-varying** than factors
2. Individual stocks have **noisier estimates**
3. Sector information improves estimates

### For Portfolio Optimization

**Recommendation:** Use **multiple methods** and compare

**Best Practice:**
```bash
# 1. Generate all estimates
python src/programs/estimate_expected_returns.py \
  --factors-file data/results/esg_factors/esg_factors.parquet \
  --method all \
  --bootstrap 1000

# 2. Check which are statistically significant (bootstrap CI)
# 3. Use conservative estimates (shrinkage methods)
# 4. Validate out-of-sample if possible
```

## Common Pitfalls & Solutions

### ❌ Problem: Negative Expected Returns

**Example:** ESG factors all have negative expected returns

**Solutions:**
1. **Accept reality:** If factors underperformed historically, expected return is negative
2. **Use forward-looking views:** Impose positive expected returns based on theory
3. **Minimum variance portfolio:** Ignore expected returns, focus on risk reduction

### ❌ Problem: Large Estimation Error

**Symptom:** Wide confidence intervals, unstable rolling estimates

**Solutions:**
1. **Increase sample size:** Use longer lookback period
2. **Shrinkage:** James-Stein or Bayesian methods
3. **Simplify:** Reduce number of assets/factors
4. **Constraints:** Use min/max weight bounds in optimization

### ❌ Problem: Out-of-Sample Failure

**Symptom:** Estimates don't predict future returns

**Solutions:**
1. **Adaptive methods:** EWMA responds to regime changes
2. **Rebalancing frequency:** Update estimates more often
3. **Regime-switching:** Use different estimates for different market states
4. **Acceptance:** Expected returns are hard to estimate (focus on risk management)

## Output Files

All results saved to: `data/curated/results/expected_returns/`

1. **expected_returns.parquet** - Main estimates across all methods
2. **bootstrap_ci.parquet** - Confidence intervals
3. **rolling_estimates.parquet** - Time series of estimates
4. **estimates_comparison.png** - Visual comparison plot
5. **rolling_estimates.png** - Stability visualization
6. **oos_validation_errors.parquet** - Validation metrics
7. **oos_validation_comparison.parquet** - Estimate vs realized

## Integration with Portfolio Optimization

After estimating expected returns, use in optimization:

```python
import pandas as pd

# Load expected returns
expected_returns = pd.read_parquet(
    'data/curated/results/expected_returns/expected_returns.parquet'
)

# Choose method (e.g., James-Stein for ESG factors)
mu = expected_returns['James-Stein']

# Load covariance matrix (estimate separately)
# ... then use in mean-variance optimization
```

## References & Further Reading

### Academic Literature

- **Stein (1956):** "Inadmissibility of the Usual Estimator for the Mean of a Multivariate Normal Distribution"
  - Proves shrinkage dominates sample mean

- **Jorion (1986):** "Bayes-Stein Estimation for Portfolio Analysis"
  - Application to portfolio optimization

- **DeMiguel et al. (2009):** "Optimal Versus Naive Diversification"
  - Shows estimation error dominates optimization benefits

### Practical Guides

- **Ledoit & Wolf (2003):** "Improved Estimation of the Covariance Matrix"
  - Shrinkage for covariance (complement to mean estimation)

- **Meucci (2005):** "Risk and Asset Allocation"
  - Comprehensive treatment of estimation methods

## Summary Table

| Method | Best For | Pros | Cons | Typical Use |
|--------|----------|------|------|-------------|
| **Historical** | Baseline, long samples | Simple, unbiased | High error | Default |
| **EWMA** | Time-varying returns | Adaptive | Volatile | Trading strategies |
| **James-Stein** | Multiple assets | Optimal (MSE) | Complex | Academic/institutional |
| **Bayesian** | Prior beliefs | Flexible | Subjective | Incorporating views |
| **Bootstrap** | Uncertainty | Robust CI | Computational | Risk assessment |

## Key Takeaways

1. ✅ **Expected returns are hard to estimate** - Use shrinkage methods
2. ✅ **Validation is critical** - Test out-of-sample before trusting estimates
3. ✅ **Method matters less than you think** - Differences are often small vs estimation error
4. ✅ **Consider zero expected returns** - Minimum variance portfolios often outperform
5. ✅ **Bootstrap for confidence** - Know the uncertainty in your estimates

**Bottom Line:** For ESG factors with negative historical returns, use **James-Stein shrinkage** or assume **zero expected returns** and focus on risk-based portfolio construction.
