# ESG Factor Formation Methods

## Overview

The `ESGFactorBuilder` class now provides **three distinct formation methods** for constructing ESG factors from raw ESG data. Each method serves different investment strategies and research objectives.

## Architecture Design

### Key Improvement: Simplified API

**Old Design (Deprecated):**
```python
# Too many boolean flags - confusing and inflexible
builder.build_factors_for_universe(
    include_pillars=True,
    include_momentum=True,
    include_composite=True,
    # ... creates everything at once
)
```

**New Design:**
```python
# Clear, explicit formation method selection
builder.build_factors_for_universe(
    formation_method='pillar_weighted',  # Choose ONE method
    pillar_weights={'E': 0.6, 'S': 0.2, 'G': 0.2},
    include_rankings=True
)
```

### Design Principles

1. **Single Responsibility:** Each formation method has one clear purpose
2. **Explicit Over Implicit:** User explicitly chooses the formation strategy
3. **Type Safety:** `Literal` types enforce valid formation_method values
4. **Composability:** Build multiple factor sets and combine them externally

---

## Formation Method 1: ESG Score

### Purpose
Use the pre-calculated composite ESG score directly from the data provider.

### When to Use
- **Standard ESG portfolios:** Long-only or long-short ESG-based strategies
- **Simplest approach:** No customization needed
- **Quick analysis:** Fast exploration of ESG relationships with returns

### API
```python
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA'],
    start_date='2020-01-01',
    end_date='2024-12-31',
    formation_method='esg_score',  # ← Method 1
    include_rankings=True
)
```

### Output Columns
```
ticker, date, esg_score,
esg_score_zscore,      # Cross-sectional z-score
esg_score_pctrank,     # Percentile rank (0-100)
esg_score_decile       # Decile (1=lowest, 10=highest)
```

### Example Results (Latest Month)
```
ticker  esg_score  esg_score_zscore  esg_score_pctrank  esg_score_decile
  NVDA      74.65          0.707107              100.0                10
  META      60.80         -0.707107               50.0                 1
```

### Use Cases
- **Portfolio Construction:** Go long decile 10, short decile 1
- **Screening:** Filter stocks with esg_score_pctrank > 75
- **Alpha Research:** Test ESG score predictive power for returns

---

## Formation Method 2: Pillar-Weighted

### Purpose
Create a **custom ESG composite** by combining Environmental, Social, and Governance pillar scores with adjustable weights.

### When to Use
- **Thematic strategies:** Climate-focused (high E weight), social impact (high S weight)
- **Client preferences:** Match specific ESG mandates or philosophies
- **Sector-specific:** Adjust weights based on sector materiality (e.g., energy → high E)

### API
```python
# Environmental-focused strategy (60% E, 20% S, 20% G)
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'XOM', 'CVX', 'COP'],
    start_date='2020-01-01',
    end_date='2024-12-31',
    formation_method='pillar_weighted',  # ← Method 2
    pillar_weights={'E': 0.6, 'S': 0.2, 'G': 0.2},  # Custom weights
    include_rankings=True
)
```

### Output Columns
```
ticker, date, 
pillar_weighted_score,              # Custom composite: 0.6*E + 0.2*S + 0.2*G
environmental_pillar_score,         # Original E score
social_pillar_score,                # Original S score
governance_pillar_score,            # Original G score
pillar_weighted_score_zscore,       # Cross-sectional z-score
pillar_weighted_score_pctrank,      # Percentile rank
pillar_weighted_score_decile        # Decile
```

### Example Results (Environmental Focus 60/20/20)
```
ticker  pillar_weighted_score  environmental_pillar_score  pillar_weighted_score_decile
  MSFT              82.30                   77.96                           10
 GOOGL              78.67                   76.30                            9
  NVDA              71.97                   66.45                            8
   XOM              60.10                   56.34                            1  # Low environmental
```

### Comparison: Environmental vs Governance Focus
```python
# Environmental focus (60/20/20)
factors_env = builder.build_factors_for_universe(
    formation_method='pillar_weighted',
    pillar_weights={'E': 0.6, 'S': 0.2, 'G': 0.2}
)

# Governance focus (20/20/60)
factors_gov = builder.build_factors_for_universe(
    formation_method='pillar_weighted',
    pillar_weights={'E': 0.2, 'S': 0.2, 'G': 0.6}
)
```

**Decile Ranking Changes:**
```
ticker  env_focus_decile  gov_focus_decile  change
  AAPL                 6                 9      +3  # Better governance than environment
   CVX                 3                 5      +2  # Better governance
 GOOGL                 9                 6      -3  # Worse governance than environment
```

### Use Cases
- **Climate Portfolios:** High E weight (e.g., 0.7 E, 0.15 S, 0.15 G)
- **Social Impact:** High S weight (e.g., 0.2 E, 0.6 S, 0.2 G)
- **Governance-First:** High G weight (e.g., 0.2 E, 0.2 S, 0.6 G)
- **Equal-Weighted Baseline:** (0.33 E, 0.33 S, 0.34 G)

---

## Formation Method 3: ESG Momentum

### Purpose
Focus on the **rate of ESG improvement** over time rather than absolute ESG levels.

### When to Use
- **ESG improver strategies:** Target companies actively improving ESG practices
- **Momentum investing:** Combine with price momentum for dual momentum strategies
- **Transition investing:** Identify high-emitters making progress (e.g., energy sector transition)

### API
```python
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'BAC', 'XOM', 'CVX'],
    start_date='2020-01-01',
    end_date='2024-12-31',
    formation_method='momentum',  # ← Method 3
    momentum_windows=[3, 6, 12],  # Multiple lookback windows
    include_rankings=True
)
```

### Output Columns
```
ticker, date, esg_score,
esg_score_momentum_3m,           # % change over 3 months
esg_score_momentum_6m,           # % change over 6 months
esg_score_momentum_12m,          # % change over 12 months (primary)
esg_score_momentum_12m_zscore,   # Cross-sectional z-score
esg_score_momentum_12m_pctrank,  # Percentile rank
esg_score_momentum_12m_decile    # Decile
```

### Example Results (Latest Month)
```
🚀 Top ESG Improvers (12-month momentum):
ticker  esg_score  momentum_3m  momentum_6m  momentum_12m  decile
   BAC      86.37          0.0          0.0          3.07      10  # +3% ESG improvement
   COP      63.56          0.0          0.0          0.82       9  # +0.8% improvement
   CVX      65.58          0.0          0.0          0.17       8  # +0.2% improvement

📉 ESG Decliners (12-month momentum):
ticker  esg_score  momentum_3m  momentum_6m  momentum_12m  decile
   PFE      77.15          0.0          0.0         -4.37       2  # -4% ESG decline
   XOM      63.79          0.0          0.0         -6.02       1  # -6% ESG decline
```

### Momentum Consistency Check
```python
# Identify tickers with consistent improvement across all windows
consistent_improvers = factors[
    (factors['esg_score_momentum_3m'] > 0) &
    (factors['esg_score_momentum_6m'] > 0) &
    (factors['esg_score_momentum_12m'] > 0)
]['ticker'].unique()
```

### Use Cases
- **ESG Improver Portfolio:** Long decile 10 (improvers), short decile 1 (decliners)
- **Transition Screening:** Find energy stocks with positive momentum (improving ESG)
- **ESG Momentum Factor:** Test if ESG improvement predicts future returns
- **Combined Strategy:** High ESG level + positive momentum

---

## Implementation Examples

### Example 1: Standard ESG Long-Short Strategy
```python
from market import ESGFactorBuilder, ESGManager
from universe import SP500Universe

universe = SP500Universe()
esg_mgr = ESGManager(universe=universe)
builder = ESGFactorBuilder(esg_mgr, universe)

# Build ESG score factors for full S&P 500
factors = builder.build_factors_for_universe(
    formation_method='esg_score',
    start_date='2020-01-01',
    end_date='2024-12-31',
    include_rankings=True
)

# Portfolio construction
latest = factors[factors['date'] == factors['date'].max()]
long_portfolio = latest[latest['esg_score_decile'] >= 9]['ticker']   # Top 20%
short_portfolio = latest[latest['esg_score_decile'] <= 2]['ticker']  # Bottom 20%
```

### Example 2: Climate-Focused Thematic Portfolio
```python
# Environmental-heavy weighting for climate strategy
factors = builder.build_factors_for_universe(
    formation_method='pillar_weighted',
    pillar_weights={'E': 0.7, 'S': 0.15, 'G': 0.15},  # 70% environmental focus
    start_date='2020-01-01',
    end_date='2024-12-31',
    include_rankings=True
)

# Select top environmental performers
latest = factors[factors['date'] == factors['date'].max()]
climate_leaders = latest[latest['pillar_weighted_score_decile'] >= 8]['ticker']
```

### Example 3: ESG Improver Strategy
```python
# Focus on ESG momentum
factors = builder.build_factors_for_universe(
    formation_method='momentum',
    momentum_windows=[3, 6, 12],
    start_date='2020-01-01',
    end_date='2024-12-31',
    include_rankings=True
)

# Select consistent improvers with positive 12-month momentum
latest = factors[factors['date'] == factors['date'].max()]
esg_improvers = latest[
    (latest['esg_score_momentum_12m'] > 0) &
    (latest['esg_score_momentum_12m_decile'] >= 8)
]['ticker']
```

### Example 4: Combined ESG + Momentum Strategy
```python
# Build both factor sets
esg_levels = builder.build_factors_for_universe(
    formation_method='esg_score',
    start_date='2020-01-01',
    end_date='2024-12-31'
)

esg_momentum = builder.build_factors_for_universe(
    formation_method='momentum',
    momentum_windows=[12],
    start_date='2020-01-01',
    end_date='2024-12-31'
)

# Merge and filter
combined = esg_levels.merge(esg_momentum, on=['ticker', 'date'], how='inner')
latest = combined[combined['date'] == combined['date'].max()]

# High ESG + positive momentum = best of both worlds
quality_improvers = latest[
    (latest['esg_score_decile'] >= 7) &           # High ESG level
    (latest['esg_score_momentum_12m_decile'] >= 7)  # Improving ESG
]['ticker']
```

---

## Saving and Loading Factors

### Built-in Persistence Methods

The `ESGFactorBuilder` class has **built-in methods** for saving and loading factor datasets. You don't need to manually handle file I/O.

### Save Factors

```python
# Method 1: Save with custom name
factors = builder.build_factors_for_universe(...)
saved_path = builder.save_factors(factors, "my_esg_factors.parquet")
# Saves to: data/results/esg_factors/my_esg_factors.parquet

# Method 2: Auto-generated timestamp (if you don't specify a name)
saved_path = builder.save_factors(factors)
# Saves to: data/results/esg_factors/esg_factors_20251124_164357.parquet

# Method 3: Name without extension (automatically adds .parquet)
saved_path = builder.save_factors(factors, "my_factors")
# Saves to: data/results/esg_factors/my_factors.parquet
```

### Load Factors

```python
# Load previously saved factors
factors = builder.load_factors("my_esg_factors.parquet")

# Can omit .parquet extension
factors = builder.load_factors("my_esg_factors")  # Same result

# Get summary info during load
# Output: 📂 Loaded factors from: data/results/esg_factors/my_esg_factors.parquet (300 records, 5 tickers)
```

### Get Summary Statistics

```python
# Get detailed statistics for any factor DataFrame
summary = builder.get_factor_summary(factors)

print(f"Total records: {summary['total_records']}")
print(f"Tickers: {summary['num_tickers']}")
print(f"Date range: {summary['date_range']}")
print(f"Factor columns: {summary['factor_columns']}")

# Statistics for each factor
for col, stats in summary['factor_statistics'].items():
    print(f"{col}: mean={stats['mean']:.4f}, std={stats['std']:.4f}, coverage={stats['coverage']:.1%}")
```

### File Locations

All factor files are automatically saved to:
```
{data_root}/results/esg_factors/
```

Default location:
```
data/results/esg_factors/
├── esg_score_factors.parquet
├── pillar_weighted_environmental_focus.parquet
├── pillar_weighted_governance_focus.parquet
├── esg_momentum_factors.parquet
└── my_custom_factors.parquet
```

### Manual Access (if needed)

You can also manually read the Parquet files:
```python
import pandas as pd

# Direct file access
df = pd.read_parquet('data/results/esg_factors/my_esg_factors.parquet')
```

### Example: Complete Workflow

```python
from market import ESGFactorBuilder, ESGManager
from universe import SP500Universe

# Initialize
universe = SP500Universe()
esg_mgr = ESGManager(universe=universe)
builder = ESGFactorBuilder(esg_mgr, universe)

# Build factors
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2023-01-01',
    end_date='2024-12-31',
    formation_method='esg_score',
    include_rankings=True
)

# Save immediately after building
saved_path = builder.save_factors(factors, "esg_scores_2023_2024.parquet")
print(f"✅ Saved to: {saved_path}")

# Later... load for analysis
loaded_factors = builder.load_factors("esg_scores_2023_2024.parquet")

# Get summary
summary = builder.get_factor_summary(loaded_factors)
print(f"Loaded {summary['total_records']} records for {summary['num_tickers']} tickers")
```

### Demo Scripts

See complete examples in:
- **`examples/demo_formation_methods.py`**: Shows save functionality for all three methods
- **`examples/esg_factor_save_load_example.py`**: Focused save/load workflow demonstration

---

## Best Practices

### 1. Data Quality Checks
```python
# Check coverage before building factors
factors = builder.build_factors_for_universe(...)
coverage = factors.groupby('ticker')['esg_score'].count()
print(f"Data points per ticker:\n{coverage.describe()}")
```

### 2. Handling Missing Data
- ESG scores often have **sparse coverage** (monthly/quarterly updates)
- Missing pillar scores are **more common** than composite scores
- Method 1 (ESG Score) typically has **best coverage**
- Method 2 (Pillar-Weighted) may have **more NaNs** (requires all 3 pillars)
- Method 3 (Momentum) requires **historical data** (minimum 12 months)

### 3. Factor Persistence
```python
# Save factors for reuse
builder.save_factors(factors, "esg_factors_momentum_sp500.parquet")

# Load later
loaded_factors = builder.load_factors("esg_factors_momentum_sp500.parquet")
```

### 4. Summary Statistics
```python
# Get quick overview
summary = builder.get_factor_summary(factors)
print(summary)
```

---

## Comparison Table

| Feature | ESG Score | Pillar-Weighted | Momentum |
|---------|-----------|-----------------|----------|
| **Complexity** | Simple | Medium | High |
| **Customization** | None | High (weights) | Medium (windows) |
| **Data Coverage** | Best | Good | Requires history |
| **Best For** | Standard ESG | Thematic strategies | ESG improver strategies |
| **Output** | 1 score + rankings | 1 score + 3 pillars + rankings | 1 score + 3 windows + rankings |
| **Lookback** | Point-in-time | Point-in-time | Time-series |
| **Strategy Type** | Cross-sectional | Cross-sectional | Time-series + cross-sectional |

---

## Technical Notes

### Cross-Sectional Rankings
All methods calculate **cross-sectional** (within-date) rankings:
- **Z-score:** `(x - mean) / std` → mean=0, std=1
- **Percentile rank:** Percentage of tickers with lower scores (0-100)
- **Decile:** Group into 10 equal buckets (1=worst, 10=best)

### Momentum Calculation
```python
# Formula: % change over window
momentum = (current_score - lagged_score) / lagged_score * 100

# Example: ESG score 70 → 75 over 12 months
momentum_12m = (75 - 70) / 70 * 100 = 7.14%  # +7.14% improvement
```

### Missing Data Handling
- **Forward fill:** Not applied (avoids look-ahead bias)
- **NaN propagation:** If any pillar is missing, pillar_weighted_score is NaN
- **Momentum:** Requires valid score at both t and t-window

---

## Migration Guide

### From Old API to New API

**Old code:**
```python
# Deprecated - too many flags
factors = builder.build_factors_for_universe(
    include_pillars=True,
    include_momentum=True,
    include_composite=True,
    momentum_windows=[12]
)
```

**New code:**
```python
# Method 1: ESG Score
factors_esg = builder.build_factors_for_universe(
    formation_method='esg_score'
)

# Method 2: Pillar-Weighted
factors_pillars = builder.build_factors_for_universe(
    formation_method='pillar_weighted',
    pillar_weights={'E': 0.4, 'S': 0.3, 'G': 0.3}
)

# Method 3: Momentum
factors_momentum = builder.build_factors_for_universe(
    formation_method='momentum',
    momentum_windows=[3, 6, 12]
)
```

---

## Demo Script

Run the comprehensive demo:
```bash
python examples/demo_formation_methods.py
```

This script demonstrates:
1. **Method 1:** ESG Score formation with 5 tech stocks
2. **Method 2:** Pillar-weighted formation with environmental vs governance focus
3. **Method 3:** ESG momentum with multi-sector analysis and consistency checks

---

## References

- **ESG Data Structure:** `docs/BUILD_SP500_ESG_FACTORS.md`
- **Implementation Details:** `src/market/esg_factor_builder.py`
- **ESG Manager:** `src/market/esg_manager.py`
- **Demo Examples:** `examples/demo_formation_methods.py`
