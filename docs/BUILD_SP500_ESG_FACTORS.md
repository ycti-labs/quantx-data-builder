# Build S&P 500 ESG Factors

Production program to build comprehensive ESG factor datasets for all S&P 500 historical members.

## Overview

This program constructs quantitative ESG factors from raw ESG scores for the entire S&P 500 historical universe, enabling:
- Long-short ESG factor portfolios
- ESG-tilted investment strategies  
- Academic research on ESG factor premiums
- Backtesting ESG-based trading strategies

## Features

- **Universe Coverage**: All historical S&P 500 members during the specified period
- **Factor Types**:
  - Cross-sectional: Z-scores, percentile ranks, deciles (within each date)
  - Time-series: ESG momentum (6-month, 12-month improvement rates)
  - Composite: Quality-momentum, pillar-weighted (custom E/S/G weights)
- **Output**: Parquet file ready for backtesting and research
- **Flexible**: Command-line options to customize factor construction

## Usage

### Basic Usage

```bash
# Build factors for 2020-2024 with all factor types
python -m programs.build_sp500_esg_factors

# Custom date range
python -m programs.build_sp500_esg_factors --start-date 2018-01-01 --end-date 2023-12-31
```

### Advanced Options

```bash
# Skip pillar-specific factors (faster, smaller output)
python -m programs.build_sp500_esg_factors --no-pillars

# Skip momentum factors
python -m programs.build_sp500_esg_factors --no-momentum

# Custom momentum windows (3, 6, 9 months)
python -m programs.build_sp500_esg_factors --momentum-windows 3 6 9

# Minimal factors (ESG score only, no pillars, no momentum, no composite)
python -m programs.build_sp500_esg_factors --no-pillars --no-momentum --no-composite
```

## Command-Line Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--start-date` | str | 2020-01-01 | Start date (YYYY-MM-DD) |
| `--end-date` | str | 2024-12-31 | End date (YYYY-MM-DD) |
| `--no-pillars` | flag | False | Skip E/S/G pillar factors |
| `--no-momentum` | flag | False | Skip momentum factors |
| `--momentum-windows` | int+ | [6, 12] | Momentum window lengths (months) |
| `--no-composite` | flag | False | Skip composite factors |

## Output

### File Location

Factors are saved to: `data/results/esg_factors/sp500_esg_factors_YYYYMMDD_HHMMSS.parquet`

### Output Schema

**Base Columns:**
- `ticker`: Stock symbol
- `date`: End-of-month date
- `esg_score`: Raw ESG score (0-100)
- `env_score`, `soc_score`, `gov_score`: Pillar scores (if `--no-pillars` not used)

**Cross-Sectional Factor Columns:**
- `esg_score_zscore`: Z-score (mean=0, std=1 within each date)
- `esg_score_pctrank`: Percentile rank (0-100 within each date)
- `esg_score_decile`: Decile assignment (1=lowest, 10=highest within each date)
- _(Same pattern for pillar scores if enabled)_

**Momentum Factor Columns:**
- `ESG_momentum_6m`: 6-month ESG score percent change
- `ESG_momentum_12m`: 12-month ESG score percent change
- `ESG_trend_6m`, `ESG_trend_12m`: Linear trend slopes

**Composite Factor Columns:**
- `ESG_quality_momentum`: Combines ESG level (z-score) + momentum
- `ESG_composite`: Pillar-weighted composite (default: E=40%, S=30%, G=30%)

## Example Workflow

### 1. Build Factors

```bash
python -m programs.build_sp500_esg_factors --start-date 2015-01-01 --end-date 2024-12-31
```

### 2. Load and Use in Research

```python
import pandas as pd

# Load factors
factors = pd.read_parquet('data/results/esg_factors/sp500_esg_factors_20251124_121012.parquet')

# Get latest date rankings
latest = factors[factors['date'] == factors['date'].max()]
high_esg = latest[latest['esg_score_decile'] >= 8]['ticker'].tolist()
low_esg = latest[latest['esg_score_decile'] <= 3]['ticker'].tolist()

print(f"Long (High ESG): {high_esg}")
print(f"Short (Low ESG): {low_esg}")
```

### 3. Backtest Long-Short Portfolio

```python
# Quarterly rebalancing
factors['quarter'] = pd.PeriodIndex(factors['date'], freq='Q')
rebalance_dates = factors.groupby('quarter')['date'].max()

for date in rebalance_dates:
    quarter_data = factors[factors['date'] == date]
    
    # Long: Top 30% ESG
    long = quarter_data[quarter_data['esg_score_decile'] >= 8]
    
    # Short: Bottom 30% ESG
    short = quarter_data[quarter_data['esg_score_decile'] <= 3]
    
    # ... combine with price data for returns
```

## Prerequisites

1. **S&P 500 Membership Data** must be built first:
   ```bash
   python -m programs.build_sp500_membership
   ```

2. **ESG Data** must be available in:
   ```
   data/curated/esg/exchange=us/ticker={TICKER}/esg_data.parquet
   ```

## Performance Notes

- **Runtime**: ~5-15 minutes for 500+ stocks over 5 years (depends on ESG data availability)
- **Memory**: ~500MB for full factor set with all options
- **Output Size**: ~10-50MB Parquet file (compressed)

## Factor Interpretation

### Cross-Sectional Factors
- **Z-scores**: Standardized within each date → comparable across time
- **Percentile ranks**: Relative position (0-100) → easy interpretation
- **Deciles**: Portfolio assignment (1-10) → direct trading signals

### Momentum Factors
- **Positive momentum**: ESG improving → potential ESG leaders
- **Negative momentum**: ESG declining → potential ESG laggards
- **Use cases**: Momentum strategies, ESG improver portfolios

### Composite Factors
- **Quality-Momentum**: Combines level + change → high ESG + improving
- **Pillar-Weighted**: Custom E/S/G emphasis → sector-specific strategies

## Troubleshooting

### Issue: "Membership file not found"
**Solution**: Run membership builder first:
```bash
python -m programs.build_sp500_membership
```

### Issue: "No factors generated"
**Cause**: ESG data not available for the period
**Solution**: Check ESG data availability or adjust date range

### Issue: Many "No ESG data found" warnings
**Expected**: Not all S&P 500 members have ESG data coverage
**Impact**: Factors built only for stocks with available ESG data

## Related Programs

- `build_sp500_membership.py`: Build S&P 500 membership intervals
- `demo_esg_factor_builder.py`: Interactive demonstration with examples
- `demo_esg_factor_builder_simple.py`: Simple demo with synthetic data

## Citation

If using these factors in research, please cite the methodology:
- Cross-sectional normalization ensures time-series comparability
- Momentum captures ESG improvement rates
- Composite factors combine multiple ESG dimensions

## Support

For questions or issues:
1. Check ESG data availability: `data/curated/esg/`
2. Verify membership data: `data/curated/membership/universe=sp500/`
3. Review logs for specific error messages
4. Consult `ESGFactorBuilder` class documentation

---

**Last Updated**: November 24, 2025
