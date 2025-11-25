# Calculate Universe Betas - Quick Reference

## Program: `src/programs/calculate_universe_betas.py`

Calculates 60-month rolling market beta and alpha for all S&P 500 universe members using the MarketBetaManager class.

## Quick Usage

### Calculate All Continuous ESG Tickers (Recommended)

```bash
# Preview first (dry-run)
python src/programs/calculate_universe_betas.py --continuous-esg-only --dry-run

# Execute calculation
python src/programs/calculate_universe_betas.py --continuous-esg-only

# Resume if interrupted (skip already calculated)
python src/programs/calculate_universe_betas.py --continuous-esg-only --skip-existing
```

### Calculate All Universe Members

```bash
# All historical members from 2014-2024
python src/programs/calculate_universe_betas.py

# Skip already calculated tickers
python src/programs/calculate_universe_betas.py --skip-existing

# Specific date range
python src/programs/calculate_universe_betas.py --start-date 2014-01-01 --end-date 2024-12-31
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-date` | Start date for membership filter (YYYY-MM-DD) | From config |
| `--end-date` | End date for membership filter (YYYY-MM-DD) | From config |
| `--continuous-esg-only` | Only process 427 tickers with continuous ESG data | All members |
| `--skip-existing` | Skip tickers that already have beta results | Process all |
| `--dry-run` | Preview without calculating | Execute |
| `--window-months` | Rolling window size in months | 60 |
| `--min-observations` | Minimum observations required | 36 |

## Output

### Per-Ticker Files

Each successful calculation creates:

```
data/curated/tickers/exchange=us/ticker=SYMBOL/results/betas/market_beta.parquet
```

Contains:
- `date` - End date of rolling window
- `beta` - Market beta coefficient
- `alpha` - Jensen's alpha (annualized)
- `r_squared` - R² (explanatory power)
- Statistical metrics (standard errors, t-stats, p-values)
- `observations` - Number of months in window
- `correlation` - Stock-market correlation

### Console Output

```
================================================================================
MARKET BETA CALCULATION FOR UNIVERSE
================================================================================

📊 Universe:      S&P 500
📅 Period:        2014-01-01 to 2025-11-11
🪟  Window:        60 months
📏 Min Obs:       36 observations

🎯 Loading continuous ESG tickers...
   Found 427 tickers with continuous ESG data

================================================================================
CALCULATING BETAS
================================================================================

[   1/427] A      ... ✅ β= 1.241, α=-0.0854, R²=0.478 ( 83 estimates)
[   2/427] AAPL   ... ✅ β= 1.089, α= 0.0422, R²=0.450 ( 83 estimates)
[   3/427] ABBV   ... ✅ β= 0.328, α= 0.1798, R²=0.098 ( 83 estimates)
...

================================================================================
SUMMARY
================================================================================
Total tickers:    427
✅ Successful:    420 (98.4%)
⚠️  No data:       5 (1.2%)
❌ Failed:        2 (0.5%)

💾 Results saved to: data/curated/tickers/exchange=us/ticker=*/results/betas/market_beta.parquet

================================================================================
UNIVERSE BETA STATISTICS
================================================================================

Latest Beta Estimates (n=420):
  Mean:             0.9845
  Median:           0.9721
  Std Dev:          0.3210
  Min:              0.2156
  Max:              2.1234
  25th percentile:  0.7893
  75th percentile:  1.1567

Latest Alpha Estimates (Annualized):
  Mean:             0.0234 (2.34%)
  Median:           0.0189 (1.89%)
  Std Dev:          0.0876

Latest R-Squared:
  Mean:             0.4123
  Median:           0.4056

Risk Categories:
  Defensive (β<0.9):  145 (34.5%)
  Neutral (0.9-1.1):  178 (42.4%)
  Aggressive (β>1.1): 97 (23.1%)
```

## Typical Workflow

### 1. Initial Calculation (Continuous ESG Tickers)

```bash
# Preview what will be calculated
python src/programs/calculate_universe_betas.py --continuous-esg-only --dry-run

# Execute (will take ~10-20 minutes for 427 tickers)
python src/programs/calculate_universe_betas.py --continuous-esg-only
```

Expected: ~420/427 successful (98%+)

### 2. Resume After Interruption

```bash
# Skip already calculated tickers
python src/programs/calculate_universe_betas.py --continuous-esg-only --skip-existing
```

### 3. Verify Results

```bash
# Count calculated tickers
find data/curated/tickers/exchange=us/*/results/betas -name "market_beta.parquet" | wc -l

# List tickers with results
find data/curated/tickers/exchange=us/*/results/betas -name "market_beta.parquet" \
  -exec dirname {} \; | sed 's|.*/ticker=||' | sed 's|/results.*||' | sort

# Analyze specific ticker
python tests/demo_market_beta.py AAPL

# Compare multiple tickers
python tests/analyze_beta.py AAPL MSFT GOOGL TSLA
```

## Performance

- **Speed**: ~1-2 seconds per ticker
- **Time for 427 tickers**: ~10-20 minutes
- **Memory**: Minimal (processes one ticker at a time)
- **Storage**: ~16KB per ticker

## Common Use Cases

### Portfolio Construction

Calculate betas for all tickers, then filter by risk profile:

```python
from pathlib import Path
import pandas as pd

# Load all betas
betas = {}
for beta_file in Path('data/curated/tickers/exchange=us').glob('ticker=*/results/betas/market_beta.parquet'):
    ticker = beta_file.parts[-4].replace('ticker=', '')
    df = pd.read_parquet(beta_file)
    if not df.empty:
        betas[ticker] = df.iloc[-1]['beta']

# Filter defensive stocks (β < 0.9)
defensive = {t: b for t, b in betas.items() if b < 0.9}
print(f"Defensive stocks: {len(defensive)}")

# Filter aggressive stocks (β > 1.1)
aggressive = {t: b for t, b in betas.items() if b > 1.1}
print(f"Aggressive stocks: {len(aggressive)}")
```

### ESG-Beta Analysis

Combine with ESG data:

```bash
# 1. Calculate betas for continuous ESG tickers
python src/programs/calculate_universe_betas.py --continuous-esg-only

# 2. Load and analyze
python tests/demo_esg_beta_universe.py
```

### Periodic Updates

Recalculate monthly as new data arrives:

```bash
# Only calculate for tickers without results
python src/programs/calculate_universe_betas.py --continuous-esg-only --skip-existing
```

## Troubleshooting

### No Market Data (SPY)

**Error**: Market data not found

**Solution**: Ensure SPY monthly data exists:
```bash
ls -la data/curated/references/ticker=SPY/prices/freq=monthly/
```

If missing, fetch SPY data first.

### Insufficient Observations

**Warning**: `⚠️  No data (insufficient observations)`

**Causes**:
- Ticker recently added to S&P 500
- Missing monthly price data
- Less than 36 months of data

**Solution**: These are expected for new additions. Check with:
```python
python tests/demo_market_beta.py TICKER
```

### Memory Issues

Unlikely with current implementation (processes one ticker at a time), but if encountered:

**Solution**: Process in smaller batches by filtering tickers first.

## Integration with Analysis Tools

After calculation, use these tools:

```bash
# Single ticker analysis
python tests/demo_market_beta.py AAPL

# Compare multiple tickers
python tests/analyze_beta.py AAPL MSFT GOOGL TSLA

# ESG-Beta analysis
python tests/demo_esg_beta_universe.py
```

## Technical Details

- **Algorithm**: OLS regression on monthly returns
- **Window**: 60 months (5 years) rolling
- **Minimum**: 36 observations required
- **Benchmark**: SPY (S&P 500 ETF)
- **Data**: Adjusted close prices for returns
- **Alpha**: Annualized (monthly * 12)

## See Also

- [Market Beta Calculator Documentation](MARKET_BETA_CALCULATOR.md) - Full technical details
- [MarketBetaManager Source](../src/market/market_beta_manager.py) - Implementation
- [Demo Programs](../tests/demo_market_beta.py) - Usage examples
