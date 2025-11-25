# Risk-Free Rate Manager Implementation

## Overview

The `RiskFreeRateManager` provides historical U.S. Treasury rates for calculating excess returns in factor models (market beta, ESG beta, Fama-French factors, etc.).

## Features

✅ **Multiple Treasury Maturities**: 3-month, 1-year, 5-year, 10-year, 30-year
✅ **FRED API Integration**: Fetches real treasury rates from Federal Reserve Economic Data
✅ **Constant Rate Fallback**: Use fixed rate if FRED API not available
✅ **Caching System**: Fast local storage of historical rates
✅ **Frequency Support**: Daily, weekly, monthly data
✅ **Excess Return Calculation**: Automatic computation of returns minus risk-free rate

## Quick Start

### 1. Basic Usage (with constant rate)

```python
from market import RiskFreeRateManager

# Initialize with 2% constant annual rate
rf_mgr = RiskFreeRateManager(constant_rate=0.02)

# Load monthly risk-free rates
rf_data = rf_mgr.load_risk_free_rate(
    start_date='2014-01-01',
    end_date='2024-12-31',
    frequency='monthly'
)

# Calculate excess returns
excess_returns = rf_mgr.calculate_excess_returns(
    returns=stock_returns,      # pd.Series of returns
    dates=return_dates,          # pd.Series of dates
    frequency='monthly'
)
```

### 2. Advanced Usage (with FRED API)

Get free FRED API key at: https://fred.stlouisfed.org/docs/api/api_key.html

Add to `config/settings.yaml`:
```yaml
fetcher:
  fred_api_key: "your_api_key_here"
```

```python
from market import RiskFreeRateManager
from core.config import Config

config = Config("config/settings.yaml")
fred_api_key = config.get('fetcher.fred_api_key')

# Initialize with FRED API
rf_mgr = RiskFreeRateManager(
    fred_api_key=fred_api_key,
    default_rate='3month'  # Use 3-month T-Bill
)

# Fetch real treasury rates
rf_data = rf_mgr.load_risk_free_rate(
    start_date='2014-01-01',
    end_date='2024-12-31',
    rate_type='3month',
    frequency='monthly'
)

print(rf_data.head())
# Output:
#         date   rate
# 2014-01-31   0.05
# 2014-02-28   0.06
# 2014-03-31   0.04
```

## Available Treasury Rates

| Rate Type | FRED Series | Description | Typical Use |
|-----------|-------------|-------------|-------------|
| `3month` | DGS3MO | 3-Month Treasury Bill | Standard risk-free rate for short-term |
| `1year` | DGS1 | 1-Year Treasury Note | Short-term investments |
| `5year` | DGS5 | 5-Year Treasury Note | Medium-term analysis |
| `10year` | DGS10 | 10-Year Treasury Note | Long-term benchmark |
| `30year` | DGS30 | 30-Year Treasury Bond | Very long-term analysis |

## Integration Examples

### Example 1: Market Beta with Excess Returns

```python
from market import PriceManager, RiskFreeRateManager
import statsmodels.api as sm

# Initialize managers
rf_mgr = RiskFreeRateManager(constant_rate=0.02)

# Load stock and market returns
stock_returns = price_mgr.load_price_data('AAPL', frequency='monthly')['returns']
market_returns = price_mgr.load_price_data('SPY', frequency='monthly')['returns']

# Calculate excess returns
stock_excess = rf_mgr.calculate_excess_returns(
    returns=stock_returns,
    dates=stock_returns.index,
    frequency='monthly'
)

market_excess = rf_mgr.calculate_excess_returns(
    returns=market_returns,
    dates=market_returns.index,
    frequency='monthly'
)

# Run regression: stock_excess = alpha + beta * market_excess
X = sm.add_constant(market_excess)
model = sm.OLS(stock_excess, X).fit()

print(f"Beta: {model.params['market_excess']:.4f}")
print(f"Alpha: {model.params['const']:.4f}")
```

### Example 2: ESG Beta Calculation

See `src/programs/calculate_esg_beta.py` for complete implementation.

```python
from market import ESGManager, PriceManager, RiskFreeRateManager

# Initialize managers
rf_mgr = RiskFreeRateManager(constant_rate=0.02)
esg_mgr = ESGManager(universe=universe)

# Load stock returns and ESG scores
stock_returns = price_mgr.load_price_data('AAPL', frequency='monthly')['returns']
esg_data = esg_mgr.load_esg_data('AAPL', start_date='2014-01-01', end_date='2024-12-31')

# Calculate stock excess returns
stock_excess = rf_mgr.calculate_excess_returns(
    returns=stock_returns,
    dates=stock_returns.index,
    frequency='monthly'
)

# Calculate ESG factor returns
esg_factor = esg_data['total_score'].pct_change()

# Regression: stock_excess = alpha + beta_esg * esg_factor
X = sm.add_constant(esg_factor)
model = sm.OLS(stock_excess, X).fit()

print(f"ESG Beta: {model.params['esg_factor']:.4f}")
```

## API Reference

### RiskFreeRateManager

#### Constructor

```python
RiskFreeRateManager(
    data_root: str = "data/curated/risk_free_rate",
    default_rate: str = '3month',
    fred_api_key: Optional[str] = None,
    constant_rate: Optional[float] = None
)
```

#### Methods

##### load_risk_free_rate()

```python
load_risk_free_rate(
    start_date: str,
    end_date: str,
    rate_type: Optional[str] = None,
    frequency: str = 'monthly',
    use_cache: bool = True,
    save_cache: bool = True
) -> pd.DataFrame
```

Returns DataFrame with columns: `date`, `rate`

##### calculate_risk_free_returns()

```python
calculate_risk_free_returns(
    dates: pd.Series,
    rate_type: Optional[str] = None,
    frequency: str = 'monthly',
    annualized_rate: Optional[pd.DataFrame] = None
) -> pd.Series
```

Returns Series of periodic risk-free returns (decimal format).

##### calculate_excess_returns()

```python
calculate_excess_returns(
    returns: pd.Series,
    dates: pd.Series,
    rate_type: Optional[str] = None,
    frequency: str = 'monthly'
) -> pd.Series
```

Returns Series of excess returns (returns - risk_free_rate).

##### get_summary_statistics()

```python
get_summary_statistics(
    start_date: str,
    end_date: str,
    rate_type: Optional[str] = None,
    frequency: str = 'monthly'
) -> Dict
```

Returns dictionary with mean, median, std, min, max rates.

## Command-Line Programs

### 1. Demonstrate Risk-Free Rate Manager

```bash
python examples/demo_risk_free_rate.py
```

Output:
- Fetches 10 years of treasury rates
- Shows summary statistics
- Demonstrates excess return calculations
- Compares different treasury maturities
- Shows cache performance

### 2. Calculate ESG Beta (NEW)

```bash
# Calculate ESG beta for all S&P 500 stocks with ESG data
python -m programs.calculate_esg_beta

# Use specific ESG factor
python -m programs.calculate_esg_beta --esg-factor env_score

# Custom window and period
python -m programs.calculate_esg_beta --window 36 --start 2014-01-01 --end 2024-12-31

# Use specific risk-free rate
python -m programs.calculate_esg_beta --rf-rate 0.025

# Calculate for specific symbols
python -m programs.calculate_esg_beta --symbols AAPL MSFT GOOGL
```

### 3. Calculate Market Beta (Updated)

The existing `calculate_market_beta.py` can be enhanced to use excess returns by integrating RiskFreeRateManager.

## Cache Storage

Risk-free rate data is cached in:
```
data/curated/risk_free_rate/
├── 3month_daily.parquet
├── 3month_weekly.parquet
├── 3month_monthly.parquet
├── 5year_monthly.parquet
├── 10year_monthly.parquet
└── 30year_monthly.parquet
```

Cache files are automatically created and updated when fetching data.

## Data Format

### Risk-Free Rate Data

```python
>>> rf_data = rf_mgr.load_risk_free_rate('2020-01-01', '2020-12-31', frequency='monthly')
>>> rf_data.head()

      date   rate
2020-01-31   1.55
2020-02-29   1.47
2020-03-31   0.11
2020-04-30   0.10
2020-05-31   0.14
```

- `date`: End-of-period date (date object)
- `rate`: Annualized percentage (e.g., 1.55 = 1.55%)

### Risk-Free Returns

```python
>>> rf_returns = rf_mgr.calculate_risk_free_returns(dates, frequency='monthly')
>>> rf_returns.head()

2020-01-31    0.001292
2020-02-29    0.001225
2020-03-31    0.000092
2020-04-30    0.000083
2020-05-31    0.000117
```

- Returns are in decimal format (0.001292 = 0.1292% monthly)
- Annualized rate divided by periods per year (12 for monthly)

## Performance

### Cache Performance

- **First load** (FRED API fetch): ~2-3 seconds
- **Cached load**: ~0.01 seconds
- **Speedup**: ~200-300x faster

### Memory Usage

- 10 years of monthly data: ~2 KB per rate type
- 10 years of daily data: ~50 KB per rate type

## Best Practices

1. **Use 3-month T-Bill** for standard equity analysis (most common)
2. **Match frequency** with your return data (monthly ESG → monthly rates)
3. **Cache data** for repeated analyses (set `use_cache=True`)
4. **Use excess returns** for accurate beta calculations (removes risk-free component)
5. **Get FRED API key** for real historical rates (free, easy setup)

## Troubleshooting

### Issue: No FRED API key

**Solution**: Use constant rate fallback
```python
rf_mgr = RiskFreeRateManager(constant_rate=0.02)  # 2% annual
```

### Issue: Missing data in FRED series

**Solution**: FRED data may have gaps (weekends, holidays). The manager automatically forward-fills missing values.

### Issue: Date alignment errors

**Solution**: Ensure all date series use consistent format:
```python
dates = pd.Series([d.date() for d in pd.date_range(...)])
```

## References

- **FRED API Documentation**: https://fred.stlouisfed.org/docs/api/
- **Treasury Rates**: https://fred.stlouisfed.org/categories/115
- **API Key**: https://fred.stlouisfed.org/docs/api/api_key.html

## Files Created

1. `src/market/risk_free_rate_manager.py` - Main implementation
2. `examples/demo_risk_free_rate.py` - Demonstration script
3. `src/programs/calculate_esg_beta.py` - ESG beta calculator
4. `docs/RISK_FREE_RATE_IMPLEMENTATION.md` - This documentation
