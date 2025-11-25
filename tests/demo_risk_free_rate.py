"""
Demonstrate Risk-Free Rate Manager

Fetches and displays U.S. Treasury rates for use in excess return calculations.
Shows how to integrate with market beta and ESG beta calculations.
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import pandas as pd

from core.config import Config
from market import RiskFreeRateManager


def main():
    """Demonstrate risk-free rate manager functionality."""

    print("=" * 80)
    print("Risk-Free Rate Manager Demonstration")
    print("=" * 80)

    # Initialize components
    print("\n1. Initializing components...")
    config = Config("config/settings.yaml")

    # Get FRED API key from config
    fred_api_key = config.get('fetcher.fred.api_key')

    if not fred_api_key:
        raise ValueError(
            "FRED API key not found in config/settings.yaml. "
            "Please add: fetcher.fred.api_key"
        )

    print(f"✓ FRED API key found: {fred_api_key[:8]}...")

    # Create RiskFreeRateManager with FRED API
    rf_mgr = RiskFreeRateManager(
        fred_api_key=fred_api_key,
        default_rate='3month'
    )

    # Define research period (10 years)
    start_date = '2014-01-01'
    end_date = '2024-12-31'

    # Test 1: Fetch monthly risk-free rates
    print("\n" + "=" * 80)
    print("Test 1: Fetch Monthly 3-Month Treasury Bill Rates")
    print("=" * 80)

    monthly_rf = rf_mgr.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type='3month',
        frequency='monthly'
    )

    print(f"\nFetched {len(monthly_rf)} monthly observations")
    print("\nFirst 10 observations:")
    print(monthly_rf.head(10).to_string(index=False))
    print("\nLast 10 observations:")
    print(monthly_rf.tail(10).to_string(index=False))

    # Test 2: Summary statistics
    print("\n" + "=" * 80)
    print("Test 2: Summary Statistics")
    print("=" * 80)

    stats = rf_mgr.get_summary_statistics(
        start_date=start_date,
        end_date=end_date,
        rate_type='3month',
        frequency='monthly'
    )

    print("\n3-Month Treasury Bill Statistics (2014-2024):")
    print(f"  Observations:  {stats['observations']}")
    print(f"  Date Range:    {stats['start_date']} to {stats['end_date']}")
    print(f"  Mean Rate:     {stats['mean_rate']:.4f}%")
    print(f"  Median Rate:   {stats['median_rate']:.4f}%")
    print(f"  Std Dev:       {stats['std_rate']:.4f}%")
    print(f"  Min Rate:      {stats['min_rate']:.4f}%")
    print(f"  Max Rate:      {stats['max_rate']:.4f}%")

    # Test 3: Calculate risk-free returns for specific dates
    print("\n" + "=" * 80)
    print("Test 3: Calculate Risk-Free Returns")
    print("=" * 80)

    # Create sample dates (monthly)
    sample_dates = pd.date_range(start='2020-01-31', end='2020-12-31', freq='ME')
    sample_dates = pd.Series([d.date() for d in sample_dates])

    rf_returns = rf_mgr.calculate_risk_free_returns(
        dates=sample_dates,
        rate_type='3month',
        frequency='monthly'
    )

    print("\nMonthly Risk-Free Returns for 2020:")
    results_df = pd.DataFrame({
        'date': sample_dates,
        'rf_return': rf_returns
    })
    print(results_df.to_string(index=False))
    print(f"\nAverage monthly return: {rf_returns.mean():.6f} ({rf_returns.mean()*100:.4f}%)")
    print(f"Annualized return:      {rf_returns.mean()*12:.6f} ({rf_returns.mean()*12*100:.4f}%)")

    # Test 4: Calculate excess returns
    print("\n" + "=" * 80)
    print("Test 4: Calculate Excess Returns (Example with SPY)")
    print("=" * 80)

    # Simulate some stock returns
    sample_returns = pd.Series([0.02, 0.015, -0.03, 0.025, 0.01, 0.018, 0.022, -0.01, 0.015, 0.028, 0.012, 0.019])

    excess_returns = rf_mgr.calculate_excess_returns(
        returns=sample_returns,
        dates=sample_dates,
        rate_type='3month',
        frequency='monthly'
    )

    print("\nExample Stock Returns vs. Excess Returns (2020):")
    comparison_df = pd.DataFrame({
        'date': sample_dates,
        'stock_return': sample_returns,
        'rf_return': rf_returns,
        'excess_return': excess_returns
    })
    print(comparison_df.to_string(index=False))

    # Test 5: Compare different treasury maturities
    print("\n" + "=" * 80)
    print("Test 5: Compare Different Treasury Maturities")
    print("=" * 80)

    print("\nFetching different treasury rates for comparison...")

    comparison_data = {}
    for rate_type in ['3month', '5year', '10year', '30year']:
        try:
            df = rf_mgr.load_risk_free_rate(
                start_date='2020-01-01',
                end_date='2024-12-31',
                rate_type=rate_type,
                frequency='monthly'
            )
            if len(df) > 0:
                comparison_data[rate_type] = df
                print(f"  ✓ {rate_type:10s}: {len(df)} observations, mean rate = {df['rate'].mean():.4f}%")
        except Exception as e:
            print(f"  ✗ {rate_type:10s}: Error - {e}")

    # Plot comparison if we have data
    if len(comparison_data) > 0:
        print("\nRecent rates comparison (last 5 months):")
        recent_comparison = pd.DataFrame()
        for rate_type, df in comparison_data.items():
            recent = df.tail(5).copy()
            recent_comparison[rate_type] = recent['rate'].values

        recent_comparison.index = [str(d) for d in comparison_data['3month'].tail(5)['date'].values]
        print(recent_comparison.to_string())

    # Test 6: Cache demonstration
    print("\n" + "=" * 80)
    print("Test 6: Cache Performance")
    print("=" * 80)

    import time

    print("\nFirst load (fetch from Tiingo)...")
    start_time = time.time()
    rf_mgr.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type='3month',
        frequency='monthly',
        use_cache=False,
        save_cache=True
    )
    fetch_time = time.time() - start_time
    print(f"  Time: {fetch_time:.2f} seconds")

    print("\nSecond load (from cache)...")
    start_time = time.time()
    rf_mgr.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type='3month',
        frequency='monthly',
        use_cache=True
    )
    cache_time = time.time() - start_time
    print(f"  Time: {cache_time:.2f} seconds")
    print(f"  Speedup: {fetch_time/cache_time:.1f}x faster")

    # Usage guide
    print("\n" + "=" * 80)
    print("Usage Guide for ESG Beta Calculation")
    print("=" * 80)
    print("""
To calculate ESG beta with excess returns:

1. Load risk-free rate:
   rf_mgr = RiskFreeRateManager(tiingo=tiingo)
   rf_data = rf_mgr.load_risk_free_rate(
       start_date='2014-01-01',
       end_date='2024-12-31',
       rate_type='3month',
       frequency='monthly'
   )

2. Calculate excess returns for stock and market:
   stock_excess = rf_mgr.calculate_excess_returns(
       returns=stock_returns,
       dates=stock_dates,
       frequency='monthly'
   )

   market_excess = rf_mgr.calculate_excess_returns(
       returns=market_returns,
       dates=market_dates,
       frequency='monthly'
   )

3. Run OLS regression:
   import statsmodels.api as sm
   X = sm.add_constant(market_excess)
   model = sm.OLS(stock_excess, X).fit()
   beta = model.params['market_excess']

4. For ESG beta, replace market_excess with esg_factor_excess returns.

Cache files are stored in: data/curated/risk_free_rate/
    """)

    print("\n" + "=" * 80)
    print("✓ Demonstration Complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
