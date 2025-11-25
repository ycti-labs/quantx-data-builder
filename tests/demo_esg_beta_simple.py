"""
Simple ESG Beta Demonstration with Excess Returns

Demonstrates how to calculate ESG beta using real risk-free rates from FRED.
This example uses a simplified approach focusing on the correlation between
stock excess returns and changes in ESG scores.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import numpy as np
import pandas as pd
import statsmodels.api as sm
from tiingo import TiingoClient

from core.config import Config
from market import ESGManager, PriceManager, RiskFreeRateManager
from universe import SP500Universe


def calculate_esg_beta_demo(symbol: str, start_date: str, end_date: str):
    """Calculate ESG beta for a single stock."""

    print(f"\n{'=' * 80}")
    print(f"ESG Beta Calculation for {symbol}")
    print(f"{'=' * 80}")

    # Initialize components
    config = Config("config/settings.yaml")
    universe = SP500Universe()

    tiingo = TiingoClient({
        'api_key': config.get('fetcher.tiingo.api_key'),
        'session': True
    })

    price_mgr = PriceManager(tiingo=tiingo, universe=universe)
    esg_mgr = ESGManager(universe=universe)

    fred_api_key = config.get('fetcher.fred.api_key')
    rf_mgr = RiskFreeRateManager(fred_api_key=fred_api_key)

    # 1. Load stock price data (monthly)
    print(f"\n1. Loading monthly price data for {symbol}...")
    price_data = price_mgr.load_price_data(
        symbol=symbol,
        frequency='monthly',
        start_date=start_date,
        end_date=end_date
    )

    if price_data is None or len(price_data) == 0:
        print(f"❌ No price data available for {symbol}")
        return None

    print(f"   ✓ Loaded {len(price_data)} months of price data")

    # 2. Calculate stock returns
    print(f"\n2. Calculating stock returns...")
    price_data['date'] = pd.to_datetime(price_data['date']).dt.date
    price_data = price_data.sort_values('date')
    stock_returns = price_data['adj_close'].pct_change().dropna()
    return_dates = price_data['date'].iloc[1:].reset_index(drop=True)
    stock_returns = stock_returns.reset_index(drop=True)

    print(f"   ✓ Calculated {len(stock_returns)} monthly returns")
    print(f"   Mean return: {stock_returns.mean()*100:.2f}%")
    print(f"   Std dev: {stock_returns.std()*100:.2f}%")

    # 3. Load risk-free rate
    print(f"\n3. Loading risk-free rates (3-month T-Bill)...")
    rf_data = rf_mgr.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type='3month',
        frequency='monthly'
    )

    print(f"   ✓ Loaded {len(rf_data)} months of risk-free rates")
    print(f"   Mean rate: {rf_data['rate'].mean():.2f}%")
    print(f"   Range: {rf_data['rate'].min():.2f}% - {rf_data['rate'].max():.2f}%")

    # 4. Calculate stock excess returns
    print(f"\n4. Calculating excess returns (returns - risk-free rate)...")

    # Create return series with dates
    returns_df = pd.DataFrame({
        'date': return_dates,
        'return': stock_returns
    })

    stock_excess = rf_mgr.calculate_excess_returns(
        returns=returns_df['return'],
        dates=returns_df['date'],
        frequency='monthly'
    )

    print(f"   ✓ Calculated {len(stock_excess)} excess returns")
    print(f"   Mean excess return: {stock_excess.mean()*100:.2f}%")

    # 5. Load ESG data
    print(f"\n5. Loading ESG data...")
    esg_data = esg_mgr.load_esg_data(
        ticker=symbol,
        start_date=start_date,
        end_date=end_date
    )

    if esg_data.empty or 'ESG Score' not in esg_data.columns:
        print(f"❌ No ESG data available for {symbol}")
        return None

    print(f"   ✓ Loaded {len(esg_data)} months of ESG data")

    # Align ESG data to end-of-month dates
    esg_data['date'] = pd.to_datetime(esg_data['date'])
    esg_data['eom_date'] = esg_data['date'] + pd.offsets.MonthEnd(0)
    esg_data['eom_date'] = esg_data['eom_date'].dt.date

    # Calculate ESG score changes (month-over-month)
    esg_data = esg_data.sort_values('date')
    esg_data['esg_change'] = esg_data['ESG Score'].pct_change()

    print(f"   Mean ESG Score: {esg_data['ESG Score'].mean():.2f}")
    print(f"   ESG Score range: {esg_data['ESG Score'].min():.2f} - {esg_data['ESG Score'].max():.2f}")

    # 6. Align all data
    print(f"\n6. Aligning data on end-of-month dates...")

    # Merge excess returns with ESG changes
    analysis_df = returns_df.copy()
    analysis_df['excess_return'] = stock_excess.values

    # Merge with ESG data
    esg_subset = esg_data[['eom_date', 'ESG Score', 'esg_change']].copy()
    esg_subset = esg_subset.rename(columns={'eom_date': 'date'})

    analysis_df = analysis_df.merge(esg_subset, on='date', how='inner')
    analysis_df = analysis_df.dropna()

    print(f"   ✓ Aligned dataset: {len(analysis_df)} observations")

    if len(analysis_df) < 24:
        print(f"❌ Insufficient data for regression: {len(analysis_df)} months (need >= 24)")
        return None

    # 7. Calculate ESG Beta using OLS regression
    print(f"\n7. Calculating ESG Beta (OLS Regression)...")
    print(f"   Model: excess_return = alpha + beta_ESG * esg_change + epsilon")

    # Prepare regression data
    X = analysis_df['esg_change']
    y = analysis_df['excess_return']

    # Add constant for intercept
    X_with_const = sm.add_constant(X)

    # Run regression
    model = sm.OLS(y, X_with_const).fit()

    # Extract results
    beta_esg = model.params['esg_change']
    alpha = model.params['const']
    r_squared = model.rsquared
    p_value = model.pvalues['esg_change']

    print(f"\n   Results:")
    print(f"   ─────────────────────────────────────")
    print(f"   ESG Beta:      {beta_esg:>8.4f}")
    print(f"   Alpha:         {alpha*100:>8.4f}%")
    print(f"   R-squared:     {r_squared:>8.4f}")
    print(f"   P-value:       {p_value:>8.4f}")
    print(f"   Observations:  {len(analysis_df):>8d}")
    print(f"   ─────────────────────────────────────")

    # Interpretation
    print(f"\n   Interpretation:")
    if abs(p_value) < 0.05:
        direction = "positively" if beta_esg > 0 else "negatively"
        print(f"   ✓ ESG changes are {direction} correlated with excess returns (statistically significant)")
    else:
        print(f"   ⚠ ESG changes show no statistically significant correlation with excess returns")

    if beta_esg > 0:
        print(f"   → When ESG score improves by 1%, excess returns increase by {beta_esg:.4f}%")
    else:
        print(f"   → When ESG score improves by 1%, excess returns decrease by {abs(beta_esg):.4f}%")

    # Show sample data
    print(f"\n8. Sample Data (First 36 months):")
    print(f"{'=' * 80}")
    sample_df = analysis_df.head(36)[['date', 'return', 'excess_return', 'ESG Score', 'esg_change']]
    sample_df['return'] = sample_df['return'] * 100
    sample_df['excess_return'] = sample_df['excess_return'] * 100
    sample_df['esg_change'] = sample_df['esg_change'] * 100
    sample_df.columns = ['Date', 'Return(%)', 'Excess Return(%)', 'ESG Score', 'ESG Change(%)']
    print(sample_df.to_string(index=False, float_format=lambda x: f'{x:.2f}'))

    return {
        'symbol': symbol,
        'beta_esg': beta_esg,
        'alpha': alpha,
        'r_squared': r_squared,
        'p_value': p_value,
        'observations': len(analysis_df)
    }


def main():
    """Main demonstration function."""

    print("=" * 80)
    print("ESG Beta Calculation with Excess Returns - Demonstration")
    print("=" * 80)
    print("\nThis demo calculates ESG beta using:")
    print("  • Real FRED risk-free rates (3-month T-Bill)")
    print("  • Stock excess returns (returns - risk-free rate)")
    print("  • ESG score changes (month-over-month)")
    print("  • OLS regression: excess_return = alpha + beta_ESG * esg_change")

    # Calculate for multiple stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    start_date = '2020-01-01'
    end_date = '2024-12-31'

    results = []
    for symbol in symbols:
        result = calculate_esg_beta_demo(symbol, start_date, end_date)
        if result:
            results.append(result)

    # Summary
    if results:
        print(f"\n\n{'=' * 80}")
        print("SUMMARY: ESG Beta Results")
        print(f"{'=' * 80}")

        summary_df = pd.DataFrame(results)
        summary_df['beta_esg'] = summary_df['beta_esg'].apply(lambda x: f'{x:.4f}')
        summary_df['alpha'] = summary_df['alpha'].apply(lambda x: f'{x*100:.4f}%')
        summary_df['r_squared'] = summary_df['r_squared'].apply(lambda x: f'{x:.4f}')
        summary_df['p_value'] = summary_df['p_value'].apply(lambda x: f'{x:.4f}')

        summary_df.columns = ['Symbol', 'ESG Beta', 'Alpha', 'R²', 'P-value', 'N']
        print(summary_df.to_string(index=False))

        print(f"\n{'=' * 80}")
        print("Key Takeaways:")
        print(f"{'=' * 80}")
        print("• ESG Beta measures sensitivity of excess returns to ESG score changes")
        print("• Positive beta: Stock performs better when ESG improves")
        print("• Negative beta: Stock performs worse when ESG improves (or vice versa)")
        print("• R² shows how much variance is explained by ESG changes")
        print("• P-value < 0.05 indicates statistical significance")
        print("• Excess returns account for risk-free rate, isolating excess performance")


if __name__ == "__main__":
    main()
