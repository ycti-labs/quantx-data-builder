"""
ESG Beta Calculation for Entire S&P 500 Historical Universe

Demonstrates calculating ESG beta for all historical S&P 500 members using:
- Real FRED risk-free rates (3-month T-Bill)
- Stock excess returns (returns - risk-free rate)
- ESG score changes (month-over-month)
- OLS regression: excess_return = alpha + beta_ESG * esg_change

Generates comprehensive summary statistics and distributions.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd
import statsmodels.api as sm
from tiingo import TiingoClient
from tqdm import tqdm

from core.config import Config
from market import ESGManager, PriceManager, RiskFreeRateManager
from universe import SP500Universe


def calculate_esg_beta_single(symbol: str, start_date: str, end_date: str) -> dict:
    """Calculate ESG beta for a single stock (parallelizable)."""

    try:
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

        # Load stock price data (monthly)
        price_data = price_mgr.load_price_data(
            symbol=symbol,
            frequency='monthly',
            start_date=start_date,
            end_date=end_date
        )

        if price_data is None or len(price_data) == 0:
            return {'symbol': symbol, 'status': 'no_price_data', 'error': 'No price data available'}

        # Calculate stock returns
        price_data['date'] = pd.to_datetime(price_data['date']).dt.date
        price_data = price_data.sort_values('date')
        stock_returns = price_data['adj_close'].pct_change().dropna()
        return_dates = price_data['date'].iloc[1:].reset_index(drop=True)
        stock_returns = stock_returns.reset_index(drop=True)

        # Load risk-free rate
        rf_data = rf_mgr.load_risk_free_rate(
            start_date=start_date,
            end_date=end_date,
            rate_type='3month',
            frequency='monthly'
        )

        # Calculate stock excess returns
        returns_df = pd.DataFrame({
            'date': return_dates,
            'return': stock_returns
        })

        stock_excess = rf_mgr.calculate_excess_returns(
            returns=returns_df['return'],
            dates=returns_df['date'],
            frequency='monthly'
        )

        # Load ESG data
        esg_data = esg_mgr.load_esg_data(
            ticker=symbol,
            start_date=start_date,
            end_date=end_date
        )

        if esg_data.empty or 'ESG Score' not in esg_data.columns:
            return {'symbol': symbol, 'status': 'no_esg_data', 'error': 'No ESG data available'}

        # Align ESG data to end-of-month dates
        esg_data['date'] = pd.to_datetime(esg_data['date'])
        esg_data['eom_date'] = esg_data['date'] + pd.offsets.MonthEnd(0)
        esg_data['eom_date'] = esg_data['eom_date'].dt.date

        # Calculate ESG score changes
        esg_data = esg_data.sort_values('date')
        esg_data['esg_change'] = esg_data['ESG Score'].pct_change()

        # Align all data
        analysis_df = returns_df.copy()
        analysis_df['excess_return'] = stock_excess.values

        esg_subset = esg_data[['eom_date', 'ESG Score', 'esg_change']].copy()
        esg_subset = esg_subset.rename(columns={'eom_date': 'date'})

        analysis_df = analysis_df.merge(esg_subset, on='date', how='inner')
        analysis_df = analysis_df.dropna()

        if len(analysis_df) < 24:
            return {
                'symbol': symbol,
                'status': 'insufficient_data',
                'error': f'Only {len(analysis_df)} months available (need >= 24)',
                'observations': len(analysis_df)
            }

        # Calculate ESG Beta using OLS regression
        X = analysis_df['esg_change']
        y = analysis_df['excess_return']
        X_with_const = sm.add_constant(X)

        model = sm.OLS(y, X_with_const).fit()

        # Calculate additional statistics
        mean_return = returns_df['return'].mean()
        std_return = returns_df['return'].std()
        mean_esg = esg_data['ESG Score'].mean()
        std_esg = esg_data['ESG Score'].std()

        return {
            'symbol': symbol,
            'status': 'success',
            'beta_esg': model.params['esg_change'],
            'alpha': model.params['const'],
            'r_squared': model.rsquared,
            'p_value': model.pvalues['esg_change'],
            'observations': len(analysis_df),
            'mean_return': mean_return,
            'std_return': std_return,
            'mean_excess_return': analysis_df['excess_return'].mean(),
            'mean_esg_score': mean_esg,
            'std_esg_score': std_esg,
            'esg_score_range': esg_data['ESG Score'].max() - esg_data['ESG Score'].min(),
            'error': None
        }

    except Exception as e:
        return {
            'symbol': symbol,
            'status': 'error',
            'error': str(e)
        }


def main():
    """Main demonstration function."""

    print("=" * 100)
    print("ESG Beta Calculation - Full S&P 500 Historical Universe")
    print("=" * 100)
    print("\nCalculating ESG betas for all historical S&P 500 members using:")
    print("  • Real FRED risk-free rates (3-month T-Bill)")
    print("  • Stock excess returns (returns - risk-free rate)")
    print("  • ESG score changes (month-over-month)")
    print("  • OLS regression: excess_return = alpha + beta_ESG * esg_change")
    print()

    # Setup
    start_date = '2020-01-01'
    end_date = '2024-12-31'

    config = Config("config/settings.yaml")
    universe = SP500Universe()

    # Get all historical tickers
    print(f"Loading S&P 500 historical members from {start_date} to {end_date}...")

    # Load membership intervals
    intervals_path = universe.get_membership_path(mode='intervals') / f"{universe.name}_membership_intervals.parquet"

    if not intervals_path.exists():
        print(f"❌ Membership file not found: {intervals_path}")
        print(f"Please run: python -m programs.build_sp500_membership first")
        return

    membership_df = pd.read_parquet(intervals_path)

    # Filter for the period
    membership_df['start_date'] = pd.to_datetime(membership_df['start_date']).dt.date
    membership_df['end_date'] = pd.to_datetime(membership_df['end_date']).dt.date
    period_start = pd.to_datetime(start_date).date()
    period_end = pd.to_datetime(end_date).date()

    # Get tickers that were members at any point during the period
    active_members = membership_df[
        (membership_df['start_date'] <= period_end) &
        (membership_df['end_date'] >= period_start)
    ]

    unique_tickers = sorted(active_members['ticker'].unique())
    print(f"Found {len(unique_tickers)} unique tickers with S&P 500 membership during the period")
    print()

    # Calculate ESG betas in parallel
    print(f"Calculating ESG betas (this may take a few minutes)...")
    print(f"Using parallel processing for faster execution...")
    print()

    results = []
    max_workers = 4  # Adjust based on your CPU cores

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(calculate_esg_beta_single, symbol, start_date, end_date): symbol
            for symbol in unique_tickers
        }

        # Collect results with progress bar
        with tqdm(total=len(unique_tickers), desc="Processing tickers") as pbar:
            for future in as_completed(future_to_symbol):
                result = future.result()
                results.append(result)
                pbar.update(1)

    # Analyze results
    print()
    print("=" * 100)
    print("PROCESSING SUMMARY")
    print("=" * 100)

    status_counts = pd.Series([r['status'] for r in results]).value_counts()
    print(f"\nTotal tickers processed: {len(results)}")
    for status, count in status_counts.items():
        print(f"  {status}: {count} ({count/len(results)*100:.1f}%)")

    # Filter successful results
    successful = [r for r in results if r['status'] == 'success']

    if not successful:
        print("\n❌ No successful ESG beta calculations. Please check data availability.")
        return

    print(f"\n✓ Successfully calculated ESG betas for {len(successful)} stocks")
    print()

    # Create results DataFrame
    results_df = pd.DataFrame(successful)

    # Statistical Summary
    print("=" * 100)
    print("ESG BETA STATISTICS")
    print("=" * 100)

    print(f"\nESG Beta Distribution:")
    print(f"  Mean:        {results_df['beta_esg'].mean():>8.4f}")
    print(f"  Median:      {results_df['beta_esg'].median():>8.4f}")
    print(f"  Std Dev:     {results_df['beta_esg'].std():>8.4f}")
    print(f"  Min:         {results_df['beta_esg'].min():>8.4f}")
    print(f"  25th pctl:   {results_df['beta_esg'].quantile(0.25):>8.4f}")
    print(f"  75th pctl:   {results_df['beta_esg'].quantile(0.75):>8.4f}")
    print(f"  Max:         {results_df['beta_esg'].max():>8.4f}")

    # Beta sign analysis
    positive_betas = (results_df['beta_esg'] > 0).sum()
    negative_betas = (results_df['beta_esg'] < 0).sum()
    print(f"\nBeta Sign:")
    print(f"  Positive: {positive_betas} ({positive_betas/len(results_df)*100:.1f}%)")
    print(f"  Negative: {negative_betas} ({negative_betas/len(results_df)*100:.1f}%)")

    # Statistical significance
    significant = (results_df['p_value'] < 0.05).sum()
    print(f"\nStatistical Significance (p < 0.05):")
    print(f"  Significant:     {significant} ({significant/len(results_df)*100:.1f}%)")
    print(f"  Not Significant: {len(results_df)-significant} ({(len(results_df)-significant)/len(results_df)*100:.1f}%)")

    # R-squared distribution
    print(f"\nR-squared Distribution:")
    print(f"  Mean:        {results_df['r_squared'].mean():>8.4f}")
    print(f"  Median:      {results_df['r_squared'].median():>8.4f}")
    print(f"  Max:         {results_df['r_squared'].max():>8.4f}")

    # Top and bottom ESG betas
    print()
    print("=" * 100)
    print("TOP 10 HIGHEST ESG BETAS")
    print("=" * 100)
    top_10 = results_df.nlargest(10, 'beta_esg')[
        ['symbol', 'beta_esg', 'alpha', 'r_squared', 'p_value', 'observations']
    ].copy()
    top_10['alpha'] = top_10['alpha'] * 100
    top_10.columns = ['Symbol', 'ESG Beta', 'Alpha(%)', 'R²', 'P-value', 'N']
    print(top_10.to_string(index=False, float_format=lambda x: f'{x:.4f}' if abs(x) < 100 else f'{x:.0f}'))

    print()
    print("=" * 100)
    print("TOP 10 LOWEST ESG BETAS")
    print("=" * 100)
    bottom_10 = results_df.nsmallest(10, 'beta_esg')[
        ['symbol', 'beta_esg', 'alpha', 'r_squared', 'p_value', 'observations']
    ].copy()
    bottom_10['alpha'] = bottom_10['alpha'] * 100
    bottom_10.columns = ['Symbol', 'ESG Beta', 'Alpha(%)', 'R²', 'P-value', 'N']
    print(bottom_10.to_string(index=False, float_format=lambda x: f'{x:.4f}' if abs(x) < 100 else f'{x:.0f}'))

    # Most statistically significant
    print()
    print("=" * 100)
    print("TOP 10 MOST STATISTICALLY SIGNIFICANT ESG BETAS (Lowest P-values)")
    print("=" * 100)
    most_sig = results_df.nsmallest(10, 'p_value')[
        ['symbol', 'beta_esg', 'alpha', 'r_squared', 'p_value', 'observations']
    ].copy()
    most_sig['alpha'] = most_sig['alpha'] * 100
    most_sig.columns = ['Symbol', 'ESG Beta', 'Alpha(%)', 'R²', 'P-value', 'N']
    print(most_sig.to_string(index=False, float_format=lambda x: f'{x:.4f}' if abs(x) < 100 else f'{x:.0f}'))

    # Return characteristics
    print()
    print("=" * 100)
    print("RETURN CHARACTERISTICS")
    print("=" * 100)
    print(f"\nMean Monthly Return (across all stocks):")
    print(f"  Mean:   {results_df['mean_return'].mean()*100:>6.2f}%")
    print(f"  Median: {results_df['mean_return'].median()*100:>6.2f}%")

    print(f"\nMean Excess Return (across all stocks):")
    print(f"  Mean:   {results_df['mean_excess_return'].mean()*100:>6.2f}%")
    print(f"  Median: {results_df['mean_excess_return'].median()*100:>6.2f}%")

    # ESG characteristics
    print()
    print("=" * 100)
    print("ESG SCORE CHARACTERISTICS")
    print("=" * 100)
    print(f"\nMean ESG Score (across all stocks):")
    print(f"  Mean:   {results_df['mean_esg_score'].mean():>6.2f}")
    print(f"  Median: {results_df['mean_esg_score'].median():>6.2f}")
    print(f"  Range:  {results_df['mean_esg_score'].min():.2f} - {results_df['mean_esg_score'].max():.2f}")

    print(f"\nESG Score Volatility (std dev across time):")
    print(f"  Mean:   {results_df['std_esg_score'].mean():>6.2f}")
    print(f"  Median: {results_df['std_esg_score'].median():>6.2f}")

    # Save results
    output_dir = Path("data/results/esg_betas")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"esg_beta_universe_{timestamp}.parquet"

    # Save all results including errors
    all_results_df = pd.DataFrame(results)
    all_results_df.to_parquet(output_file, index=False)

    print()
    print("=" * 100)
    print(f"✓ Results saved to: {output_file}")
    print("=" * 100)

    # Final summary
    print()
    print("=" * 100)
    print("KEY TAKEAWAYS")
    print("=" * 100)
    print(f"• Processed {len(results)} S&P 500 historical members")
    print(f"• Successfully calculated ESG betas for {len(successful)} stocks ({len(successful)/len(results)*100:.1f}%)")
    print(f"• Mean ESG Beta: {results_df['beta_esg'].mean():.4f} (median: {results_df['beta_esg'].median():.4f})")
    print(f"• {significant} stocks ({significant/len(results_df)*100:.1f}%) show statistically significant ESG betas (p < 0.05)")
    print(f"• {positive_betas} stocks ({positive_betas/len(results_df)*100:.1f}%) have positive ESG betas")
    print(f"• Mean R²: {results_df['r_squared'].mean():.4f} - ESG changes explain limited variance on average")
    print()
    print("Interpretation:")
    print("• ESG betas measure sensitivity of excess returns to ESG score changes")
    print("• Positive beta: Stock performs better when ESG improves")
    print("• Most stocks show weak or insignificant ESG betas, suggesting:")
    print("  - ESG effects may require longer time horizons")
    print("  - Sector-specific or portfolio-level analysis may be more appropriate")
    print("  - ESG integration might work through other channels (e.g., risk reduction)")


if __name__ == "__main__":
    main()
