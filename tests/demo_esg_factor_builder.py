"""
ESG Factor Builder Demo

Demonstrates how to use ESGFactorBuilder to construct quantitative ESG factors
from raw ESG scores for portfolio construction and backtesting.

Usage:
    python examples/demo_esg_factor_builder.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import logging
from datetime import datetime

import pandas as pd

from market import ESGFactorBuilder, ESGManager
from universe import SP500Universe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

start_date = '2014-01-01'
end_date = '2024-12-31'


def demo_basic_factors():
    """
    Demo 1: Basic factor construction for a small set of stocks

    Shows how to:
    - Initialize ESGManager and ESGFactorBuilder
    - Build factors for a few tickers
    - Inspect cross-sectional factors (rankings, z-scores, deciles)
    """
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 1: Basic Factor Construction")
    logger.info("=" * 80)

    # Initialize components
    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Select diverse tickers across sectors (from notebook that we know have ESG data)
    tickers = [
        'AAPL',  # Tech
        'MSFT',  # Tech
        'GOOGL', # Tech
        'META',  # Tech
        'AMZN'   # Tech/Consumer
    ]

    # Build factors for recent period
    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        include_pillars=True,
        include_momentum=True,
        momentum_windows=[6, 12],
        include_composite=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Display sample of results
    logger.info("\n📊 Sample Factor Data (Latest Month):")
    logger.info(f"Available columns: {factors.columns.tolist()}")

    latest_date = factors['date'].max()
    display_cols = ['ticker', 'esg_score']

    # Add factor columns if they exist
    for col in ['esg_score_zscore', 'esg_score_pctrank', 'esg_score_decile']:
        if col in factors.columns:
            display_cols.append(col)

    latest_data = factors[factors['date'] == latest_date][display_cols].sort_values('esg_score', ascending=False)

    print("\n" + latest_data.to_string(index=False))

    # Show factor summary statistics
    logger.info("\n📈 Factor Summary Statistics:")
    summary = builder.get_factor_summary(factors)

    print(f"\nTotal Records: {summary['total_records']:,}")
    print(f"Tickers: {summary['num_tickers']}")
    print(f"Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"\nFactor Columns Generated:")
    for col in summary['factor_columns']:
        stats = summary['factor_statistics'][col]
        print(f"  {col}:")
        if stats['mean'] is not None:
            print(f"    Mean: {stats['mean']:.3f}, Std: {stats['std']:.3f}")
            print(f"    Range: [{stats['min']:.3f}, {stats['max']:.3f}]")
        else:
            print(f"    Type: {stats.get('note', 'categorical')}")
        print(f"    Coverage: {stats['coverage']*100:.1f}%")

    return factors


def demo_momentum_analysis():
    """
    Demo 2: ESG Momentum Analysis

    Shows how to:
    - Focus on momentum factors
    - Identify ESG improvers vs. decliners
    - Analyze momentum distribution
    """
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 2: ESG Momentum Analysis")
    logger.info("=" * 80)

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Larger set of tickers for momentum analysis
    tickers = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',  # Big Tech
        'JPM', 'BAC', 'WFC', 'GS', 'MS',           # Financials
        'XOM', 'CVX', 'COP', 'SLB',                # Energy
        'JNJ', 'UNH', 'PFE', 'ABBV',               # Healthcare
        'PG', 'KO', 'PEP', 'WMT'                   # Consumer
    ]

    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        include_pillars=False,  # Focus on ESG score only
        include_momentum=True,
        momentum_windows=[12],  # 12-month momentum
        include_composite=False
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Analyze latest momentum
    latest_date = factors['date'].max()
    latest_momentum = factors[factors['date'] == latest_date][
        ['ticker', 'esg_score', 'ESG_momentum_12m']
    ].dropna()

    latest_momentum = latest_momentum.sort_values('ESG_momentum_12m', ascending=False)

    logger.info("\n🚀 Top 5 ESG Improvers (12-month momentum):")
    print("\n" + latest_momentum.head(5).to_string(index=False))

    logger.info("\n📉 Top 5 ESG Decliners (12-month momentum):")
    print("\n" + latest_momentum.tail(5).to_string(index=False))

    # Momentum distribution statistics
    momentum_col = 'ESG_momentum_12m'
    momentum_stats = factors[momentum_col].describe()

    logger.info(f"\n📊 ESG Momentum Distribution ({momentum_col}):")
    print(momentum_stats)

    return factors


def demo_composite_factors():
    """
    Demo 3: Composite Factor Construction

    Shows how to:
    - Build quality-momentum composite (level + change)
    - Build pillar-weighted composite (custom E/S/G weights)
    - Compare different composite approaches
    """
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 3: Composite Factor Construction")
    logger.info("=" * 80)

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    tickers = [
        'AAPL', 'MSFT', 'GOOGL', 'TSLA',    # Tech
        'JPM', 'BAC', 'GS',                  # Finance
        'XOM', 'CVX',                        # Energy
        'JNJ', 'UNH'                         # Healthcare
    ]

    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        include_pillars=True,
        include_momentum=True,
        momentum_windows=[12],
        include_composite=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Compare composite factors at latest date
    latest_date = factors['date'].max()
    composite_cols = ['ticker', 'esg_score', 'ESG_quality_momentum', 'ESG_composite']

    if all(col in factors.columns for col in composite_cols):
        latest_composites = factors[factors['date'] == latest_date][composite_cols]
        latest_composites = latest_composites.sort_values('ESG_quality_momentum', ascending=False)

        logger.info("\n🎯 Composite Factor Rankings (Latest Month):")
        print("\n" + latest_composites.to_string(index=False))

        # Correlation between composite approaches
        corr = latest_composites[['ESG_quality_momentum', 'ESG_composite']].corr()
        logger.info(f"\n📐 Correlation between composite factors:")
        print(corr)
    else:
        logger.warning("Some composite factor columns not found")

    return factors


def demo_factor_persistence():
    """
    Demo 4: Save and Load Factors

    Shows how to:
    - Save factor dataset to Parquet
    - Load previously saved factors
    - Use saved factors for backtesting
    """
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 4: Factor Persistence")
    logger.info("=" * 80)

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Build factors for a moderate set of stocks
    tickers = ['AAPL', 'MSFT', 'JPM', 'XOM', 'JNJ', 'PG', 'TSLA', 'NVDA']

    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        include_pillars=True,
        include_momentum=True,
        include_composite=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Save factors with custom name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = builder.save_factors(
        factors,
        output_name=f"demo_factors_{timestamp}"
    )

    logger.info(f"\n💾 Factors saved to: {output_path}")

    # Load factors back
    loaded_factors = builder.load_factors(output_path.name)

    # Verify integrity
    assert len(loaded_factors) == len(factors), "Row count mismatch"
    assert list(loaded_factors.columns) == list(factors.columns), "Column mismatch"

    logger.info("✅ Factor persistence verified - save/load successful")

    # Show what can be done with loaded factors
    logger.info("\n📊 Using Loaded Factors for Analysis:")

    # Example: Find stocks in top decile of quality-momentum
    if 'ESG_quality_momentum_decile' in loaded_factors.columns:
        latest_date = loaded_factors['date'].max()
        top_decile = loaded_factors[
            (loaded_factors['date'] == latest_date) &
            (loaded_factors['ESG_quality_momentum_decile'] == 10)
        ]['ticker'].tolist()

        logger.info(f"Top Decile Quality-Momentum Stocks: {top_decile}")

    return loaded_factors


def demo_portfolio_construction():
    """
    Demo 5: Portfolio Construction with ESG Factors

    Shows how to:
    - Use decile rankings for long-short portfolios
    - Create ESG-tilted portfolios
    - Rebalance based on factor signals
    """
    logger.info("\n" + "=" * 80)
    logger.info("DEMO 5: Portfolio Construction with ESG Factors")
    logger.info("=" * 80)

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Use larger universe for portfolio construction
    tickers = [
        # Tech
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'ORCL', 'ADBE', 'CRM',
        # Finance
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW',
        # Healthcare
        'JNJ', 'UNH', 'PFE', 'ABBV', 'TMO', 'MRK', 'LLY',
        # Consumer
        'PG', 'KO', 'PEP', 'WMT', 'HD', 'MCD', 'NKE',
        # Energy
        'XOM', 'CVX', 'COP', 'SLB'
    ]

    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        include_pillars=False,
        include_momentum=False,
        include_composite=False
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Example: Construct quarterly rebalancing portfolios
    logger.info("\n📅 Quarterly Portfolio Rebalancing Example:")

    # Get quarterly end dates
    factors['quarter'] = pd.PeriodIndex(factors['date'], freq='Q')
    rebalance_dates = factors.groupby('quarter')['date'].max()

    for i, rebal_date in enumerate(rebalance_dates.tail(4)):  # Last 4 quarters
        quarter_data = factors[factors['date'] == rebal_date].copy()

        if 'esg_score_decile' not in quarter_data.columns:
            continue

        # Long portfolio: Top 3 deciles (high ESG)
        long_portfolio = quarter_data[
            quarter_data['esg_score_decile'].isin([8, 9, 10])
        ]['ticker'].tolist()

        # Short portfolio: Bottom 3 deciles (low ESG)
        short_portfolio = quarter_data[
            quarter_data['esg_score_decile'].isin([1, 2, 3])
        ]['ticker'].tolist()

        logger.info(f"\n  Rebalance Date: {rebal_date.date()}")
        logger.info(f"    Long (High ESG):  {len(long_portfolio)} stocks - {long_portfolio[:5]}...")
        logger.info(f"    Short (Low ESG):  {len(short_portfolio)} stocks - {short_portfolio[:5]}...")
        logger.info(f"    Net Exposure: {len(long_portfolio) - len(short_portfolio)} stocks")

    logger.info("\n💡 Next Steps:")
    logger.info("  - Combine with PriceManager to calculate portfolio returns")
    logger.info("  - Use RiskFreeRateManager for Sharpe ratio calculation")
    logger.info("  - Implement transaction costs and rebalancing rules")
    logger.info("  - Backtest long-short ESG factor portfolio")

    return factors


def main():
    """Run all demos"""
    logger.info("\n" + "=" * 80)
    logger.info("ESG FACTOR BUILDER DEMO")
    logger.info("=" * 80)
    logger.info("Demonstrating ESG factor construction capabilities")
    logger.info("=" * 80)

    try:
        # Run demos in sequence
        demo_basic_factors()
        demo_momentum_analysis()
        demo_composite_factors()
        demo_factor_persistence()
        demo_portfolio_construction()

        logger.info("\n" + "=" * 80)
        logger.info("✅ ALL DEMOS COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("\n📚 Key Takeaways:")
        logger.info("  1. ESGFactorBuilder transforms raw ESG scores into tradeable factors")
        logger.info("  2. Cross-sectional factors (z-scores, rankings, deciles) for relative comparison")
        logger.info("  3. Time-series factors (momentum, trends) capture ESG improvement")
        logger.info("  4. Composite factors combine multiple dimensions (quality + momentum)")
        logger.info("  5. Factors can be persisted and loaded for backtesting")
        logger.info("  6. Decile rankings enable long-short portfolio construction")
        logger.info("\n🚀 Ready to integrate with your quantitative research workflow!")

    except Exception as e:
        logger.error(f"\n❌ Demo failed: {e}", exc_info=True)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
