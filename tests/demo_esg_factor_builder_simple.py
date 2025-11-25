"""
ESG Factor Builder Simple Demo

Simple demonstration of ESGFactorBuilder with synthetic data to show capabilities.
This avoids data loading issues and focuses on the factor construction logic.

Usage:
    python examples/demo_esg_factor_builder_simple.py
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import logging
from datetime import datetime

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_synthetic_esg_data():
    """Create synthetic ESG data for demonstration"""
    np.random.seed(42)

    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'BAC', 'XOM']
    dates = pd.date_range('2014-01-01', '2024-12-01', freq='MS') + pd.offsets.MonthEnd(0)

    data = []
    for ticker in tickers:
        # Each ticker has a base ESG score with some variation
        base_esg = np.random.uniform(50, 90)
        base_env = np.random.uniform(40, 85)
        base_soc = np.random.uniform(45, 90)
        base_gov = np.random.uniform(50, 95)

        for date in dates:
            # Add time-based trend and noise
            months_elapsed = (date - dates[0]).days / 30
            trend = months_elapsed * 0.1  # Slight upward trend
            noise = np.random.normal(0, 3)

            esg_score = np.clip(base_esg + trend + noise, 0, 100)
            env_score = np.clip(base_env + trend * 0.8 + np.random.normal(0, 4), 0, 100)
            soc_score = np.clip(base_soc + trend * 1.2 + np.random.normal(0, 3), 0, 100)
            gov_score = np.clip(base_gov + trend * 0.5 + np.random.normal(0, 2), 0, 100)

            data.append({
                'ticker': ticker,
                'date': date,
                'esg_score': esg_score,
                'env_score': env_score,
                'soc_score': soc_score,
                'gov_score': gov_score
            })

    df = pd.DataFrame(data)
    logger.info(f"✓ Created synthetic ESG data: {len(df):,} records for {df['ticker'].nunique()} tickers")
    return df


def demo_cross_sectional_factors():
    """Demo 1: Cross-sectional factor construction"""
    from market.esg_factor_builder import ESGFactorBuilder

    logger.info("\n" + "=" * 80)
    logger.info("DEMO 1: Cross-Sectional ESG Factors")
    logger.info("=" * 80)

    # Create synthetic data
    esg_data = create_synthetic_esg_data()

    # Mock builder (we'll use the methods directly)
    class MockBuilder:
        logger = logging.getLogger(__name__)

    builder = MockBuilder()

    # Calculate cross-sectional factors
    from market.esg_factor_builder import ESGFactorBuilder
    factors = ESGFactorBuilder.calculate_cross_sectional_factors(
        builder, esg_data, 'esg_score'
    )

    # Show latest month results
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date][
        ['ticker', 'esg_score', 'esg_score_zscore', 'esg_score_pctrank', 'esg_score_decile']
    ].sort_values('esg_score', ascending=False)

    logger.info(f"\n📊 Cross-Sectional Factors ({latest_date.date()}):")
    print("\n" + latest.to_string(index=False))

    logger.info("\n✅ Cross-sectional factors successfully calculated!")
    logger.info("   - Z-scores normalize within each date")
    logger.info("   - Percentile ranks (0-100) show relative position")
    logger.info("   - Deciles (1-10) enable portfolio formation")

    return factors


def demo_momentum_factors():
    """Demo 2: ESG Momentum"""
    from market.esg_factor_builder import ESGFactorBuilder

    logger.info("\n" + "=" * 80)
    logger.info("DEMO 2: ESG Momentum Factors")
    logger.info("=" * 80)

    esg_data = create_synthetic_esg_data()

    class MockBuilder:
        logger = logging.getLogger(__name__)

    builder = MockBuilder()

    # Calculate momentum
    factors = ESGFactorBuilder.calculate_momentum_factors(
        builder, esg_data, 'esg_score', windows=[6, 12]
    )

    # Show latest momentum
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date][
        ['ticker', 'esg_score', 'ESG_momentum_6m', 'ESG_momentum_12m']
    ].dropna().sort_values('ESG_momentum_12m', ascending=False)

    logger.info(f"\n📊 ESG Momentum ({latest_date.date()}):")
    print("\n" + latest.to_string(index=False))

    logger.info("\n✅ Momentum factors successfully calculated!")
    logger.info("   - Positive momentum = ESG improving")
    logger.info("   - Negative momentum = ESG declining")
    logger.info("   - Can identify ESG leaders and laggards")

    return factors


def demo_composite_factors():
    """Demo 3: Composite ESG Factors"""
    from market.esg_factor_builder import ESGFactorBuilder

    logger.info("\n" + "=" * 80)
    logger.info("DEMO 3: Composite ESG Factors")
    logger.info("=" * 80)

    esg_data = create_synthetic_esg_data()

    class MockBuilder:
        logger = logging.getLogger(__name__)

    builder = MockBuilder()

    # Calculate cross-sectional first
    factors = ESGFactorBuilder.calculate_cross_sectional_factors(
        builder, esg_data, 'esg_score'
    )

    # Calculate for pillars
    for col in ['env_score', 'soc_score', 'gov_score']:
        factors = ESGFactorBuilder.calculate_cross_sectional_factors(
            builder, factors, col
        )

    # Calculate momentum
    factors = ESGFactorBuilder.calculate_momentum_factors(
        builder, factors, 'esg_score', windows=[12]
    )

    # Quality-Momentum composite
    factors = ESGFactorBuilder.calculate_composite_factor(
        builder, factors, method='quality_momentum'
    )

    # Pillar-weighted composite
    factors = ESGFactorBuilder.calculate_composite_factor(
        builder, factors, method='pillar_weighted',
        pillar_weights={'E': 0.4, 'S': 0.3, 'G': 0.3}
    )

    # Show results
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date][
        ['ticker', 'esg_score', 'ESG_quality_momentum', 'ESG_composite']
    ].sort_values('ESG_quality_momentum', ascending=False)

    logger.info(f"\n📊 Composite Factors ({latest_date.date()}):")
    print("\n" + latest.to_string(index=False))

    logger.info("\n✅ Composite factors successfully calculated!")
    logger.info("   - Quality-Momentum: High ESG level + improving trend")
    logger.info("   - Pillar-Weighted: Custom E/S/G weights (40/30/30)")
    logger.info("   - Both normalize cross-sectionally for portfolio use")

    return factors


def demo_portfolio_construction():
    """Demo 4: Portfolio Construction"""
    from market.esg_factor_builder import ESGFactorBuilder

    logger.info("\n" + "=" * 80)
    logger.info("DEMO 4: Portfolio Construction with ESG Factors")
    logger.info("=" * 80)

    esg_data = create_synthetic_esg_data()

    class MockBuilder:
        logger = logging.getLogger(__name__)

    builder = MockBuilder()

    # Build complete factor set
    factors = ESGFactorBuilder.calculate_cross_sectional_factors(
        builder, esg_data, 'esg_score'
    )

    # Get quarterly rebalancing dates (last 4 quarters)
    factors['quarter'] = pd.PeriodIndex(factors['date'], freq='Q')
    rebalance_dates = factors.groupby('quarter')['date'].max().tail(4)

    logger.info("\n📅 Quarterly Long-Short ESG Portfolio:")

    for rebal_date in rebalance_dates:
        quarter_data = factors[factors['date'] == rebal_date].copy()

        if 'esg_score_decile' in quarter_data.columns:
            # Long: Top 3 deciles
            long = quarter_data[quarter_data['esg_score_decile'].isin([8, 9, 10])]['ticker'].tolist()

            # Short: Bottom 3 deciles
            short = quarter_data[quarter_data['esg_score_decile'].isin([1, 2, 3])]['ticker'].tolist()

            logger.info(f"\n  {rebal_date.date()}:")
            logger.info(f"    Long  (High ESG): {long}")
            logger.info(f"    Short (Low ESG):  {short}")

    logger.info("\n✅ Portfolio construction complete!")
    logger.info("   - Decile rankings enable systematic portfolio formation")
    logger.info("   - Quarterly rebalancing reduces turnover")
    logger.info("   - Long-short captures ESG factor premium")


def main():
    """Run all demos"""
    logger.info("\n" + "=" * 80)
    logger.info("ESG FACTOR BUILDER - SIMPLE DEMONSTRATION")
    logger.info("=" * 80)
    logger.info("Using synthetic data to demonstrate factor construction")
    logger.info("=" * 80)

    try:
        demo_cross_sectional_factors()
        demo_momentum_factors()
        demo_composite_factors()
        demo_portfolio_construction()

        logger.info("\n" + "=" * 80)
        logger.info("✅ ALL DEMOS COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("\n📚 Key Takeaways:")
        logger.info("  1. Cross-sectional factors normalize within each time period")
        logger.info("  2. Momentum factors capture ESG improvement over time")
        logger.info("  3. Composite factors combine multiple dimensions")
        logger.info("  4. Decile rankings enable systematic portfolio formation")
        logger.info("  5. All factors are designed for quantitative backtesting")
        logger.info("\n🚀 ESGFactorBuilder is ready for production use!")

        return 0

    except Exception as e:
        logger.error(f"\n❌ Demo failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
