"""
ESG Factor Formation Methods Demo

Demonstrates the three formation methods available in ESGFactorBuilder:
1. ESG Score Formation: Use composite esg_score directly
2. Pillar-Weighted Formation: Combine E/S/G pillars with custom weights
3. ESG Momentum Formation: Rate of ESG improvement over time

Usage:
    python examples/demo_formation_methods.py
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

start_date = '2020-01-01'
end_date = '2024-12-31'


def demo_method1_esg_score():
    """
    Formation Method 1: ESG Score

    Use the composite ESG score directly from the data.
    This is the simplest approach using the pre-calculated ESG score.
    """
    logger.info("\n" + "=" * 80)
    logger.info("FORMATION METHOD 1: ESG SCORE")
    logger.info("=" * 80)
    logger.info("Using composite esg_score directly")

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Select tech stocks that we know have ESG data
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA']

    # Build factors using ESG score formation
    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        formation_method='esg_score',
        include_rankings=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Display results
    logger.info("\n📊 Sample Results (Latest Month):")
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date]

    display_cols = ['ticker', 'esg_score', 'esg_score_zscore', 'esg_score_pctrank', 'esg_score_decile']
    display_cols = [c for c in display_cols if c in latest.columns]

    print("\n" + latest[display_cols].sort_values('esg_score', ascending=False).to_string(index=False))

    # Save the factors
    saved_path = builder.save_factors(factors, "esg_score_factors.parquet")
    logger.info(f"✅ Saved to: {saved_path}")

    return factors


def demo_method2_pillar_weighted():
    """
    Formation Method 2: Pillar-Weighted

    Combine E/S/G pillars with custom weights to create a custom ESG score.
    Allows emphasizing specific ESG dimensions (e.g., focus on Environmental).
    """
    logger.info("\n" + "=" * 80)
    logger.info("FORMATION METHOD 2: PILLAR-WEIGHTED")
    logger.info("=" * 80)
    logger.info("Combining E/S/G pillars with custom weights")

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    tickers = ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'XOM', 'CVX', 'COP']

    # Example: Environmental-focused strategy (60% E, 20% S, 20% G)
    environmental_focus = {'E': 0.6, 'S': 0.2, 'G': 0.2}

    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        formation_method='pillar_weighted',
        pillar_weights=environmental_focus,
        include_rankings=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Display results
    logger.info("\n📊 Sample Results (Latest Month) - Environmental Focus:")
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date]

    display_cols = ['ticker', 'pillar_weighted_score',
                   'environmental_pillar_score', 'social_pillar_score', 'governance_pillar_score',
                   'pillar_weighted_score_zscore', 'pillar_weighted_score_decile']
    display_cols = [c for c in display_cols if c in latest.columns]

    print("\n" + latest[display_cols].sort_values('pillar_weighted_score', ascending=False).to_string(index=False))

    # Save the environmental-focused factors
    saved_path = builder.save_factors(factors, "pillar_weighted_environmental_focus.parquet")
    logger.info(f"✅ Saved to: {saved_path}")

    # Compare with Governance-focused strategy
    logger.info("\n🔄 Re-running with Governance-focused weights (20% E, 20% S, 60% G):")
    governance_focus = {'E': 0.2, 'S': 0.2, 'G': 0.6}

    factors_gov = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        formation_method='pillar_weighted',
        pillar_weights=governance_focus,
        include_rankings=True
    )

    if not factors_gov.empty:
        latest_gov = factors_gov[factors_gov['date'] == factors_gov['date'].max()]
        comparison = latest[['ticker', 'pillar_weighted_score_decile']].merge(
            latest_gov[['ticker', 'pillar_weighted_score_decile']],
            on='ticker',
            suffixes=('_env_focus', '_gov_focus')
        )
        print("\n📊 Decile Ranking Comparison (Environmental vs Governance Focus):")
        print(comparison.to_string(index=False))

        # Save governance-focused factors too
        saved_path_gov = builder.save_factors(factors_gov, "pillar_weighted_governance_focus.parquet")
        logger.info(f"✅ Saved governance factors to: {saved_path_gov}")

    return factors


def demo_method3_momentum():
    """
    Formation Method 3: ESG Momentum

    Focus on the rate of ESG improvement over time.
    Identifies companies with improving vs declining ESG trends.
    """
    logger.info("\n" + "=" * 80)
    logger.info("FORMATION METHOD 3: ESG MOMENTUM")
    logger.info("=" * 80)
    logger.info("Measuring rate of ESG improvement over time")

    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)
    builder = ESGFactorBuilder(esg_mgr, universe)

    # Mix of sectors to see momentum differences
    tickers = [
        'AAPL', 'MSFT', 'GOOGL',  # Tech
        'JPM', 'BAC', 'WFC',       # Finance
        'XOM', 'CVX', 'COP',       # Energy
        'JNJ', 'UNH', 'PFE'        # Healthcare
    ]

    # Calculate momentum with multiple windows
    factors = builder.build_factors_for_universe(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        formation_method='momentum',
        momentum_windows=[3, 6, 12],  # 3, 6, and 12 month momentum
        include_rankings=True
    )

    if factors.empty:
        logger.warning("No factors generated")
        return None

    # Display results
    logger.info("\n📊 ESG Momentum Analysis (Latest Month):")
    latest_date = factors['date'].max()
    latest = factors[factors['date'] == latest_date]

    momentum_cols = ['ticker', 'esg_score',
                    'esg_score_momentum_3m', 'esg_score_momentum_6m', 'esg_score_momentum_12m',
                    'esg_score_momentum_12m_zscore', 'esg_score_momentum_12m_decile']
    momentum_cols = [c for c in momentum_cols if c in latest.columns]

    latest_sorted = latest[momentum_cols].sort_values('esg_score_momentum_12m', ascending=False)

    print("\n🚀 Top ESG Improvers (12-month momentum):")
    print(latest_sorted.head(5).to_string(index=False))

    print("\n📉 ESG Decliners (12-month momentum):")
    print(latest_sorted.tail(5).to_string(index=False))

    # Consistency analysis
    if all(c in latest.columns for c in ['esg_score_momentum_3m', 'esg_score_momentum_6m', 'esg_score_momentum_12m']):
        logger.info("\n📈 Momentum Consistency Check:")
        consistent_improvers = latest[
            (latest['esg_score_momentum_3m'] > 0) &
            (latest['esg_score_momentum_6m'] > 0) &
            (latest['esg_score_momentum_12m'] > 0)
        ]['ticker'].tolist()

        consistent_decliners = latest[
            (latest['esg_score_momentum_3m'] < 0) &
            (latest['esg_score_momentum_6m'] < 0) &
            (latest['esg_score_momentum_12m'] < 0)
        ]['ticker'].tolist()

        print(f"  Consistent Improvers (positive 3/6/12m): {consistent_improvers}")
        print(f"  Consistent Decliners (negative 3/6/12m): {consistent_decliners}")

    # Save the momentum factors
    saved_path = builder.save_factors(factors, "esg_momentum_factors.parquet")
    logger.info(f"✅ Saved to: {saved_path}")

    return factors


def main():
    """Run all formation method demos"""
    logger.info("\n" + "=" * 80)
    logger.info("ESG FACTOR FORMATION METHODS DEMONSTRATION")
    logger.info("=" * 80)
    logger.info("Showcasing three ways to construct ESG factors")
    logger.info("=" * 80)

    try:
        # Demo 1: ESG Score Formation
        demo_method1_esg_score()

        # Demo 2: Pillar-Weighted Formation
        demo_method2_pillar_weighted()

        # Demo 3: ESG Momentum Formation
        demo_method3_momentum()

        logger.info("\n" + "=" * 80)
        logger.info("✅ ALL FORMATION METHOD DEMOS COMPLETED")
        logger.info("=" * 80)
        logger.info("\n📚 Summary of Formation Methods:")
        logger.info("\n1️⃣  ESG SCORE FORMATION")
        logger.info("   - Use: Simplest approach with pre-calculated scores")
        logger.info("   - Best for: Standard ESG-based portfolio construction")
        logger.info("   - Output: Cross-sectional rankings (z-scores, deciles)")

        logger.info("\n2️⃣  PILLAR-WEIGHTED FORMATION")
        logger.info("   - Use: Custom E/S/G emphasis (e.g., climate-focused portfolios)")
        logger.info("   - Best for: Thematic strategies or client-specific ESG preferences")
        logger.info("   - Output: Custom composite score with rankings")
        logger.info("   - Flexibility: Adjust weights to match investment philosophy")

        logger.info("\n3️⃣  ESG MOMENTUM FORMATION")
        logger.info("   - Use: Focus on ESG improvement trends")
        logger.info("   - Best for: ESG improver/decliner strategies")
        logger.info("   - Output: Rate of change metrics (% improvement)")
        logger.info("   - Timing: Multiple windows (3/6/12 months) for robustness")

        logger.info("\n🎯 Usage Recommendations:")
        logger.info("   • Portfolio Construction: Use Method 1 (ESG Score) with decile rankings")
        logger.info("   • Thematic Strategies: Use Method 2 (Pillar-Weighted) for customization")
        logger.info("   • ESG Improvers: Use Method 3 (Momentum) to capture positive trends")
        logger.info("   • Combined Approach: Blend methods (e.g., high ESG + positive momentum)")

        # Demonstrate loading saved factors
        logger.info("\n" + "=" * 80)
        logger.info("📂 LOADING SAVED FACTORS DEMO")
        logger.info("=" * 80)

        universe = SP500Universe()
        esg_mgr = ESGManager(universe=universe)
        builder = ESGFactorBuilder(esg_mgr, universe)

        # Load ESG Score factors
        logger.info("\n🔄 Loading saved ESG Score factors...")
        loaded_esg = builder.load_factors("esg_score_factors.parquet")
        logger.info(f"   Records: {len(loaded_esg):,}")
        logger.info(f"   Tickers: {loaded_esg['ticker'].nunique()}")
        logger.info(f"   Columns: {', '.join(loaded_esg.columns.tolist())}")

        # Get summary statistics
        logger.info("\n📊 Factor Summary Statistics:")
        summary = builder.get_factor_summary(loaded_esg)
        logger.info(f"   Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
        logger.info(f"   Factor Columns: {len(summary['factor_columns'])}")
        for factor_col in summary['factor_columns']:
            stats = summary['factor_statistics'][factor_col]
            logger.info(f"   {factor_col}:")
            logger.info(f"      Mean: {stats['mean']:.4f}, Std: {stats['std']:.4f}, Coverage: {stats['coverage']:.1%}")

    except Exception as e:
        logger.error(f"\n❌ Demo failed: {e}", exc_info=True)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
