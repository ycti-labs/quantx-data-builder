"""
ESG Factor Save/Load Example

Shows how to save and load ESG factors using ESGFactorBuilder.
The builder has built-in methods for persistence.

Usage:
    python examples/esg_factor_save_load_example.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import logging

from market import ESGFactorBuilder, ESGManager
from universe import SP500Universe

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize builder
universe = SP500Universe()
esg_mgr = ESGManager(universe=universe)
builder = ESGFactorBuilder(esg_mgr, universe)

# ============================================================================
# METHOD 1: BUILD AND SAVE IN ONE GO
# ============================================================================

logger.info("\n" + "=" * 80)
logger.info("METHOD 1: Build factors and save immediately")
logger.info("=" * 80)

# Build factors
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2023-01-01',
    end_date='2024-12-31',
    formation_method='esg_score',
    include_rankings=True
)

# Save with custom name
saved_path = builder.save_factors(factors, "my_esg_factors.parquet")
logger.info(f"✅ Saved to: {saved_path}")

# ============================================================================
# METHOD 2: AUTO-GENERATED TIMESTAMP NAME
# ============================================================================

logger.info("\n" + "=" * 80)
logger.info("METHOD 2: Save with auto-generated timestamp")
logger.info("=" * 80)

# Build different factors
momentum_factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2023-01-01',
    end_date='2024-12-31',
    formation_method='momentum',
    momentum_windows=[12]
)

# Save without specifying name (timestamp auto-generated)
saved_path = builder.save_factors(momentum_factors)  # No output_name parameter
logger.info(f"✅ Saved with timestamp: {saved_path}")

# ============================================================================
# METHOD 3: LOAD SAVED FACTORS
# ============================================================================

logger.info("\n" + "=" * 80)
logger.info("METHOD 3: Load previously saved factors")
logger.info("=" * 80)

# Load by filename (with or without .parquet extension)
loaded_factors = builder.load_factors("my_esg_factors.parquet")
logger.info(f"✅ Loaded {len(loaded_factors)} records")
logger.info(f"   Tickers: {loaded_factors['ticker'].unique().tolist()}")
logger.info(f"   Date range: {loaded_factors['date'].min()} to {loaded_factors['date'].max()}")

# Can also load without .parquet extension
loaded_factors2 = builder.load_factors("my_esg_factors")  # Same result
logger.info(f"✅ Loaded again (without .parquet): {len(loaded_factors2)} records")

# ============================================================================
# METHOD 4: GET SUMMARY STATISTICS
# ============================================================================

logger.info("\n" + "=" * 80)
logger.info("METHOD 4: Get factor summary statistics")
logger.info("=" * 80)

summary = builder.get_factor_summary(loaded_factors)

logger.info(f"Total records: {summary['total_records']:,}")
logger.info(f"Number of tickers: {summary['num_tickers']}")
logger.info(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
logger.info(f"Factor columns: {summary['factor_columns']}")

for col, stats in summary['factor_statistics'].items():
    if stats['note'] == 'categorical column':
        logger.info(f"\n{col}: {stats['note']}")
    else:
        logger.info(f"\n{col}:")
        logger.info(f"  Mean: {stats['mean']:.4f}")
        logger.info(f"  Std: {stats['std']:.4f}")
        logger.info(f"  Range: [{stats['min']:.4f}, {stats['max']:.4f}]")
        logger.info(f"  Coverage: {stats['coverage']:.1%}")

# ============================================================================
# SUMMARY: FILE LOCATIONS
# ============================================================================

logger.info("\n" + "=" * 80)
logger.info("📁 FILE LOCATIONS")
logger.info("=" * 80)
logger.info(f"All factors are saved to: {universe.data_root}/results/esg_factors/")
logger.info("\nTo manually access:")
logger.info("  import pandas as pd")
logger.info("  df = pd.read_parquet('data/results/esg_factors/my_esg_factors.parquet')")
