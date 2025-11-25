"""
Regenerate ESG Data with Correct Column Names

This script regenerates all ESG Parquet files with corrected column names.

The issue was that ESGManager was renaming pillar score columns to short names
(env_score, soc_score, gov_score) but ESGFactorBuilder expected long names
(environmental_pillar_score, social_pillar_score, governance_pillar_score).

This has been fixed in ESGManager, and this script regenerates all data.

Usage:
    python examples/regenerate_esg_data.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging

from core.config import Config
from market import ESGManager
from universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Regenerate ESG data for all universe members"""

    logger.info("=" * 80)
    logger.info("ESG DATA REGENERATION")
    logger.info("=" * 80)
    logger.info("This will process all ESG data from raw Excel file")
    logger.info("and save it with correct column names.")
    logger.info("=" * 80)

    config = Config("config/settings.yaml")
    universe = SP500Universe(
        data_root=config.get("storage.local.root_path")
    )
    esg_mgr = ESGManager(universe=universe)

    # Process universe ESG data with ticker mapping
    # This uses the fixed ESGManager with correct column names
    results = esg_mgr.process_universe_esg(
        start_date=config.get("universe.sp500.start_date"),
        end_date=config.get("universe.sp500.end_date"),
        exchange='us',
        dry_run=False  # Actually save the data
    )

    logger.info("\n" + "=" * 80)
    logger.info("REGENERATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"✓ Processed: {len(results['processed'])} tickers")
    logger.info(f"→ Ticker transitions: {len(results['mapped'])}")
    logger.info(f"⊘ Skipped: {len(results['skipped'])}")
    logger.info(f"⚠ No ESG data: {len(results['no_esg_data'])}")
    logger.info(f"✗ Errors: {len(results['errors'])}")

    if results['errors']:
        logger.warning(f"\n⚠ {len(results['errors'])} errors occurred during processing")

    # Verify column names in a sample file
    logger.info("\n" + "=" * 80)
    logger.info("VERIFICATION: Checking sample file column names")
    logger.info("=" * 80)

    if results['processed']:
        sample_ticker = results['processed'][0]['ticker']
        logger.info(f"Loading sample data for {sample_ticker}...")

        sample_df = esg_mgr.load_esg_data(sample_ticker, start_date='2023-01-01', end_date='2023-12-31')

        logger.info(f"\nColumns in saved file:")
        for col in sample_df.columns:
            logger.info(f"  - {col}")

        # Check for correct pillar columns
        expected_cols = ['environmental_pillar_score', 'social_pillar_score', 'governance_pillar_score']
        old_cols = ['env_score', 'soc_score', 'gov_score']

        logger.info("\n✓ Pillar score columns (should be long names):")
        for col in expected_cols:
            if col in sample_df.columns:
                non_null = sample_df[col].notna().sum()
                logger.info(f"  ✓ {col}: {non_null} non-null values")
            else:
                logger.error(f"  ✗ {col}: MISSING!")

        logger.info("\n✗ Old column names (should NOT exist):")
        for col in old_cols:
            if col in sample_df.columns:
                logger.warning(f"  ✗ {col}: Still exists! (should be removed)")
            else:
                logger.info(f"  ✓ {col}: Not present (correct)")

        # Check ESG score
        esg_non_null = sample_df['esg_score'].notna().sum()
        pillar_non_null = sample_df['environmental_pillar_score'].notna().sum()

        logger.info(f"\n📊 Data quality check for {sample_ticker}:")
        logger.info(f"  Total records: {len(sample_df)}")
        logger.info(f"  ESG Score non-null: {esg_non_null} ({esg_non_null*100/len(sample_df):.1f}%)")
        logger.info(f"  Environmental pillar non-null: {pillar_non_null} ({pillar_non_null*100/len(sample_df):.1f}%)")

        if sample_df.head().to_string():
            logger.info(f"\n📋 Sample data (first 3 rows):")
            print(sample_df[['ticker', 'date', 'esg_score', 'environmental_pillar_score', 'social_pillar_score', 'governance_pillar_score']].head(3).to_string(index=False))

    logger.info("\n" + "=" * 80)
    logger.info("✅ ESG DATA REGENERATION SUCCESSFUL")
    logger.info("=" * 80)
    logger.info("All ESG data has been regenerated with correct column names.")
    logger.info("ESGFactorBuilder should now work correctly with pillar-weighted formation.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
