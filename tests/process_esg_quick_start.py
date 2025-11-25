"""
Quick Start: Process ESG Data for Universe

This example demonstrates how to process ESG data for the entire universe
with automatic ticker mapping support.
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from core.config import Config
from market import ESGManager
from universe import SP500Universe


def main():
    """Process ESG data with ticker mapping"""

    # Initialize
    config = Config("config/settings.yaml")
    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)

    print("=" * 80)
    print("ESG Universe Processing - Quick Start")
    print("=" * 80)

    # Step 1: Dry run first to see what will happen
    print("\n🔍 Step 1: DRY RUN - Preview what will be processed")
    print("-" * 80)

    dry_results = esg_mgr.process_universe_esg(
        start_date=config.get('universe.sp500.start_date'),
        end_date=config.get('universe.sp500.end_date'),
        exchange='us',
        dry_run=True
    )

    print(f"\nDry run complete:")
    print(f"  - Would process: {len(dry_results['processed']) + len([x for x in dry_results.get('mapped', {}).keys()])} tickers")
    print(f"  - Ticker transitions: {len(dry_results['mapped'])}")
    print(f"  - Will skip: {len(dry_results['skipped'])}")

    # Step 2: Ask user confirmation
    print("\n" + "=" * 80)
    response = input("\n📝 Proceed with actual processing? (yes/no): ")

    if response.lower() not in ['yes', 'y']:
        print("❌ Processing cancelled")
        return

    # Step 3: Process for real
    print("\n✅ Step 2: LIVE RUN - Processing ESG data")
    print("-" * 80)

    live_results = esg_mgr.process_universe_esg(
        start_date=config.get('universe.sp500.start_date'),
        end_date=config.get('universe.sp500.end_date'),
        exchange='us',
        dry_run=False
    )

    # Step 4: Show ticker transitions that were applied
    if live_results['mapped']:
        print("\n" + "=" * 80)
        print("TICKER TRANSITIONS APPLIED")
        print("=" * 80)
        for old, new in sorted(live_results['mapped'].items()):
            print(f"  {old:10s} → {new}")

    # Step 5: Show sample of processed tickers
    if live_results['processed']:
        print("\n" + "=" * 80)
        print("SAMPLE OF PROCESSED TICKERS (first 10)")
        print("=" * 80)
        for item in live_results['processed'][:10]:
            ticker = item['ticker']
            records = item['records']
            years = item['years']
            print(f"  {ticker:10s} {records:4d} records, years {min(years)}-{max(years)}")

    print("\n" + "=" * 80)
    print("✅ ESG Processing Complete!")
    print("=" * 80)
    print(f"\nData saved to: data/curated/tickers/exchange=us/ticker=*/esg/")
    print(f"Total tickers processed: {len(live_results['processed'])}")


if __name__ == "__main__":
    main()
