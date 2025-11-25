"""
Process ESG Data for Universe with Ticker Mapping

This script processes ESG data for the entire universe following a three-step procedure:
1. Fetch historical memberships of universe for research period
2. Go through ESG raw data one by one, with gvkey-ticker mapping
3. Use TickerMapper to find if mapping is needed for missing tickers

Usage:
    python -m src.programs.process_esg_universe [--dry-run]
"""

import sys
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from esg import ESGManager
from universe import SP500Universe

def main(dry_run: bool = False):
    """Process ESG data for entire universe"""

    # Initialize components
    config = Config("config/settings.yaml")
    universe = SP500Universe()
    esg_mgr = ESGManager(universe=universe)

    # Get date range from config
    start_date = config.get('universe.sp500.start_date')
    end_date = config.get('universe.sp500.end_date')

    print("=" * 80)
    print("ESG Universe Data Processing")
    print("=" * 80)
    print(f"Research Period: {start_date} to {end_date}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print("=" * 80)

    # Process ESG data
    results = esg_mgr.process_universe_esg(
        start_date=start_date,
        end_date=end_date,
        exchange='us',
        dry_run=dry_run
    )

    # Additional detailed summary
    print("\n" + "=" * 80)
    print("DETAILED RESULTS")
    print("=" * 80)

    if results['processed']:
        print(f"\n✅ Successfully Processed ({len(results['processed'])} tickers):")
        for item in results['processed'][:20]:  # Show first 20
            ticker = item['ticker']
            original = item['original']
            records = item['records']
            years = item['years']

            if original:
                print(f"  {original} → {ticker}: {records} records, years {min(years)}-{max(years)}")
            else:
                print(f"  {ticker}: {records} records, years {min(years)}-{max(years)}")

        if len(results['processed']) > 20:
            print(f"  ... and {len(results['processed']) - 20} more")

    if results['skipped']:
        print(f"\n⊘ Skipped ({len(results['skipped'])} tickers):")
        for ticker in results['skipped'][:20]:  # Show first 20
            print(f"  {ticker}")
        if len(results['skipped']) > 20:
            print(f"  ... and {len(results['skipped']) - 20} more")

    if results['no_esg_data']:
        print(f"\n⚠ No ESG Data ({len(results['no_esg_data'])} tickers):")
        for ticker in results['no_esg_data'][:20]:  # Show first 20
            print(f"  {ticker}")
        if len(results['no_esg_data']) > 20:
            print(f"  ... and {len(results['no_esg_data']) - 20} more")

    print("\n" + "=" * 80)
    print("Processing Complete!")
    print("=" * 80)

    return results


if __name__ == "__main__":
    # Check for --dry-run flag
    dry_run = '--dry-run' in sys.argv
    if dry_run:
        print("\n🔍 Running in DRY RUN mode - no data will be saved\n")

    results = main(dry_run=dry_run)
