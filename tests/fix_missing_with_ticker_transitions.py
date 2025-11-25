"""
Fix Missing Data with Ticker Resolution

This script fetches data for symbols that are missing due to ticker transitions
(mergers, acquisitions, rebrands). It uses TickerMapper to resolve old tickers
to their current symbols where Tiingo has migrated the historical data.

Usage:
    # Dry run (preview what will happen)
    python examples/fix_missing_with_ticker_transitions.py --dry-run

    # Actually fetch the data
    python examples/fix_missing_with_ticker_transitions.py
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from core.ticker_mapper import TickerMapper
from market import PriceManager
from universe import SP500Universe


def main():
    """Fix missing data using ticker resolution"""

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Fix missing data with ticker transitions')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview changes without fetching data')
    parser.add_argument('--start-date', default='2014-01-01',
                       help='Start date (default: 2014-01-01)')
    parser.add_argument('--end-date', default='2024-12-31',
                       help='End date (default: 2024-12-31)')
    args = parser.parse_args()

    print("=" * 80)
    print("Fix Missing Data with Ticker Resolution")
    print("=" * 80)

    if args.dry_run:
        print("\n⚠️  DRY RUN MODE - No data will be fetched")

    print(f"\nResearch Period: {args.start_date} to {args.end_date}")
    print("=" * 80)

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo_config = {
        'api_key': config.get('fetcher.tiingo.api_key'),
        'session': True
    }
    tiingo = TiingoClient(tiingo_config)
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)

    # Create ticker mapper
    ticker_mapper = TickerMapper()

    # List of symbols identified as missing due to ticker transitions
    # These are from the missing_data_report analysis
    missing_symbols = [
        # Rebrands
        'FB',       # → META
        'ANTM',     # → ELV
        'ABC',      # → COR
        'FLT',      # → CPAY
        'PKI',      # → RVTY
        'TMK',      # → GL
        'COG',      # → CTRA
        'HFC',      # → DINO
        'WLTW',     # → WTW
        'RE',       # → EG
        'ADS',      # → BFH
        'PEAK',     # → DOC

        # Multi-step transitions
        'CBS',      # → VIAC → PARA

        # Mergers
        'CCE',      # → CCEP
        'FBHS',     # → FBIN

        # Class share formats
        'BRK.B',    # → BRK-B
        'BF.B',     # → BF-B

        # These will be skipped (delisted/acquired)
        'LIFE',     # Acquired by TMO
        'SNDK',     # Acquired by WDC
        'POM',      # Acquired by EXC
        'FRC',      # Failed bank
        'ENDP',     # Bankruptcy
        'MNK',      # Bankruptcy
        'WIN',      # Bankruptcy
        'ESV',      # Complex restructuring
        'IGT',      # Merger issues
        'GPS',      # Check if needed
        'BLL',      # Check if needed
    ]

    print(f"\nProcessing {len(missing_symbols)} symbols with potential ticker transitions")
    print("\nSymbols to process:")
    print(f"  {', '.join(missing_symbols)}")

    # Show what will be resolved
    print("\n" + "=" * 80)
    print("TICKER RESOLUTION PREVIEW")
    print("=" * 80)

    for symbol in missing_symbols:
        resolved = ticker_mapper.resolve(symbol)
        if resolved is None:
            print(f"  {symbol:10} → None (will be skipped - delisted/acquired)")
        elif resolved != symbol:
            print(f"  {symbol:10} → {resolved}")
        else:
            print(f"  {symbol:10} → (no change)")

    # Ask for confirmation if not dry run
    if not args.dry_run:
        print("\n" + "=" * 80)
        response = input("\nProceed with fetching data? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Aborted by user")
            return

    # Fetch data with ticker resolution
    print("\n" + "=" * 80)
    print("FETCHING DATA")
    print("=" * 80)

    results = price_mgr.fetch_missing_with_ticker_resolution(
        symbols=missing_symbols,
        frequency='daily',
        start_date=args.start_date,
        end_date=args.end_date,
        ticker_mapper=ticker_mapper,
        dry_run=args.dry_run
    )

    # Print detailed results
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)

    if results['fetched']:
        print(f"\n✅ Successfully Fetched ({len(results['fetched'])} symbols):")
        for item in results['fetched']:
            orig = item['original']
            fetched = item['fetched_as']
            rows = item['rows']
            date_range = item['date_range']
            print(f"  {orig:10} (as {fetched:10}): {rows:5d} rows | {date_range[0]} to {date_range[1]}")

    if results['skipped']:
        print(f"\n⊘ Skipped - Delisted/Acquired ({len(results['skipped'])} symbols):")
        print(f"  {', '.join(results['skipped'])}")
        print("  These symbols have no successor ticker and cannot be fetched")

    if results['failed']:
        print(f"\n❌ Failed ({len(results['failed'])} symbols):")
        for item in results['failed']:
            symbol = item['symbol']
            resolved = item.get('resolved', 'N/A')
            error = item.get('error', item.get('reason', 'Unknown'))
            print(f"  {symbol} (→ {resolved}): {error}")

    # Final summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total = len(missing_symbols)
    fetched = len(results['fetched'])
    skipped = len(results['skipped'])
    failed = len(results['failed'])

    print(f"""
Total symbols processed:     {total}
✓ Successfully fetched:      {fetched}
⊘ Skipped (no successor):    {skipped}
✗ Failed:                    {failed}

Data saved to: data/prices/exchange=us/ticker=<SYMBOL>/...
    """)

    if args.dry_run:
        print("⚠️  This was a DRY RUN. Run without --dry-run to actually fetch data.")
    else:
        print("✅ Data fetch complete!")
        print("\nNext steps:")
        print("  1. Run check_universe_overlap_aware.py to verify completeness")
        print("  2. Check that the 'missing' symbols now show as 'complete'")


if __name__ == "__main__":
    main()
