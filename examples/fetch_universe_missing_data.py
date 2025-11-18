#!/usr/bin/env python3
"""
Example: Fetch Missing Data for Universe

Demonstrates the intelligent missing data fetching functionality.
Only fetches data that's actually missing, skipping what we already have.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetcher.config_loader import FetcherConfig
from fetcher.price_data_builder import PriceDataManager


def main():
    print()
    print("=" * 80)
    print("Intelligent Missing Data Fetcher")
    print("=" * 80)
    print()

    # Load config
    try:
        config = FetcherConfig("config/settings.yaml")
        api_key = config.fetcher.tiingo.api_key
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return 1

    # Initialize builder
    builder = PriceDataManager(api_key=api_key, data_root="data/curated")

    # Example 1: Check single ticker
    print("=" * 80)
    print("Example 1: Check Single Ticker")
    print("=" * 80)
    print()

    symbol = "AAPL"
    required_start = "2020-01-01"
    required_end = "2024-12-31"

    print(f"Checking {symbol} for period {required_start} to {required_end}...")
    print()

    # Check what's missing
    check = builder.check_missing_data(
        symbol=symbol,
        required_start=required_start,
        required_end=required_end,
        tolerance_days=2
    )

    print(f"Status: {check['status']}")
    if check['actual_start']:
        print(f"Actual data range: {check['actual_start']} to {check['actual_end']}")
        print(f"Missing at start: {check['missing_start_days']} days")
        print(f"Missing at end: {check['missing_end_days']} days")
    else:
        print("No existing data found")

    print()

    # Fetch only what's missing
    if check['status'] != 'complete':
        print(f"Fetching missing data for {symbol}...")
        df, paths = builder.fetch_missing_data(
            symbol=symbol,
            required_start=required_start,
            required_end=required_end,
            tolerance_days=2
        )

        if not df.empty:
            print(f"✅ Fetched {len(df)} rows")
            print(f"   Saved to {len(paths)} files")
        else:
            print("ℹ️  No new data fetched")
    else:
        print(f"✅ {symbol} already has complete data")

    print()

    # Example 2: Fetch missing data for entire universe
    print("=" * 80)
    print("Example 2: Fetch Missing Data for Entire Universe")
    print("=" * 80)
    print()

    universe = "sp500"
    start_date = "2014-01-01"
    end_date = "2024-12-31"

    print(f"Fetching missing data for {universe.upper()} universe")
    print(f"Period: {start_date} to {end_date}")
    print(f"Tolerance: ±2 days")
    print()
    print("This will:")
    print("  • Check all historical S&P 500 members")
    print("  • Skip tickers with complete data")
    print("  • Fetch only missing portions for partial data")
    print("  • Fetch entire period for tickers with no data")
    print()

    proceed = input("Proceed with universe data fetch? (y/n): ")
    if proceed.lower() != 'y':
        print("Skipped universe fetch")
        return 0

    print()
    results = builder.fetch_universe_missing_data(
        universe=universe,
        start_date=start_date,
        end_date=end_date,
        tolerance_days=2,
        skip_errors=True
    )

    # Print summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print()

    by_status = {}
    for symbol, result in results.items():
        status = result['status']
        if status not in by_status:
            by_status[status] = []
        by_status[status].append(symbol)

    total = len(results)
    print(f"Total symbols processed: {total}")
    print()

    for status in ['complete', 'fetched', 'error']:
        if status in by_status:
            count = len(by_status[status])
            pct = count / total * 100
            print(f"{status.upper():>10}: {count:>4} ({pct:>5.1f}%)")

    print()

    # Show fetched tickers
    if 'fetched' in by_status:
        print(f"Fetched data for {len(by_status['fetched'])} tickers:")
        for symbol in by_status['fetched'][:20]:
            rows = results[symbol]['fetched_rows']
            print(f"  • {symbol}: {rows} rows")
        if len(by_status['fetched']) > 20:
            print(f"  ... and {len(by_status['fetched']) - 20} more")
        print()

    # Show errors
    if 'error' in by_status:
        print(f"Errors for {len(by_status['error'])} tickers:")
        for symbol in by_status['error'][:10]:
            msg = results[symbol]['message']
            print(f"  • {symbol}: {msg}")
        if len(by_status['error']) > 10:
            print(f"  ... and {len(by_status['error']) - 10} more")
        print()

    print("=" * 80)
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
