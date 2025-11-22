#!/usr/bin/env python3
"""
Test Ticker Correction Feature

Demonstrates how to use the automatic ticker correction feature in PriceDataManager
when fetching missing data. The system automatically tries alternative tickers based
on gvkey mapping when the original ticker fails or has no data.

Example: ANTM (old ticker) -> ELV (new ticker after name change)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market import PriceManager
from src.universe import SP500Universe


def test_ticker_correction():
    """Test ticker correction feature with known ticker changes"""
    print("=" * 80)
    print("Test Ticker Correction Feature")
    print("=" * 80)
    print()

    # Load config
    config = Config("config/settings.yaml")

    # Create universe
    sp500 = SP500Universe(data_root=config.get("storage.local.root_path", "data"))

    # Create price data manager
    manager = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        universe=sp500
    )

    print("Testing ticker corrections:")
    print("-" * 80)

    # Example 1: ANTM (Anthem) -> ELV (Elevance Health) - name change in 2022
    print("\n1. Testing ANTM (renamed to ELV in 2022)")
    print("-" * 40)
    df, paths, corrected = manager.fetch_missing_with_correction(
        symbol='ANTM',
        required_start='2022-01-01',
        required_end='2024-12-31',
        tolerance_days=2
    )

    if corrected:
        print(f"✅ Successfully used corrected ticker: {corrected}")
        print(f"   Fetched {len(df)} rows")
        print(f"   Saved to {len(paths)} files")
    elif not df.empty:
        print(f"✅ Original ticker ANTM worked")
        print(f"   Fetched {len(df)} rows")
    else:
        print("⚠️  No data available")

    # Example 2: FB (Facebook) -> META (Meta Platforms) - name change in 2021
    print("\n2. Testing FB (renamed to META in 2021)")
    print("-" * 40)
    df, paths, corrected = manager.fetch_missing_with_correction(
        symbol='FB',
        required_start='2021-01-01',
        required_end='2024-12-31',
        tolerance_days=2
    )

    if corrected:
        print(f"✅ Successfully used corrected ticker: {corrected}")
        print(f"   Fetched {len(df)} rows")
        print(f"   Saved to {len(paths)} files")
    elif not df.empty:
        print(f"✅ Original ticker FB worked")
        print(f"   Fetched {len(df)} rows")
    else:
        print("⚠️  No data available")

    # Show available corrections
    print("\n" + "=" * 80)
    print("Available Ticker Corrections")
    print("=" * 80)
    corrections = sp500.get_ticker_corrections()
    print(f"Total ticker corrections available: {len(corrections)}")
    print()
    print("Sample corrections (first 20):")
    for i, (old_ticker, alternatives) in enumerate(list(corrections.items())[:20]):
        print(f"  {old_ticker} -> {', '.join(alternatives)}")
    print()


def demo_fetch_universe_with_corrections():
    """Demonstrate fetching missing data for entire universe with corrections"""
    print("=" * 80)
    print("Fetch Universe Missing Data with Corrections")
    print("=" * 80)
    print()

    # Load config
    config = Config("config/settings.yaml")

    # Create universe
    sp500 = SP500Universe(data_root=config.get("storage.local.root_path", "data"))

    # Create price data manager
    manager = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        universe=sp500
    )

    # Get historical members for a period
    print("Fetching historical members for 2023...")
    members = sp500.get_all_historical_members('2023-01-01', '2023-12-31')
    print(f"Found {len(members)} historical members")
    print()

    # Demo: Check first 5 tickers for missing data
    print("Checking first 5 tickers for missing data (with correction):")
    print("-" * 80)

    results = {}
    for symbol in members[:5]:
        print(f"\nProcessing {symbol}...")
        df, paths, corrected = manager.fetch_missing_with_correction(
            symbol=symbol,
            required_start='2023-01-01',
            required_end='2023-12-31',
            tolerance_days=2
        )

        results[symbol] = {
            'corrected': corrected,
            'rows': len(df),
            'files': len(paths)
        }

        if corrected:
            print(f"  ✅ Used corrected ticker: {corrected} (fetched {len(df)} rows)")
        elif not df.empty:
            print(f"  ✅ Fetched {len(df)} new rows")
        elif paths:
            print(f"  ✅ Data already complete")
        else:
            print(f"  ⚠️  No data available")

    # Summary
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    for symbol, result in results.items():
        status = f"→ {result['corrected']}" if result['corrected'] else "✓"
        print(f"  {symbol:6s} {status:15s} {result['rows']:4d} rows, {result['files']:2d} files")
    print()


if __name__ == "__main__":
    print("\n")

    # Test 1: Ticker correction feature
    test_ticker_correction()

    # Test 2: Fetch with corrections (demo mode - only first 5)
    # Uncomment to run:
    # demo_fetch_universe_with_corrections()

    print("\n" + "=" * 80)
    print("✅ Tests Complete")
    print("=" * 80)
    print()
    print("Usage in your code:")
    print("-" * 80)
    print("""
from src.core.config import Config
from src.market_data import PriceDataManager
from src.universe import SP500Universe

# Setup
config = Config("config/settings.yaml")
sp500 = SP500Universe()
manager = PriceDataManager(
    api_key=config.get("fetcher.tiingo.api_key"),
    universe=sp500
)

# Fetch with automatic ticker correction
df, paths, corrected_ticker = manager.fetch_missing_with_correction(
    symbol='ANTM',  # Old ticker
    required_start='2022-01-01',
    required_end='2024-12-31'
)

if corrected_ticker:
    print(f"Used corrected ticker: {corrected_ticker}")
    """)
    print()
