#!/usr/bin/env python3
"""
Simple PriceDataManager Usage Examples

Demonstrates the clean, simple API of the PriceDataManager class.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market import PriceManager


def example_basic_usage():
    """Example 1: Basic single symbol fetch"""
    print("=" * 80)
    print("Example 1: Fetch Single Symbol")
    print("=" * 80)

    # Load config (gets API key from environment or config file)
    config = Config("config/settings.yaml")

    # Create fetcher
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
    )

    # Fetch AAPL data for last month
    df = fetcher.fetch_eod('AAPL', start_date='2024-10-01', end_date='2024-10-31')

    print(f"\nFetched {len(df)} rows for AAPL")
    print("\nColumns:", df.columns.tolist())
    print("\nFirst few rows:")
    print(df.head())
    print()


def example_multiple_symbols():
    """Example 2: Fetch multiple symbols"""
    print("=" * 80)
    print("Example 2: Fetch Multiple Symbols")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Fetch data for multiple symbols
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA']
    results = fetcher.fetch_multiple_eod(
        symbols=symbols,
        start_date='2024-11-01',
        end_date='2024-11-08'
    )

    print(f"\nFetched data for {len(results)} symbols:")
    for symbol, df in results.items():
        print(f"  {symbol}: {len(df)} rows, "
              f"price range ${df['close'].min():.2f} - ${df['close'].max():.2f}")
    print()


def example_universe_members():
    """Example 3: Get universe members"""
    print("=" * 80)
    print("Example 3: Get Universe Members")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Get current SP500 members
    members = fetcher.get_current_members('sp500')

    if members:
        print(f"\nFound {len(members)} members in SP500")
        print(f"First 20: {members[:20]}")
    else:
        print("\n⚠️  Membership data not yet loaded")
        print("   (Will work after running universe builder)")
    print()


def example_universe_data():
    """Example 4: Fetch entire universe"""
    print("=" * 80)
    print("Example 4: Fetch Universe Data")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Fetch data for all SP500 members (as of specific date)
    print("\nFetching SP500 data for November 2024...")
    print("(This would fetch 500+ symbols - demo with small subset)\n")

    # In practice, you'd do:
    # results = fetcher.fetch_universe_data(
    #     universe='sp500',
    #     start_date='2024-11-01',
    #     end_date='2024-11-30',
    #     as_of_date='2024-11-01'
    # )

    print("Usage:")
    print("  results = fetcher.fetch_universe_data(")
    print("      universe='sp500',")
    print("      start_date='2024-11-01',")
    print("      end_date='2024-11-30',")
    print("      as_of_date='2024-11-01'  # Use members as of this date")
    print("  )")
    print()


def example_point_in_time():
    """Example 5: Point-in-time universe membership"""
    print("=" * 80)
    print("Example 5: Point-in-Time Universe Membership")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Get members as of specific historical date
    dates = ['2014-01-01', '2020-01-01', '2022-01-01', '2024-01-01']

    print("\nSP500 members over time:")
    for date in dates:
        members = fetcher.get_universe_members('sp500', as_of_date=date)
        if members:
            print(f"  {date}: {len(members)} members")
        else:
            print(f"  {date}: No membership data")
    print()


if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "PriceDataManager Usage Examples" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    try:
        example_basic_usage()
        example_multiple_symbols()
        example_universe_members()
        example_universe_data()
        example_point_in_time()

        print("=" * 80)
        print("✅ All examples completed!")
        print("=" * 80)
        print()

    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("\nPlease set your TIINGO_API_KEY:")
        print("  export TIINGO_API_KEY=your_key_here")
        print()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
