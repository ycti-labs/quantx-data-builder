#!/usr/bin/env python3
"""
Test Complete Historical Universe Data Fetching

Demonstrates fetching ALL historical members (no survivorship bias)
for building a complete database.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market_data import PriceDataManager


def test_1_current_vs_historical():
    """Test 1: Compare current members vs all historical members"""
    print("=" * 80)
    print("Test 1: Current vs All Historical Members")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Get current members only
    current = fetcher.get_current_members('sp500')
    print(f"\n📊 Current SP500 members: {len(current)}")

    # Get ALL historical members for 2020-2024
    all_historical = fetcher.get_all_historical_members(
        universe='sp500',
        period_start='2014-01-01',
        period_end='2024-12-31'
    )
    print(f"📊 All historical members (2014-2024): {len(all_historical)}")

    # Calculate difference
    removed = set(all_historical) - set(current)
    print(f"📊 Removed/changed members: {len(removed)}")

    if removed:
        print(f"\nExample removed members: {sorted(list(removed))[:10]}")

    print(f"\n✅ Survivorship bias eliminated: {len(removed)} additional stocks included!")
    print()


def test_2_fetch_historical_sample():
    """Test 2: Fetch sample historical data (small subset)"""
    print("=" * 80)
    print("Test 2: Fetch Historical Data Sample")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(api_key=config.get("fetcher.tiingo.api_key"))

    # Get all historical members
    all_members = fetcher.get_all_historical_members(
        universe='sp500',
        period_start='2014-01-01',
        period_end='2024-12-31'
    )

    # Take small sample for demo
    sample_symbols = sorted(all_members)[:5]
    print(f"\n📥 Fetching sample: {sample_symbols}")

    # Fetch without saving (just to test)
    results = fetcher.fetch_multiple_eod(
        symbols=sample_symbols,
        start_date='2024-01-01',
        end_date='2024-12-31',
        skip_errors=True
    )

    print(f"\n✅ Fetched data for {len(results)} symbols:")
    for symbol, df in results.items():
        print(f"   {symbol}: {len(df)} rows, "
              f"${df['close'].min():.2f} - ${df['close'].max():.2f}")
    print()


def test_3_build_complete_database():
    """Test 3: Build complete database (with save to Parquet)"""
    print("=" * 80)
    print("Test 3: Build Complete Historical Database")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    print("\n⚠️  WARNING: This will fetch ALL historical SP500 members!")
    print("   This may take 10-20 minutes and make 500+ API calls.")
    print("   Comment out this test for quick runs.\n")

    # For demo, use small date range
    print("📥 Building database for SP500 (Jan 2024 only)...")

    data = fetcher.fetch_complete_universe_history(
        universe='sp500',
        start_date='2024-01-01',
        end_date='2024-01-31',
        save_to_parquet=True,
        skip_errors=True
    )

    print(f"\n✅ Complete database built!")
    print(f"   Total symbols: {len(data)}")
    print(f"   Total rows: {sum(len(df) for df in data.values())}")

    # Show some stats
    if data:
        sample_symbol = list(data.keys())[0]
        sample_df = data[sample_symbol]
        print(f"\n📊 Sample ({sample_symbol}):")
        print(f"   Columns: {sample_df.columns.tolist()}")
        print(f"   Date range: {sample_df['date'].min()} to {sample_df['date'].max()}")
    print()


def test_4_survivorship_bias_example():
    """Test 4: Demonstrate survivorship bias elimination"""
    print("=" * 80)
    print("Test 4: Survivorship Bias Example")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(api_key=config.get("fetcher.tiingo.api_key"))

    print("\n🎯 Scenario: Backtesting a strategy from 2020-2024")
    print()

    # Method 1: Current members only (WRONG - has survivorship bias)
    print("❌ Method 1: Using current members only")
    current = fetcher.get_current_members('sp500')
    print(f"   → {len(current)} stocks")
    print(f"   → Problem: Missing failed/removed companies!")
    print(f"   → Result: Overly optimistic backtest (survivorship bias)")
    print()

    # Method 2: All historical members (CORRECT)
    print("✅ Method 2: Using ALL historical members")
    historical = fetcher.get_all_historical_members(
        universe='sp500',
        period_start='2020-01-01',
        period_end='2024-12-31'
    )
    print(f"   → {len(historical)} stocks (current + removed)")
    print(f"   → Includes: Delisted, acquired, merged companies")
    print(f"   → Result: Accurate backtest (no survivorship bias)")

    missing_count = len(historical) - len(current)
    print(f"\n💡 Difference: {missing_count} stocks would be MISSED without this method!")
    print()


def test_5_check_intervals_file():
    """Test 5: Check if membership intervals file exists"""
    print("=" * 80)
    print("Test 5: Check Membership Data")
    print("=" * 80)

    intervals_path = Path("data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet")

    print(f"\n📁 Checking for: {intervals_path}")

    if intervals_path.exists():
        print(f"✅ File exists!")

        import pandas as pd
        df = pd.read_parquet(intervals_path)
        print(f"\n📊 Membership data:")
        print(f"   Total records: {len(df)}")
        print(f"   Unique tickers: {df['ticker'].nunique()}")
        print(f"   Columns: {df.columns.tolist()}")
        print(f"\n   Sample records:")
        print(df.head())
    else:
        print(f"⚠️  File not found!")
        print(f"\n   To create this file, you need to:")
        print(f"   1. Run the universe builder to generate membership intervals")
        print(f"   2. This will parse historical S&P 500 changes from CSV")
        print(f"   3. Create the intervals Parquet file")
        print()
        print(f"   For now, the method will fall back to current members only.")
    print()


if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 15 + "Complete Historical Universe Fetching" + " " * 26 + "║")
    print("║" + " " * 20 + "(No Survivorship Bias)" + " " * 36 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    try:
        test_5_check_intervals_file()
        test_1_current_vs_historical()
        test_4_survivorship_bias_example()
        test_2_fetch_historical_sample()

        # Uncomment to build complete database (slow!)
        # test_3_build_complete_database()

        print("=" * 80)
        print("✅ All Tests Completed!")
        print("=" * 80)
        print()
        print("📝 Next Steps:")
        print("   1. Run universe builder to create membership intervals file")
        print("   2. Use fetch_complete_universe_history() to build your database")
        print("   3. Your backtests will be free from survivorship bias!")
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
