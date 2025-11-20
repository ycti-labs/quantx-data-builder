#!/usr/bin/env python3
"""
Build Complete Historical Database - Practical Example

This script builds a complete, survivorship-bias-free database
for backtesting and analysis.
"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market_data import PriceDataManager


def build_sp500_database(start_year: int, end_year: int):
    """
    Build complete S&P 500 database for a date range

    This includes ALL stocks that were ever in the S&P 500 during this period,
    eliminating survivorship bias.

    Args:
        start_year: Starting year (e.g., 2020)
        end_year: Ending year (e.g., 2024)
    """
    print("=" * 80)
    print(f"Building Complete S&P 500 Database: {start_year}-{end_year}")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    print(f"\n📅 Date range: {start_date} to {end_date}")
    print(f"📊 Universe: S&P 500")
    print(f"💾 Storage: Hive-style Parquet (data/curated/prices/)")
    print()

    # Step 1: Get all historical members
    print("Step 1: Identifying all historical members...")
    all_members = fetcher.get_all_historical_members(
        universe='sp500',
        period_start=start_date,
        period_end=end_date
    )

    print(f"✅ Found {len(all_members)} unique stocks")
    print(f"   (includes current members + removed/delisted companies)")
    print()

    # Step 2: Estimate API calls and time
    print("Step 2: Planning fetch operation...")
    api_calls = len(all_members)
    estimated_time_minutes = api_calls * 0.15 / 60  # ~150ms per request

    print(f"   API calls needed: {api_calls}")
    print(f"   Estimated time: {estimated_time_minutes:.1f} minutes")
    print(f"   Rate limit: 500 req/hour (free tier)")
    print()

    # Step 3: Confirm before proceeding
    print("⚠️  This will make many API calls. Continue? (y/n)")
    response = input("   > ").strip().lower()

    if response != 'y':
        print("\n❌ Cancelled by user")
        return

    # Step 4: Fetch and save all data
    print("\nStep 3: Fetching data (this will take a while)...")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    start_time = datetime.now()

    results = fetcher.fetch_complete_universe_history(
        universe='sp500',
        start_date=start_date,
        end_date=end_date,
        save_to_parquet=True,
        skip_errors=True
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60

    # Step 5: Report results
    print()
    print("=" * 80)
    print("✅ Database Build Complete!")
    print("=" * 80)

    successful = len(results)
    failed = len(all_members) - successful
    total_rows = sum(len(df) for df in results.values())

    print(f"\n📊 Statistics:")
    print(f"   ✅ Successful: {successful} symbols")
    print(f"   ❌ Failed: {failed} symbols")
    print(f"   📈 Total rows: {total_rows:,}")
    print(f"   ⏱️  Duration: {duration:.1f} minutes")
    print(f"   🚀 Rate: {successful/duration:.1f} symbols/min")

    print(f"\n💾 Storage:")
    data_path = Path("data/curated/prices/exchange=us")
    if data_path.exists():
        parquet_files = list(data_path.rglob("*.parquet"))
        total_size = sum(f.stat().st_size for f in parquet_files)
        print(f"   Files: {len(parquet_files)}")
        print(f"   Total size: {total_size/1024/1024:.1f} MB")
        print(f"   Average per symbol: {total_size/successful/1024:.1f} KB")

    print(f"\n✅ Your survivorship-bias-free database is ready!")
    print(f"   Location: data/curated/prices/")
    print()


def build_incremental_update():
    """
    Incrementally update database with latest data

    Use this for daily/weekly updates after initial build
    """
    print("=" * 80)
    print("Incremental Update - S&P 500 Latest Data")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Get current date
    today = datetime.now().strftime('%Y-%m-%d')

    # Update with last 5 days of data (to catch any gaps)
    from datetime import timedelta
    five_days_ago = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')

    print(f"\n📅 Updating: {five_days_ago} to {today}")
    print()

    # Get current members only (for incremental updates)
    current_members = fetcher.get_current_members('sp500')
    print(f"📊 Updating {len(current_members)} current members")
    print()

    # Fetch and save
    results = fetcher.fetch_and_save_multiple(
        symbols=current_members,
        start_date=five_days_ago,
        end_date=today,
        skip_errors=True
    )

    successful = len(results)
    print(f"\n✅ Updated {successful} symbols")
    print()


def build_sample_database():
    """
    Build a small sample database for testing (fast)
    """
    print("=" * 80)
    print("Build Sample Database - Quick Test")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Just one month of data
    print("\n📅 Building sample: January 2024")
    print()

    results = fetcher.fetch_complete_universe_history(
        universe='sp500',
        start_date='2024-01-01',
        end_date='2024-01-31',
        save_to_parquet=True,
        skip_errors=True
    )

    print(f"\n✅ Sample database built!")
    print(f"   Symbols: {len(results)}")
    print(f"   Total rows: {sum(len(df) for df in results.values()):,}")
    print()


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Build Complete Historical Database - Menu")
    print("=" * 80)
    print()
    print("1. Build Complete Database (2020-2024) - ~30 minutes")
    print("2. Build Sample Database (Jan 2024 only) - ~5 minutes")
    print("3. Incremental Update (last 5 days) - ~2 minutes")
    print("4. Custom Date Range")
    print("5. Exit")
    print()

    choice = input("Select option (1-5): ").strip()
    print()

    try:
        if choice == '1':
            build_sp500_database(2020, 2024)
        elif choice == '2':
            build_sample_database()
        elif choice == '3':
            build_incremental_update()
        elif choice == '4':
            start_year = int(input("Start year: ").strip())
            end_year = int(input("End year: ").strip())
            build_sp500_database(start_year, end_year)
        elif choice == '5':
            print("👋 Goodbye!")
        else:
            print("❌ Invalid choice")

    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("\nPlease set your TIINGO_API_KEY:")
        print("  export TIINGO_API_KEY=your_key_here")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
