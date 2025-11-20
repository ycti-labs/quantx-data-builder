#!/usr/bin/env python3
"""
Quick Reference: PriceDataManager Save to Parquet

Common usage patterns for saving and loading data.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market_data import PriceDataManager


def setup_fetcher():
    """Initialize fetcher with config"""
    config = Config("config/settings.yaml")
    return PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )


def example_1_fetch_and_save():
    """Example 1: Fetch and save historical data"""
    print("Example 1: Fetch and Save Historical Data")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Fetch and save 5 years of AAPL data
    df, paths = fetcher.fetch_and_save(
        symbol='AAPL',
        start_date='2020-01-01',
        end_date='2024-12-31'
    )

    print(f"✅ Saved {len(df)} rows to {len(paths)} files")
    print()


def example_2_daily_update():
    """Example 2: Daily incremental update"""
    print("Example 2: Daily Incremental Update")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Fetch today's data (will merge with existing)
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')

    df, paths = fetcher.fetch_and_save(
        symbol='AAPL',
        start_date=today
    )

    print(f"✅ Updated with {len(df)} new rows")
    print()


def example_3_batch_save():
    """Example 3: Batch save multiple symbols"""
    print("Example 3: Batch Save Multiple Symbols")
    print("-" * 60)

    fetcher = setup_fetcher()

    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA']

    results = fetcher.fetch_and_save_multiple(
        symbols=symbols,
        start_date='2024-01-01',
        end_date='2024-12-31'
    )

    for symbol, (df, paths) in results.items():
        print(f"✅ {symbol}: {len(df)} rows")
    print()


def example_4_load_data():
    """Example 4: Load saved data"""
    print("Example 4: Load Saved Data")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Load AAPL data for 2024
    df = fetcher.load_price_data(
        symbol='AAPL',
        start_date='2024-01-01',
        end_date='2024-12-31'
    )

    print(f"✅ Loaded {len(df)} rows")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Columns: {len(df.columns)}")
    print()


def example_5_backfill():
    """Example 5: Backfill missing data"""
    print("Example 5: Backfill Missing Data")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Backfill from 2000 to 2024
    df, paths = fetcher.fetch_and_save(
        symbol='AAPL',
        start_date='2000-01-01',
        end_date='2024-12-31'
    )

    print(f"✅ Backfilled {len(df)} rows")
    print(f"✅ Saved to {len(paths)} year partitions")
    print()


def example_6_resume_failed():
    """Example 6: Resume failed batch operation"""
    print("Example 6: Resume Failed Batch Operation")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Large list of symbols
    all_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
                   'TSLA', 'META', 'NFLX', 'INVALID', 'AAPL']

    # This will skip errors and continue
    results = fetcher.fetch_and_save_multiple(
        symbols=all_symbols,
        start_date='2024-01-01',
        skip_errors=True  # Continue even if some fail
    )

    print(f"✅ Successfully saved {len(results)}/{len(all_symbols)} symbols")
    print()


def example_7_custom_exchange():
    """Example 7: Save data for different exchanges"""
    print("Example 7: Different Exchanges")
    print("-" * 60)

    fetcher = setup_fetcher()

    # Save US stock
    df_us, _ = fetcher.fetch_and_save(
        symbol='AAPL',
        start_date='2024-01-01',
        exchange='us',
        currency='USD'
    )
    print(f"✅ US: AAPL - {len(df_us)} rows")

    # Note: For HK stocks, you'd need to use the correct symbol format
    # and possibly a different data source
    print()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("PriceDataManager Save to Parquet - Quick Reference")
    print("=" * 60 + "\n")

    examples = [
        ("1. Fetch and Save Historical", example_1_fetch_and_save),
        ("2. Daily Incremental Update", example_2_daily_update),
        ("3. Batch Save Multiple Symbols", example_3_batch_save),
        ("4. Load Saved Data", example_4_load_data),
        ("5. Backfill Historical Data", example_5_backfill),
        ("6. Resume Failed Operations", example_6_resume_failed),
        ("7. Multiple Exchanges", example_7_custom_exchange),
    ]

    print("Available examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")

    print("\nRun specific example:")
    print("  python examples/save_quick_reference.py")
    print()

    # Uncomment to run all examples:
    # for name, func in examples:
    #     try:
    #         func()
    #     except Exception as e:
    #         print(f"❌ Error in {name}: {e}\n")
