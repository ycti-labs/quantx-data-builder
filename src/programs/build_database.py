#!/usr/bin/env python3
"""
Build Complete Historical Universe Data with Tiingo

Fetching ALL historical members (no survivorship bias)
for building a complete database.
"""

import sys
from asyncio.log import logger
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.market_data import FetcherConfig, PriceDataManager
from src.universe import SP500Universe


def build_complete_database():
    print("=" * 80)
    print("Test 3: Build Complete Historical Database")
    print("=" * 80)

    config = FetcherConfig("config/settings.yaml")

    sp500_universe = SP500Universe(
        data_root="data/curated",
        raw_data_root="data/raw"
    )

    price_data_mgr = PriceDataManager(
        api_key=config.fetcher.tiingo.api_key,
        data_root="data/curated",
        universe=sp500_universe
    )

    print("\n⚠️  WARNING: This will fetch ALL historical SP500 members!")
    print("   This may take 10-20 minutes and make 500+ API calls.")
    print("   Comment out this test for quick runs.\n")

    # For demo, use small date range
    print("📥 Building database for SP500 (2014-2024)...")

    data = price_data_mgr.fetch_complete_universe_history(
        start_date='2014-01-01',
        end_date='2024-12-31',
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

if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 15 + "Complete Historical Universe Fetching" + " " * 26 + "║")
    print("║" + " " * 20 + "(No Survivorship Bias)" + " " * 36 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    try:
        build_complete_database()

        # print("   1. Run universe builder to create membership intervals file")
        # print("   2. Use fetch_complete_universe_history() to build your database")
        # print("   3. Your backtests will be free from survivorship bias!")
        # print()

    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("\nPlease set your TIINGO_API_KEY:")
        print("  export TIINGO_API_KEY=your_key_here")
        print()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
