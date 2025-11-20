#!/usr/bin/env python3
"""
Build Complete Historical Universe Data with Tiingo

Fetching ALL historical members (no survivorship bias)
for building a complete database.
"""

import datetime
import sys
from asyncio.log import logger
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market_data import PriceDataManager
from universe import SP500Universe

def build_sp500_membership():
    """Build S&P 500 membership datasets from raw historical data."""
    config = Config("config/settings.yaml")

    # Initialize builder with default paths
    sp500 = SP500Universe(
        data_root=config.get("storage.local.root_path"),
    )

    stats = sp500.build_membership(min_date=config.get('universes.sp500.start_date'))

    return stats

def get_spy_historical_data():
    config = Config("config/settings.yaml")
    start_date = config.get("universe.sp500.start_date")
    end_date = config.get("universe.sp500.end_date")

    price_data_mgr = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        universe=SP500Universe(data_root=config.get("storage.local.root_path"))
    )

    result = price_data_mgr.fetch_and_save('SPY', start_date, end_date)
    print( f"Fetched {len(result)} rows for SPY from {start_date} to {end_date}" )
    return result

def build_sp500_database():
    """
    Build complete S&P 500 database for a date range

    This includes ALL stocks that were ever in the S&P 500 during this period,
    eliminating survivorship bias.
    """
    config = Config("config/settings.yaml")
    start_date = config.get("universe.sp500.start_date")
    end_date = config.get("universe.sp500.end_date")


    print("=" * 80)
    print(f"Building Complete S&P 500 Database: {start_date}-{end_date}")
    print("=" * 80)

    prica_data_mgr = PriceDataManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        universe=SP500Universe(data_root=config.get("storage.local.root_path"))
    )


    print(f"\n📅 Date range: {start_date} to {end_date}")
    print(f"📊 Universe: S&P 500")
    print(f"💾 Storage: Hive-style Parquet (data/curated/prices/)")
    print()

    # Step 1: Get all historical members
    print("\nFetching data (this will take a while)...")
    results = prica_data_mgr.fetch_complete_universe_history(
        start_date=start_date,
        end_date=end_date,
        save_to_parquet=True,
        skip_errors=True
    )

    # Step 5: Report results
    print()
    print("=" * 80)
    print("✅ Database Build Complete!")
    print("=" * 80)

    successful = len(results)

    total_rows = sum(len(df) for df in results.values())


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