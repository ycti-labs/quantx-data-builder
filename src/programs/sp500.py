"""
Build Complete Historical Universe Data with Tiingo

Fetching ALL historical members (no survivorship bias)
for building a complete database.
"""

import datetime
import sys
from pathlib import Path

from tiingo import TiingoClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import PriceManager
from universe import SP500Universe

config = Config("config/settings.yaml")
sp500_universe = SP500Universe(
    data_root=config.get("storage.local.root_path"),
)
tiingo_client = TiingoClient({
    'api_key': config.get("fetcher.tiingo.api_key"),
    'session': True
})

def build_membership():
    stats = sp500_universe.build_membership(min_date=config.get('universes.sp500.start_date'))

    return stats

def get_spy_historical_data(frequency="daily"):
    start_date = config.get("universe.sp500.start_date")
    end_date = config.get("universe.sp500.end_date")

    price_data_mgr = PriceManager(
        tiingo=tiingo_client,
        universe=sp500_universe
    )

    result = price_data_mgr.fetch_eod(symbol='SPY', start_date=start_date, end_date=end_date, frequency=frequency, save=True)
    print( f"Fetched {len(result)} rows for SPY from {start_date} to {end_date}" )
    return result

def build_historic_database(frequency="daily"):
    """
    Build complete S&P 500 database for a date range

    This includes ALL stocks that were ever in the S&P 500 during this period,
    eliminating survivorship bias.
    """
    start_date = config.get("universe.sp500.start_date")
    end_date = config.get("universe.sp500.end_date")


    print("=" * 80)
    print(f"Building Complete S&P 500 Database: {start_date}-{end_date}")
    print("=" * 80)

    prica_data_mgr = PriceManager(
        tiingo=tiingo_client,
        universe=sp500_universe
    )

    print(f"\n📅 Date range: {start_date} to {end_date}")
    print(f"📊 Universe: S&P 500")
    print()

    # Step 1: Get all historical members
    print("\nFetching data (this will take a while)...")
    results = prica_data_mgr.fetch_universe_eod(
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        scope="historical",
        skip_errors=True,
        save=True
    )

    # Report results
    print("✅ Database Build Complete!")

    successful = len(results)
    total_rows = sum(len(df) for df in results.values())