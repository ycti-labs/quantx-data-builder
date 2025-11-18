#!/usr/bin/env python3
"""
Fetch Missing Data - Fetch data for missing tickers identified by check_missing_data.py

This script reads the output from check_missing_data.py and fetches the missing data.
"""

import sys
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.market_data import FetcherConfig, PriceDataManager


def fetch_missing_tickers(
    missing_tickers: List[str],
    start_date: str,
    end_date: str,
    api_key: str,
    exchange: str = "us",
    currency: str = "USD"
):
    """
    Fetch data for missing tickers

    Args:
        missing_tickers: List of ticker symbols to fetch
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        api_key: API key for data source
        exchange: Exchange code (default: 'us')
        currency: Currency code (default: 'USD')
    """
    print()
    print("=" * 80)
    print("Fetch Missing Data")
    print("=" * 80)
    print()
    print(f"📥 Fetching {len(missing_tickers)} missing tickers")
    print(f"📅 Period: {start_date} to {end_date}")
    print(f"🌐 Exchange: {exchange}")
    print()

    builder = PriceDataManager(api_key=api_key, data_root="data/curated")

    successful = []
    failed = []

    for i, symbol in enumerate(missing_tickers, 1):
        print(f"\n[{i}/{len(missing_tickers)}] Fetching {symbol}...", end=" ")
        try:
            builder.fetch_and_save(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                currency=currency
            )
            print("✅")
            successful.append(symbol)
        except Exception as e:
            print(f"❌ Error: {e}")
            failed.append((symbol, str(e)))

    # Summary
    print()
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print()
    print(f"✅ Successful: {len(successful)}/{len(missing_tickers)}")
    print(f"❌ Failed:     {len(failed)}/{len(missing_tickers)}")
    print()

    if failed:
        print("Failed tickers:")
        for symbol, error in failed:
            print(f"  • {symbol}: {error}")
        print()


def main():
    """Main function"""
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 25 + "Fetch Missing Data" + " " * 34 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Load config
    try:
        config = FetcherConfig("config/settings.yaml")
        api_key = config.fetcher.tiingo.api_key
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return 1

    # Example: Fetch missing S&P 500 tickers for 2020-2024
    # Update this list based on check_missing_data.py output
    missing_tickers = [
        'ABC', 'ADS', 'ANTM', 'BF.B', 'BLL', 'BRK.B', 'COG',
        'FB', 'FBHS', 'FLT', 'FRC', 'GPS', 'HFC', 'PEAK',
        'PKI', 'RE', 'WLTW'
    ]

    fetch_missing_tickers(
        missing_tickers=missing_tickers,
        start_date="2020-01-01",
        end_date="2024-12-31",
        api_key=api_key,
        exchange="us",
        currency="USD"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
