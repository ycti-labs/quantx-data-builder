#!/usr/bin/env python3
"""
Fetch Corrected Tickers

Fetches the correct/replacement tickers for missing symbols.
Based on research from investigate_tickers.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market_data import PriceDataManager


def main():
    """Main function"""
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Fetch Corrected Tickers" + " " * 35 + "║")
    print("║" + " " * 15 + "Replacement tickers for missing data" + " " * 27 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Load config
    try:
        config = Config("config/settings.yaml")
        api_key = config.get("fetcher.tiingo.api_key")
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return 1

    builder = PriceDataManager(api_key=api_key, data_root="data/curated")

    # Define ticker replacements
    replacements = {
        # Ticker changes
        'FB': 'META',           # Facebook → Meta Platforms
        'ANTM': 'ELV',         # Anthem → Elevance Health
        'WLTW': 'WTW',         # Willis Towers Watson (simplified)
        'ADS': 'ALLY',         # Alliance Data Systems → Bread Financial

        # Mergers & Acquisitions
        'COG': 'CTRA',         # Cabot Oil & Gas → Coterra Energy
        'HFC': 'HF',           # HollyFrontier → HF Sinclair

        # Corporate splits
        'FBHS': 'FBIN',        # Fortune Brands → Fortune Brands Innovations
        'PKI': 'RVTY',         # PerkinElmer → Revvity
    }

    # Tickers with special characters (try hyphen version)
    special_chars = {
        'BF.B': 'BF-B',        # Brown-Forman Class B
        'BRK.B': 'BRK-B',      # Berkshire Hathaway Class B
    }

    # Active tickers to retry
    retry_tickers = [
        'GPS',   # Gap Inc. (still public)
        'RE',    # Everest Re Group (in S&P 500)
        'BLL',   # Ball Corporation (in S&P 500)
    ]

    # Cannot fetch (delisted/failed)
    cannot_fetch = {
        'FRC': 'Bank failure (2023-05-01)',
        'FLT': 'Taken private (2021)',
    }

    # Print plan
    print("📋 FETCH PLAN:")
    print()
    print(f"  Ticker replacements:  {len(replacements)} tickers")
    print(f"  Special characters:   {len(special_chars)} tickers")
    print(f"  Retry active:         {len(retry_tickers)} tickers")
    print(f"  Cannot fetch:         {len(cannot_fetch)} tickers")
    print()

    successful = []
    failed = []

    # Fetch replacement tickers
    print("=" * 80)
    print("1️⃣  Fetching Replacement Tickers")
    print("=" * 80)
    print()

    for old_ticker, new_ticker in replacements.items():
        print(f"[{old_ticker} → {new_ticker}] Fetching...", end=" ")
        try:
            builder.fetch_and_save(
                symbol=new_ticker,
                start_date='2014-01-01',
                end_date='2024-12-31',
                exchange='us'
            )
            print("✅")
            successful.append(f"{old_ticker} → {new_ticker}")
        except Exception as e:
            print(f"❌ Error: {e}")
            failed.append((f"{old_ticker} → {new_ticker}", str(e)))

    # Try special character tickers
    print()
    print("=" * 80)
    print("2️⃣  Fetching Tickers with Special Characters")
    print("=" * 80)
    print()

    for old_ticker, new_ticker in special_chars.items():
        print(f"[{old_ticker} → {new_ticker}] Fetching...", end=" ")
        try:
            builder.fetch_and_save(
                symbol=new_ticker,
                start_date='2014-01-01',
                end_date='2024-12-31',
                exchange='us'
            )
            print("✅")
            successful.append(f"{old_ticker} → {new_ticker}")
        except Exception as e:
            print(f"❌ Error: {e}")
            failed.append((f"{old_ticker} → {new_ticker}", str(e)))

    # Retry active tickers
    print()
    print("=" * 80)
    print("3️⃣  Retrying Active Tickers")
    print("=" * 80)
    print()

    for ticker in retry_tickers:
        print(f"[{ticker}] Fetching...", end=" ")
        try:
            builder.fetch_and_save(
                symbol=ticker,
                start_date='2014-01-01',
                end_date='2024-12-31',
                exchange='us'
            )
            print("✅")
            successful.append(ticker)
        except Exception as e:
            print(f"❌ Error: {e}")
            failed.append((ticker, str(e)))

    # Summary
    print()
    print("=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)
    print()
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed:     {len(failed)}")
    print()

    if successful:
        print("Successful fetches:")
        for item in successful:
            print(f"  ✅ {item}")
        print()

    if failed:
        print("Failed fetches:")
        for item, error in failed:
            print(f"  ❌ {item}")
            print(f"     Error: {error}")
        print()

    if cannot_fetch:
        print("Cannot fetch (known issues):")
        for ticker, reason in cannot_fetch.items():
            print(f"  ⚠️  {ticker}: {reason}")
        print()

    # Next steps
    print("=" * 80)
    print("🎯 NEXT STEPS")
    print("=" * 80)
    print()
    print("1. Run check_missing_data.py again to verify coverage improved")
    print("2. For failed tickers, check Tiingo availability manually")
    print("3. Consider alternative data sources for delisted companies")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
