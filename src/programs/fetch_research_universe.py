"""
Fetch Complete Research Universe Data (Daily, Weekly, Monthly)

This program fetches price data for the entire S&P 500 historical universe
across multiple frequencies for the research period defined in config/settings.yaml.

Features:
- Fetches daily, weekly, and monthly OHLCV data
- Uses research period from config (2014-01-01 to 2024-12-01)
- Handles all historical members (no survivorship bias)
- Parallel fetching with progress tracking
- Automatic retry on failures
- Saves data in partitioned Parquet format

Usage:
    # Fetch all frequencies
    python src/programs/fetch_research_universe.py

    # Fetch specific frequencies
    python src/programs/fetch_research_universe.py --frequencies daily weekly

    # Dry run to see what would be fetched
    python src/programs/fetch_research_universe.py --dry-run

    # Skip already-fetched tickers
    python src/programs/fetch_research_universe.py --skip-existing
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

from tiingo import TiingoClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import PriceManager
from universe import SP500Universe


def get_existing_tickers(data_root: str, frequency: str) -> Set[str]:
    """
    Scan for tickers that already have data for a given frequency

    Args:
        data_root: Root data directory path
        frequency: 'daily', 'weekly', or 'monthly'

    Returns:
        Set of ticker symbols that have existing data
    """
    existing = set()
    tickers_path = Path(data_root) / "curated" / "tickers" / "exchange=us"

    if not tickers_path.exists():
        return existing

    for ticker_dir in tickers_path.iterdir():
        if not ticker_dir.is_dir():
            continue

        ticker = ticker_dir.name.replace("ticker=", "")
        freq_path = ticker_dir / "prices" / f"freq={frequency}"

        if freq_path.exists() and any(freq_path.iterdir()):
            existing.add(ticker)

    return existing


def fetch_universe_data(
    frequency: str,
    start_date: str,
    end_date: str,
    config: Config,
    tiingo_client: TiingoClient,
    sp500_universe: SP500Universe,
    skip_existing: bool = False,
    dry_run: bool = False,
) -> Dict[str, str | int]:
    """
    Fetch data for entire universe at specified frequency

    Args:
        frequency: 'daily', 'weekly', or 'monthly'
        start_date: Start date (ISO format)
        end_date: End date (ISO format)
        config: Configuration object
        tiingo_client: Tiingo API client
        sp500_universe: Universe object
        skip_existing: Skip tickers with existing data
        dry_run: Show what would be fetched without fetching

    Returns:
        Dictionary with statistics
    """
    print(f"\n{'=' * 80}")
    print(f"Fetching {frequency.upper()} Data")
    print(f"{'=' * 80}")
    print(f"📅 Period: {start_date} to {end_date}")

    # Get historical members
    members = sp500_universe.get_all_historical_members(
        start_date=start_date, end_date=end_date
    )

    total_tickers = len(members)
    print(f"📊 Universe: {total_tickers} historical tickers")

    # Check for existing data
    to_fetch = members
    if skip_existing:
        existing = get_existing_tickers(
            config.get("storage.local.root_path"), frequency
        )
        to_fetch = [t for t in members if t not in existing]
        print(f"⏭️  Skipping: {len(existing)} tickers (already have data)")
        print(f"📥 To fetch: {len(to_fetch)} tickers")
    else:
        print(f"📥 To fetch: {len(to_fetch)} tickers (including existing)")

    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No data will be fetched")
        print(f"Would fetch {frequency} data for {len(to_fetch)} tickers:")
        for i, ticker in enumerate(to_fetch[:10], 1):
            print(f"  {i}. {ticker}")
        if len(to_fetch) > 10:
            print(f"  ... and {len(to_fetch) - 10} more")
        return {
            "frequency": frequency,
            "total": total_tickers,
            "existing": len(members) - len(to_fetch),
            "to_fetch": len(to_fetch),
            "fetched": 0,
            "failed": 0,
        }

    if not to_fetch:
        print(f"✅ All tickers already have {frequency} data!")
        return {
            "frequency": frequency,
            "total": total_tickers,
            "existing": total_tickers,
            "to_fetch": 0,
            "fetched": 0,
            "failed": 0,
        }

    # Initialize PriceManager
    price_manager = PriceManager(tiingo=tiingo_client, universe=sp500_universe)

    print(f"\n⏳ Fetching data (this may take a while)...")
    print(f"🔄 Max workers: {config.get('fetcher.max_workers', 10)}")

    # Fetch data for universe
    results = price_manager.fetch_universe_eod(
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        scope="historical",
        skip_errors=True,
        save=True,
    )

    # Calculate statistics
    successful = len([r for r in results.values() if r is not None and len(r) > 0])
    failed = len(to_fetch) - successful
    total_rows = sum(len(df) for df in results.values() if df is not None)

    print(f"\n✅ {frequency.upper()} Data Fetch Complete!")
    print(f"   Successfully fetched: {successful}/{len(to_fetch)} tickers")
    print(f"   Total data rows: {total_rows:,}")
    if failed > 0:
        print(f"   ⚠️  Failed: {failed} tickers")

    return {
        "frequency": frequency,
        "total": total_tickers,
        "existing": len(members) - len(to_fetch),
        "to_fetch": len(to_fetch),
        "fetched": successful,
        "failed": failed,
        "rows": total_rows,
    }


def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(
        description="Fetch complete research universe data at multiple frequencies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all frequencies (daily, weekly, monthly)
  python src/programs/fetch_research_universe.py

  # Fetch only daily and weekly
  python src/programs/fetch_research_universe.py --frequencies daily weekly

  # Skip tickers with existing data
  python src/programs/fetch_research_universe.py --skip-existing

  # Dry run to preview
  python src/programs/fetch_research_universe.py --dry-run

  # Custom date range (overrides config)
  python src/programs/fetch_research_universe.py --start-date 2020-01-01 --end-date 2024-12-31
        """,
    )

    parser.add_argument(
        "--frequencies",
        nargs="+",
        choices=["daily", "weekly", "monthly"],
        default=["daily", "weekly", "monthly"],
        help="Frequencies to fetch (default: all)",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD). Overrides config setting.",
    )

    parser.add_argument(
        "--end-date", type=str, help="End date (YYYY-MM-DD). Overrides config setting."
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers that already have data for the frequency",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be fetched without actually fetching",
    )

    parser.add_argument(
        "--rebuild-membership",
        action="store_true",
        help="Rebuild S&P 500 membership data before fetching",
    )

    args = parser.parse_args()

    # Load configuration
    print("=" * 80)
    print("QuantX Research Universe Data Fetcher")
    print("=" * 80)
    print(f"⚙️  Loading configuration from config/settings.yaml...")

    config = Config("config/settings.yaml")

    # Get date range from config or command line
    start_date = args.start_date or config.get("universe.sp500.start_date")
    end_date = args.end_date or config.get("universe.sp500.end_date")

    print(f"\n📊 Research Period:")
    print(f"   Start: {start_date}")
    print(f"   End: {end_date}")
    print(f"   Frequencies: {', '.join(args.frequencies)}")

    if args.skip_existing:
        print(f"   Mode: Skip existing data")
    else:
        print(f"   Mode: Fetch all (may overwrite)")

    if args.dry_run:
        print(f"   🔍 DRY RUN MODE - No data will be fetched")

    # Initialize universe
    print(f"\n🌍 Initializing S&P 500 Universe...")
    sp500_universe = SP500Universe(
        data_root=config.get("storage.local.root_path"),
    )

    # Rebuild membership if requested
    if args.rebuild_membership:
        print(f"\n🔄 Rebuilding membership data...")
        membership_file = config.get("universe.sp500.membership_file")
        stats = sp500_universe.build_membership(
            min_date=start_date, rebuild=True, membership_filename=membership_file
        )
        print(
            f"✅ Membership rebuilt: {stats['unique_tickers']} tickers, "
            f"{stats['first_date']} to {stats['last_date']}"
        )

    # Initialize Tiingo client
    print(f"\n🔌 Connecting to Tiingo API...")
    tiingo_client = TiingoClient(
        {"api_key": config.get("fetcher.tiingo.api_key"), "session": True}
    )
    print(f"✅ Connected")

    # Fetch data for each frequency
    all_stats = []
    total_start = datetime.now()

    for frequency in args.frequencies:
        freq_start = datetime.now()

        stats = fetch_universe_data(
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            config=config,
            tiingo_client=tiingo_client,
            sp500_universe=sp500_universe,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
        )

        freq_duration = (datetime.now() - freq_start).total_seconds()
        stats["duration_seconds"] = int(freq_duration)
        all_stats.append(stats)

    total_duration = (datetime.now() - total_start).total_seconds()

    # Print final summary
    print(f"\n{'=' * 80}")
    print("📊 FINAL SUMMARY")
    print(f"{'=' * 80}")

    for stats in all_stats:
        print(f"\n{stats['frequency'].upper()}:")
        print(f"   Universe size: {stats['total']} tickers")
        if stats["existing"] > 0:
            print(f"   Already had data: {stats['existing']} tickers")
        print(f"   Attempted fetch: {stats['to_fetch']} tickers")
        if not args.dry_run:
            print(f"   Successfully fetched: {stats['fetched']} tickers")
            if stats.get("rows"):
                print(f"   Total data rows: {stats['rows']:,}")
            if stats["failed"] > 0:
                print(f"   ⚠️  Failed: {stats['failed']} tickers")
        print(f"   Duration: {stats['duration_seconds']:.1f} seconds")

    print(
        f"\n⏱️  Total execution time: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)"
    )

    if not args.dry_run:
        total_fetched = sum(s["fetched"] for s in all_stats)
        total_failed = sum(s["failed"] for s in all_stats)
        total_rows = sum(s.get("rows", 0) for s in all_stats)

        print(f"\n✅ Complete!")
        print(f"   Total tickers fetched: {total_fetched}")
        print(f"   Total data rows: {total_rows:,}")
        if total_failed > 0:
            print(f"   ⚠️  Total failures: {total_failed}")

        # Data location
        print(f"\n📁 Data saved to:")
        print(
            f"   {config.get('storage.local.root_path')}/curated/tickers/exchange=us/"
        )
        print(f"\n💡 Next steps:")
        print(f"   1. Validate data: python tests/check_universe_overlap_aware.py")
        print(
            f"   2. Check ESG continuity: python src/programs/check_esg_continuity.py"
        )
        print(f"   3. Build factors: python tests/demo_esg_factor_builder.py")
    else:
        print(f"\n🔍 DRY RUN COMPLETE - No data was fetched")
        print(f"   Remove --dry-run flag to actually fetch the data")


if __name__ == "__main__":
    main()
