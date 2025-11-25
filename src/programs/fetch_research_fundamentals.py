"""
Fetch Complete Research Universe Fundamental Data

This program fetches fundamental data (financial statements and daily metrics) for the
entire S&P 500 historical universe for the research period defined in config/settings.yaml.

Features:
- Fetches financial statements via get_fundamentals_statements (quarterly/annual)
  * Income statements
  * Balance sheets
  * Cash flow statements
- Fetches daily metrics via get_fundamentals_daily (daily)
  * Market cap, enterprise value
  * P/E ratio, P/B ratio, dividend yield
  * And many more valuation metrics
- Uses research period from config (2014-01-01 to 2024-12-01)
- Handles all historical members (no survivorship bias)
- Parallel fetching with progress tracking
- Automatic retry on failures
- Saves data in partitioned Parquet format

Usage:
    # Fetch both statements and daily metrics
    python src/programs/fetch_research_fundamentals.py

    # Fetch only statements (get_fundamentals_statements)
    python src/programs/fetch_research_fundamentals.py --data-types statements

    # Fetch only daily metrics (get_fundamentals_daily)
    python src/programs/fetch_research_fundamentals.py --data-types daily

    # Dry run to see what would be fetched
    python src/programs/fetch_research_fundamentals.py --dry-run

    # Skip already-fetched tickers
    python src/programs/fetch_research_fundamentals.py --skip-existing
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

from tiingo import TiingoClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import FundamentalManager
from universe import SP500Universe


def get_existing_tickers(data_root: str, data_type: str) -> Set[str]:
    """
    Scan for tickers that already have fundamental data

    Args:
        data_root: Root data directory path
        data_type: 'statements' or 'daily'

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
        fund_path = ticker_dir / "fundamentals"

        if not fund_path.exists():
            continue

        # Check based on data type
        if data_type == "statements":
            # Check for any statement type directories (income, balance, cashflow)
            statement_dirs = [
                p
                for p in fund_path.iterdir()
                if p.is_dir()
                and p.name.startswith("statement=")
                and p.name != "statement=metrics"
                and p.name != "statement=daily"
            ]
            if statement_dirs and any(
                any((sd / "year=*").glob("*")) for sd in statement_dirs
            ):
                existing.add(ticker)
        elif data_type == "daily":
            # Check for daily metrics directory
            daily_path = fund_path / "statement=daily"
            if not daily_path.exists():
                # Also check legacy 'metrics' naming
                daily_path = fund_path / "statement=metrics"
            if daily_path.exists() and any(daily_path.iterdir()):
                existing.add(ticker)

    return existing


def fetch_universe_statements(
    start_date: str,
    end_date: str,
    config: Config,
    tiingo_client: TiingoClient,
    sp500_universe: SP500Universe,
    skip_existing: bool = False,
    dry_run: bool = False,
) -> Dict[str, str | int]:
    """
    Fetch financial statements (quarterly/annual) for entire universe

    Uses Tiingo's get_fundamentals_statements API which returns:
    - Income statements
    - Balance sheets
    - Cash flow statements

    Data is quarterly and annual, not daily.

    Args:
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
    print(f"Fetching FINANCIAL STATEMENTS (via get_fundamentals_statements)")
    print(f"{'=' * 80}")
    print(f"📅 Period: {start_date} to {end_date}")
    print(f"📊 Types: Income statements, Balance sheets, Cash flow statements")
    print(f"📆 Frequency: Quarterly and Annual")

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
            config.get("storage.local.root_path"), "statements"
        )
        to_fetch = [t for t in members if t not in existing]
        print(f"⏭️  Skipping: {len(existing)} tickers (already have statements)")
        print(f"📥 To fetch: {len(to_fetch)} tickers")
    else:
        print(f"📥 To fetch: {len(to_fetch)} tickers (including existing)")

    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No data will be fetched")
        print(f"Would fetch statements for {len(to_fetch)} tickers:")
        for i, ticker in enumerate(to_fetch[:10], 1):
            print(f"  {i}. {ticker}")
        if len(to_fetch) > 10:
            print(f"  ... and {len(to_fetch) - 10} more")
        return {
            "data_type": "statements",
            "total": total_tickers,
            "existing": len(members) - len(to_fetch),
            "to_fetch": len(to_fetch),
            "fetched": 0,
            "failed": 0,
        }

    if not to_fetch:
        print(f"✅ All tickers already have statements data!")
        return {
            "data_type": "statements",
            "total": total_tickers,
            "existing": total_tickers,
            "to_fetch": 0,
            "fetched": 0,
            "failed": 0,
        }

    # Initialize FundamentalManager
    fundamental_manager = FundamentalManager(
        tiingo=tiingo_client, universe=sp500_universe
    )

    print(f"\n⏳ Fetching statements (this may take a while)...")
    print(f"💡 API: tiingo.get_fundamentals_statements(symbol, startDate, endDate)")
    print(f"💡 Note: Fundamentals API may be slower than price API")

    # Fetch data for universe
    successful = 0
    failed = 0
    total_records = 0

    for i, symbol in enumerate(to_fetch, 1):
        if i % 10 == 0 or i == len(to_fetch):
            print(
                f"  Progress: {i}/{len(to_fetch)} ({successful} success, {failed} failed)"
            )

        try:
            df, paths = fundamental_manager.fetch_fundamentals(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                save=True,
            )

            if df is not None and not df.empty:
                successful += 1
                total_records += len(df)
            else:
                failed += 1

        except Exception as e:
            print(f"⚠️  Error fetching {symbol}: {e}")
            failed += 1

    print(f"\n✅ STATEMENTS Data Fetch Complete!")
    print(f"   Successfully fetched: {successful}/{len(to_fetch)} tickers")
    print(f"   Total records: {total_records:,}")
    if failed > 0:
        print(f"   ⚠️  Failed: {failed} tickers")

    return {
        "data_type": "statements",
        "total": total_tickers,
        "existing": len(members) - len(to_fetch),
        "to_fetch": len(to_fetch),
        "fetched": successful,
        "failed": failed,
        "records": total_records,
    }


def fetch_universe_daily(
    start_date: str,
    end_date: str,
    config: Config,
    tiingo_client: TiingoClient,
    sp500_universe: SP500Universe,
    skip_existing: bool = False,
    dry_run: bool = False,
) -> Dict[str, str | int]:
    """
    Fetch daily fundamental metrics for entire universe

    Uses Tiingo's get_fundamentals_daily API which returns daily metrics:
    - Market cap
    - Enterprise value
    - P/E ratio
    - P/B ratio
    - Dividend yield
    - And many more valuation metrics

    Data is daily, aligned with trading days.

    Args:
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
    print(f"Fetching DAILY FUNDAMENTAL METRICS (via get_fundamentals_daily)")
    print(f"{'=' * 80}")
    print(f"📅 Period: {start_date} to {end_date}")
    print(f"📊 Metrics: Market cap, P/E, P/B, EV, dividend yield, etc.")
    print(f"📆 Frequency: Daily (aligned with trading days)")

    # Get historical members (to avoid survivorship bias)
    members = sp500_universe.get_all_historical_members(
        start_date=start_date, end_date=end_date
    )

    total_tickers = len(members)
    print(f"📊 Universe: {total_tickers} historical tickers")

    # Check for existing data
    to_fetch = members
    if skip_existing:
        existing = get_existing_tickers(config.get("storage.local.root_path"), "daily")
        to_fetch = [t for t in members if t not in existing]
        print(f"⏭️  Skipping: {len(existing)} tickers (already have daily metrics)")
        print(f"📥 To fetch: {len(to_fetch)} tickers")
    else:
        print(f"📥 To fetch: {len(to_fetch)} tickers (including existing)")

    if dry_run:
        print(f"\n🔍 DRY RUN MODE - No data will be fetched")
        print(f"Would fetch daily metrics for {len(to_fetch)} tickers:")
        for i, ticker in enumerate(to_fetch[:10], 1):
            print(f"  {i}. {ticker}")
        if len(to_fetch) > 10:
            print(f"  ... and {len(to_fetch) - 10} more")
        return {
            "data_type": "daily",
            "total": total_tickers,
            "existing": len(members) - len(to_fetch),
            "to_fetch": len(to_fetch),
            "fetched": 0,
            "failed": 0,
        }

    if not to_fetch:
        print(f"✅ All tickers already have daily metrics data!")
        return {
            "data_type": "daily",
            "total": total_tickers,
            "existing": total_tickers,
            "to_fetch": 0,
            "fetched": 0,
            "failed": 0,
        }

    # Initialize FundamentalManager
    fundamental_manager = FundamentalManager(
        tiingo=tiingo_client, universe=sp500_universe
    )

    print(f"\n⏳ Fetching daily metrics (this may take a while)...")
    print(f"💡 API: tiingo.get_fundamentals_daily(symbol, startDate, endDate)")
    print(f"💡 Note: Daily metrics provide P/E, market cap, and valuation ratios")

    # Fetch data for universe
    successful = 0
    failed = 0
    total_records = 0

    for i, symbol in enumerate(to_fetch, 1):
        if i % 10 == 0 or i == len(to_fetch):
            print(
                f"  Progress: {i}/{len(to_fetch)} ({successful} success, {failed} failed)"
            )

        try:
            df, paths = fundamental_manager.fetch_metrics(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                save=True,
            )

            if df is not None and not df.empty:
                successful += 1
                total_records += len(df)
            else:
                failed += 1

        except Exception as e:
            print(f"⚠️  Error fetching {symbol}: {e}")
            failed += 1

    print(f"\n✅ DAILY METRICS Data Fetch Complete!")
    print(f"   Successfully fetched: {successful}/{len(to_fetch)} tickers")
    print(f"   Total records: {total_records:,}")
    if failed > 0:
        print(f"   ⚠️  Failed: {failed} tickers")

    return {
        "data_type": "daily",
        "total": total_tickers,
        "existing": len(members) - len(to_fetch),
        "to_fetch": len(to_fetch),
        "fetched": successful,
        "failed": failed,
        "records": total_records,
    }


def main():
    """Main execution function"""

    parser = argparse.ArgumentParser(
        description="Fetch complete research universe fundamental data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch both statements and daily metrics
  python src/programs/fetch_research_fundamentals.py

  # Fetch only statements (quarterly/annual financial statements)
  python src/programs/fetch_research_fundamentals.py --data-types statements

  # Fetch only daily metrics (P/E, market cap, etc.)
  python src/programs/fetch_research_fundamentals.py --data-types daily

  # Skip tickers with existing data
  python src/programs/fetch_research_fundamentals.py --skip-existing

  # Dry run to preview
  python src/programs/fetch_research_fundamentals.py --dry-run

  # Custom date range for statements (overrides config)
  python src/programs/fetch_research_fundamentals.py --start-date 2020-01-01 --end-date 2024-12-31
        """,
    )

    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=["statements", "daily", "both"],
        default=["both"],
        help="Types of fundamental data to fetch: 'statements' (quarterly/annual via get_fundamentals_statements) or 'daily' (daily metrics via get_fundamentals_daily) (default: both)",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for statements (YYYY-MM-DD). Overrides config setting.",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for statements (YYYY-MM-DD). Overrides config setting.",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers that already have data",
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

    # Normalize data types
    data_types = []
    if "both" in args.data_types:
        data_types = ["statements", "daily"]
    else:
        data_types = args.data_types

    # Load configuration
    print("=" * 80)
    print("QuantX Research Universe Fundamental Data Fetcher")
    print("=" * 80)
    print(f"⚙️  Loading configuration from config/settings.yaml...")

    config = Config("config/settings.yaml")

    # Get date range from config or command line
    start_date = args.start_date or config.get("universe.sp500.start_date")
    end_date = args.end_date or config.get("universe.sp500.end_date")

    print(f"\n📊 Research Period (for statements):")
    print(f"   Start: {start_date}")
    print(f"   End: {end_date}")
    print(f"   Data Types: {', '.join(data_types)}")

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

    # Fetch data for each type
    all_stats = []
    total_start = datetime.now()

    if "statements" in data_types:
        stats_start = datetime.now()

        stats = fetch_universe_statements(
            start_date=start_date,
            end_date=end_date,
            config=config,
            tiingo_client=tiingo_client,
            sp500_universe=sp500_universe,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
        )

        stats_duration = (datetime.now() - stats_start).total_seconds()
        stats["duration_seconds"] = int(stats_duration)
        all_stats.append(stats)

    if "daily" in data_types:
        daily_start = datetime.now()

        stats = fetch_universe_daily(
            start_date=start_date,
            end_date=end_date,
            config=config,
            tiingo_client=tiingo_client,
            sp500_universe=sp500_universe,
            skip_existing=args.skip_existing,
            dry_run=args.dry_run,
        )

        daily_duration = (datetime.now() - daily_start).total_seconds()
        stats["duration_seconds"] = int(daily_duration)
        all_stats.append(stats)

    total_duration = (datetime.now() - total_start).total_seconds()

    # Print final summary
    print(f"\n{'=' * 80}")
    print("📊 FINAL SUMMARY")
    print(f"{'=' * 80}")

    for stats in all_stats:
        print(f"\n{stats['data_type'].upper()}:")
        print(f"   Universe size: {stats['total']} tickers")
        if stats["existing"] > 0:
            print(f"   Already had data: {stats['existing']} tickers")
        print(f"   Attempted fetch: {stats['to_fetch']} tickers")
        if not args.dry_run:
            print(f"   Successfully fetched: {stats['fetched']} tickers")
            if stats.get("records"):
                print(f"   Total records: {stats['records']:,}")
            if stats["failed"] > 0:
                print(f"   ⚠️  Failed: {stats['failed']} tickers")
        print(f"   Duration: {stats['duration_seconds']:.1f} seconds")

    print(
        f"\n⏱️  Total execution time: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)"
    )

    if not args.dry_run:
        total_fetched = sum(s["fetched"] for s in all_stats)
        total_failed = sum(s["failed"] for s in all_stats)
        total_records = sum(s.get("records", 0) for s in all_stats)

        print(f"\n✅ Complete!")
        print(f"   Total tickers fetched: {total_fetched}")
        print(f"   Total records: {total_records:,}")
        if total_failed > 0:
            print(f"   ⚠️  Total failures: {total_failed}")

        # Data location
        print(f"\n📁 Data saved to:")
        print(
            f"   {config.get('storage.local.root_path')}/curated/tickers/exchange=us/"
        )
        print(f"   Structure: ticker=SYMBOL/fundamentals/statement=TYPE/year=YYYY/")
        print(f"\n💡 Next steps:")
        print(f"   1. Validate data: Check file structure and content")
        print(f"   2. Analyze coverage: Which tickers have complete data")
        print(f"   3. Build features: Use fundamentals in factor models")
    else:
        print(f"\n🔍 DRY RUN COMPLETE - No data was fetched")
        print(f"   Remove --dry-run flag to actually fetch the data")


if __name__ == "__main__":
    main()
