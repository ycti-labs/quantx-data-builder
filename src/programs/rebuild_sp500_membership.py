"""
Rebuild S&P 500 membership data from updated CSV

This script rebuilds the membership datasets using the latest CSV file
configured in settings.yaml. You can choose to:
1. Rebuild from scratch (delete old data)
2. Update incrementally (merge with existing data)

Usage:
    # Rebuild from scratch
    python examples/rebuild_sp500_membership.py --rebuild

    # Update incrementally (default)
    python examples/rebuild_sp500_membership.py
"""

import argparse
import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from core.config import Config
from universe import SP500Universe


def main():
    """Rebuild S&P 500 membership data"""

    parser = argparse.ArgumentParser(
        description="Rebuild S&P 500 membership data from updated CSV"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Rebuild from scratch (delete existing data). Default: update incrementally",
    )
    parser.add_argument(
        "--min-date",
        type=str,
        default="2000-01-01",
        help="Minimum date to include (ISO format YYYY-MM-DD). Default: 2000-01-01",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("S&P 500 Membership Data Builder")
    print("=" * 80)

    mode = "REBUILD FROM SCRATCH" if args.rebuild else "UPDATE INCREMENTALLY"
    print(f"\nMode: {mode}")
    print(f"Min Date: {args.min_date}")

    if args.rebuild:
        print("\n⚠️  WARNING: This will DELETE existing membership data!")
        response = input("Continue? (yes/no): ")
        if response.lower() != "yes":
            print("Cancelled.")
            return

    print("\n" + "=" * 80)

    # Load configuration
    config = Config(str(project_root / "config/settings.yaml"))
    membership_file = config.get("universe.sp500.membership_file")

    # Initialize universe
    universe = SP500Universe(
        data_root=str(project_root / config.get("storage.local.root_path"))
    )

    # Build membership with configured filename
    stats = universe.build_membership(
        min_date=args.min_date,
        rebuild=args.rebuild,
        membership_filename=membership_file,
    )

    print("\n" + "=" * 80)
    print("Build Complete!")
    print("=" * 80)
    print(f"\n📊 Statistics:")
    print(f"   Source file:     {membership_file}")
    print(f"   First date:      {stats['first_date']}")
    print(f"   Last date:       {stats['last_date']}")
    print(f"   Unique tickers:  {stats['unique_tickers']}")
    print(f"   Daily rows:      {stats['daily_rows']:,}")
    print(f"   Interval rows:   {stats['interval_rows']:,}")
    print(f"\n✅ Membership data is ready!")
    print(
        f"\n💡 Tip: Run check_universe_overlap_aware.py to validate data completeness"
    )


if __name__ == "__main__":
    main()
