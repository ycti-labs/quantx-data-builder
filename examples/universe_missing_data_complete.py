"""
Fetch Missing Data for Entire Universe - Complete Example

This script demonstrates the universe-wide missing data fetch functionality.
It checks what data is missing and then fetches only the gaps, respecting
membership intervals.
"""

import logging
import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from market import PriceManager
from universe import SP500Universe

# Configure logging to see info messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Fetch missing data for entire universe"""

    print("=" * 80)
    print("Universe-Wide Missing Data Fetch")
    print("=" * 80)

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo = TiingoClient(api_key=config.get('tiingo_api_key'))
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)

    # Example 1: Check first, then decide whether to fetch
    print("\n" + "=" * 80)
    print("Example 1: Check before fetch (dry run)")
    print("=" * 80)

    check_result = price_mgr.check_universe_missing_data(
        required_start='2024-01-01',
        required_end='2024-12-31',
        use_current_members=True
    )

    summary = check_result['summary']
    print(f"\n📊 Data Status:")
    print(f"  Complete: {summary['complete']}/{summary['total_symbols']} ({summary['complete_pct']:.1f}%)")
    print(f"  Partial: {summary['partial']}")
    print(f"  Missing: {summary['missing']}")

    # Ask user if they want to proceed
    incomplete_count = summary['partial'] + summary['missing']
    if incomplete_count > 0:
        print(f"\n⚠️  {incomplete_count} symbols need data fetch")

        by_status = check_result['by_status']
        print("\nSymbols that would be fetched:")
        print(f"  Partial: {by_status['partial'][:5]}... ({len(by_status['partial'])} total)")
        print(f"  Missing: {by_status['missing'][:5]}... ({len(by_status['missing'])} total)")

    # Example 2: Fetch missing data (membership-aware)
    print("\n" + "=" * 80)
    print("Example 2: Fetch missing data for Q1 2024")
    print("=" * 80)
    print("\nNote: This will fetch data for symbols with gaps only")
    print("      Respects membership intervals (won't fetch pre-membership data)")

    # Uncomment to actually fetch:
    # result = price_mgr.fetch_universe_missing_data(
    #     required_start='2024-01-01',
    #     required_end='2024-03-31',
    #     use_current_members=True,
    #     skip_complete=True,  # Skip symbols with complete data
    #     skip_errors=True     # Continue on errors
    # )
    #
    # fetch_summary = result['fetch_summary']
    # print(f"\n✅ Fetched: {fetch_summary['symbols_fetched']}")
    # print(f"⏭️  Skipped: {fetch_summary['symbols_skipped']}")
    # print(f"❌ Failed: {fetch_summary['symbols_failed']}")

    print("\n(Fetch commented out in example - uncomment to actually fetch)")

    # Example 3: Show what would be fetched for specific symbols
    print("\n" + "=" * 80)
    print("Example 3: Detailed fetch plan for incomplete symbols")
    print("=" * 80)

    by_status = check_result['by_status']
    partial_symbols = by_status['partial'][:5]  # First 5 partial symbols

    if partial_symbols:
        print("\nFetch plan for symbols with partial data:")
        symbols_details = check_result['symbols']

        for symbol in partial_symbols:
            details = symbols_details[symbol]
            print(f"\n  {symbol}:")
            print(f"    Status: {details['status']}")
            print(f"    Current data: {details['actual_start']} to {details['actual_end']}")
            print(f"    Missing: {details['missing_start_days']}d at start, {details['missing_end_days']}d at end")
            if details['membership_start']:
                print(f"    Membership: {details['membership_start']} to {details['membership_end']}")
            print(f"    Will fetch: {details['fetch_start']} to {details['fetch_end']}")

    # Example 4: Typical workflows
    print("\n" + "=" * 80)
    print("Typical Workflows")
    print("=" * 80)
    print("""
1. Daily Update (incremental):
   result = price_mgr.fetch_universe_missing_data(
       required_start='2024-01-01',
       required_end='2024-12-31',
       skip_complete=True  # Only fetch gaps
   )

2. Full Backfill (historical):
   result = price_mgr.fetch_universe_missing_data(
       required_start='2020-01-01',
       required_end='2024-12-31',
       use_current_members=False,  # All historical members
       skip_complete=False  # Fetch everything
   )

3. Targeted Fill (specific period):
   result = price_mgr.fetch_universe_missing_data(
       required_start='2024-06-01',
       required_end='2024-06-30',
       skip_complete=True
   )

Benefits:
- Membership-aware: Won't fetch pre-membership data
- Efficient: Only fetches gaps, not complete data
- Smart recommendations: Respects tolerance for near-complete data
- Robust: Can skip errors and continue
- Auditable: Full logging and statistics
    """)

if __name__ == "__main__":
    main()
