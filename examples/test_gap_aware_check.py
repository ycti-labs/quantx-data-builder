"""
Test Gap-Aware Missing Data Check

This script demonstrates the new gap-aware missing data checking functionality
for symbols that were removed and re-added to the universe.
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
    """Test gap-aware missing data check"""

    print("=" * 80)
    print("Gap-Aware Missing Data Check")
    print("=" * 80)

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo_config = {
        'api_key': config.get('fetcher.tiingo.api_key'),
        'session': True
    }
    tiingo = TiingoClient(tiingo_config)
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)

    # Test symbols with multiple intervals (from check_multiple_intervals.py)
    test_symbols = [
        ('AMD', 'Removed 2013, re-added 2017 (3.5 year gap)'),
        ('HLT', 'Removed 2007, re-added 2017 (9.7 year gap)'),
        ('DELL', 'Removed 2013, re-added 2024 (11 year gap)'),
        ('PCG', 'Removed 2019, re-added 2022 (3.7 year gap - bankruptcy)'),
        ('AAPL', 'Continuous member (no gaps)')
    ]

    required_start = '2000-01-01'
    required_end = '2024-12-31'

    print(f"\nChecking period: {required_start} to {required_end}")
    print("=" * 80)

    for symbol, description in test_symbols:
        print(f"\n{'─' * 80}")
        print(f"Symbol: {symbol}")
        print(f"History: {description}")
        print(f"{'─' * 80}")

        # Get membership intervals
        intervals = universe.get_membership_intervals(symbol)
        print(f"\nMembership intervals: {len(intervals)}")
        for idx, (start, end) in enumerate(intervals, 1):
            duration = (end - start).days / 365.25
            print(f"  Interval {idx}: {start} to {end} ({duration:.1f} years)")

        # Check with gap awareness
        print("\n🔍 Gap-Aware Check:")
        gap_result = price_mgr.check_missing_data(
            symbol=symbol,
            required_start=required_start,
            required_end=required_end,
            handle_gaps=True
        )

        print(f"  Overall Status: {gap_result['status'].upper()}")

        # Check if we have per-interval results (gap-aware mode)
        if 'summary' in gap_result:
            print(f"  Total Intervals: {gap_result['summary']['total_intervals']}")
            print(f"  Has Gaps: {gap_result['summary']['has_gaps']}")
            print(f"  Total Missing Days: {gap_result['summary']['total_missing_days']}")

            if gap_result['intervals']:
                print(f"\n  Per-Interval Details:")
                for interval in gap_result['intervals']:
                    print(f"    Interval {interval['interval']}:")
                    print(f"      Membership: {interval['membership_start']} to {interval['membership_end']}")
                    print(f"      Effective: {interval['effective_start']} to {interval['effective_end']}")
                    print(f"      Status: {interval['status']}")
                    if interval['status'] == 'missing':
                        print(f"      Missing: {interval['missing_days']} days (no data)")
                    elif interval['status'] == 'partial':
                        print(f"      Data: {interval['actual_start']} to {interval['actual_end']}")
                        print(f"      Missing: {interval['missing_start_days']}d at start, "
                              f"{interval['missing_end_days']}d at end")
                    else:
                        print(f"      Data: {interval['actual_start']} to {interval['actual_end']}")
        else:
            # Simple mode output
            print(f"  Membership: {gap_result['membership_start']} to {gap_result['membership_end']}")
            if gap_result['actual_start']:
                print(f"  Data: {gap_result['actual_start']} to {gap_result['actual_end']}")
                print(f"  Missing: {gap_result['missing_start_days']}d at start, "
                      f"{gap_result['missing_end_days']}d at end")

        # Compare with simple check (handle_gaps=False)
        print("\n📊 Simple Check (for comparison):")
        simple_result = price_mgr.check_missing_data(
            symbol=symbol,
            required_start=required_start,
            required_end=required_end,
            handle_gaps=False
        )
        print(f"  Status: {simple_result['status'].upper()}")
        print(f"  Membership: {simple_result['membership_start']} to {simple_result['membership_end']}")
        if simple_result['actual_start']:
            print(f"  Data: {simple_result['actual_start']} to {simple_result['actual_end']}")
            print(f"  Missing: {simple_result['missing_start_days']}d at start, "
                  f"{simple_result['missing_end_days']}d at end")

    # Example: Check what happens during the gap period
    print("\n" + "=" * 80)
    print("Example: Checking during gap period (AMD 2014-2016)")
    print("=" * 80)

    gap_result = price_mgr.check_missing_data(
        symbol='AMD',
        required_start='2014-01-01',
        required_end='2016-12-31',
        handle_gaps=True
    )

    print(f"\nStatus: {gap_result['status'].upper()}")
    if 'summary' in gap_result:
        print(f"Checked Intervals: {gap_result['summary']['checked_intervals']}")
    else:
        print("Checked Intervals: 0 (period outside membership)")
    print("Explanation: AMD was not a member during 2014-2016, so data is 'complete'")
    print("             (no missing data for non-membership periods)")

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print("""
Key Differences:

1. Simple Check (handle_gaps=False):
   - Uses earliest start to latest end (full span)
   - Treats gaps between memberships as one continuous period
   - Good for: Quick checks, symbols with continuous membership

2. Gap-Aware Check (handle_gaps=True, default):
   - Checks each membership interval separately
   - Handles gaps between memberships properly
   - Reports per-interval status
   - Good for: Accurate checks, symbols with membership gaps

Use Cases:
- Historical analysis: Use gap-aware to respect actual membership
- Daily updates: Either mode works
- Data quality reports: Gap-aware provides more detail
    """)

if __name__ == "__main__":
    main()
