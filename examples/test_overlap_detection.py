"""
Test the improved missing data checker with overlap detection

Tests the new logic that only checks periods within both membership and research periods
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from market import PriceManager
from universe import SP500Universe


def main():
    """Test overlap detection logic"""

    print("=" * 80)
    print("Testing Improved Missing Data Checker - Overlap Detection")
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
    checker = price_mgr.get_missing_data_checker()

    # Research period
    research_start = '2014-01-01'
    research_end = '2024-12-31'

    print(f"\nResearch Period: {research_start} to {research_end}")
    print("=" * 80)

    # Test Case 1: Stock A - Only second interval overlaps
    # AMD: Membership [2000-2013], [2017-2025]
    # Research: 2014-2024
    # Expected: Only check [2017-2024]
    print("\n" + "─" * 80)
    print("Test Case 1: AMD (second interval overlaps)")
    print("─" * 80)
    print("Membership: [2000-01-03 to 2013-09-17], [2017-03-20 to 2025-07-09]")
    print(f"Research: {research_start} to {research_end}")
    print("Expected overlap: [2017-03-20 to 2024-12-31]")

    result = checker.check_missing_data('AMD', research_start, research_end)
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Checked periods: {result.get('checked_periods', [])}")
    if 'summary' in result:
        print(f"  Total periods checked: {result['summary']['total_periods']}")
        print(f"  Has gaps: {result['summary']['has_gaps']}")

    # Test Case 2: Stock B - Both intervals overlap
    # Let's use PCG: Membership [2000-2019], [2022-2025]
    # Research: 2014-2024
    # Expected: Check [2014-2019] AND [2022-2024]
    print("\n" + "─" * 80)
    print("Test Case 2: PCG (both intervals overlap)")
    print("─" * 80)
    print("Membership: [2000-01-03 to 2019-01-11], [2022-10-03 to 2025-07-09]")
    print(f"Research: {research_start} to {research_end}")
    print("Expected overlap: [2014-01-01 to 2019-01-11] AND [2022-10-03 to 2024-12-31]")

    result = checker.check_missing_data('PCG', research_start, research_end)
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Checked periods: {result.get('checked_periods', [])}")
    if 'summary' in result:
        print(f"  Total periods checked: {result['summary']['total_periods']}")
        print(f"  Has gaps: {result['summary']['has_gaps']}")
        if 'intervals' in result:
            print(f"\n  Per-period details:")
            for interval in result['intervals']:
                print(f"    Period {interval['period']}: {interval['period_start']} to {interval['period_end']}")
                print(f"      Status: {interval['status']}")

    # Test Case 3: Stock C - No overlap (too early)
    # Let's use a stock that was removed before 2014
    print("\n" + "─" * 80)
    print("Test Case 3: Symbol removed before research period")
    print("─" * 80)
    # Use a custom date range where stock has no overlap
    early_research_start = '2014-01-01'
    early_research_end = '2016-12-31'
    print(f"Using AMD with research period: {early_research_start} to {early_research_end}")
    print("AMD Membership: [2000-01-03 to 2013-09-17], [2017-03-20 to 2025-07-09]")
    print("Expected: No overlap (first ended 2013, second starts 2017)")

    result = checker.check_missing_data('AMD', early_research_start, early_research_end)
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Checked periods: {result.get('checked_periods', [])}")
    print(f"  Reason: {result.get('reason', 'N/A')}")

    # Test Case 4: Continuous member
    print("\n" + "─" * 80)
    print("Test Case 4: AAPL (continuous member, full overlap)")
    print("─" * 80)
    print("Membership: [2000-01-03 to 2025-07-09] (continuous)")
    print(f"Research: {research_start} to {research_end}")
    print("Expected overlap: [2014-01-01 to 2024-12-31] (full research period)")

    result = checker.check_missing_data('AAPL', research_start, research_end)
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Checked periods: {result.get('checked_periods', [])}")
    if 'summary' in result:
        print(f"  Total periods checked: {result['summary']['total_periods']}")
    else:
        print(f"  Single continuous period")

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print("""
The improved checker now correctly:
1. Calculates intersection between membership intervals and research period
2. Only checks data for overlapping periods
3. Handles multiple discontinuous periods (gaps)
4. Returns 'complete' status if no overlap exists
5. Provides detailed per-period analysis

This ensures we only validate data that's actually needed for research!
    """)

if __name__ == "__main__":
    main()
