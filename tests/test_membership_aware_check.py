"""
Test membership-aware missing data check

This script demonstrates the new membership-aware functionality in check_missing_data().
It shows how the system only considers data as missing if it falls within the symbol's
membership interval in the universe.
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
    """Test membership-aware missing data check"""

    print("=" * 80)
    print("Testing Membership-Aware Missing Data Check")
    print("=" * 80)

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo = TiingoClient(api_key=config.get('tiingo_api_key'))
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)

    # Test Case 1: Symbol that joined SP500 recently (TSLA joined Dec 21, 2020)
    print("\n" + "=" * 80)
    print("Test Case 1: TSLA (joined SP500 on 2020-12-21)")
    print("=" * 80)
    print("\nScenario: Requesting data from 2018 (before membership)")
    result = price_mgr.check_missing_data(
        symbol='TSLA',
        required_start='2018-01-01',
        required_end='2024-12-31'
    )
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Membership period: {result.get('membership_start')} to {result.get('membership_end')}")
    print(f"  Actual data range: {result['actual_start']} to {result['actual_end']}")
    print(f"  Missing days: start={result['missing_start_days']}, end={result['missing_end_days']}")
    print(f"  Recommended fetch: {result['fetch_start']} to {result['fetch_end']}")

    # Test Case 2: Long-standing SP500 member (AAPL)
    print("\n" + "=" * 80)
    print("Test Case 2: AAPL (long-standing member)")
    print("=" * 80)
    print("\nScenario: Requesting data from 2020")
    result = price_mgr.check_missing_data(
        symbol='AAPL',
        required_start='2020-01-01',
        required_end='2024-12-31'
    )
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Membership period: {result.get('membership_start')} to {result.get('membership_end')}")
    print(f"  Actual data range: {result['actual_start']} to {result['actual_end']}")
    print(f"  Missing days: start={result['missing_start_days']}, end={result['missing_end_days']}")
    print(f"  Recommended fetch: {result['fetch_start']} to {result['fetch_end']}")

    # Test Case 3: Symbol with no membership data (should fall back to required period)
    print("\n" + "=" * 80)
    print("Test Case 3: Non-SP500 symbol (should use full required period)")
    print("=" * 80)
    print("\nScenario: Requesting data for symbol without membership info")
    # Note: This will fail gracefully and use the full required period
    result = price_mgr.check_missing_data(
        symbol='FAKE',
        required_start='2020-01-01',
        required_end='2024-12-31'
    )
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Membership period: {result.get('membership_start')} to {result.get('membership_end')}")
    print(f"  Actual data range: {result['actual_start']} to {result['actual_end']}")

    # Test Case 4: Required period completely outside membership
    print("\n" + "=" * 80)
    print("Test Case 4: Required period before membership (TSLA before 2020)")
    print("=" * 80)
    print("\nScenario: Requesting 2018-2019 data for TSLA (joined in 2020)")
    result = price_mgr.check_missing_data(
        symbol='TSLA',
        required_start='2018-01-01',
        required_end='2019-12-31'
    )
    print(f"\nResult:")
    print(f"  Status: {result['status']}")
    print(f"  Membership period: {result.get('membership_start')} to {result.get('membership_end')}")
    print(f"  Actual data range: {result['actual_start']} to {result['actual_end']}")
    print(f"  Missing days: {result['missing_start_days']}")
    print(f"  Explanation: Data is 'complete' because the required period is outside membership")

    print("\n" + "=" * 80)
    print("All tests completed!")
    print("=" * 80)
    print("\nKey Observations:")
    print("1. TSLA: Only checks data from 2020-12-21 onward (membership start)")
    print("2. AAPL: Checks full period (long-standing member)")
    print("3. Unknown symbols: Falls back to full required period")
    print("4. Pre-membership requests: Returns 'complete' status")

if __name__ == "__main__":
    main()
