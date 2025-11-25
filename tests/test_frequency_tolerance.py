"""
Test frequency-aware tolerance in missing data checking

Demonstrates automatic tolerance calculation for different frequencies:
- Daily: ±2 days (weekends/holidays)
- Weekly: ±6 days (any weekday in the week)
- Monthly: ±3 days (month-end adjustments)
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from market import PriceManager
from programs.check_missing_data import get_tolerance_for_frequency
from universe import SP500Universe


def main():
    """Test frequency-aware tolerance"""

    print("=" * 80)
    print("Testing Frequency-Aware Tolerance Calculation")
    print("=" * 80)

    # Test tolerance calculation function
    print("\n1. Tolerance Calculation for Different Frequencies:")
    print("-" * 80)

    frequencies = ['daily', 'weekly', 'monthly', 'unknown']
    for freq in frequencies:
        tolerance = get_tolerance_for_frequency(freq)
        print(f"  {freq.capitalize():10s} → {tolerance} days tolerance")

    # Initialize components
    print("\n2. Testing with Real Data Check:")
    print("-" * 80)

    config = Config("config/settings.yaml")
    tiingo_config = {
        'api_key': config.get('fetcher.tiingo.api_key'),
        'session': True
    }
    tiingo = TiingoClient(tiingo_config)
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)
    checker = price_mgr.get_missing_data_checker()

    # Test symbol with data
    test_symbol = 'AAPL'
    test_period = ('2020-01-01', '2020-12-31')

    print(f"\nChecking {test_symbol} for period {test_period[0]} to {test_period[1]}:\n")

    # Test each frequency
    for freq in ['daily', 'weekly', 'monthly']:
        print(f"{freq.upper()} Frequency:")

        # Without explicit tolerance (auto-calculated)
        result_auto = checker.check_missing_data(
            symbol=test_symbol,
            required_start=test_period[0],
            required_end=test_period[1],
            frequency=freq,
            tolerance_days=None  # Auto-calculate
        )

        auto_tolerance = get_tolerance_for_frequency(freq)
        print(f"  Auto-calculated tolerance: {auto_tolerance} days")
        print(f"  Status: {result_auto['status']}")

        if result_auto.get('actual_start'):
            print(f"  Data range: {result_auto['actual_start']} to {result_auto['actual_end']}")
            print(f"  Missing at start: {result_auto.get('missing_start_days', 0)} days")
            print(f"  Missing at end: {result_auto.get('missing_end_days', 0)} days")
        else:
            print(f"  No data found")

        # With explicit tolerance
        result_explicit = checker.check_missing_data(
            symbol=test_symbol,
            required_start=test_period[0],
            required_end=test_period[1],
            frequency=freq,
            tolerance_days=10  # Explicit override
        )

        print(f"  With explicit tolerance (10 days): {result_explicit['status']}")
        print()

    print("=" * 80)
    print("Understanding Frequency Tolerances:")
    print("=" * 80)
    print("""
DAILY (±2 days):
  - Accounts for weekends and occasional holidays
  - If data starts Monday (2024-01-01) but required Wednesday (2024-01-03):
    Gap = 2 days → COMPLETE (within tolerance)

WEEKLY (±6 days):
  - Weekly data can be on any weekday (Monday-Friday)
  - If required start is Wednesday (2024-01-03) but data starts Monday (2024-01-01):
    Gap = 2 days → COMPLETE (within tolerance)
  - If required end is Tuesday (2024-12-31) but data ends Friday (2024-12-27):
    Gap = 4 days → COMPLETE (within tolerance)
  - Maximum gap within same week is ~6 days

MONTHLY (±3 days):
  - Monthly data typically uses last trading day of month
  - Last trading day can be 1-3 days before calendar month-end
  - If required is 2024-01-31 but data is 2024-01-29 (last trading day):
    Gap = 2 days → COMPLETE (within tolerance)

CUSTOM TOLERANCE:
  - You can override auto-calculated tolerance by passing explicit value
  - Useful for specific requirements or quality checks
    """)

    print("=" * 80)
    print("Test Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
