"""
Comprehensive example of fetching and checking data with different frequencies

Demonstrates:
1. Fetching daily, weekly, and monthly data
2. Checking completeness with frequency-aware tolerance
3. Understanding tolerance behavior for each frequency
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
    """Demonstrate frequency-aware data operations"""

    print("=" * 80)
    print("Multi-Frequency Data Operations Demo")
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

    # Test parameters
    test_symbols = ['AAPL', 'MSFT']
    test_period = ('2023-01-01', '2023-12-31')

    print(f"\nTest Period: {test_period[0]} to {test_period[1]}")
    print(f"Test Symbols: {', '.join(test_symbols)}")

    # Process each frequency
    for frequency in ['daily', 'weekly', 'monthly']:
        print("\n" + "=" * 80)
        print(f"{frequency.upper()} FREQUENCY DATA")
        print("=" * 80)

        for symbol in test_symbols:
            print(f"\n{symbol} ({frequency}):")
            print("-" * 80)

            # Check if data exists
            existing_range = price_mgr.get_existing_date_range(
                symbol=symbol,
                frequency=frequency
            )

            if existing_range:
                print(f"✓ Existing data: {existing_range[0]} to {existing_range[1]}")
            else:
                print(f"⚠ No existing data found")
                print(f"  To fetch: price_mgr.fetch_eod('{symbol}', '{test_period[0]}', "
                      f"'{test_period[1]}', frequency='{frequency}')")
                continue

            # Check completeness with auto-calculated tolerance
            result = checker.check_missing_data(
                symbol=symbol,
                required_start=test_period[0],
                required_end=test_period[1],
                frequency=frequency,
                tolerance_days=None  # Auto-calculate based on frequency
            )

            print(f"  Status: {result['status'].upper()}")

            if result['status'] == 'complete':
                print(f"  ✓ Data is complete for the period")
            elif result['status'] == 'partial':
                print(f"  ⚠ Partial data:")
                print(f"    - Missing at start: {result.get('missing_start_days', 0)} days")
                print(f"    - Missing at end: {result.get('missing_end_days', 0)} days")
                if result.get('fetch_start'):
                    print(f"    - Need to fetch: {result['fetch_start']} to {result['fetch_end']}")
            else:
                print(f"  ✗ No data available")

            # Load and show sample
            if existing_range:
                df = price_mgr.load_price_data(
                    symbol=symbol,
                    start_date=test_period[0],
                    end_date=test_period[1],
                    frequency=frequency
                )

                if not df.empty:
                    print(f"  Loaded: {len(df)} rows")
                    print(f"    First date: {df['date'].min()}")
                    print(f"    Last date: {df['date'].max()}")
                    print(f"    Sample close prices: ${df['close'].head(3).tolist()}")

    print("\n" + "=" * 80)
    print("FREQUENCY COMPARISON SUMMARY")
    print("=" * 80)
    print("""
Frequency   Tolerance   Typical Count   Use Case
------------------------------------------------------------------------
Daily       ±2 days     ~252/year       Intraday strategies, detailed analysis
Weekly      ±6 days     ~52/year        Medium-term trends, reduced noise
Monthly     ±3 days     ~12/year        Long-term trends, fundamental analysis

KEY POINTS:
-----------
1. Tolerance is auto-calculated based on frequency
   - Can be overridden with tolerance_days parameter

2. Weekly data considerations:
   - Can be on any weekday (Mon-Fri)
   - ±6 days accounts for week alignment differences
   - Example: Required start=Wed, Data start=Mon → 2 days gap → OK

3. Monthly data considerations:
   - Typically uses last trading day of month
   - ±3 days accounts for month-end vs trading day
   - Example: Required=Jan 31, Data=Jan 29 → 2 days gap → OK

4. Fetching different frequencies:
   price_mgr.fetch_eod(symbol, start, end, frequency='daily')
   price_mgr.fetch_eod(symbol, start, end, frequency='weekly')
   price_mgr.fetch_eod(symbol, start, end, frequency='monthly')

5. Checking completeness:
   checker.check_missing_data(symbol, start, end, frequency='daily')
   # Tolerance auto-calculated as 2, 6, or 3 days respectively
    """)

    print("=" * 80)


if __name__ == "__main__":
    main()
