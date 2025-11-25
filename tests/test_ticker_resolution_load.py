"""
Test ticker resolution in PriceManager load operations

Verifies that loading data with old ticker symbols (FB, ANTM, etc.)
automatically resolves to new ticker symbols (META, ELV, etc.)
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from core.ticker_mapper import TickerMapper
from market import PriceManager
from universe import SP500Universe


def main():
    """Test ticker resolution in load operations"""

    print("=" * 80)
    print("Testing Ticker Resolution in PriceManager.load_price_data()")
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
    mapper = TickerMapper()

    # Test cases: old ticker -> expected new ticker
    test_cases = [
        ('FB', 'META'),
        ('ANTM', 'ELV'),
        ('ABC', 'COR'),
        ('CBS', 'PARA'),  # Multi-step: CBS -> VIAC -> PARA
        ('LIFE', None),   # Delisted (acquired)
        ('FRC', None),    # Delisted (bankruptcy)
        ('META', 'META'), # Already resolved
    ]

    print("\nTest Cases:")
    print("-" * 80)

    for old_ticker, expected_new in test_cases:
        print(f"\n{old_ticker}:")

        # Check resolution
        resolved = mapper.resolve(old_ticker)
        print(f"  Resolution: {old_ticker} -> {resolved}")

        if resolved != expected_new:
            print(f"  ❌ FAIL: Expected {expected_new}, got {resolved}")
            continue

        # Try to load data
        print(f"  Loading data with symbol '{old_ticker}'...")
        df = price_mgr.load_price_data(
            frequency='daily',
            symbol=old_ticker,
            start_date='2020-01-01',
            end_date='2020-12-31'
        )

        if resolved is None:
            if df.empty:
                print(f"  ✅ PASS: Correctly returned empty DataFrame for delisted symbol")
            else:
                print(f"  ❌ FAIL: Expected empty DataFrame, got {len(df)} rows")
        else:
            if not df.empty:
                print(f"  ✅ PASS: Loaded {len(df)} rows")
                print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
                # Check columns in the dataframe
                if 'ticker_id' in df.columns:
                    print(f"  Ticker ID: {df['ticker_id'].iloc[0]}")
            else:
                print(f"  ⚠️  WARNING: No data found (might not have been fetched yet)")

    print("\n" + "=" * 80)
    print("Testing get_existing_date_range()")
    print("=" * 80)

    for old_ticker, expected_new in test_cases[:5]:  # Test first 5
        print(f"\n{old_ticker}:")
        resolved = mapper.resolve(old_ticker)
        print(f"  Resolution: {old_ticker} -> {resolved}")

        date_range = price_mgr.get_existing_date_range(old_ticker, frequency='daily')

        if resolved is None:
            if date_range is None:
                print(f"  ✅ PASS: Correctly returned None for delisted symbol")
            else:
                print(f"  ❌ FAIL: Expected None, got {date_range}")
        else:
            if date_range:
                print(f"  ✅ PASS: Found data range: {date_range[0]} to {date_range[1]}")
            else:
                print(f"  ⚠️  WARNING: No data found (might not have been fetched yet)")

    print("\n" + "=" * 80)
    print("Test Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
