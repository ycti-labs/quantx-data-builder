"""
Test FundamentalManager

Demonstrates usage of FundamentalManager for fetching and managing fundamental data.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tiingo import TiingoClient

from core.config import Config
from market import FundamentalManager
from universe import SP500Universe


def test_fundamental_manager():
    """Test fundamental manager with various methods"""

    print("=" * 80)
    print("FundamentalManager Test")
    print("=" * 80)
    print()

    try:
        # Load configuration
        config = Config("config/settings.yaml")
        print("✅ Configuration loaded")
        api_key = config.get("fetcher.tiingo.api_key")
        print(f"   API Key: {api_key[:10]}...")
        print()

        # Initialize Tiingo client
        tiingo_config = {
            'api_key': api_key,
            'session': True
        }
        tiingo = TiingoClient(tiingo_config)
        print("✅ TiingoClient initialized")
        print()

        # Initialize Universe
        sp500 = SP500Universe(data_root="data")
        print("✅ SP500Universe initialized")
        print()

        # Create FundamentalManager
        fund_mgr = FundamentalManager(tiingo=tiingo, universe=sp500)
        print("✅ FundamentalManager created")
        print()

        # Test 1: Fetch fundamentals for single symbol
        print("=" * 80)
        print("Test 1: Fetch Fundamentals for Single Symbol")
        print("=" * 80)
        print("\nFetching AAPL fundamentals...")

        df, paths = fund_mgr.fetch_fundamentals(
            symbol='AAPL',
            start_date='2020-01-01',
            end_date='2024-12-31',
            save=True
        )

        if not df.empty:
            print(f"✅ Fetched {len(df)} fundamental records for AAPL")
            print(f"   Saved to {len(paths)} files")
            print(f"\nColumns: {df.columns.tolist()}")
            print(f"\nFirst few rows:")
            print(df.head())
        else:
            print("⚠️  No data returned")
        print()

        # Test 2: Fetch metrics
        print("=" * 80)
        print("Test 2: Fetch Daily Metrics")
        print("=" * 80)
        print("\nFetching MSFT metrics...")

        df_metrics, paths_metrics = fund_mgr.fetch_metrics(
            symbol='MSFT',
            save=True
        )

        if not df_metrics.empty:
            print(f"✅ Fetched {len(df_metrics)} metric records for MSFT")
            print(f"   Saved to {len(paths_metrics)} files")
            print(f"\nColumns: {df_metrics.columns.tolist()}")
        else:
            print("⚠️  No metrics data returned")
        print()

        # Test 3: Fetch multiple symbols
        print("=" * 80)
        print("Test 3: Fetch Multiple Symbols")
        print("=" * 80)

        symbols = ['AAPL', 'MSFT', 'GOOGL']
        print(f"\nFetching fundamentals for {symbols}...")

        results = fund_mgr.fetch_multiple_fundamentals(
            symbols=symbols,
            start_date='2023-01-01',
            save=False  # Just fetch, don't save
        )

        print(f"\n✅ Fetched data for {len(results)} symbols:")
        for symbol, df in results.items():
            print(f"   {symbol}: {len(df)} records")
        print()

        # Test 4: Read saved data
        print("=" * 80)
        print("Test 4: Read Saved Fundamental Data")
        print("=" * 80)
        print("\nReading AAPL fundamentals from disk...")

        df_read = fund_mgr.read_fundamental_data(
            symbol='AAPL',
            start_date='2023-01-01',
            end_date='2024-12-31'
        )

        if not df_read.empty:
            print(f"✅ Read {len(df_read)} records from disk")
            print(f"\nDate range: {df_read['date'].min()} to {df_read['date'].max()}")
        else:
            print("⚠️  No saved data found")
        print()

        # Test 5: Check missing data
        print("=" * 80)
        print("Test 5: Check Missing Data")
        print("=" * 80)

        check = fund_mgr.check_missing_data(
            symbol='AAPL',
            required_start='2020-01-01',
            required_end='2024-12-31'
        )

        print(f"\nData availability check for AAPL:")
        print(f"   Status: {check['status']}")
        print(f"   Has data: {check['has_data']}")
        if check['actual_start']:
            print(f"   Actual range: {check['actual_start']} to {check['actual_end']}")
        print()

        print("=" * 80)
        print("✅ All tests completed!")
        print("=" * 80)
        print()

        print("Usage examples:")
        print("  # Fetch fundamentals")
        print("  df, paths = fund_mgr.fetch_fundamentals('AAPL', '2020-01-01', '2024-12-31')")
        print()
        print("  # Fetch metrics")
        print("  df, paths = fund_mgr.fetch_metrics('MSFT')")
        print()
        print("  # Fetch multiple symbols")
        print("  results = fund_mgr.fetch_multiple_fundamentals(['AAPL', 'MSFT'], save=False)")
        print()
        print("  # Read saved data")
        print("  df = fund_mgr.read_fundamental_data('AAPL', start_date='2023-01-01')")
        print()

        return 0

    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("\nPlease set your TIINGO_API_KEY:")
        print("  export TIINGO_API_KEY=your_key_here")
        print()
        return 1

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(test_fundamental_manager())
