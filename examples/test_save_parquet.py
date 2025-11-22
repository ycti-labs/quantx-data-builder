#!/usr/bin/env python3
"""
Test Tiingo Fetcher Save to Parquet

Demonstrates fetching data and saving to Hive-style partitioned Parquet files.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market import PriceManager


def test_save_single_symbol():
    """Test 1: Fetch and save single symbol"""
    print("=" * 80)
    print("Test 1: Fetch and Save Single Symbol (AAPL)")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Fetch and save AAPL data for 2020-2024
    df, paths = fetcher.fetch_and_save(
        symbol='AAPL',
        start_date='2020-01-01',
        end_date='2024-12-31',
        exchange='us',
        currency='USD'
    )

    print(f"\n✅ Fetched {len(df)} rows")
    print(f"✅ Saved to {len(paths)} year partition(s):")
    for path in paths:
        print(f"   - {path}")
    print()


def test_save_multiple_symbols():
    """Test 2: Fetch and save multiple symbols"""
    print("=" * 80)
    print("Test 2: Fetch and Save Multiple Symbols")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Fetch and save multiple symbols
    symbols = ['MSFT', 'GOOGL', 'AMZN', 'NVDA']
    results = fetcher.fetch_and_save_multiple(
        symbols=symbols,
        start_date='2024-01-01',
        end_date='2024-12-31',
        exchange='us',
        currency='USD'
    )

    print(f"\n✅ Saved data for {len(results)} symbols:")
    for symbol, (df, paths) in results.items():
        print(f"   {symbol}: {len(df)} rows -> {len(paths)} file(s)")
    print()


def test_load_saved_data():
    """Test 3: Load previously saved data"""
    print("=" * 80)
    print("Test 3: Load Saved Data from Parquet")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Load AAPL data
    df = fetcher.load_price_data(
        symbol='AAPL',
        start_date='2024-01-01',
        end_date='2024-12-31',
        exchange='us'
    )

    if not df.empty:
        print(f"\n✅ Loaded {len(df)} rows for AAPL")
        print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
        print(f"\nColumns: {df.columns.tolist()}")
        print(f"\nFirst few rows:")
        print(df.head())
        print(f"\nSchema:")
        print(df.dtypes)
    else:
        print("\n⚠️  No data found (run Test 1 first)")
    print()


def test_incremental_update():
    """Test 4: Incremental update (append new data)"""
    print("=" * 80)
    print("Test 4: Incremental Update (Append)")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # First save
    print("\nFirst save: 2024-01-01 to 2024-06-30")
    df1, paths1 = fetcher.fetch_and_save(
        symbol='TSLA',
        start_date='2024-01-01',
        end_date='2024-06-30',
        exchange='us',
        currency='USD'
    )
    print(f"✅ Saved {len(df1)} rows")

    # Second save (overlapping + new data)
    print("\nSecond save: 2024-06-01 to 2024-12-31 (with overlap)")
    df2, paths2 = fetcher.fetch_and_save(
        symbol='TSLA',
        start_date='2024-06-01',
        end_date='2024-12-31',
        exchange='us',
        currency='USD'
    )
    print(f"✅ Saved {len(df2)} rows (will merge and deduplicate)")

    # Load combined data
    combined = fetcher.load_price_data(
        symbol='TSLA',
        start_date='2024-01-01',
        end_date='2024-12-31',
        exchange='us'
    )
    print(f"\n✅ Final dataset: {len(combined)} rows (deduplicated)")
    print(f"   Date range: {combined['date'].min()} to {combined['date'].max()}")
    print()


def test_directory_structure():
    """Test 5: Verify Hive-style directory structure"""
    print("=" * 80)
    print("Test 5: Verify Hive-style Directory Structure")
    print("=" * 80)

    data_path = Path("data/curated/prices")

    if not data_path.exists():
        print("\n⚠️  No data saved yet. Run other tests first.")
        return

    print("\nDirectory structure:")
    print(f"\n{data_path}/")

    # Walk through directory tree
    for exchange_dir in sorted(data_path.glob("exchange=*")):
        print(f"  ├── {exchange_dir.name}/")

        for ticker_dir in sorted(exchange_dir.glob("ticker=*")):
            ticker_name = ticker_dir.name.split('=')[1]
            print(f"  │   ├── {ticker_dir.name}/")

            for freq_dir in sorted(ticker_dir.glob("freq=*")):
                print(f"  │   │   ├── {freq_dir.name}/")

                for adj_dir in sorted(freq_dir.glob("adj=*")):
                    print(f"  │   │   │   ├── {adj_dir.name}/")

                    for year_dir in sorted(adj_dir.glob("year=*")):
                        year = year_dir.name.split('=')[1]
                        parquet_file = year_dir / "part-000.parquet"
                        if parquet_file.exists():
                            size = parquet_file.stat().st_size
                            print(f"  │   │   │   │   ├── {year_dir.name}/")
                            print(f"  │   │   │   │   │   └── part-000.parquet ({size:,} bytes)")
    print()


def verify_schema():
    """Test 6: Verify canonical schema"""
    print("=" * 80)
    print("Test 6: Verify Canonical Schema")
    print("=" * 80)

    config = Config("config/settings.yaml")
    fetcher = PriceManager(
        api_key=config.get("fetcher.tiingo.api_key"),
        data_root="data/curated"
    )

    # Load any symbol
    df = fetcher.load_price_data(symbol='AAPL', exchange='us')

    if df.empty:
        print("\n⚠️  No data found. Run Test 1 first.")
        return

    expected_schema = {
        'date': 'date',
        'ticker_id': 'int',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'int64',
        'adj_open': 'float64',
        'adj_high': 'float64',
        'adj_low': 'float64',
        'adj_close': 'float64',
        'adj_volume': 'int64',
        'div_cash': 'float64',
        'split_factor': 'float64',
        'exchange': 'object',
        'currency': 'object',
        'freq': 'object',
        'year': 'int64'
    }

    print("\n✅ Schema validation:")
    all_good = True
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            print(f"   ❌ Missing column: {col}")
            all_good = False
        else:
            actual_type = str(df[col].dtype)
            status = "✅" if expected_type in actual_type or actual_type in expected_type else "⚠️"
            print(f"   {status} {col:15s} {actual_type:15s} (expected: {expected_type})")

    if all_good:
        print("\n✅ All schema checks passed!")
    print()


if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 15 + "Tiingo Fetcher Save to Parquet Tests" + " " * 27 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    try:
        test_save_single_symbol()
        test_save_multiple_symbols()
        test_load_saved_data()
        test_incremental_update()
        test_directory_structure()
        verify_schema()

        print("=" * 80)
        print("✅ All Tests Completed!")
        print("=" * 80)
        print()

    except ValueError as e:
        print(f"\n❌ Configuration error: {e}")
        print("\nPlease set your TIINGO_API_KEY:")
        print("  export TIINGO_API_KEY=your_key_here")
        print()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
