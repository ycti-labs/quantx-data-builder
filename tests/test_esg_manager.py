#!/usr/bin/env python3
"""
Test ESGManager - Demonstrate ESG data loading and management

This script demonstrates how to use ESGManager to:
1. Load ESG data from Excel file
2. Map GVKEY identifiers to ticker symbols
3. Query ESG scores for specific companies
4. Save ESG data to Parquet format
5. Generate coverage reports

Usage:
    python examples/test_esg_manager.py

Author: QuantX Data Builder
Date: 2025-11-23
"""

import sys
from pathlib import Path

import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from market import ESGManager
from universe import SP500Universe


def test_basic_queries():
    """Test basic ESG data queries"""
    print("=" * 80)
    print("TEST 1: Basic ESG Data Queries")
    print("=" * 80)

    # Initialize
    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    # Test 1: Get ESG data for AAPL
    print("\n📊 Querying ESG data for AAPL...")
    aapl_esg = esg_mgr.get_esg_data(symbol='AAPL')

    if not aapl_esg.empty:
        print(f"✅ Found {len(aapl_esg)} ESG records for AAPL")
        print(f"   Years: {aapl_esg['year'].min()} - {aapl_esg['year'].max()}")
        print(f"   Average ESG Score: {aapl_esg['esg_score'].mean():.2f}")
        print(f"\n   Sample records:")
        print(aapl_esg.head(10).to_string(index=False))
    else:
        print("⚠️  No ESG data found for AAPL")

    # Test 2: Get ESG data for multiple symbols
    print("\n" + "=" * 80)
    print("\n📊 Querying ESG data for multiple symbols...")
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

    esg_data = esg_mgr.get_multiple_esg_data(symbols, start_year=2020)

    print(f"\n✅ Retrieved ESG data for {len(esg_data)} symbols:")
    for symbol, df in esg_data.items():
        if not df.empty:
            avg_score = df['esg_score'].mean()
            years = f"{df['year'].min()}-{df['year'].max()}"
            print(f"   {symbol:8s}: {len(df):3d} records ({years}) | Avg ESG: {avg_score:5.2f}")


def test_coverage_analysis():
    """Test ESG coverage analysis"""
    print("\n" + "=" * 80)
    print("TEST 2: ESG Coverage Analysis")
    print("=" * 80)

    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    # Get coverage summary
    print("\n📊 ESG Data Coverage by Year:")
    summary = esg_mgr.get_coverage_summary()

    print(f"\n{'Year':<8s} {'Companies':<15s} {'Records':<15s} {'Avg Records/Co':<20s}")
    print("-" * 60)

    for _, row in summary.iterrows():
        year = int(row['year'])
        companies = int(row['num_companies'])
        records = int(row['num_records'])
        avg_records = records / companies if companies > 0 else 0

        print(f"{year:<8d} {companies:<15,} {records:<15,} {avg_records:<20.1f}")

    # Total summary
    print("-" * 60)
    total_companies = summary['num_companies'].sum()
    total_records = summary['num_records'].sum()
    unique_companies = esg_mgr._load_esg_data()['gvkey'].nunique()

    print(f"{'TOTAL':<8s} {total_companies:<15,} {total_records:<15,}")
    print(f"{'UNIQUE':<8s} {unique_companies:<15,}")


def test_year_specific_queries():
    """Test year-specific ESG queries"""
    print("\n" + "=" * 80)
    print("TEST 3: Year-Specific Queries")
    print("=" * 80)

    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    test_years = [2015, 2020, 2023]

    for year in test_years:
        print(f"\n📅 Companies with ESG data in {year}:")
        tickers = esg_mgr.get_available_tickers(year=year)
        print(f"   Total: {len(tickers)} companies")
        print(f"   Sample: {', '.join(tickers[:20])}")
        if len(tickers) > 20:
            print(f"   ... and {len(tickers) - 20} more")


def test_save_and_load():
    """Test saving and loading ESG data"""
    print("\n" + "=" * 80)
    print("TEST 4: Save and Load ESG Data")
    print("=" * 80)

    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    # Test symbol
    test_symbol = 'AAPL'

    # Get and save ESG data
    print(f"\n💾 Saving ESG data for {test_symbol}...")
    df = esg_mgr.get_esg_data(symbol=test_symbol)

    if not df.empty:
        saved_paths = esg_mgr.save_esg_data(df, test_symbol)
        print(f"✅ Saved {len(saved_paths)} file(s):")
        for path in saved_paths:
            print(f"   {path}")

        # Load back the data
        print(f"\n📂 Loading ESG data for {test_symbol}...")
        loaded_df = esg_mgr.load_esg_data(test_symbol)

        if not loaded_df.empty:
            print(f"✅ Loaded {len(loaded_df)} records")
            print(f"   Years: {loaded_df['year'].min()} - {loaded_df['year'].max()}")

            # Verify data matches
            if len(df) == len(loaded_df):
                print("✅ Data integrity verified (same number of records)")
            else:
                print(f"⚠️  Record count mismatch: {len(df)} saved vs {len(loaded_df)} loaded")
        else:
            print("⚠️  Failed to load saved data")
    else:
        print(f"⚠️  No ESG data available for {test_symbol}")


def test_score_analysis():
    """Test ESG score analysis"""
    print("\n" + "=" * 80)
    print("TEST 5: ESG Score Analysis")
    print("=" * 80)

    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    # Get data for multiple tech companies
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'META', 'TSLA', 'NVDA', 'AMD']
    print(f"\n📊 Analyzing ESG scores for {len(symbols)} tech companies...")

    esg_data = esg_mgr.get_multiple_esg_data(symbols, start_year=2020)

    if esg_data:
        print(f"\n{'Symbol':<8s} {'Years':<12s} {'ESG':<8s} {'ENV':<8s} {'SOC':<8s} {'GOV':<8s}")
        print("-" * 56)

        for symbol in symbols:
            if symbol in esg_data and not esg_data[symbol].empty:
                df = esg_data[symbol]
                years = f"{df['year'].min()}-{df['year'].max()}"
                esg_avg = df['esg_score'].mean()
                env_avg = df['env_score'].mean()
                soc_avg = df['soc_score'].mean()
                gov_avg = df['gov_score'].mean()

                print(f"{symbol:<8s} {years:<12s} {esg_avg:<8.2f} {env_avg:<8.2f} {soc_avg:<8.2f} {gov_avg:<8.2f}")
            else:
                print(f"{symbol:<8s} {'No data':<12s}")


def test_export_batch():
    """Test batch export of ESG data"""
    print("\n" + "=" * 80)
    print("TEST 6: Batch Export ESG Data")
    print("=" * 80)

    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    # Export data for a subset of symbols
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT']
    print(f"\n💾 Exporting ESG data for {len(symbols)} symbols...")

    results = esg_mgr.export_to_parquet(symbols, start_year=2015)

    print(f"\n✅ Export completed for {len(results)} symbols:")
    for symbol, paths in results.items():
        print(f"   {symbol}: {len(paths)} year(s) saved")

    # Show storage structure
    esg_root = Path("data/curated/esg")
    if esg_root.exists():
        ticker_dirs = [d for d in esg_root.iterdir() if d.is_dir()]
        print(f"\n📂 Storage structure: {len(ticker_dirs)} ticker directories")
        print(f"   Location: {esg_root}")


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("ESG MANAGER TEST SUITE")
    print("=" * 80)

    tests = [
        ("Basic Queries", test_basic_queries),
        ("Coverage Analysis", test_coverage_analysis),
        ("Year-Specific Queries", test_year_specific_queries),
        ("Save and Load", test_save_and_load),
        ("Score Analysis", test_score_analysis),
        ("Batch Export", test_export_batch),
    ]

    for test_name, test_func in tests:
        try:
            test_func()
        except Exception as e:
            print(f"\n❌ Error in {test_name}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 80)
    print("✅ TEST SUITE COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()
