#!/usr/bin/env python3
"""
Simple ESG Manager Usage Example

Quick demonstration of how to use ESGManager for common tasks.

Author: QuantX Data Builder
Date: 2025-11-23
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from market import ESGManager
from universe import SP500Universe


def main():
    # Initialize
    universe = SP500Universe(data_root="./data")
    esg_mgr = ESGManager(universe)

    print("=" * 80)
    print("ESG MANAGER - QUICK START EXAMPLES")
    print("=" * 80)

    # Example 1: Get ESG data for a single company
    print("\n📊 Example 1: Get ESG data for Apple (AAPL)")
    print("-" * 80)

    aapl = esg_mgr.get_esg_data(symbol='AAPL', start_year=2020)
    print(aapl[['ticker', 'year', 'esg_score', 'env_score', 'soc_score', 'gov_score']])

    # Example 2: Get ESG data for multiple companies
    print("\n\n📊 Example 2: Compare ESG scores for tech companies")
    print("-" * 80)

    tech_stocks = ['AAPL', 'MSFT', 'GOOGL', 'META']
    data = esg_mgr.get_multiple_esg_data(tech_stocks, start_year=2023)

    for symbol, df in data.items():
        if not df.empty:
            latest = df.iloc[-1]
            print(f"{symbol:8s}: ESG={latest['esg_score']:.1f}, "
                  f"ENV={latest['env_score']:.1f}, "
                  f"SOC={latest['soc_score']:.1f}, "
                  f"GOV={latest['gov_score']:.1f}")

    # Example 3: Get coverage summary
    print("\n\n📊 Example 3: ESG data coverage over time")
    print("-" * 80)

    summary = esg_mgr.get_coverage_summary()
    recent = summary[summary['year'] >= 2020]
    print(recent.to_string(index=False))

    # Example 4: Export to Parquet
    print("\n\n💾 Example 4: Export ESG data to Parquet")
    print("-" * 80)

    symbols = ['AAPL', 'MSFT']
    results = esg_mgr.export_to_parquet(symbols, start_year=2020)

    for symbol, paths in results.items():
        print(f"✅ {symbol}: Saved {len(paths)} year(s) to data/curated/esg/ticker={symbol}/")

    # Example 5: Load from Parquet
    print("\n\n📂 Example 5: Load ESG data from Parquet")
    print("-" * 80)

    loaded = esg_mgr.load_esg_data('AAPL', start_year=2022)
    print(f"✅ Loaded {len(loaded)} records for AAPL (2022+)")
    print(loaded[['ticker', 'year', 'esg_score']])


if __name__ == "__main__":
    main()
