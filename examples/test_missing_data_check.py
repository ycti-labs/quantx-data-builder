#!/usr/bin/env python3
"""
Quick test of the new missing data check functionality
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetcher.config_loader import FetcherConfig
from fetcher.price_data_builder import PriceDataManager

# Load config
config = FetcherConfig("config/settings.yaml")
api_key = config.fetcher.tiingo.api_key

# Initialize builder
builder = PriceDataManager(api_key=api_key, data_root="data/curated")

# Test 1: Check existing data range
print("Test 1: Get existing data range for AAPL")
print("-" * 60)
date_range = builder.get_existing_date_range("AAPL", exchange="us")
if date_range:
    print(f"AAPL data range: {date_range[0]} to {date_range[1]}")
else:
    print("No data found for AAPL")
print()

# Test 2: Check missing data
print("Test 2: Check what's missing for AAPL (2020-2024)")
print("-" * 60)
check = builder.check_missing_data(
    symbol="AAPL",
    required_start="2020-01-01",
    required_end="2024-12-31",
    tolerance_days=2
)
print(f"Status: {check['status']}")
print(f"Actual start: {check['actual_start']}")
print(f"Actual end: {check['actual_end']}")
print(f"Missing at start: {check['missing_start_days']} days")
print(f"Missing at end: {check['missing_end_days']} days")
if check['fetch_start']:
    print(f"Should fetch from: {check['fetch_start']}")
if check['fetch_end']:
    print(f"Should fetch until: {check['fetch_end']}")
print()

# Test 3: Check a ticker with no data
print("Test 3: Check missing data for ticker with no data")
print("-" * 60)
check_missing = builder.check_missing_data(
    symbol="NEWCO",  # Doesn't exist
    required_start="2020-01-01",
    required_end="2024-12-31"
)
print(f"Status: {check_missing['status']}")
print(f"Should fetch: {check_missing['fetch_start']} to {check_missing['fetch_end']}")
print()

print("✅ Tests complete!")
