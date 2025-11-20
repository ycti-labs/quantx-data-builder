#!/usr/bin/env python3
"""
Add GVKEY Mapping to SP500 Membership Data

Reads data_mapping.xlsx and:
1. Creates a clean gvkey-ticker mapping parquet file
2. Updates SP500 membership intervals with gvkey column

Author: QuantX Data Builder
Date: 2025-11-20
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def clean_ticker_symbol(ticker: str) -> str:
    """
    Clean ticker symbol by removing suffixes like .1, .2, etc.

    Args:
        ticker: Raw ticker symbol (e.g., 'AAPL.1', 'MSFT')

    Returns:
        Cleaned ticker symbol (e.g., 'AAPL', 'MSFT')
    """
    if pd.isna(ticker):
        return ticker

    ticker = str(ticker).strip().upper()

    # Remove common suffixes
    # .1, .2, etc. (duplicate listings)
    # . at the end (trailing dot)
    if '.' in ticker:
        parts = ticker.split('.')
        # Keep only the first part if second part is numeric or empty
        if len(parts) == 2 and (parts[1].isdigit() or parts[1] == ''):
            return parts[0]

    return ticker


def build_gvkey_ticker_mapping():
    """
    Task 1: Build a clean gvkey-ticker mapping parquet file

    Reads data/raw/data_mapping.xlsx and creates:
    data/curated/metadata/gvkey_ticker_mapping.parquet

    Returns:
        DataFrame with gvkey-ticker mapping
    """
    print("=" * 80)
    print("Task 1: Building GVKEY-Ticker Mapping")
    print("=" * 80)

    # Read the mapping file
    input_path = Path("data/raw/data_mapping.xlsx")
    print(f"Reading: {input_path}")
    df_mapping = pd.read_excel(input_path)

    print(f"  Loaded {len(df_mapping):,} rows")
    print(f"  Columns: {df_mapping.columns.tolist()}")

    # Clean the data
    df_mapping = df_mapping.copy()
    df_mapping['ticker_raw'] = df_mapping['tic']
    df_mapping['ticker'] = df_mapping['tic'].apply(clean_ticker_symbol)
    df_mapping['gvkey'] = df_mapping['gvkey'].astype(int)

    # Reorder columns
    df_mapping = df_mapping[['gvkey', 'ticker', 'ticker_raw']]

    # Remove duplicates (prefer first occurrence)
    print(f"  Cleaning duplicates...")
    before_dedup = len(df_mapping)
    df_mapping = df_mapping.drop_duplicates(subset=['gvkey', 'ticker'], keep='first')
    after_dedup = len(df_mapping)
    print(f"  Removed {before_dedup - after_dedup:,} duplicate rows")

    # Sort by gvkey
    df_mapping = df_mapping.sort_values('gvkey').reset_index(drop=True)

    # Save to parquet
    output_path = Path("data/curated/metadata/gvkey_ticker_mapping.parquet")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_mapping.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )

    print(f"✅ Saved: {output_path}")
    print(f"  Final shape: {df_mapping.shape}")
    print(f"  Unique gvkeys: {df_mapping['gvkey'].nunique():,}")
    print(f"  Unique tickers: {df_mapping['ticker'].nunique():,}")
    print()

    return df_mapping


def update_sp500_membership_with_gvkey(df_mapping: pd.DataFrame):
    """
    Task 2: Update SP500 membership intervals with gvkey column

    Adds gvkey column to:
    data/curated/membership/universe=sp500/mode=intervals/sp500_membership_intervals.parquet

    Args:
        df_mapping: GVKEY-ticker mapping DataFrame
    """
    print("=" * 80)
    print("Task 2: Updating SP500 Membership with GVKEY")
    print("=" * 80)

    # Read current membership file
    membership_path = Path(
        "data/curated/membership/universe=sp500/mode=intervals/"
        "sp500_membership_intervals.parquet"
    )
    print(f"Reading: {membership_path}")
    df_membership = pd.read_parquet(membership_path)

    print(f"  Current shape: {df_membership.shape}")
    print(f"  Current columns: {df_membership.columns.tolist()}")

    # Create a lookup dictionary for faster matching
    # For each ticker, get the gvkey (handle multiple gvkeys per ticker)
    ticker_to_gvkey = {}
    for _, row in df_mapping.iterrows():
        ticker = row['ticker']
        gvkey = row['gvkey']
        if ticker not in ticker_to_gvkey:
            ticker_to_gvkey[ticker] = gvkey

    print(f"  Created lookup for {len(ticker_to_gvkey):,} tickers")

    # Map gvkey to membership data
    df_membership['gvkey'] = df_membership['ticker'].map(ticker_to_gvkey)

    # Count matches
    matched = df_membership['gvkey'].notna().sum()
    unmatched = df_membership['gvkey'].isna().sum()

    print(f"  Matched: {matched}/{len(df_membership)} ({matched/len(df_membership)*100:.1f}%)")
    if unmatched > 0:
        print(f"  ⚠️  Unmatched: {unmatched} tickers")
        print("  Unmatched tickers:")
        unmatched_tickers = df_membership[df_membership['gvkey'].isna()]['ticker'].unique()
        for ticker in sorted(unmatched_tickers[:20]):  # Show first 20
            print(f"    - {ticker}")
        if len(unmatched_tickers) > 20:
            print(f"    ... and {len(unmatched_tickers) - 20} more")

    # Reorder columns: gvkey, ticker, start_date, end_date
    df_membership = df_membership[['gvkey', 'ticker', 'start_date', 'end_date']]

    # Convert gvkey to Int64 (nullable integer type)
    df_membership['gvkey'] = df_membership['gvkey'].astype('Int64')

    # Backup original file
    backup_path = membership_path.with_suffix('.parquet.backup')
    if membership_path.exists() and not backup_path.exists():
        import shutil
        shutil.copy2(membership_path, backup_path)
        print(f"  Created backup: {backup_path}")

    # Save updated file
    df_membership.to_parquet(
        membership_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )

    print(f"✅ Updated: {membership_path}")
    print(f"  New shape: {df_membership.shape}")
    print(f"  New columns: {df_membership.columns.tolist()}")
    print()

    # Show sample of updated data
    print("Sample of updated data:")
    print(df_membership.head(10))
    print()

    return df_membership


def verify_update():
    """Verify the update was successful"""
    print("=" * 80)
    print("Verification")
    print("=" * 80)

    # Read the updated file
    membership_path = Path(
        "data/curated/membership/universe=sp500/mode=intervals/"
        "sp500_membership_intervals.parquet"
    )
    df = pd.read_parquet(membership_path)

    print(f"✅ File readable: {membership_path}")
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {df.columns.tolist()}")
    print(f"  Has gvkey: {'gvkey' in df.columns}")
    print(f"  GVKEY coverage: {df['gvkey'].notna().sum()}/{len(df)} "
          f"({df['gvkey'].notna().sum()/len(df)*100:.1f}%)")
    print()

    # Show statistics
    print("Statistics:")
    print(f"  Total tickers: {len(df)}")
    print(f"  Unique tickers: {df['ticker'].nunique()}")
    print(f"  Unique gvkeys: {df['gvkey'].nunique()}")
    print(f"  GVKEY range: {df['gvkey'].min()} - {df['gvkey'].max()}")
    print()


def main():
    """Main execution"""
    print()
    print("=" * 80)
    print("Add GVKEY Mapping to SP500 Membership Data")
    print("=" * 80)
    print()

    try:
        # Task 1: Build gvkey-ticker mapping
        df_mapping = build_gvkey_ticker_mapping()

        # Task 2: Update SP500 membership with gvkey
        df_membership = update_sp500_membership_with_gvkey(df_mapping)

        # Verify
        verify_update()

        print("=" * 80)
        print("✅ All tasks completed successfully!")
        print("=" * 80)
        print()
        print("Output files:")
        print("  1. data/curated/metadata/gvkey_ticker_mapping.parquet")
        print("  2. data/curated/membership/universe=sp500/mode=intervals/"
              "sp500_membership_intervals.parquet (updated)")
        print("  3. Backup created: sp500_membership_intervals.parquet.backup")
        print()

    except Exception as e:
        print()
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
