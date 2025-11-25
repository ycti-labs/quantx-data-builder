"""
Check for symbols with multiple membership intervals

This script analyzes the membership intervals to find symbols that were
removed and re-added to the universe (have multiple non-contiguous intervals).
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

import pandas as pd

from universe import SP500Universe


def main():
    """Check for symbols with multiple membership intervals"""

    print("=" * 80)
    print("Checking for Symbols with Multiple Membership Intervals")
    print("=" * 80)

    # Initialize universe
    universe = SP500Universe()

    # Read membership intervals
    intervals_path = (
        universe.get_membership_path(mode='intervals') /
        f"{universe.name.lower()}_membership_intervals.parquet"
    )

    if not intervals_path.exists():
        print(f"❌ Membership intervals file not found: {intervals_path}")
        return

    print(f"\n📂 Reading: {intervals_path}")
    df = pd.read_parquet(intervals_path)

    # Convert dates
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])

    print(f"\n📊 Total intervals: {len(df)}")
    print(f"📊 Unique symbols: {df['ticker'].nunique()}")

    # Count intervals per symbol
    interval_counts = df.groupby('ticker').size()

    # Find symbols with multiple intervals
    multiple_intervals = interval_counts[interval_counts > 1].sort_values(ascending=False)

    print(f"\n{'=' * 80}")
    print(f"Symbols with Multiple Intervals: {len(multiple_intervals)}")
    print(f"{'=' * 80}")

    if len(multiple_intervals) == 0:
        print("\n✅ No symbols have multiple intervals")
        print("   All symbols have continuous membership (or single interval)")
    else:
        print(f"\n⚠️  Found {len(multiple_intervals)} symbols with multiple intervals:\n")

        for symbol, count in multiple_intervals.items():
            print(f"\n{'─' * 80}")
            print(f"Symbol: {symbol} ({count} intervals)")
            print(f"{'─' * 80}")

            # Get all intervals for this symbol
            symbol_intervals = df[df['ticker'] == symbol].sort_values('start_date')

            for idx, row in symbol_intervals.iterrows():
                duration_days = (row['end_date'] - row['start_date']).days
                print(f"  Interval {idx + 1}:")
                print(f"    Start: {row['start_date'].date()}")
                print(f"    End:   {row['end_date'].date()}")
                print(f"    Duration: {duration_days} days ({duration_days / 365.25:.1f} years)")
                if 'gvkey' in row and pd.notna(row['gvkey']):
                    print(f"    GVKEY: {row['gvkey']}")

            # Calculate gaps between intervals
            if count > 1:
                print(f"\n  Gaps between intervals:")
                for i in range(len(symbol_intervals) - 1):
                    interval1 = symbol_intervals.iloc[i]
                    interval2 = symbol_intervals.iloc[i + 1]
                    gap_start = interval1['end_date']
                    gap_end = interval2['start_date']
                    gap_days = (gap_end - gap_start).days
                    print(f"    Gap {i + 1}: {gap_start.date()} to {gap_end.date()} ({gap_days} days)")

    # Summary statistics
    print(f"\n{'=' * 80}")
    print("Summary Statistics")
    print(f"{'=' * 80}")

    single_interval = len(interval_counts[interval_counts == 1])
    multi_interval = len(interval_counts[interval_counts > 1])

    print(f"Symbols with single interval:   {single_interval} ({single_interval/len(interval_counts)*100:.1f}%)")
    print(f"Symbols with multiple intervals: {multi_interval} ({multi_interval/len(interval_counts)*100:.1f}%)")

    if len(multiple_intervals) > 0:
        max_intervals = interval_counts.max()
        max_symbol = interval_counts.idxmax()
        print(f"\nMax intervals: {max_intervals} (symbol: {max_symbol})")

        # Show distribution
        print(f"\nDistribution of interval counts:")
        for count in sorted(interval_counts.unique()):
            num_symbols = len(interval_counts[interval_counts == count])
            print(f"  {count} interval(s): {num_symbols} symbols")

    # Check for overlapping intervals (data quality issue)
    print(f"\n{'=' * 80}")
    print("Data Quality Check: Overlapping Intervals")
    print(f"{'=' * 80}")

    overlaps_found = False
    for symbol in df['ticker'].unique():
        symbol_intervals = df[df['ticker'] == symbol].sort_values('start_date')
        if len(symbol_intervals) > 1:
            for i in range(len(symbol_intervals) - 1):
                interval1 = symbol_intervals.iloc[i]
                interval2 = symbol_intervals.iloc[i + 1]
                if interval1['end_date'] >= interval2['start_date']:
                    if not overlaps_found:
                        print("\n⚠️  Found overlapping intervals:")
                        overlaps_found = True
                    print(f"\n  {symbol}:")
                    print(f"    Interval 1: {interval1['start_date'].date()} to {interval1['end_date'].date()}")
                    print(f"    Interval 2: {interval2['start_date'].date()} to {interval2['end_date'].date()}")
                    print(f"    Overlap: {(interval1['end_date'] - interval2['start_date']).days} days")

    if not overlaps_found:
        print("\n✅ No overlapping intervals found (data is clean)")

    print(f"\n{'=' * 80}")

if __name__ == "__main__":
    main()
