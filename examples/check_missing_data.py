#!/usr/bin/env python3
"""
Check Missing Data - Compare Existing Data vs Universe Members

Compares what tickers exist in the data directory against all universe members
for a specified period and identifies missing tickers.
"""

import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import Config
from src.market_data import PriceDataManager


class DataCoverageChecker:
    """Check data coverage for universe members"""

    def __init__(self, data_root: str = "data/curated"):
        self.data_root = Path(data_root)
        self.prices_root = self.data_root / "prices"
        self.membership_root = self.data_root / "membership"

    def get_existing_tickers(self, exchange: str = "us") -> Set[str]:
        """
        Get list of tickers that exist in the data directory

        Args:
            exchange: Exchange code (default: 'us')

        Returns:
            Set of ticker symbols that have data
        """
        exchange_path = self.prices_root / f"exchange={exchange}"

        if not exchange_path.exists():
            print(f"⚠️  Exchange directory not found: {exchange_path}")
            return set()

        tickers = set()
        for ticker_dir in exchange_path.iterdir():
            if ticker_dir.is_dir() and ticker_dir.name.startswith("ticker="):
                ticker = ticker_dir.name.replace("ticker=", "")
                tickers.add(ticker)

        return tickers

    def get_existing_ticker_coverage(
        self,
        ticker: str,
        exchange: str = "us"
    ) -> Dict[str, List[int]]:
        """
        Get year coverage for a specific ticker

        Args:
            ticker: Ticker symbol
            exchange: Exchange code

        Returns:
            Dictionary with freq -> list of years available
        """
        ticker_path = self.prices_root / f"exchange={exchange}" / f"ticker={ticker}"

        if not ticker_path.exists():
            return {}

        coverage = defaultdict(list)

        # Check daily adjusted data
        daily_adj_path = ticker_path / "freq=daily" / "adj=true"
        if daily_adj_path.exists():
            years = []
            for year_dir in daily_adj_path.iterdir():
                if year_dir.is_dir() and year_dir.name.startswith("year="):
                    year = int(year_dir.name.replace("year=", ""))
                    years.append(year)
            if years:
                coverage["daily_adj"] = sorted(years)

        return coverage

    def get_membership_intervals(
        self,
        universe: str
    ) -> pd.DataFrame:
        """
        Load membership intervals to determine when each stock was in the universe

        Args:
            universe: Universe name (e.g., 'sp500')

        Returns:
            DataFrame with columns: ticker, start_date, end_date
        """
        intervals_path = (
            self.membership_root / f"universe={universe.lower()}" /
            "mode=intervals" / f"{universe.lower()}_membership_intervals.parquet"
        )

        if not intervals_path.exists():
            raise FileNotFoundError(
                f"Membership intervals file not found: {intervals_path}\n"
                f"Run universe builder to create this file first."
            )

        df = pd.read_parquet(intervals_path)
        # Ensure dates are datetime
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])

        return df

    def get_date_range_from_parquet(
        self,
        ticker: str,
        exchange: str = "us"
    ) -> Tuple[str, str]:
        """
        Get actual date range from Parquet files

        Args:
            ticker: Ticker symbol
            exchange: Exchange code

        Returns:
            Tuple of (min_date, max_date) as strings, or (None, None) if no data
        """
        ticker_path = self.prices_root / f"exchange={exchange}" / f"ticker={ticker}"
        daily_adj_path = ticker_path / "freq=daily" / "adj=true"

        if not daily_adj_path.exists():
            return (None, None)

        all_dates = []
        for year_dir in daily_adj_path.iterdir():
            if year_dir.is_dir() and year_dir.name.startswith("year="):
                for parquet_file in year_dir.glob("*.parquet"):
                    try:
                        df = pd.read_parquet(parquet_file, columns=["date"])
                        all_dates.extend(df["date"].tolist())
                    except Exception as e:
                        print(f"⚠️  Error reading {parquet_file}: {e}")

        if not all_dates:
            return (None, None)

        all_dates_sorted = sorted(all_dates)
        min_date = str(all_dates_sorted[0])
        max_date = str(all_dates_sorted[-1])

        return (min_date, max_date)

    def check_missing_data(
        self,
        universe: str,
        start_date: str,
        end_date: str,
        api_key: str,
        exchange: str = "us",
        tolerance_days: int = 2
    ) -> Dict:
        """
        Check which universe members are missing data for POINT-IN-TIME membership periods.

        Only checks if data exists during the period when the stock was actually a member.
        For example, if TSLA joined S&P 500 on 2020-12-21, we only check if data exists
        from 2020-12-21 onwards (within the research period), not from 2014.

        Args:
            universe: Universe name (e.g., 'sp500')
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            api_key: API key for fetching universe members
            exchange: Exchange code (default: 'us')
            tolerance_days: Ignore gaps of this many days or less (default: 2)
                           Useful for ignoring weekend/holiday alignment issues

        Returns:
            Dictionary with analysis results
        """
        print()
        print("=" * 80)
        print(f"Point-in-Time Data Coverage Check: {universe.upper()}")
        print("=" * 80)
        print()
        print(f"📅 Research Period: {start_date} to {end_date}")
        print(f"📊 Universe: {universe}")
        print(f"🌐 Exchange: {exchange}")
        print(f"⏱️  Tolerance: Ignoring gaps ≤ {tolerance_days} days")
        print()
        print("ℹ️  Checking data coverage ONLY during membership periods")
        print("   (not requiring full period coverage for all tickers)")
        print()

        research_start = pd.to_datetime(start_date)
        research_end = pd.to_datetime(end_date)

        # Step 1: Load membership intervals
        print("🔍 Step 1: Loading point-in-time membership data...")
        try:
            membership_df = self.get_membership_intervals(universe)
            print(f"   ✅ Loaded membership intervals for {len(membership_df)} records")
            print(f"   ℹ️  Unique tickers: {membership_df['ticker'].nunique()}")
        except FileNotFoundError as e:
            print(f"   ❌ Error: {e}")
            print()
            print("   Please run the universe builder first to generate membership data.")
            return {}

        # Step 2: Filter to tickers active during research period
        print()
        print("🔍 Step 2: Filtering to tickers active during research period...")

        # Ticker was active if: start_date <= research_end AND end_date >= research_start
        active_mask = (
            (membership_df['start_date'] <= research_end) &
            (membership_df['end_date'] >= research_start)
        )
        active_members = membership_df[active_mask].copy()

        # Calculate required date range for each ticker (intersection of membership & research period)
        active_members['required_start'] = active_members.apply(
            lambda row: max(row['start_date'], research_start), axis=1
        )
        active_members['required_end'] = active_members.apply(
            lambda row: min(row['end_date'], research_end), axis=1
        )

        print(f"   ✅ Found {len(active_members)} membership periods")
        print(f"   ℹ️  Unique tickers active in period: {active_members['ticker'].nunique()}")

        # Step 3: Get existing data for each ticker
        print()
        print("🔍 Step 3: Checking existing data coverage...")
        existing_tickers = self.get_existing_tickers(exchange=exchange)
        print(f"   ✅ Found {len(existing_tickers)} tickers with data in directory")

        # Step 4: Check coverage for each ticker's required period
        print()
        print("🔍 Step 4: Analyzing point-in-time coverage...")

        complete_tickers = []
        partial_tickers = []
        missing_tickers = []

        checked_tickers = set()

        for _, row in active_members.iterrows():
            ticker = row['ticker']

            # Skip if we already checked this ticker
            if ticker in checked_tickers:
                continue
            checked_tickers.add(ticker)

            required_start = row['required_start']
            required_end = row['required_end']

            # Check if ticker has any data
            if ticker not in existing_tickers:
                missing_tickers.append({
                    'ticker': ticker,
                    'required_start': required_start,
                    'required_end': required_end,
                    'reason': 'No data directory'
                })
                continue

            # Get actual date range from parquet files
            actual_min, actual_max = self.get_date_range_from_parquet(ticker, exchange)

            if actual_min is None:
                missing_tickers.append({
                    'ticker': ticker,
                    'required_start': required_start,
                    'required_end': required_end,
                    'reason': 'No parquet files'
                })
                continue

            actual_min_date = pd.to_datetime(actual_min)
            actual_max_date = pd.to_datetime(actual_max)

            # Check if actual coverage overlaps with required period
            # Complete: actual data covers the entire required period (within tolerance)
            # Partial: actual data partially covers required period (gaps > tolerance)
            # Missing: no overlap

            # Calculate gaps at start and end
            start_gap_days = max(0, (actual_min_date - required_start).days)
            end_gap_days = max(0, (required_end - actual_max_date).days)

            # Check if gaps are within tolerance
            if start_gap_days <= tolerance_days and end_gap_days <= tolerance_days:
                # Complete coverage (within tolerance)
                complete_tickers.append({
                    'ticker': ticker,
                    'required_start': required_start,
                    'required_end': required_end,
                    'actual_start': actual_min_date,
                    'actual_end': actual_max_date
                })
            elif actual_max_date < required_start or actual_min_date > required_end:
                # No overlap - missing
                missing_tickers.append({
                    'ticker': ticker,
                    'required_start': required_start,
                    'required_end': required_end,
                    'actual_start': actual_min_date,
                    'actual_end': actual_max_date,
                    'reason': f'Data range mismatch'
                })
            else:
                # Partial overlap - gaps exceed tolerance
                gap_info = []
                if actual_min_date > required_start:
                    gap_days = (actual_min_date - required_start).days
                    if gap_days > tolerance_days:
                        gap_info.append(f'Missing start: {gap_days} days')
                if actual_max_date < required_end:
                    gap_days = (required_end - actual_max_date).days
                    if gap_days > tolerance_days:
                        gap_info.append(f'Missing end: {gap_days} days')

                # Only add to partial if there are significant gaps
                if gap_info:
                    partial_tickers.append({
                        'ticker': ticker,
                        'required_start': required_start,
                        'required_end': required_end,
                        'actual_start': actual_min_date,
                        'actual_end': actual_max_date,
                        'gaps': ', '.join(gap_info)
                    })
                else:
                    # Gaps within tolerance - treat as complete
                    complete_tickers.append({
                        'ticker': ticker,
                        'required_start': required_start,
                        'required_end': required_end,
                        'actual_start': actual_min_date,
                        'actual_end': actual_max_date
                    })

        # Step 5: Summary
        total = len(checked_tickers)
        print()
        print("=" * 80)
        print("📊 POINT-IN-TIME COVERAGE SUMMARY")
        print("=" * 80)
        print()
        print(f"Total Unique Tickers:    {total:>6}")
        print(f"Complete Coverage:       {len(complete_tickers):>6} ({len(complete_tickers)/total*100:.1f}%)")
        print(f"Partial Coverage:        {len(partial_tickers):>6} ({len(partial_tickers)/total*100:.1f}%)")
        print(f"Missing Data:            {len(missing_tickers):>6} ({len(missing_tickers)/total*100:.1f}%)")
        print()
        print(f"ℹ️  'Complete' = data exists for entire membership period (±{tolerance_days} days)")
        print(f"ℹ️  'Partial'  = data missing > {tolerance_days} days at start/end of membership period")
        print("ℹ️  'Missing'  = no data available for membership period")
        print()

        # Step 6: Detailed reports
        if missing_tickers:
            print("=" * 80)
            print(f"❌ MISSING DATA ({len(missing_tickers)} tickers)")
            print("=" * 80)
            print()
            for i, info in enumerate(sorted(missing_tickers, key=lambda x: x['ticker']), 1):
                ticker = info['ticker']
                req_start = info['required_start'].strftime('%Y-%m-%d')
                req_end = info['required_end'].strftime('%Y-%m-%d')
                reason = info.get('reason', 'Unknown')
                print(f"  {i:>3}. {ticker:>6}: Required {req_start} to {req_end} - {reason}")
            print()

        if partial_tickers:
            print("=" * 80)
            print(f"⚠️  PARTIAL COVERAGE ({len(partial_tickers)} tickers)")
            print("=" * 80)
            print()
            for info in sorted(partial_tickers, key=lambda x: x['ticker']):
                ticker = info['ticker']
                req_start = info['required_start'].strftime('%Y-%m-%d')
                req_end = info['required_end'].strftime('%Y-%m-%d')
                act_start = info['actual_start'].strftime('%Y-%m-%d')
                act_end = info['actual_end'].strftime('%Y-%m-%d')
                gaps = info['gaps']
                print(f"  {ticker:>6}: Required [{req_start} to {req_end}]")
                print(f"          Actual   [{act_start} to {act_end}] - {gaps}")
            print()

        if complete_tickers:
            print("=" * 80)
            print(f"✅ COMPLETE COVERAGE ({len(complete_tickers)} tickers)")
            print("=" * 80)
            print()
            # Show first 20
            for i, info in enumerate(sorted(complete_tickers, key=lambda x: x['ticker'])[:20], 1):
                ticker = info['ticker']
                req_start = info['required_start'].strftime('%Y-%m-%d')
                req_end = info['required_end'].strftime('%Y-%m-%d')
                print(f"  {i:>3}. {ticker:>6}: {req_start} to {req_end} ✓")

            if len(complete_tickers) > 20:
                print(f"  ... and {len(complete_tickers) - 20} more tickers")
            print()

        return {
            "universe": universe,
            "period": f"{start_date} to {end_date}",
            "total_tickers": total,
            "complete": len(complete_tickers),
            "partial": len(partial_tickers),
            "missing": len(missing_tickers),
            "complete_tickers": [t['ticker'] for t in complete_tickers],
            "partial_tickers": [t['ticker'] for t in partial_tickers],
            "missing_tickers": [t['ticker'] for t in missing_tickers],
        }


def main():
    """Main function"""
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Data Coverage Checker" + " " * 37 + "║")
    print("║" + " " * 15 + "Compare Existing vs Universe Members" + " " * 26 + "║")
    print("╚" + "=" * 78 + "╝")
    print()

    # Load config
    try:
        config = Config("config/settings.yaml")
        api_key = config.get("fetcher.tiingo.api_key")
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return 1

    # Create checker
    checker = DataCoverageChecker(data_root="data/curated")

    # Example 1: Check S&P 500 for 2020-2024
    print("\n" + "=" * 80)
    print("Example 1: S&P 500 (2020-2024)")
    print("=" * 80)

    results = checker.check_missing_data(
        universe="sp500",
        start_date="2014-01-01",
        end_date="2024-12-31",
        api_key=api_key,
        exchange="us"
    )

    # Generate fetch command for missing tickers
    if results and results["missing_tickers"]:
        print()
        print("=" * 80)
        print("🚀 TO FETCH MISSING DATA:")
        print("=" * 80)
        print()
        print("Run the following command:")
        print()
        missing_list = ", ".join([f"'{t}'" for t in results["missing_tickers"][:10]])
        if len(results["missing_tickers"]) > 10:
            missing_list += ", ..."

        print(f"""
builder = PriceDataManager(api_key=api_key)
missing_symbols = {results["missing_tickers"][:50]}

for symbol in missing_symbols:
    try:
        builder.fetch_and_save(
            symbol=symbol,
            start_date="{results['period'].split(' to ')[0]}",
            end_date="{results['period'].split(' to ')[1]}",
            exchange="us"
        )
    except Exception as e:
        print(f"Error fetching {{symbol}}: {{e}}")
""")

    # Example 2: Check for a different period (optional)
    # Uncomment to check a different period
    # print("\n" + "=" * 80)
    # print("Example 2: S&P 500 (2010-2015)")
    # print("=" * 80)
    #
    # checker.check_missing_data(
    #     universe="sp500",
    #     start_date="2010-01-01",
    #     end_date="2015-12-31",
    #     api_key=api_key,
    #     exchange="us"
    # )

    return 0


if __name__ == "__main__":
    sys.exit(main())
