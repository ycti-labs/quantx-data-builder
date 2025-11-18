"""
S&P 500 Historical Membership Data Builder

Reads raw historical S&P 500 constituent data and generates:
1. Daily membership snapshots (ticker-date pairs)
2. Membership intervals (ticker with start/end dates for consecutive runs)

Input:  /data/raw/S&P 500 Historical Components & Changes(MM-DD-YYYY).csv
Output: /data/curated/membership/*.parquet (Snappy compressed)

Data Contract:
- Daily:     [ticker: str, date: date]
- Intervals: [ticker: str, start_date: date, end_date: date]
"""

import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
from universe import Universe


class SP500Universe(Universe):
    """
    S&P 500 Universe

    Extends Universe to provide S&P 500-specific membership data processing.
    Handles loading raw historical constituent data and synthesizing membership intervals.
    """

    identifier = "sp500"

    def get_members(self, as_of_date: str | None = None) -> List[str]:
        """
        Get S&P 500 universe members (as of given date or current)

        Args:
            as_of_date: ISO date string (YYYY-MM-DD) or None for current members

        Returns:
            List of member ticker symbols
        """
        return super().get_members(self.identifier, as_of_date)

    def get_current_members(self) -> List[str]:
        return super().get_current_members(self.identifier)

    def get_all_historical_members(
        self,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        Get all historical S&P 500 members between start and end dates.

        Args:
            start_date: ISO date string (YYYY-MM-DD)
            end_date: ISO date string (YYYY-MM-DD)
        Returns:
            List of historical member ticker symbols
        """
        return super().get_all_historical_members(
            self.identifier,
            start_date,
            end_date
        )

    def __init__(
        self,
        config_path: Optional[str] = None,
        data_root: str = "data/curated",
        raw_data_root: str = "data/raw"
    ):
        """
        Initialize S&P 500 Universe Builder

        Args:
            config_path: Path to universe configuration YAML file (optional)
            data_root: Root directory for curated data storage
            raw_data_root: Root directory for raw data files
        """
        super().__init__(config_path=config_path, data_root=data_root)
        self.raw_data_root = Path(raw_data_root)

    @staticmethod
    def load_and_explode_daily_membership(
        raw_csv_path: str, min_date: str = "2000-01-01"
    ) -> pd.DataFrame:
        """
        Load CSV with (date, tickers) columns and expand to (date, ticker) rows.

        Args:
            raw_csv_path: Path to raw CSV file
            min_date: Filter dates >= this value (ISO format YYYY-MM-DD)

        Returns:
            DataFrame with columns [date: datetime, ticker: str]
        """
        df = pd.read_csv(raw_csv_path, engine="python")
        assert {"date", "tickers"}.issubset(
            df.columns
        ), f"CSV must have 'date' and 'tickers' columns, got {df.columns.tolist()}"

        # Parse dates
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        # Explode comma-separated tickers
        records = []
        for date_val, tickers_str in zip(
            df["date"].tolist(), df["tickers"].astype(str).tolist()
        ):
            # Clean: strip quotes/spaces, split by comma
            tickers_clean = tickers_str.strip().strip('"').replace(" ", "")
                # Clean: strip quotes/spaces, split by comma
            tickers_clean = tickers_str.strip().strip('"').replace(" ", "")
            ticker_list = [t.upper() for t in tickers_clean.split(",") if t]
            for ticker in ticker_list:
                records.append((date_val, ticker))

        membership_daily = (
            pd.DataFrame(records, columns=["date", "ticker"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Filter to QuantX scope (2000-01-01 forward)
        min_date_ts = pd.Timestamp(min_date)
        membership_daily = membership_daily[membership_daily["date"] >= min_date_ts]

        return membership_daily

    @staticmethod
    def synthesize_membership_intervals(membership_daily: pd.DataFrame) -> pd.DataFrame:
        """
        Detect runs of consecutive presence across recorded dates to build intervals.

        Algorithm:
        1. For each ticker, mark which dates it appears on
        2. Find runs where ticker is present continuously
        3. Record (ticker, start_date, end_date) for each run

        Args:
            membership_daily: DataFrame with [date, ticker]

        Returns:
            DataFrame with [ticker, start_date, end_date]
        """
        # Get sorted unique dates across entire dataset
        all_dates = pd.Index(membership_daily["date"].sort_values().unique())

        interval_rows = []
        for ticker, group in membership_daily.groupby("ticker"):
            # Dates where this ticker is present
            present_dates = pd.Index(group["date"].unique())

            # Create binary series across all dates
            present = pd.Series(0, index=all_dates, dtype=int)
            present.loc[present_dates] = 1

            # Find run boundaries
            # Start: present=1 and previous=0
            starts = present.index[(present == 1) & (present.shift(1, fill_value=0) == 0)]
            # End: present=1 and next=0
            ends = present.index[(present == 1) & (present.shift(-1, fill_value=0) == 0)]

            for start, end in zip(starts, ends):
                interval_rows.append((ticker, start.date(), end.date()))

        membership_intervals = (
            pd.DataFrame(interval_rows, columns=["ticker", "start_date", "end_date"])
            .sort_values(["ticker", "start_date"])
            .reset_index(drop=True)
        )

        return membership_intervals

    def build_sp500_membership(
        self,
        min_date: str = "2000-01-01",
        universe_name: str = "sp500"
    ) -> dict:
        """
        Build S&P 500 membership datasets from raw historical data.

        Args:
            min_date: Minimum date to include (ISO format YYYY-MM-DD)
            universe_name: Universe name for output paths (default: 'sp500')

        Returns:
            Dictionary with statistics and output paths
        """
        # Find raw CSV (flexible naming with date stamp)
        raw_csv_files = list(self.raw_data_root.glob("S&P 500 Historical Components*.csv"))
        if not raw_csv_files:
            raise FileNotFoundError(
                f"No S&P 500 historical CSV found in {self.raw_data_root}"
            )

        raw_csv_path = raw_csv_files[0]  # Use first match
        print(f"📂 Loading: {raw_csv_path}")

        # Step 1: Load and expand daily membership
        membership_daily = self.load_and_explode_daily_membership(
            str(raw_csv_path), min_date=min_date
        )

        # Step 2: Synthesize membership intervals
        membership_intervals = self.synthesize_membership_intervals(membership_daily)

        # Step 3: Write Snappy-compressed Parquet files using parent method
        curated_membership_path = self.data_root / "membership" / f"universe={universe_name}"

        daily_output = curated_membership_path / "mode=daily" / f"{universe_name}_membership_daily.parquet"
        intervals_output = curated_membership_path / "mode=intervals" / f"{universe_name}_membership_intervals.parquet"

        self.write_snappy_parquet(membership_daily, daily_output)
        self.write_snappy_parquet(membership_intervals, intervals_output)

        # Summary stats
        stats = {
            "first_date": str(membership_daily["date"].min().date()),
            "last_date": str(membership_daily["date"].max().date()),
            "unique_tickers": int(membership_daily["ticker"].nunique()),
            "daily_rows": len(membership_daily),
            "interval_rows": len(membership_intervals),
            "daily_output": str(daily_output),
            "intervals_output": str(intervals_output),
        }

        print(f"\n✅ Success!")
        print(f"   First date:      {stats['first_date']}")
        print(f"   Last date:       {stats['last_date']}")
        print(f"   Unique tickers:  {stats['unique_tickers']}")
        print(f"   Daily rows:      {stats['daily_rows']:,}")
        print(f"   Interval rows:   {stats['interval_rows']:,}")
        print(f"\n📁 Output:")
        print(f"   {daily_output}")
        print(f"   {intervals_output}")

        # Show sample for AAPL
        print("\n📊 Sample: AAPL membership intervals")
        aapl_intervals = membership_intervals[membership_intervals["ticker"] == "AAPL"]
        print(aapl_intervals.to_string(index=False))

        return stats


