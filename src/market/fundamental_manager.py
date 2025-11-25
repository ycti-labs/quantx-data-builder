"""
Fundamental Data Manager

Manages fundamental data for stock universes - fetching, storing, and reading.
Provides methods to fetch financial statements, ratios, and metrics,
and persist data to Hive-style partitioned Parquet files.

Implementation uses Tiingo Fundamentals API via the official tiingo-python library.
"""

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from tiingo import TiingoClient

from universe import Universe

logger = logging.getLogger(__name__)


class FundamentalManager:
    """
    Fundamental data manager for stock fundamental databases

    Manages the full lifecycle of fundamental data including:
    - Fetching: Financial statements (income, balance sheet, cash flow) and metrics
    - Storing: Hive-style partitioned Parquet storage with automatic retry
    - Reading: Load and query saved fundamental data
    - Validation: Check coverage and detect missing data

    Uses Universe for universe membership queries.
    Implementation agnostic - currently uses Tiingo Fundamentals API.
    """

    def __init__(
        self,
        tiingo: TiingoClient,
        universe: Universe,
    ):
        """
        Initialize fundamental data manager

        Args:
            tiingo: TiingoClient instance for data source
            universe: Universe instance for membership and data root
        """
        self.tiingo = tiingo
        self.universe = universe
        self.logger = logging.getLogger(__name__)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def fetch_fundamentals(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        save: bool = True,
    ) -> Tuple[pd.DataFrame, List[Path]]:
        """
        Fetch fundamental data for a symbol using tiingo-python library

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional, defaults to today)
            save: If True, automatically save to Parquet files (default: True)

        Returns:
            Tuple of (DataFrame with fundamental data, List of saved file paths)
            If save=False, returns (DataFrame, empty list)
            DataFrame columns: date, ticker, statement_type, field, value, period_type, etc.

        Raises:
            RuntimeError: On API errors
            ValueError: On invalid symbol or data
        """
        try:
            # Use tiingo-python library to fetch fundamentals
            # get_fundamentals_statements returns financial statements data
            df = self.tiingo.get_fundamentals_statements(
                symbol, startDate=start_date, endDate=end_date
            )

            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                self.logger.warning(f"No fundamental data returned for {symbol}")
                return pd.DataFrame(), []

            # If result is not a DataFrame, convert it
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # Add ticker symbol column if not present
            if "ticker" not in df.columns:
                df["ticker"] = symbol

            # Parse date if needed
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
            elif "quarter" in df.columns:
                # Handle quarterly data
                df["date"] = pd.to_datetime(df["quarter"]).dt.date

            self.logger.info(f"✅ Fetched {len(df)} fundamental records for {symbol}")

            # Save to Parquet if requested
            if save:
                saved_paths = self.save_fundamental_data(df, symbol)
                return df, saved_paths
            else:
                return df, []

        except Exception as e:
            self.logger.error(f"Error fetching fundamentals for {symbol}: {e}")
            # Return empty DataFrame on error instead of raising
            # (unless it's a permanent error like invalid symbol)
            if "404" in str(e) or "not found" in str(e).lower():
                self.logger.warning(f"Symbol not found: {symbol}")
                return pd.DataFrame(), []
            raise

    def fetch_multiple_fundamentals(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skip_errors: bool = True,
        save: bool = True,
    ):
        """
        Fetch fundamental data for multiple symbols

        Args:
            symbols: List of ticker symbols
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            skip_errors: If True, skip symbols that error; if False, raise on first error
            save: If True, save to Parquet files and return paths (default: True)

        Returns:
            If save=True: Dict[str, Tuple[pd.DataFrame, List[Path]]] - symbol -> (DataFrame, saved_paths)
            If save=False: Dict[str, pd.DataFrame] - symbol -> DataFrame
        """
        results = {}

        for symbol in symbols:
            try:
                df, paths = self.fetch_fundamentals(
                    symbol, start_date, end_date, save=save
                )
                if not df.empty:
                    if save:
                        results[symbol] = (df, paths)
                    else:
                        results[symbol] = df
                else:
                    self.logger.warning(f"Skipped {symbol} (no data)")

            except Exception as e:
                self.logger.error(f"Error fetching {symbol}: {e}")
                if not skip_errors:
                    raise

        return results

    def fetch_universe_fundamentals(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        as_of_date: Optional[str] = None,
        skip_errors: bool = True,
        save: bool = False,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch fundamental data for all members of a universe

        Args:
            start_date: Start date for fundamental data
            end_date: End date for fundamental data (defaults to today)
            as_of_date: Date for universe membership (defaults to today)
            skip_errors: Skip symbols that error
            save: If True, save to Parquet files (default: False)

        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        if self.universe is None:
            self.logger.error("Universe not initialized")
            return {}

        # Get universe members
        symbols = self.universe.get_members(as_of_date)

        if not symbols:
            self.logger.warning(f"No members found for {self.universe.name}")
            return {}

        self.logger.info(
            f"Fetching fundamental data for {len(symbols)} symbols in {self.universe.name}"
        )

        # Fetch data for all symbols (no save by default for this method)
        return self.fetch_multiple_fundamentals(
            symbols, start_date, end_date, skip_errors, save=save
        )

    def save_fundamental_data(
        self, df: pd.DataFrame, symbol: str, statement_type: str = "mixed"
    ) -> List[Path]:
        """
        Save fundamental data to Hive-style partitioned Parquet files

        Storage structure (unified with prices and ESG):
        data/curated/tickers/
            exchange={exchange}/
                ticker={symbol}/
                    fundamentals/
                        statement={statement_type}/
                            year={year}/
                                part-000.parquet

        Args:
            df: DataFrame with fundamental data
            symbol: Ticker symbol
            statement_type: Type of statement ('income', 'balance', 'cashflow', 'metrics', 'mixed')

        Returns:
            List of saved file paths
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {symbol}, skipping save")
            return []

        saved_paths = []

        try:
            # Determine statement type if not specified
            if "statementType" in df.columns:
                # Group by statement type
                for stmt_type in df["statementType"].unique():
                    stmt_df = df[df["statementType"] == stmt_type].copy()
                    paths = self._save_statement_partition(stmt_df, symbol, stmt_type)
                    saved_paths.extend(paths)
            else:
                # Save as mixed
                paths = self._save_statement_partition(df, symbol, statement_type)
                saved_paths.extend(paths)

            return saved_paths

        except Exception as e:
            self.logger.error(f"Error saving fundamental data for {symbol}: {e}")
            raise

    def _save_statement_partition(
        self, df: pd.DataFrame, symbol: str, statement_type: str
    ) -> List[Path]:
        """
        Save fundamental data partition for a specific statement type

        Args:
            df: DataFrame with fundamental data
            symbol: Ticker symbol
            statement_type: Statement type

        Returns:
            List of saved file paths
        """
        saved_paths = []

        # Add metadata columns
        df["exchange"] = self.universe.exchange
        df["ticker"] = symbol

        # Extract year from date
        if "date" in df.columns:
            df["year"] = pd.to_datetime(df["date"]).dt.year
        elif "quarter" in df.columns:
            df["year"] = pd.to_datetime(df["quarter"]).dt.year
        else:
            df["year"] = datetime.now().year

        # Group by year and save
        for year in df["year"].unique():
            year_df = df[df["year"] == year].copy()

            # Build path: tickers/exchange={ex}/ticker={sym}/fundamentals/statement={type}/year={yr}/
            partition_path = (
                self.universe.data_root
                / "curated"
                / "tickers"
                / f"exchange={self.universe.exchange}"
                / f"ticker={symbol}"
                / "fundamentals"
                / f"statement={statement_type}"
                / f"year={year}"
            )

            partition_path.mkdir(parents=True, exist_ok=True)
            file_path = partition_path / "part-000.parquet"

            # If file exists, append (merge)
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                # Combine and remove duplicates
                combined_df = pd.concat([existing_df, year_df], ignore_index=True)

                # Deduplicate based on date and field (if applicable)
                if "date" in combined_df.columns and "dataCode" in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(
                        subset=["date", "dataCode"], keep="last"
                    )
                elif (
                    "quarter" in combined_df.columns
                    and "dataCode" in combined_df.columns
                ):
                    combined_df = combined_df.drop_duplicates(
                        subset=["quarter", "dataCode"], keep="last"
                    )

                combined_df.to_parquet(file_path, compression="snappy", index=False)
                self.logger.info(f"📝 Updated {file_path} ({len(combined_df)} records)")
            else:
                # Save new file
                year_df.to_parquet(file_path, compression="snappy", index=False)
                self.logger.info(f"💾 Saved {file_path} ({len(year_df)} records)")

            saved_paths.append(file_path)

        return saved_paths

    def read_fundamental_data(
        self,
        symbol: str,
        statement_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read saved fundamental data for a symbol

        Args:
            symbol: Ticker symbol
            statement_type: Statement type filter (optional: 'income', 'balance', 'cashflow', 'metrics')
            start_date: Start date filter in 'YYYY-MM-DD' format (optional)
            end_date: End date filter in 'YYYY-MM-DD' format (optional)

        Returns:
            DataFrame with fundamental data
        """
        base_path = (
            self.universe.data_root
            / "curated"
            / "tickers"
            / f"exchange={self.universe.exchange}"
            / f"ticker={symbol}"
            / "fundamentals"
        )

        if not base_path.exists():
            self.logger.warning(f"No fundamental data found for {symbol}")
            return pd.DataFrame()

        all_dfs = []

        # Determine which statement types to read
        if statement_type:
            statement_paths = [base_path / f"statement={statement_type}"]
        else:
            # Read all statement types
            statement_paths = [
                p
                for p in base_path.iterdir()
                if p.is_dir() and p.name.startswith("statement=")
            ]

        for stmt_path in statement_paths:
            if not stmt_path.exists():
                continue

            # Read all year partitions
            for year_path in stmt_path.iterdir():
                if not year_path.is_dir() or not year_path.name.startswith("year="):
                    continue

                parquet_file = year_path / "part-000.parquet"
                if parquet_file.exists():
                    try:
                        df = pd.read_parquet(parquet_file)
                        all_dfs.append(df)
                    except Exception as e:
                        self.logger.error(f"Error reading {parquet_file}: {e}")

        if not all_dfs:
            return pd.DataFrame()

        # Combine all data
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Apply date filters if provided
        if start_date or end_date:
            date_col = "date" if "date" in combined_df.columns else "quarter"
            if date_col in combined_df.columns:
                combined_df[date_col] = pd.to_datetime(combined_df[date_col])
                if start_date:
                    combined_df = combined_df[
                        combined_df[date_col] >= pd.to_datetime(start_date)
                    ]
                if end_date:
                    combined_df = combined_df[
                        combined_df[date_col] <= pd.to_datetime(end_date)
                    ]

        return combined_df

    def check_missing_data(
        self, symbol: str, required_start: str, required_end: str
    ) -> Dict:
        """
        Check if fundamental data exists for a symbol and identify gaps

        Args:
            symbol: Ticker symbol
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format

        Returns:
            Dictionary with status and gap information:
            {
                'status': 'complete' | 'partial' | 'missing',
                'actual_start': date or None,
                'actual_end': date or None,
                'has_data': bool
            }
        """
        df = self.read_fundamental_data(symbol)

        if df.empty:
            return {
                "status": "missing",
                "actual_start": None,
                "actual_end": None,
                "has_data": False,
            }

        # Get date range
        date_col = "date" if "date" in df.columns else "quarter"
        if date_col not in df.columns:
            return {
                "status": "missing",
                "actual_start": None,
                "actual_end": None,
                "has_data": False,
            }

        df[date_col] = pd.to_datetime(df[date_col])
        actual_start = df[date_col].min()
        actual_end = df[date_col].max()

        req_start = pd.to_datetime(required_start)
        req_end = pd.to_datetime(required_end)

        # For fundamentals, we check if we have any data in the period
        # (fundamentals are quarterly/annual, not daily)
        has_overlap = not (actual_end < req_start or actual_start > req_end)

        if has_overlap:
            status = "complete"  # We have some data in the period
        else:
            status = "partial"  # Data exists but not in required period

        return {
            "status": status,
            "actual_start": actual_start.date(),
            "actual_end": actual_end.date(),
            "has_data": True,
        }

    def fetch_metrics(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        save: bool = True,
    ) -> Tuple[pd.DataFrame, List[Path]]:
        """
        Fetch daily fundamental metrics for a symbol (P/E ratio, market cap, etc.)

        Uses Tiingo's get_fundamentals_daily() which returns daily fundamental metrics.

        Args:
            symbol: Ticker symbol
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional, defaults to today)
            save: If True, save to Parquet files (default: True)

        Returns:
            Tuple of (DataFrame with metrics, List of saved file paths)
        """
        try:
            # Fetch daily fundamental metrics from Tiingo
            df = self.tiingo.get_fundamentals_daily(
                symbol, startDate=start_date, endDate=end_date
            )

            if df is None or (isinstance(df, pd.DataFrame) and df.empty):
                self.logger.warning(f"No metrics data returned for {symbol}")
                return pd.DataFrame(), []

            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)

            # Add ticker symbol
            if "ticker" not in df.columns:
                df["ticker"] = symbol

            # Parse date if needed
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date

            self.logger.info(f"✅ Fetched {len(df)} metric records for {symbol}")

            if save:
                saved_paths = self.save_fundamental_data(
                    df, symbol, statement_type="metrics"
                )
                return df, saved_paths
            else:
                return df, []

        except Exception as e:
            self.logger.error(f"Error fetching metrics for {symbol}: {e}")
            if "404" in str(e) or "not found" in str(e).lower():
                self.logger.warning(f"Metrics not found: {symbol}")
                return pd.DataFrame(), []
            raise
