"""
Price Data Manager

Manages price data for stock universes - fetching, storing, and reading.
Provides methods to fetch EOD prices, read universe membership,
and persist data to Hive-style partitioned Parquet files.

Implementation uses Tiingo API via the official tiingo-python library.
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

TOLERANCE_DAYS_DEFAULT = 2

class PriceManager:
    """
    Price data manager for stock price databases

    Manages the full lifecycle of price data including:
    - Fetching: End-of-day price data (OHLCV) from external sources
    - Storing: Hive-style partitioned Parquet storage with automatic retry
    - Reading: Load and query saved price data
    - Validation: Check coverage and detect missing data

    Uses Universe for universe membership queries.
    Implementation agnostic - currently uses Tiingo API.
    """

    def __init__(
        self,
        tiingo: TiingoClient,
        universe: Universe,
    ):
        """
        Initialize price data manager

        Args:
            tiingo: TiingoClient instance for data source
            universe: Universe instance for membership and data root
        """
        self.tiingo = tiingo
        self.universe = universe
        self.logger = logging.getLogger(__name__)

    def get_missing_data_checker(self):
        """
        Create a MissingDataChecker instance for comprehensive data validation

        Returns:
            MissingDataChecker: Checker instance configured with this PriceManager

        Example:
            >>> checker = price_mgr.get_missing_data_checker()
            >>> result = checker.check_missing_data('AAPL', '2020-01-01', '2024-12-31')
        """
        from programs.check_missing_data import MissingDataChecker
        return MissingDataChecker(self)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_eod(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "daily",
        save: bool = True
    ) -> pd.DataFrame:
        """
        Fetch end-of-day price data for a symbol using tiingo-python library

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional, defaults to today)
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            save: If True, automatically save to Parquet files (default: True)

        Returns:
            Tuple of (DataFrame with price data, List of saved file paths)
            If save=False, returns (DataFrame, empty list)
            DataFrame columns: date, open, high, low, close, volume,
                              adjClose, adjHigh, adjLow, adjOpen, adjVolume,
                              divCash, splitFactor

        Raises:
            RuntimeError: On API errors
            ValueError: On invalid symbol or data
        """
        try:
            # Use tiingo-python library to fetch data
            df = self.tiingo.get_dataframe(
                symbol,
                startDate=start_date,
                endDate=end_date,
                frequency=frequency
            )

            if df.empty:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()

            # Reset index to make date a column
            df = df.reset_index()

            # Rename columns to match our expected schema
            # Tiingo returns: date, close, high, low, open, volume, adjClose, adjHigh, adjLow, adjOpen, adjVolume, divCash, splitFactor
            # This already matches our schema!

            # Convert date to date type (not datetime)
            df['date'] = pd.to_datetime(df['date']).dt.date

            # Check data completeness
            actual_start = df['date'].min()
            actual_end = df['date'].max()
            num_rows = len(df)

            # Build completeness report
            if start_date and end_date:
                req_start = pd.to_datetime(start_date).date()
                req_end = pd.to_datetime(end_date).date()

                # Calculate expected trading days (rough estimate: ~252 trading days per year)
                total_days = (req_end - req_start).days
                expected_rows = int(total_days * 252 / 365) if total_days > 0 else 0

                # Calculate gaps at start and end
                start_gap_days = (actual_start - req_start).days if actual_start > req_start else 0
                end_gap_days = (req_end - actual_end).days if actual_end < req_end else 0

                # Check if we got the requested range (with tolerance)
                is_complete = (start_gap_days <= TOLERANCE_DAYS_DEFAULT) and (end_gap_days <= TOLERANCE_DAYS_DEFAULT)

                if is_complete:
                    completeness = "COMPLETE"
                    coverage_pct = 100.0 if expected_rows == 0 else min(100.0, (num_rows / expected_rows) * 100)
                    tolerance_note = f" (±{TOLERANCE_DAYS_DEFAULT}d)" if start_gap_days > 0 or end_gap_days > 0 else ""
                    self.logger.info(
                        f"✅ {symbol}: {num_rows} rows | {completeness}{tolerance_note} "
                        f"({actual_start} to {actual_end}) | Coverage: {coverage_pct:.1f}%"
                    )
                else:
                    completeness = "PARTIAL"
                    self.logger.warning(
                        f"⚠️  {symbol}: {num_rows} rows | {completeness} "
                        f"({actual_start} to {actual_end}) | "
                        f"Missing: {start_gap_days}d at start, {end_gap_days}d at end "
                        f"(tolerance: ±{TOLERANCE_DAYS_DEFAULT}d)"
                    )
            else:
                # No date range specified, just report what we got
                self.logger.info(
                    f"✅ {symbol}: {num_rows} rows ({actual_start} to {actual_end})"
                )

            # Save to Parquet if requested
            if save:
                saved_paths = self.save_price_data(df, symbol)

            return df

        except Exception as e:
            self.logger.error(f"Error fetching {symbol}: {e}")
            # Return empty DataFrame on error instead of raising
            # (unless it's a permanent error like invalid symbol)
            if "404" in str(e) or "not found" in str(e).lower():
                self.logger.warning(f"Symbol not found: {symbol}")
                return pd.DataFrame()
            raise

    def fetch_multiple_eod(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "daily",
        skip_errors: bool = True,
        save: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch EOD data for multiple symbols

        Args:
            symbols: List of ticker symbols
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            frequency: Data frequency ('daily', 'weekly', etc.)
            skip_errors: If True, skip symbols that error; if False, raise on first error
            save: If True, save to Parquet files and return paths (default: True)

        Returns:
            If save=True: Dict[str, Tuple[pd.DataFrame, List[Path]]] - symbol -> (DataFrame, saved_paths)
            If save=False: Dict[str, pd.DataFrame] - symbol -> DataFrame
        """
        results = {}

        for symbol in symbols:
            try:
                df = self.fetch_eod(symbol, start_date, end_date, frequency=frequency, save=save)
                if not df.empty:
                    results[symbol] = df
                else:
                    self.logger.warning(f"Skipped {symbol} (no data)")

            except Exception as e:
                self.logger.error(f"Error fetching {symbol}: {e}")
                if not skip_errors:
                    raise

        return results

    def fetch_universe_eod(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        as_of_date: Optional[str] = None,
        frequency: str = "daily",
        scope: str = "current",
        skip_errors: bool = True,
        save: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for all members of a universe

        Args:
            start_date: Start date for price data
            end_date: End date for price data (defaults to today)
            as_of_date: Date for universe membership (defaults to today)
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            scope: Scope of membership ('current', 'historical')
            skip_errors: Skip symbols that error
            save: If True, save to Parquet files and return paths (default: True)
        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        if self.universe is None:
            self.logger.error("Universe not initialized")
            return {}

        # Get universe members
        if scope == "historical":
            symbols = self.universe.get_all_historical_members(
                start_date,
                end_date or datetime.now().strftime('%Y-%m-%d')
            )
        else:
            symbols = self.universe.get_members(as_of_date)

        if not symbols:
            self.logger.warning(f"No members found for {self.universe.name}")
            return {}

        self.logger.info(
            f"Fetching data for {len(symbols)} symbols in {self.universe.name}"
        )

        # Fetch data for all symbols (no save by default for this method)
        results = self.fetch_multiple_eod(symbols, start_date, end_date, frequency=frequency, skip_errors=skip_errors, save=save)

        return results

    def _prepare_price_dataframe(
        self,
        df: pd.DataFrame,
        symbol: str,
        frequency: str = "daily",
    ) -> pd.DataFrame:
        """
        Transform Tiingo data to match the canonical price schema

        Args:
            df: Raw DataFrame from Tiingo API
            symbol: Ticker symbol
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            DataFrame with canonical schema
        """
        if df.empty:
            return df

        # Generate gvkey
        gvkey = self.universe.get_gvkey_for_symbol(symbol)

        # Create canonical DataFrame
        result = pd.DataFrame({
            'gvkey': gvkey,
            'date': pd.to_datetime(df['date']).dt.date,
            'open': df['open'].astype(float),
            'high': df['high'].astype(float),
            'low': df['low'].astype(float),
            'close': df['close'].astype(float),
            'volume': df['volume'].astype('int64'),
            'adj_open': df['adjOpen'].astype(float),
            'adj_high': df['adjHigh'].astype(float),
            'adj_low': df['adjLow'].astype(float),
            'adj_close': df['adjClose'].astype(float),
            'adj_volume': df['adjVolume'].astype('int64'),
            'div_cash': df['divCash'].astype(float),
            'split_factor': df['splitFactor'].astype(float),
            'exchange': self.universe.exchange,
            'currency': self.universe.currency,
            'freq': frequency,
        })

        # Add year column for partitioning
        result['year'] = pd.to_datetime(result['date']).dt.year

        return result

    def save_price_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        frequency: str = "daily",
    ) -> List[Path]:
        """
        Save price data to Hive-style partitioned Parquet files

        Structure:
        data/curated/prices/exchange={exchange}/ticker={symbol}/freq={frequency}/year={year}/part-000.parquet

        Args:
            df: DataFrame with price data (from fetch_eod)
            symbol: Ticker symbol
            frequency: Frequency of the data (default: "daily")

        Returns:
            List of paths where files were saved
        """
        if df.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return []

        # Transform to canonical schema
        canonical_df = self._prepare_price_dataframe(df, symbol)

        # Group by year and save separately
        saved_paths = []

        for year, year_df in canonical_df.groupby('year'):
            # Build path following Hive partitioning
            dir_path = self.universe.get_ticker_prices_path(
                symbol=symbol,
                frequency=frequency
            ) / f"year={year}"

            dir_path.mkdir(parents=True, exist_ok=True)

            file_path = dir_path / "part-000.parquet"

            # Check if file exists and merge if needed
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                # Combine and deduplicate by date
                combined_df = pd.concat([existing_df, year_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
                combined_df = combined_df.sort_values('date')
                year_df = combined_df
                self.logger.info(f"Merged with existing data for {symbol} year {year}")

            # Save to Parquet with snappy compression
            year_df.to_parquet(
                file_path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )

            saved_paths.append(file_path)
            self.logger.info(
                f"✅ Saved {len(year_df)} rows to {file_path.relative_to(self.universe.data_root.parent)}"
            )

        return saved_paths

    def get_existing_date_range(
        self,
        symbol: str,
        frequency: str = "daily"
    ) -> Optional[Tuple[date, date]]:
        """
        Get the date range of existing data for a symbol

        Args:
            symbol: Ticker symbol
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            Tuple of (min_date, max_date) or None if no data exists
        """
        ticker_path = self.universe.get_ticker_prices_path(
            symbol=symbol,
            frequency=frequency
        )

        if not ticker_path.exists():
            return None

        # Collect dates from all year partitions
        all_dates = []
        for year_dir in ticker_path.glob("year=*"):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                try:
                    df = pd.read_parquet(parquet_file, columns=['date'])
                    if not df.empty:
                        all_dates.extend(df['date'].tolist())
                except Exception as e:
                    self.logger.warning(f"Error reading {parquet_file}: {e}")
                    continue

        if not all_dates:
            return None

        return min(all_dates), max(all_dates)

    def load_price_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "daily"
    ) -> pd.DataFrame:
        """
        Load price data from saved Parquet files

        Args:
            symbol: Ticker symbol
            start_date: Start date filter (optional)
            end_date: End date filter (optional)
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            DataFrame with price data
        """

        # Path to ticker directory
        ticker_path = self.universe.get_ticker_prices_path(symbol=symbol, frequency=frequency)

        if not ticker_path.exists():
            self.logger.warning(f"No data found for {symbol} at {ticker_path}")
            return pd.DataFrame()

        # Load all year partitions
        all_data = []
        for year_dir in ticker_path.glob("year=*"):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                all_data.append(df)

        if not all_data:
            self.logger.warning(f"No Parquet files found for {symbol}")
            return pd.DataFrame()

        # Combine all years
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values('date')

        # Apply date filters
        if start_date:
            start = pd.to_datetime(start_date).date()
            result = result[result['date'] >= start]
        if end_date:
            end = pd.to_datetime(end_date).date()
            result = result[result['date'] <= end]

        self.logger.info(f"Loaded {len(result)} rows for {symbol}")
        return result
