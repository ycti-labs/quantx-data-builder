"""
Price Data Builder

Builds and manages price data for stock universes.
Provides methods to fetch EOD prices, read universe membership,
and persist data to Hive-style partitioned Parquet files.

Implementation uses Tiingo API via the official tiingo-python library.
"""

import hashlib
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from tiingo import TiingoClient

logger = logging.getLogger(__name__)


class PriceDataBuilder:
    """
    Price data builder for stock price databases

    Builds comprehensive price data including:
    - End-of-day price data (OHLCV) for individual symbols
    - Historical price data with date range
    - Automatic retry and rate limiting
    - Hive-style partitioned Parquet storage

    Uses UniverseBuilder for universe membership queries.
    Implementation agnostic - currently uses Tiingo API.
    """

    def __init__(
        self,
        api_key: str,
        data_root: str = "data/curated",
        universe_builder=None
    ):
        """
        Initialize market data builder

        Args:
            api_key: API key for data source (currently Tiingo)
            data_root: Root directory for data storage (default: data/curated)
            universe_builder: UniverseBuilder instance (optional, will create if not provided)
        """
        self.api_key = api_key
        self.data_root = Path(data_root)

        # Initialize or use provided UniverseBuilder
        if universe_builder is None:
            # Lazy import to avoid circular dependency
            from ..universe.universe_manager import Universe
            self.universe_builder = Universe(data_root=str(data_root))
        else:
            self.universe_builder = universe_builder

        # Initialize TiingoClient
        config = {
            'api_key': api_key,
            'session': True  # Use session for connection pooling
        }
        self.client = TiingoClient(config)

        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _generate_ticker_id(exchange: str, symbol: str) -> int:
        """
        Generate deterministic ticker_id from exchange and symbol

        Uses 32-bit hash for consistent IDs across runs

        Args:
            exchange: Exchange code (e.g., 'us', 'hk')
            symbol: Ticker symbol (e.g., 'AAPL')

        Returns:
            32-bit integer ticker_id
        """
        key = f"{exchange.upper()}:{symbol.upper()}"
        hash_obj = hashlib.sha256(key.encode('utf-8'))
        # Use first 4 bytes for 32-bit int
        return int.from_bytes(hash_obj.digest()[:4], byteorder='big', signed=False)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def fetch_eod(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch end-of-day price data for a symbol using tiingo-python library

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional, defaults to today)

        Returns:
            DataFrame with columns: date, open, high, low, close, volume,
                                   adjClose, adjHigh, adjLow, adjOpen, adjVolume,
                                   divCash, splitFactor

        Raises:
            RuntimeError: On API errors
            ValueError: On invalid symbol or data
        """
        try:
            # Use tiingo-python library to fetch data
            df = self.client.get_dataframe(
                symbol,
                startDate=start_date,
                endDate=end_date,
                frequency='daily'
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

            self.logger.info(f"✅ Fetched {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching {symbol}: {e}")
            # Return empty DataFrame on error instead of raising
            # (unless it's a permanent error like invalid symbol)
            if "404" in str(e) or "not found" in str(e).lower():
                self.logger.warning(f"Symbol not found: {symbol}")
                return pd.DataFrame()
            raise

    def fetch_multiple(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skip_errors: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch EOD data for multiple symbols

        Args:
            symbols: List of ticker symbols
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            skip_errors: If True, skip symbols that error; if False, raise on first error

        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        results = {}

        for symbol in symbols:
            try:
                df = self.fetch_eod(symbol, start_date, end_date)
                if not df.empty:
                    results[symbol] = df
                else:
                    self.logger.warning(f"Skipped {symbol} (no data)")

            except Exception as e:
                self.logger.error(f"Error fetching {symbol}: {e}")
                if not skip_errors:
                    raise

        return results

    def get_universe_members(
        self,
        universe: str,
        as_of_date: Optional[str] = None
    ) -> List[str]:
        """
        Get universe members as of a specific date

        Delegates to UniverseBuilder for membership queries.

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            as_of_date: Date in 'YYYY-MM-DD' format (defaults to today)

        Returns:
            List of ticker symbols that were members on the given date
        """
        return self.universe_builder.get_members(universe, as_of_date)

    def get_current_members(self, universe: str) -> List[str]:
        """
        Get current universe members (as of today)

        Delegates to UniverseBuilder for membership queries.

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')

        Returns:
            List of current member ticker symbols
        """
        return self.universe_builder.get_current_members(universe)

    def get_all_historical_members(
        self,
        universe: str,
        period_start: str,
        period_end: str
    ) -> List[str]:
        """
        Get ALL stocks that were members at ANY point during the period.

        Delegates to UniverseBuilder for membership queries.
        This eliminates survivorship bias by including:
        - Current members
        - Removed members (delisted, acquired, merged)
        - Stocks that joined during the period

        Example: For S&P 500 from 2020-2024, this returns ~520+ symbols
        (not just the current 500) because companies get added/removed.

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            period_start: Start date in 'YYYY-MM-DD' format
            period_end: End date in 'YYYY-MM-DD' format

        Returns:
            List of all ticker symbols that were members during any part of the period
        """
        return self.universe_builder.get_all_historical_members(
            universe, period_start, period_end
        )

    def fetch_universe_data(
        self,
        universe: str,
        start_date: str,
        end_date: Optional[str] = None,
        as_of_date: Optional[str] = None,
        skip_errors: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for all members of a universe

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            start_date: Start date for price data
            end_date: End date for price data (defaults to today)
            as_of_date: Date for universe membership (defaults to today)
            skip_errors: Skip symbols that error

        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        # Get universe members
        symbols = self.get_universe_members(universe, as_of_date)

        if not symbols:
            self.logger.warning(f"No members found for {universe}")
            return {}

        self.logger.info(
            f"Fetching data for {len(symbols)} symbols in {universe}"
        )

        # Fetch data for all symbols
        return self.fetch_multiple(symbols, start_date, end_date, skip_errors)

    def fetch_complete_universe_history(
        self,
        universe: str,
        start_date: str,
        end_date: Optional[str] = None,
        exchange: str = "us",
        currency: str = "USD",
        skip_errors: bool = True,
        save_to_parquet: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch COMPLETE historical data for a universe (no survivorship bias).

        This fetches data for ALL stocks that were EVER members during the period,
        including:
        - Current members
        - Removed members (delisted, acquired, merged)
        - Stocks that joined/left during the period

        Perfect for building a complete historical database for backtesting.

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            start_date: Start date for price data in 'YYYY-MM-DD' format
            end_date: End date for price data (defaults to today)
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')
            skip_errors: If True, skip symbols that fail; if False, raise on error
            save_to_parquet: If True, automatically save to Parquet files

        Returns:
            Dictionary mapping symbol -> DataFrame with price data

        Example:
            # Build complete S&P 500 database from 2020-2024
            # This will fetch ~520+ stocks (not just current 500)
            builder = PriceDataBuilder(api_key=api_key)
            data = builder.fetch_complete_universe_history(
                universe='sp500',
                start_date='2020-01-01',
                end_date='2024-12-31'
            )
        """
        # Use UniverseBuilder to get ALL historical members during this period
        symbols = self.universe_builder.get_all_historical_members(
            universe,
            start_date,
            end_date or datetime.now().strftime('%Y-%m-%d')
        )

        if not symbols:
            self.logger.warning(f"No historical members found for {universe}")
            return {}

        self.logger.info(
            f"Building complete historical database for {universe}: "
            f"{len(symbols)} total symbols (current + historical)"
        )

        # Fetch data
        if save_to_parquet:
            # Fetch and save to Parquet
            results = self.fetch_and_save_multiple(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                exchange=exchange,
                currency=currency,
                skip_errors=skip_errors
            )
            # Convert from (df, paths) tuples to just df
            return {symbol: df for symbol, (df, _) in results.items()}
        else:
            # Just fetch without saving
            return self.fetch_multiple(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                skip_errors=skip_errors
            )

    def fetch_universe_missing_data(
        self,
        universe: str,
        start_date: str,
        end_date: str,
        exchange: str = "us",
        currency: str = "USD",
        tolerance_days: int = 2,
        skip_errors: bool = True
    ) -> Dict[str, Dict]:
        """
        Intelligently fetch missing data for an entire universe

        Checks each historical member and fetches only what's missing:
        - Complete data (within tolerance): Skip
        - Partial data: Fetch missing start/end portions
        - No data: Fetch entire period

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            start_date: Required start date in 'YYYY-MM-DD' format
            end_date: Required end date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')
            tolerance_days: Ignore gaps of this many days or less (default: 2)
            skip_errors: If True, skip symbols that fail

        Returns:
            Dictionary mapping symbol -> result dict with:
                - status: 'complete', 'partial', 'missing', 'fetched', 'error'
                - message: Status message
                - fetched_rows: Number of rows fetched (if any)
                - saved_paths: List of saved file paths (if any)

        Example:
            builder = PriceDataBuilder(api_key=api_key)
            results = builder.fetch_universe_missing_data(
                universe='sp500',
                start_date='2014-01-01',
                end_date='2024-12-31',
                tolerance_days=2
            )

            # Print summary
            complete = sum(1 for r in results.values() if r['status'] == 'complete')
            fetched = sum(1 for r in results.values() if r['status'] == 'fetched')
            print(f"Complete: {complete}, Fetched: {fetched}")
        """
        # Use UniverseBuilder to get all historical members for the universe
        symbols = self.universe_builder.get_all_historical_members(
            universe, start_date, end_date
        )

        if not symbols:
            self.logger.warning(f"No historical members found for {universe}")
            return {}

        # Get membership intervals to determine required period per ticker
        intervals_path = (
            f"data/curated/membership/universe={universe.lower()}/"
            f"mode=intervals/{universe.lower()}_membership_intervals.parquet"
        )

        try:
            membership_df = pd.read_parquet(intervals_path)
            membership_df['start_date'] = pd.to_datetime(membership_df['start_date'])
            membership_df['end_date'] = pd.to_datetime(membership_df['end_date'])
        except Exception as e:
            self.logger.error(f"Error loading membership data: {e}")
            return {}

        research_start = pd.to_datetime(start_date)
        research_end = pd.to_datetime(end_date)

        results = {}
        total = len(symbols)

        self.logger.info(
            f"Checking missing data for {total} symbols in {universe} "
            f"(period: {start_date} to {end_date}, tolerance: ±{tolerance_days} days)"
        )

        for idx, symbol in enumerate(symbols, 1):
            try:
                # Get membership period for this symbol
                ticker_membership = membership_df[membership_df['ticker'] == symbol]

                if ticker_membership.empty:
                    self.logger.warning(f"[{idx}/{total}] {symbol}: No membership record found")
                    results[symbol] = {
                        'status': 'error',
                        'message': 'No membership record',
                        'fetched_rows': 0,
                        'saved_paths': []
                    }
                    continue

                # Get required period (intersection of membership and research period)
                # For simplicity, use the first membership period
                membership_start = max(
                    ticker_membership['start_date'].iloc[0],
                    research_start
                )
                membership_end = min(
                    ticker_membership['end_date'].iloc[0],
                    research_end
                )

                required_start_str = membership_start.strftime('%Y-%m-%d')
                required_end_str = membership_end.strftime('%Y-%m-%d')

                # Fetch missing data
                df, paths = self.fetch_missing_data(
                    symbol=symbol,
                    required_start=required_start_str,
                    required_end=required_end_str,
                    exchange=exchange,
                    currency=currency,
                    tolerance_days=tolerance_days
                )

                if df.empty and not paths:
                    # Already complete
                    results[symbol] = {
                        'status': 'complete',
                        'message': f'Already complete (±{tolerance_days} days)',
                        'fetched_rows': 0,
                        'saved_paths': []
                    }
                else:
                    # Fetched new data
                    results[symbol] = {
                        'status': 'fetched',
                        'message': f'Fetched {len(df)} rows',
                        'fetched_rows': len(df),
                        'saved_paths': [str(p) for p in paths]
                    }

                # Progress update every 50 symbols
                if idx % 50 == 0 or idx == total:
                    complete_count = sum(1 for r in results.values() if r['status'] == 'complete')
                    fetched_count = sum(1 for r in results.values() if r['status'] == 'fetched')
                    self.logger.info(
                        f"Progress: {idx}/{total} | Complete: {complete_count} | Fetched: {fetched_count}"
                    )

            except Exception as e:
                self.logger.error(f"[{idx}/{total}] Error processing {symbol}: {e}")
                results[symbol] = {
                    'status': 'error',
                    'message': str(e),
                    'fetched_rows': 0,
                    'saved_paths': []
                }
                if not skip_errors:
                    raise

        # Final summary
        complete_count = sum(1 for r in results.values() if r['status'] == 'complete')
        fetched_count = sum(1 for r in results.values() if r['status'] == 'fetched')
        error_count = sum(1 for r in results.values() if r['status'] == 'error')

        self.logger.info(
            f"✅ Universe data check complete: "
            f"Complete: {complete_count}/{total}, "
            f"Fetched: {fetched_count}/{total}, "
            f"Errors: {error_count}/{total}"
        )

        return results

    def test_connection(self) -> bool:
        """
        Test connection to Tiingo API

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to fetch a single day of data for AAPL
            df = self.fetch_eod('AAPL', start_date='2024-01-02', end_date='2024-01-02')
            if not df.empty:
                self.logger.info("✅ Tiingo API connection successful")
                return True
            else:
                self.logger.error("❌ Tiingo API returned no data")
                return False
        except Exception as e:
            self.logger.error(f"❌ Tiingo API connection failed: {e}")
            return False

    def _prepare_price_dataframe(
        self,
        df: pd.DataFrame,
        symbol: str,
        exchange: str = "us",
        currency: str = "USD"
    ) -> pd.DataFrame:
        """
        Transform Tiingo data to match the canonical price schema

        Args:
            df: Raw DataFrame from Tiingo API
            symbol: Ticker symbol
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')

        Returns:
            DataFrame with canonical schema
        """
        if df.empty:
            return df

        # Generate ticker_id
        ticker_id = self._generate_ticker_id(exchange, symbol)

        # Create canonical DataFrame
        result = pd.DataFrame({
            'date': pd.to_datetime(df['date']).dt.date,
            'ticker_id': ticker_id,
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
            'exchange': exchange,
            'currency': currency,
            'freq': 'daily',
        })

        # Add year column for partitioning
        result['year'] = pd.to_datetime(result['date']).dt.year

        return result

    def save_price_data(
        self,
        df: pd.DataFrame,
        symbol: str,
        exchange: str = "us",
        currency: str = "USD",
        adjusted: bool = True
    ) -> List[Path]:
        """
        Save price data to Hive-style partitioned Parquet files

        Structure: data/curated/prices/exchange={exchange}/ticker={symbol}/freq=daily/adj={adj}/year={year}/part-000.parquet

        Args:
            df: DataFrame with price data (from fetch_eod)
            symbol: Ticker symbol
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')
            adjusted: Whether this is adjusted data (default: True)

        Returns:
            List of paths where files were saved
        """
        if df.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return []

        # Transform to canonical schema
        canonical_df = self._prepare_price_dataframe(df, symbol, exchange, currency)

        # Group by year and save separately
        saved_paths = []
        adj_str = "true" if adjusted else "false"

        for year, year_df in canonical_df.groupby('year'):
            # Build path following Hive partitioning
            dir_path = (
                self.data_root /
                "prices" /
                f"exchange={exchange}" /
                f"ticker={symbol}" /
                "freq=daily" /
                f"adj={adj_str}" /
                f"year={year}"
            )
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
                f"✅ Saved {len(year_df)} rows to {file_path.relative_to(self.data_root.parent)}"
            )

        return saved_paths

    def fetch_and_save(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = "us",
        currency: str = "USD"
    ) -> Tuple[pd.DataFrame, List[Path]]:
        """
        Fetch data from Tiingo and save to Parquet files

        Args:
            symbol: Ticker symbol
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')

        Returns:
            Tuple of (DataFrame with fetched data, List of saved file paths)
        """
        # Fetch data
        df = self.fetch_eod(symbol, start_date, end_date)

        if df.empty:
            return df, []

        # Save to Parquet
        saved_paths = self.save_price_data(df, symbol, exchange, currency)

        return df, saved_paths

    def fetch_and_save_multiple(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = "us",
        currency: str = "USD",
        skip_errors: bool = True
    ) -> Dict[str, Tuple[pd.DataFrame, List[Path]]]:
        """
        Fetch and save data for multiple symbols

        Args:
            symbols: List of ticker symbols
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')
            skip_errors: If True, skip symbols that error

        Returns:
            Dictionary mapping symbol -> (DataFrame, saved_paths)
        """
        results = {}

        for symbol in symbols:
            try:
                df, paths = self.fetch_and_save(
                    symbol, start_date, end_date, exchange, currency
                )
                if not df.empty:
                    results[symbol] = (df, paths)
                else:
                    self.logger.warning(f"Skipped {symbol} (no data)")

            except Exception as e:
                self.logger.error(f"Error fetching/saving {symbol}: {e}")
                if not skip_errors:
                    raise

        return results

    def get_existing_date_range(
        self,
        symbol: str,
        exchange: str = "us",
        adjusted: bool = True
    ) -> Optional[Tuple[date, date]]:
        """
        Get the date range of existing data for a symbol

        Args:
            symbol: Ticker symbol
            exchange: Exchange code (default: 'us')
            adjusted: Whether to check adjusted data (default: True)

        Returns:
            Tuple of (min_date, max_date) or None if no data exists
        """
        adj_str = "true" if adjusted else "false"
        ticker_path = (
            self.data_root /
            "prices" /
            f"exchange={exchange}" /
            f"ticker={symbol}" /
            "freq=daily" /
            f"adj={adj_str}"
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

    def check_missing_data(
        self,
        symbol: str,
        required_start: str,
        required_end: str,
        exchange: str = "us",
        tolerance_days: int = 2
    ) -> Dict:
        """
        Check what data is missing for a symbol during a required period

        Args:
            symbol: Ticker symbol
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            tolerance_days: Ignore gaps of this many days or less (default: 2)

        Returns:
            Dictionary with:
                - status: 'complete', 'partial', or 'missing'
                - actual_start: Actual start date (or None)
                - actual_end: Actual end date (or None)
                - missing_start_days: Days missing at start
                - missing_end_days: Days missing at end
                - fetch_start: Recommended start date for fetching
                - fetch_end: Recommended end date for fetching
        """
        req_start = pd.to_datetime(required_start).date()
        req_end = pd.to_datetime(required_end).date()

        # Get existing date range
        existing_range = self.get_existing_date_range(symbol, exchange)

        if existing_range is None:
            # No data exists - need to fetch entire period
            return {
                'status': 'missing',
                'actual_start': None,
                'actual_end': None,
                'missing_start_days': (req_end - req_start).days,
                'missing_end_days': 0,
                'fetch_start': required_start,
                'fetch_end': required_end
            }

        actual_start, actual_end = existing_range

        # Calculate gaps
        start_gap_days = max(0, (actual_start - req_start).days)
        end_gap_days = max(0, (req_end - actual_end).days)

        # Determine status
        if start_gap_days <= tolerance_days and end_gap_days <= tolerance_days:
            status = 'complete'
            fetch_start = None
            fetch_end = None
        elif start_gap_days > 0 or end_gap_days > 0:
            status = 'partial'
            # Determine what needs to be fetched
            fetch_start = required_start if start_gap_days > tolerance_days else None
            fetch_end = required_end if end_gap_days > tolerance_days else None
        else:
            status = 'complete'
            fetch_start = None
            fetch_end = None

        return {
            'status': status,
            'actual_start': actual_start,
            'actual_end': actual_end,
            'missing_start_days': start_gap_days,
            'missing_end_days': end_gap_days,
            'fetch_start': fetch_start,
            'fetch_end': fetch_end
        }

    def fetch_missing_data(
        self,
        symbol: str,
        required_start: str,
        required_end: str,
        exchange: str = "us",
        currency: str = "USD",
        tolerance_days: int = 2,
        force: bool = False
    ) -> Tuple[pd.DataFrame, List[Path]]:
        """
        Intelligently fetch only missing data for a symbol

        Checks existing data and fetches only what's needed:
        - If no data exists, fetch entire period
        - If data is partial, fetch missing start/end portions
        - If data is complete (within tolerance), skip fetching

        Args:
            symbol: Ticker symbol
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            currency: Currency code (default: 'USD')
            tolerance_days: Ignore gaps of this many days or less (default: 2)
            force: If True, fetch entire period regardless of existing data

        Returns:
            Tuple of (DataFrame with fetched data, List of saved file paths)
        """
        if force:
            self.logger.info(f"Force fetch {symbol} for entire period {required_start} to {required_end}")
            return self.fetch_and_save(symbol, required_start, required_end, exchange, currency)

        # Check what's missing
        check = self.check_missing_data(symbol, required_start, required_end, exchange, tolerance_days)

        if check['status'] == 'complete':
            self.logger.info(f"✅ {symbol} already has complete data (±{tolerance_days} days)")
            return pd.DataFrame(), []

        if check['status'] == 'missing':
            self.logger.info(f"📥 {symbol} has no data - fetching entire period")
            return self.fetch_and_save(symbol, required_start, required_end, exchange, currency)

        # Partial data - fetch what's missing
        self.logger.info(
            f"⚠️  {symbol} has partial data - missing {check['missing_start_days']}d at start, "
            f"{check['missing_end_days']}d at end"
        )

        all_fetched = []
        all_paths = []

        # Fetch missing start portion
        if check['fetch_start'] and check['missing_start_days'] > tolerance_days:
            fetch_end_start = (check['actual_start'] - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            self.logger.info(f"  📥 Fetching start: {check['fetch_start']} to {fetch_end_start}")
            df_start, paths_start = self.fetch_and_save(
                symbol, check['fetch_start'], fetch_end_start, exchange, currency
            )
            if not df_start.empty:
                all_fetched.append(df_start)
                all_paths.extend(paths_start)

        # Fetch missing end portion
        if check['fetch_end'] and check['missing_end_days'] > tolerance_days:
            fetch_start_end = (check['actual_end'] + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            self.logger.info(f"  📥 Fetching end: {fetch_start_end} to {check['fetch_end']}")
            df_end, paths_end = self.fetch_and_save(
                symbol, fetch_start_end, check['fetch_end'], exchange, currency
            )
            if not df_end.empty:
                all_fetched.append(df_end)
                all_paths.extend(paths_end)

        # Combine results
        if all_fetched:
            combined_df = pd.concat(all_fetched, ignore_index=True)
            return combined_df, all_paths
        else:
            return pd.DataFrame(), all_paths

    def load_price_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: str = "us",
        adjusted: bool = True
    ) -> pd.DataFrame:
        """
        Load price data from saved Parquet files

        Args:
            symbol: Ticker symbol
            start_date: Start date filter (optional)
            end_date: End date filter (optional)
            exchange: Exchange code (default: 'us')
            adjusted: Whether to load adjusted data (default: True)

        Returns:
            DataFrame with price data
        """
        adj_str = "true" if adjusted else "false"

        # Path to ticker directory
        ticker_path = (
            self.data_root /
            "prices" /
            f"exchange={exchange}" /
            f"ticker={symbol}" /
            "freq=daily" /
            f"adj={adj_str}"
        )

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
