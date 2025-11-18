"""
Universe builder - orchestrates universe construction from various sources
Supports multiple market universes with configurable data sources
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class Universe:
    """
    Main class to orchestrate universe building from configured sources
    Handles multiple phases and dependencies between universes
    Also provides membership query functionality
    """

    def __init__(self, config_path: Optional[str] = None, data_root: str = "data/curated"):
        """
        Initialize universe builder

        Args:
            config_path: Path to universe configuration YAML file (optional for membership queries)
            data_root: Root directory for data storage (default: data/curated)
        """
        # Config loading is optional - needed for building universes, not for querying membership
        if config_path:
            try:
                from ..market_data.config_loader import FetcherConfig
                self.config = FetcherConfig(config_path)
            except Exception as e:
                logger.warning(f"Could not load config from {config_path}: {e}")
                self.config = None
        else:
            self.config = None

        self.data_root = Path(data_root)

    def write_snappy_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
        """
        Write DataFrame to Snappy-compressed Parquet file.

        Creates parent directories if they don't exist.

        Args:
            df: DataFrame to write
            output_path: Path object for output file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, compression="snappy", index=False, engine="pyarrow")
        logger.info(f"✅ Wrote {len(df)} rows to {output_path}")

    def get_members(
        self,
        universe: str,
        as_of_date: Optional[str] = None
    ) -> List[str]:
        """
        Get universe members as of a specific date

        Reads from membership data stored in:
        data/curated/membership/universe={universe}/mode=intervals/

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            as_of_date: Date in 'YYYY-MM-DD' format (defaults to today)

        Returns:
            List of ticker symbols that were members on the given date
        """
        if as_of_date is None:
            lookup_date = datetime.now().date()
        else:
            lookup_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()

        # Path to membership intervals file
        intervals_path = (
            self.data_root /
            "membership" /
            f"universe={universe.lower()}" /
            "mode=intervals" /
            f"{universe.lower()}_membership_intervals.parquet"
        )

        try:
            # Read membership intervals
            df = pd.read_parquet(intervals_path)

            # Filter to members active on as_of_date
            df['start_date'] = pd.to_datetime(df['start_date']).dt.date
            df['end_date'] = pd.to_datetime(df['end_date']).dt.date

            active = df[
                (df['start_date'] <= lookup_date) &
                (df['end_date'] >= lookup_date)
            ]

            symbols = active['ticker'].unique().tolist()

            logger.info(
                f"Found {len(symbols)} members in {universe} as of {lookup_date}"
            )
            return symbols

        except FileNotFoundError:
            logger.warning(
                f"Membership file not found: {intervals_path}"
            )
            return []
        except Exception as e:
            logger.error(f"Error reading membership data: {e}")
            return []

    def get_current_members(self, universe: str) -> List[str]:
        """
        Get current universe members (as of today)

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')

        Returns:
            List of current member ticker symbols
        """
        return self.get_members(universe, as_of_date=None)

    def get_all_historical_members(
        self,
        universe: str,
        period_start: str,
        period_end: str
    ) -> List[str]:
        """
        Get ALL stocks that were members at ANY point during the period.

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
        start = datetime.strptime(period_start, '%Y-%m-%d').date()
        end = datetime.strptime(period_end, '%Y-%m-%d').date()

        # Path to membership intervals file
        intervals_path = (
            self.data_root /
            "membership" /
            f"universe={universe.lower()}" /
            "mode=intervals" /
            f"{universe.lower()}_membership_intervals.parquet"
        )

        try:
            # Read membership intervals
            df = pd.read_parquet(intervals_path)

            # Convert dates
            df['start_date'] = pd.to_datetime(df['start_date']).dt.date
            df['end_date'] = pd.to_datetime(df['end_date']).dt.date

            # Find all tickers with overlapping membership periods
            # A ticker is included if: (ticker_start <= period_end) AND (ticker_end >= period_start)
            # This captures all possible overlaps
            historical_members = df[
                (df['start_date'] <= end) &
                (df['end_date'] >= start)
            ]

            symbols = historical_members['ticker'].unique().tolist()

            logger.info(
                f"Found {len(symbols)} historical members in {universe} "
                f"for period {period_start} to {period_end} "
                f"(includes current + removed members)"
            )

            # Log some stats for transparency
            current_members = df[df['end_date'] >= datetime.now().date()]
            removed_count = len(symbols) - len(current_members)
            if removed_count > 0:
                logger.info(
                    f"  → {len(current_members['ticker'].unique())} current members, "
                    f"{removed_count} removed/changed during period"
                )

            return symbols

        except FileNotFoundError:
            logger.warning(
                f"Membership file not found: {intervals_path}"
            )
            logger.warning(
                "Falling back to current members only (survivorship bias present!)"
            )
            return self.get_current_members(universe)
        except Exception as e:
            logger.error(f"Error reading membership data: {e}")
            return []
