"""
Universe builder - orchestrates universe construction from various sources
Supports multiple market universes with configurable data sources
"""

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class Universe(ABC):
    """
    Main class to orchestrate universe building from configured sources
    Handles multiple phases and dependencies between universes
    Also provides membership query functionality
    """

    def __init__(
        self,
        universe_name: str,
        exchange: str,
        currency: str,
        market_etf: str,
        data_root: str = "./data",
    ):
        """
        Initialize universe builder

        Args:
            data_root: Root directory for data storage (default: data/curated)
        """
        self.name: str = universe_name
        self.exchange: str = exchange
        self.currency: str = currency
        self.market_etf: str = market_etf
        self.data_root = Path(data_root)

    @abstractmethod
    def build_membership(self):
        pass

    def get_membership_path(self, mode: str = "daily") -> Path:
        return (
            self.data_root
            / "curated"
            / "membership"
            / f"universe={self.name.lower()}"
            / f"mode={mode}"
        )

    def get_references_path(self) -> Path:
        return (
            self.data_root
            / "curated"
            / "references"
        )

    def get_ticker_path(self, symbol: str) -> Path:
        return (
            self.data_root
            / "curated"
            / "tickers"
            / f"exchange={self.exchange}"
            / f"ticker={symbol}"
        )

    def get_ticker_prices_path(self, symbol: str, frequency: str) -> Path:
        return self.get_ticker_path(symbol) / "prices" / f"freq={frequency}"

    def get_ticker_fundamentals_path(self, symbol: str) -> Path:
        return self.get_ticker_path(symbol) / "fundamentals"

    def write_parquet(self, df: pd.DataFrame, output_path: Path) -> None:
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

    def get_members(self, as_of_date: Optional[str] = None) -> List[str]:
        """
        Get universe members as of a specific date

        Reads from membership interval data stored in Parquet files.

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')
            as_of_date: Date in 'YYYY-MM-DD' format (defaults to today)

        Returns:
            List of ticker symbols that were members on the given date
        """
        if as_of_date is None:
            lookup_date = datetime.now().date()
        else:
            lookup_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()

        # Path to membership intervals file
        intervals_path = (
            self.get_membership_path(mode="intervals")
            / f"{self.name.lower()}_membership_intervals.parquet"
        )

        try:
            # Read membership intervals
            df = pd.read_parquet(intervals_path)

            # Filter to members active on as_of_date
            df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
            df["end_date"] = pd.to_datetime(df["end_date"]).dt.date

            active = df[
                (df["start_date"] <= lookup_date) & (df["end_date"] >= lookup_date)
            ]

            symbols = active["ticker"].unique().tolist()

            logger.info(
                f"Found {len(symbols)} members in {self.name} as of {lookup_date}"
            )
            return symbols

        except FileNotFoundError:
            logger.warning(f"Membership file not found: {intervals_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading membership data: {e}")
            return []

    def get_current_members(self) -> List[str]:
        """
        Get current universe members (as of today)

        Args:
            universe: Universe name (e.g., 'sp500', 'nasdaq100')

        Returns:
            List of current member ticker symbols
        """
        return self.get_members(as_of_date=None)

    def get_ticker_corrections(self) -> Dict[str, List[str]]:
        """
        Get ticker correction mapping from membership data

        Uses the gvkey column in membership intervals to find alternative
        ticker symbols for the same company (same gvkey), including historical
        ticker changes (e.g., ANTM -> ELV, FB -> META).

        Returns:
            Dictionary mapping ticker -> list of alternative tickers
            Example: {'ANTM': ['ELV'], 'FB': ['META'], 'ELV': ['ANTM']}
        """
        intervals_path = (
            self.get_membership_path(mode="intervals")
            / f"{self.name.lower()}_membership_intervals.parquet"
        )

        try:
            # Read membership with gvkey
            df = pd.read_parquet(intervals_path)

            if "gvkey" not in df.columns:
                logger.warning(
                    f"No gvkey column in {intervals_path}, cannot provide ticker corrections"
                )
                return {}

            # Remove rows without gvkey
            df = df[df["gvkey"].notna()].copy()

            # Convert dates for sorting
            df["start_date"] = pd.to_datetime(df["start_date"])
            df["end_date"] = pd.to_datetime(df["end_date"])

            # Group by gvkey to find all tickers for same company
            corrections = {}
            for gvkey, group in df.groupby("gvkey"):
                # Sort by start_date to get chronological order
                group = group.sort_values("start_date")
                tickers = group["ticker"].unique().tolist()

                # If same company has multiple tickers (ticker changes over time)
                if len(tickers) > 1:
                    # Each ticker can be corrected to any other ticker with same gvkey
                    for ticker in tickers:
                        alternatives = [t for t in tickers if t != ticker]
                        if alternatives:
                            corrections[ticker] = alternatives

            if corrections:
                logger.info(
                    f"Loaded {len(corrections)} ticker corrections from gvkey mapping"
                )
                # Log some examples
                examples = list(corrections.items())[:5]
                for ticker, alts in examples:
                    logger.info(f"  {ticker} -> {alts}")
            else:
                logger.info(
                    "No ticker corrections found (all tickers are unique per gvkey)"
                )

            return corrections

        except Exception as e:
            logger.warning(f"Could not load ticker corrections: {e}")
            return {}

    def get_membership_intervals(
        self, symbol: str, span_mode: bool = False
    ) -> List[Tuple[date, date]]:
        """
        Get membership intervals for a symbol in this universe

        Args:
            symbol: Ticker symbol
            span_mode: If True, return single interval spanning from earliest start to latest end.
                      If False (default), return all individual intervals (handles gaps).

        Returns:
            List of (start_date, end_date) tuples, sorted chronologically
            Empty list if symbol not found or has no membership data

        Examples:
            # Symbol with gaps (removed and re-added)
            get_membership_intervals('AMD', span_mode=False)
            # Returns: [(date(2000, 1, 3), date(2013, 9, 17)),
            #           (date(2017, 3, 20), date(2025, 7, 9))]

            get_membership_intervals('AMD', span_mode=True)
            # Returns: [(date(2000, 1, 3), date(2025, 7, 9))]

            # Continuous member
            get_membership_intervals('AAPL', span_mode=False)
            # Returns: [(date(2000, 1, 3), date(2025, 7, 9))]
        """
        try:
            intervals_path = (
                self.get_membership_path(mode="intervals")
                / f"{self.name.lower()}_membership_intervals.parquet"
            )

            if not intervals_path.exists():
                return []

            df = pd.read_parquet(intervals_path)
            symbol_data = df[df["ticker"] == symbol]

            if symbol_data.empty:
                return []

            # Convert dates and sort by start_date
            start_dates = pd.to_datetime(symbol_data["start_date"])
            end_dates = pd.to_datetime(symbol_data["end_date"])

            if span_mode:
                # Return single interval spanning full membership period
                member_start = start_dates.min().date()
                member_end = end_dates.max().date()
                return [(member_start, member_end)]
            else:
                # Return all individual intervals sorted chronologically
                intervals = list(zip(start_dates.dt.date, end_dates.dt.date))
                intervals.sort(key=lambda x: x[0])
                return intervals

        except Exception as e:
            logger.warning(f"Could not get membership intervals for {symbol}: {e}")
            return []

    def get_all_historical_members(self, start_date: str, end_date: str) -> List[str]:
        """
        Get ALL stocks that were members at ANY point during the period.

        This eliminates survivorship bias by including:
        - Current members
        - Removed members (delisted, acquired, merged)
        - Stocks that joined during the period

        Example: For S&P 500 from 2020-2024, this returns ~520+ symbols
        (not just the current 500) because companies get added/removed.

        Args:
            period_start: Start date in 'YYYY-MM-DD' format
            period_end: End date in 'YYYY-MM-DD' format

        Returns:
            List of all ticker symbols that were members during any part of the period
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Path to membership intervals file
        intervals_path = (
            self.get_membership_path(mode="intervals")
            / f"{self.name.lower()}_membership_intervals.parquet"
        )

        try:
            # Read membership intervals
            df = pd.read_parquet(intervals_path)

            # Convert dates
            df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
            df["end_date"] = pd.to_datetime(df["end_date"]).dt.date

            # Find all tickers with overlapping membership periods
            # A ticker is included if: (ticker_start <= period_end) AND (ticker_end >= period_start)
            # This captures all possible overlaps
            historical_members = df[
                (df["start_date"] <= end) & (df["end_date"] >= start)
            ]

            symbols = historical_members["ticker"].unique().tolist()

            logger.info(
                f"Found {len(symbols)} historical members in {self.name} "
                f"for period {start_date} to {end_date} "
                f"(includes current + removed members)"
            )

            # Log some stats for transparency
            current_members = df[df["end_date"] >= datetime.now().date()]
            removed_count = len(symbols) - len(current_members)
            if removed_count > 0:
                logger.info(
                    f"  → {len(current_members['ticker'].unique())} current members, "
                    f"{removed_count} removed/changed during period"
                )

            return symbols

        except FileNotFoundError:
            logger.warning(f"Membership file not found: {intervals_path}")
            logger.warning(
                "Falling back to current members only (survivorship bias present!)"
            )
            return self.get_current_members()
        except Exception as e:
            logger.error(f"Error reading membership data: {e}")
            return []

    def get_gvkey_for_symbol(self, symbol: str) -> Optional[int]:
        """
        Get GVKEY for a given ticker symbol

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')

        Returns:
            GVKEY integer or None if not found
        """
        gvkey_path = self.data_root / "curated" / "metadata" / "gvkey.parquet"

        try:
            df = pd.read_parquet(gvkey_path)

            # Ensure we have the required columns
            if "ticker" not in df.columns or "gvkey" not in df.columns:
                logger.error(
                    f"Required columns (ticker, gvkey) not found in {gvkey_path}"
                )
                return None

            # Find matching ticker (case-insensitive)
            matches = df[df["ticker"].str.upper() == symbol.upper()]

            if matches.empty:
                logger.debug(f"No GVKEY found for symbol: {symbol}")
                return None

            # Return first match
            gvkey = matches.iloc[0]["gvkey"]
            return int(gvkey) if pd.notna(gvkey) else None

        except FileNotFoundError:
            logger.warning(f"GVKEY mapping file not found: {gvkey_path}")
            return None
        except Exception as e:
            logger.error(f"Error reading GVKEY mapping: {e}")
            return None

    def add_gvkey_for_symbol(self, symbol: str, gvkey: int) -> bool:
        """
        Add or update GVKEY mapping for a ticker symbol

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')
            gvkey: GVKEY integer

        Returns:
            True if successful, False otherwise
        """
        gvkey_path = self.data_root / "curated" / "metadata" / "gvkey.parquet"

        try:
            # Ensure directory exists
            gvkey_path.parent.mkdir(parents=True, exist_ok=True)

            # Load existing data or create new DataFrame
            if gvkey_path.exists():
                df = pd.read_parquet(gvkey_path)
            else:
                df = pd.DataFrame(columns=["ticker", "gvkey"])

            # Remove existing entry for this ticker if it exists
            df = df[df["ticker"].str.upper() != symbol.upper()]

            # Add new entry
            new_row = pd.DataFrame([{"ticker": symbol.upper(), "gvkey": int(gvkey)}])
            df = pd.concat([df, new_row], ignore_index=True)

            # Sort by ticker for consistency
            df = df.sort_values("ticker").reset_index(drop=True)

            # Save back to parquet
            df.to_parquet(
                gvkey_path, engine="pyarrow", compression="snappy", index=False
            )

            logger.info(f"Added/updated GVKEY mapping: {symbol} -> {gvkey}")
            return True

        except Exception as e:
            logger.error(f"Error adding GVKEY mapping for {symbol}: {e}")
            return False

    def get_symbol_for_gvkey(self, gvkey: int) -> Optional[str]:
        """
        Get ticker symbol(s) for a given GVKEY

        Args:
            gvkey: GVKEY integer

        Returns:
            Ticker symbol or None if not found
            If multiple tickers exist for same GVKEY, returns the first one
        """
        gvkey_path = self.data_root / "curated" / "metadata" / "gvkey.parquet"

        try:
            df = pd.read_parquet(gvkey_path)

            # Ensure we have the required columns
            if "ticker" not in df.columns or "gvkey" not in df.columns:
                logger.error(
                    f"Required columns (ticker, gvkey) not found in {gvkey_path}"
                )
                return None

            # Find matching gvkey
            matches = df[df["gvkey"] == int(gvkey)]

            if matches.empty:
                logger.debug(f"No ticker found for GVKEY: {gvkey}")
                return None

            # Return first match
            return matches.iloc[0]["ticker"]

        except FileNotFoundError:
            logger.warning(f"GVKEY mapping file not found: {gvkey_path}")
            return None
        except Exception as e:
            logger.error(f"Error reading GVKEY mapping: {e}")
            return None

    def get_all_gvkey_mappings(self) -> pd.DataFrame:
        """
        Get all GVKEY-ticker mappings

        Returns:
            DataFrame with columns: ticker, gvkey
            Empty DataFrame if file not found or error occurs
        """
        gvkey_path = self.data_root / "curated" / "metadata" / "gvkey.parquet"

        try:
            df = pd.read_parquet(gvkey_path)
            logger.info(f"Loaded {len(df)} GVKEY mappings from {gvkey_path}")
            return df

        except FileNotFoundError:
            logger.warning(f"GVKEY mapping file not found: {gvkey_path}")
            return pd.DataFrame(columns=["ticker", "gvkey"])
        except Exception as e:
            logger.error(f"Error reading GVKEY mapping: {e}")
            return pd.DataFrame(columns=["ticker", "gvkey"])
