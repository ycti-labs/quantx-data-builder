import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Literal, Optional

import pandas as pd
import requests

class RiskFreeRateBuilder:
    """
    Fetch risk-free rate data from FRED API and save to cache.

    Use this class when you need to download fresh data from FRED.
    Requires FRED API key.
    """

    FRED_SERIES = {
        "3month": "DGS3MO",  # 3-Month Treasury Constant Maturity
        "1year": "DGS1",  # 1-Year Treasury Constant Maturity
        "5year": "DGS5",  # 5-Year Treasury Constant Maturity
        "10year": "DGS10",  # 10-Year Treasury Constant Maturity
        "30year": "DGS30",  # 30-Year Treasury Constant Maturity
    }

    FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(
        self,
        fred_api_key: str,
        data_root: str = "data/curated/references/risk_free_rate/freq=monthly",
        default_rate: str = "3month",
    ):
        """
        Initialize RiskFreeRateBuilder.

        Args:
            fred_api_key: FRED API key (get free at https://fred.stlouisfed.org/docs/api/api_key.html)
            data_root: Root directory for storing risk-free rate data
            default_rate: Default treasury rate to use ('3month', '1year', '5year', '10year', '30year')
        """
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)

        if default_rate not in self.FRED_SERIES:
            raise ValueError(
                f"Invalid default_rate: {default_rate}. Must be one of {list(self.FRED_SERIES.keys())}"
            )

        if not fred_api_key:
            raise ValueError(
                "FRED API key is required. "
                "Get a free API key at https://fred.stlouisfed.org/docs/api/api_key.html"
            )

        self.default_rate = default_rate
        self.fred_api_key = fred_api_key
        self.logger = logging.getLogger(__name__)

    def get_cache_path(self, rate_type: str, frequency: str) -> Path:
        """
        Get cache file path for risk-free rate data.

        Args:
            rate_type: Type of treasury rate ('3month', '5year', '10year', '30year')
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            Path to cache file
        """
        return self.data_root / f"{rate_type}_{frequency}.parquet"

    def fetch_risk_free_rate(
        self,
        start_date: str,
        end_date: str,
        rate_type: Optional[str] = None,
        frequency: str = "monthly",
    ) -> pd.DataFrame:
        """
        Fetch risk-free rate data from FRED.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            DataFrame with columns: date, rate (annualized percentage)
        """
        if rate_type is None:
            rate_type = self.default_rate

        if rate_type not in self.FRED_SERIES:
            raise ValueError(
                f"Invalid rate_type: {rate_type}. Must be one of {list(self.FRED_SERIES.keys())}"
            )

        series_id = self.FRED_SERIES[rate_type]

        self.logger.info(f"Fetching {rate_type} treasury rate ({series_id}) from FRED")

        try:
            params = {
                "series_id": series_id,
                "api_key": self.fred_api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
            }

            response = requests.get(self.FRED_API_URL, params=params)
            response.raise_for_status()

            data = response.json()

            if "observations" not in data:
                self.logger.warning(f"No observations returned for {series_id}")
                return pd.DataFrame()

            # Convert to DataFrame
            observations = data["observations"]
            df = pd.DataFrame(observations)

            # Parse date and rate
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["rate"] = pd.to_numeric(df["value"], errors="coerce")

            # Remove missing values (marked as '.' in FRED)
            df = df.dropna(subset=["rate"])

            # Keep only date and rate
            df = df[["date", "rate"]].copy()

            # Resample to requested frequency if not daily
            if frequency != "daily":
                df = self._resample_to_frequency(df, frequency)

            self.logger.info(
                f"Fetched {len(df)} {frequency} risk-free rate observations"
            )

            return df

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching risk-free rate from FRED: {e}")
            raise

    def _resample_to_frequency(self, df: pd.DataFrame, frequency: str) -> pd.DataFrame:
        """
        Resample daily data to weekly or monthly frequency.

        Args:
            df: DataFrame with date and rate columns
            frequency: Target frequency ('weekly', 'monthly')

        Returns:
            Resampled DataFrame
        """
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

        if frequency == "weekly":
            # Resample to weekly (Friday end-of-week)
            df_resampled = df.resample("W-FRI").last()
        elif frequency == "monthly":
            # Resample to monthly (end-of-month)
            df_resampled = df.resample("ME").last()
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")

        # Forward-fill missing values
        df_resampled = df_resampled.ffill()

        # Reset index to get date as column
        df_resampled = df_resampled.reset_index()
        df_resampled["date"] = df_resampled["date"].dt.date

        return df_resampled

    def build_and_save(
        self,
        start_date: str,
        end_date: str,
        rate_type: Optional[str] = None,
        frequency: str = "monthly",
        merge_existing: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch risk-free rate data from FRED and save to cache.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            merge_existing: If True, merge with existing cached data

        Returns:
            DataFrame with columns: date, rate
        """
        if rate_type is None:
            rate_type = self.default_rate

        cache_path = self.get_cache_path(rate_type, frequency)

        # Fetch fresh data
        df = self.fetch_risk_free_rate(start_date, end_date, rate_type, frequency)

        if len(df) == 0:
            self.logger.warning("No data fetched from FRED")
            return df

        # Merge with existing data if requested
        if merge_existing and cache_path.exists():
            existing = pd.read_parquet(cache_path)
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            df_combined = pd.concat([existing, df], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=["date"], keep="last")
            df_combined = df_combined.sort_values("date").reset_index(drop=True)
            df_combined.to_parquet(cache_path, index=False)
            self.logger.info(
                f"Updated cache with {len(df)} new observations at {cache_path}"
            )
            return df_combined
        else:
            df.to_parquet(cache_path, index=False)
            self.logger.info(f"Saved {len(df)} observations to cache: {cache_path}")
            return df

