"""
Risk-Free Rate Manager

Fetches and manages U.S. Treasury rates for calculating excess returns in
factor models (market beta, ESG beta, etc.). Uses FRED API (Federal Reserve
Economic Data) for reliable historical treasury rates.

Treasury Rate Series (FRED):
- DGS3MO: 3-Month Treasury Constant Maturity Rate
- DGS1: 1-Year Treasury Constant Maturity Rate
- DGS5: 5-Year Treasury Constant Maturity Rate
- DGS10: 10-Year Treasury Constant Maturity Rate
- DGS30: 30-Year Treasury Constant Maturity Rate

Alternative: Use constant risk-free rate if FRED API not available.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Literal, Optional

import pandas as pd
import requests


class RiskFreeRateManager:
    """Manage risk-free rate data from U.S. Treasury securities."""

    FRED_SERIES = {
        '3month': 'DGS3MO',   # 3-Month Treasury Constant Maturity
        '1year': 'DGS1',      # 1-Year Treasury Constant Maturity
        '5year': 'DGS5',      # 5-Year Treasury Constant Maturity
        '10year': 'DGS10',    # 10-Year Treasury Constant Maturity
        '30year': 'DGS30',    # 30-Year Treasury Constant Maturity
    }

    FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(
        self,
        fred_api_key: str,
        data_root: str = "data/curated/references/risk_free_rate/freq=monthly",
        default_rate: str = '3month'
    ):
        """
        Initialize RiskFreeRateManager.

        Args:
            fred_api_key: FRED API key (get free at https://fred.stlouisfed.org/docs/api/api_key.html)
            data_root: Root directory for storing risk-free rate data
            default_rate: Default treasury rate to use ('3month', '1year', '5year', '10year', '30year')
        """
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)

        if default_rate not in self.FRED_SERIES:
            raise ValueError(f"Invalid default_rate: {default_rate}. Must be one of {list(self.FRED_SERIES.keys())}")

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
        frequency: str = 'monthly'
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
            raise ValueError(f"Invalid rate_type: {rate_type}. Must be one of {list(self.FRED_SERIES.keys())}")

        series_id = self.FRED_SERIES[rate_type]

        self.logger.info(f"Fetching {rate_type} treasury rate ({series_id}) from FRED")

        try:
            params = {
                'series_id': series_id,
                'api_key': self.fred_api_key,
                'file_type': 'json',
                'observation_start': start_date,
                'observation_end': end_date
            }

            response = requests.get(self.FRED_API_URL, params=params)
            response.raise_for_status()

            data = response.json()

            if 'observations' not in data:
                self.logger.warning(f"No observations returned for {series_id}")
                return pd.DataFrame()

            # Convert to DataFrame
            observations = data['observations']
            df = pd.DataFrame(observations)

            # Parse date and rate
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['rate'] = pd.to_numeric(df['value'], errors='coerce')

            # Remove missing values (marked as '.' in FRED)
            df = df.dropna(subset=['rate'])

            # Keep only date and rate
            df = df[['date', 'rate']].copy()

            # Resample to requested frequency if not daily
            if frequency != 'daily':
                df = self._resample_to_frequency(df, frequency)

            self.logger.info(f"Fetched {len(df)} {frequency} risk-free rate observations")

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
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        if frequency == 'weekly':
            # Resample to weekly (Friday end-of-week)
            df_resampled = df.resample('W-FRI').last()
        elif frequency == 'monthly':
            # Resample to monthly (end-of-month)
            df_resampled = df.resample('ME').last()
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")

        # Forward-fill missing values
        df_resampled = df_resampled.ffill()

        # Reset index to get date as column
        df_resampled = df_resampled.reset_index()
        df_resampled['date'] = df_resampled['date'].dt.date

        return df_resampled

    def load_risk_free_rate(
        self,
        start_date: str,
        end_date: str,
        rate_type: Optional[str] = None,
        frequency: str = 'monthly',
        use_cache: bool = True,
        save_cache: bool = True
    ) -> pd.DataFrame:
        """
        Load risk-free rate data with caching support.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            use_cache: If True, try to load from cache first
            save_cache: If True, save fetched data to cache

        Returns:
            DataFrame with columns: date, rate
        """
        if rate_type is None:
            rate_type = self.default_rate

        cache_path = self.get_cache_path(rate_type, frequency)

        # Try to load from cache
        if use_cache and cache_path.exists():
            self.logger.info(f"Loading risk-free rate from cache: {cache_path}")
            df = pd.read_parquet(cache_path)
            df['date'] = pd.to_datetime(df['date']).dt.date

            # Filter to requested date range
            df = df[
                (df['date'] >= pd.to_datetime(start_date).date()) &
                (df['date'] <= pd.to_datetime(end_date).date())
            ].copy()

            if len(df) > 0:
                self.logger.info(f"Loaded {len(df)} cached observations")
                return df
            else:
                self.logger.warning("Cache exists but no data in requested range, fetching fresh data")

        # Fetch fresh data
        df = self.fetch_risk_free_rate(start_date, end_date, rate_type, frequency)

        # Save to cache
        if save_cache and len(df) > 0:
            # If cache exists, merge with existing data
            if cache_path.exists():
                existing = pd.read_parquet(cache_path)
                existing['date'] = pd.to_datetime(existing['date']).dt.date
                df_combined = pd.concat([existing, df], ignore_index=True)
                df_combined = df_combined.drop_duplicates(subset=['date'], keep='last')
                df_combined = df_combined.sort_values('date').reset_index(drop=True)
                df_combined.to_parquet(cache_path, index=False)
                self.logger.info(f"Updated cache with {len(df)} new observations")
            else:
                df.to_parquet(cache_path, index=False)
                self.logger.info(f"Saved {len(df)} observations to cache: {cache_path}")

        return df

    def calculate_risk_free_returns(
        self,
        dates: pd.Series,
        rate_type: Optional[str] = None,
        frequency: str = 'monthly',
        annualized_rate: Optional[pd.DataFrame] = None
    ) -> pd.Series:
        """
        Calculate risk-free returns for given dates.

        Args:
            dates: Series of dates for which to calculate returns
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            annualized_rate: Optional pre-loaded risk-free rate DataFrame

        Returns:
            Series of risk-free returns (decimal format, e.g., 0.04 = 4%)
        """
        if rate_type is None:
            rate_type = self.default_rate

        # Load risk-free rate data if not provided
        if annualized_rate is None:
            min_date = dates.min()
            max_date = dates.max()
            annualized_rate = self.load_risk_free_rate(
                start_date=str(min_date),
                end_date=str(max_date),
                rate_type=rate_type,
                frequency=frequency
            )

        # Create a mapping from date to rate
        rate_map = dict(zip(annualized_rate['date'], annualized_rate['rate']))

        # Convert annualized percentage to periodic return
        # E.g., 4.5% annual -> 0.045 / 12 = 0.00375 monthly return
        if frequency == 'monthly':
            periods_per_year = 12
        elif frequency == 'weekly':
            periods_per_year = 52
        elif frequency == 'daily':
            periods_per_year = 252  # Trading days
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")

        # Map dates to risk-free returns
        rf_returns = dates.map(lambda d: rate_map.get(d, None))

        # Convert percentage to decimal and annualize to periodic
        # E.g., 4.5% -> 0.045 / 12 = 0.00375 for monthly
        rf_returns = rf_returns / 100 / periods_per_year

        # Forward-fill missing values
        rf_returns = rf_returns.ffill()

        return rf_returns

    def calculate_excess_returns(
        self,
        returns: pd.Series,
        dates: pd.Series,
        rate_type: Optional[str] = None,
        frequency: str = 'monthly'
    ) -> pd.Series:
        """
        Calculate excess returns (returns - risk_free_rate).

        Args:
            returns: Series of asset returns
            dates: Series of dates corresponding to returns
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            Series of excess returns
        """
        rf_returns = self.calculate_risk_free_returns(
            dates=dates,
            rate_type=rate_type,
            frequency=frequency
        )

        excess_returns = returns - rf_returns

        return excess_returns

    def get_summary_statistics(
        self,
        start_date: str,
        end_date: str,
        rate_type: Optional[str] = None,
        frequency: str = 'monthly'
    ) -> Dict:
        """
        Get summary statistics for risk-free rate.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            rate_type: Type of treasury rate (default: self.default_rate)
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            Dictionary with summary statistics
        """
        df = self.load_risk_free_rate(start_date, end_date, rate_type, frequency)

        if len(df) == 0:
            return {}

        return {
            'rate_type': rate_type or self.default_rate,
            'frequency': frequency,
            'observations': len(df),
            'start_date': str(df['date'].min()),
            'end_date': str(df['date'].max()),
            'mean_rate': df['rate'].mean(),
            'median_rate': df['rate'].median(),
            'std_rate': df['rate'].std(),
            'min_rate': df['rate'].min(),
            'max_rate': df['rate'].max(),
        }
