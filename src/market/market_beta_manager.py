"""
Market Beta and Alpha Manager

Calculates market beta and alpha using rolling window OLS regression.
Uses monthly returns data with 60-month rolling windows.

Market Model:
    R_i,t = α_i + β_i * R_m,t + ε_i,t

Where:
    - R_i,t: Stock return at time t
    - R_m,t: Market return (SPY) at time t
    - β_i: Market beta (sensitivity to market movements)
    - α_i: Jensen's alpha (excess return after adjusting for market risk)
    - ε_i,t: Idiosyncratic return (error term)

Beta Interpretation:
    - β > 1: Stock is more volatile than the market
    - β = 1: Stock moves with the market
    - β < 1: Stock is less volatile than the market
    - β < 0: Stock moves inversely to the market (rare)

Alpha Interpretation:
    - α > 0: Stock outperforms market-adjusted expectations
    - α = 0: Stock performs as expected given its beta
    - α < 0: Stock underperforms market-adjusted expectations

Window: 60 months (5 years) - minimum for stable beta estimation
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm

from universe import Universe

logger = logging.getLogger(__name__)


class MarketBetaManager:
    """
    Market beta and alpha calculator using OLS regression

    Calculates rolling market beta and alpha for stocks relative to
    the market benchmark (SPY). Uses monthly returns with 60-month
    rolling windows for stable estimation.

    Data Structure:
        Input (Ticker): data/curated/tickers/exchange=us/ticker=SYMBOL/prices/freq=monthly/year=*/part-000.parquet
        Input (Market): data/curated/references/ticker=SPY/prices/freq=monthly/year=*/part-000.parquet
        Output: data/curated/tickers/exchange=us/ticker=SYMBOL/results/betas/market_beta.parquet

    Output Schema:
        - date: End date of rolling window
        - beta: Market beta coefficient
        - alpha: Jensen's alpha (annualized)
        - r_squared: R² of regression (explanatory power)
        - std_error_beta: Standard error of beta estimate
        - std_error_alpha: Standard error of alpha estimate
        - t_stat_beta: t-statistic for beta
        - t_stat_alpha: t-statistic for alpha
        - p_value_beta: p-value for beta significance
        - p_value_alpha: p-value for alpha significance
        - observations: Number of observations in window
        - correlation: Correlation between stock and market returns
    """

    def __init__(
        self,
        universe: Universe,
        window_months: int = 60,
        min_observations: int = 36,
        market_ticker: str = "SPY",
    ):
        """
        Initialize market beta manager

        Args:
            universe: Universe instance for membership and data root
            window_months: Rolling window size in months (default: 60)
            min_observations: Minimum observations required for calculation (default: 36)
            market_ticker: Market benchmark ticker symbol (default: "SPY")
        """
        self.universe = universe
        self.window_months = window_months
        self.min_observations = min_observations
        self.market_ticker = market_ticker
        self.logger = logging.getLogger(__name__)

        self.data_root = Path(universe.data_root)
        self.market_path = (
            self.data_root / "curated" / "references" / f"ticker={market_ticker}"
        )

        # Cache for market returns
        self._market_returns = None

    def _load_market_returns(self) -> pd.DataFrame:
        """
        Load market (SPY) monthly returns

        Returns:
            DataFrame with columns: date, market_return
        """
        if self._market_returns is not None:
            return self._market_returns

        # Load all monthly SPY data
        price_dir = self.market_path / "prices" / "freq=monthly"
        if not price_dir.exists():
            raise FileNotFoundError(
                f"Market data not found: {price_dir}\n"
                f"Please ensure SPY monthly data is available."
            )

        # Load all years
        all_data = []
        for year_dir in sorted(price_dir.glob("year=*")):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                try:
                    df = pd.read_parquet(parquet_file)
                    all_data.append(df)
                except Exception as e:
                    self.logger.warning(f"Error reading {parquet_file}: {e}")

        if not all_data:
            raise ValueError(f"No market data found in {price_dir}")

        # Combine and process
        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        # Calculate returns using adjusted close
        df["market_return"] = df["adj_close"].pct_change()

        # Keep only date and return
        result = df[["date", "market_return"]].copy()
        result = result.dropna()

        self._market_returns = result
        self.logger.info(
            f"Loaded {len(result)} monthly market returns from {result['date'].min()} to {result['date'].max()}"
        )

        return result

    def _load_ticker_returns(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load monthly returns for a specific ticker

        Args:
            ticker: Stock ticker symbol

        Returns:
            DataFrame with columns: date, ticker_return, or None if data not found
        """
        ticker_path = self.universe.get_ticker_path(ticker)
        price_dir = ticker_path / "prices" / "freq=monthly"

        if not price_dir.exists():
            self.logger.warning(f"No monthly price data for {ticker}")
            return None

        # Load all years
        all_data = []
        for year_dir in sorted(price_dir.glob("year=*")):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                try:
                    df = pd.read_parquet(parquet_file)
                    all_data.append(df)
                except Exception as e:
                    self.logger.warning(f"Error reading {parquet_file}: {e}")

        if not all_data:
            self.logger.warning(f"No readable parquet files for {ticker}")
            return None

        # Combine and process
        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        # Calculate returns using adjusted close
        df["ticker_return"] = df["adj_close"].pct_change()

        # Keep only date and return
        result = df[["date", "ticker_return"]].copy()
        result = result.dropna()

        return result

    def _calculate_rolling_beta(
        self, ticker_returns: pd.DataFrame, market_returns: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Calculate rolling beta and alpha using OLS regression

        Args:
            ticker_returns: DataFrame with columns: date, ticker_return
            market_returns: DataFrame with columns: date, market_return

        Returns:
            DataFrame with beta, alpha, and regression statistics
        """
        # Merge ticker and market returns
        merged = pd.merge(ticker_returns, market_returns, on="date", how="inner")
        merged = merged.sort_values("date")

        if len(merged) < self.min_observations:
            self.logger.warning(
                f"Insufficient data: {len(merged)} observations (minimum: {self.min_observations})"
            )
            return pd.DataFrame()

        results = []

        # Rolling window calculation
        for i in range(self.window_months - 1, len(merged)):
            window_start = i - self.window_months + 1
            window_data = merged.iloc[window_start : i + 1]

            if len(window_data) < self.min_observations:
                continue

            # Extract returns
            y = window_data["ticker_return"].values  # Dependent variable
            X = window_data["market_return"].values  # Independent variable
            window_end_date = window_data.iloc[-1]["date"]

            # OLS regression using statsmodels: y = alpha + beta * X + error
            # Add intercept term
            X_with_intercept = sm.add_constant(X)

            try:
                # Fit OLS model
                model = sm.OLS(y, X_with_intercept)
                results_ols = model.fit()

                # Extract coefficients
                alpha_monthly = results_ols.params[0]  # Intercept
                beta = results_ols.params[1]  # Slope

                # Extract statistics
                r_squared = results_ols.rsquared
                n = len(y)

                # Standard errors
                se_alpha = results_ols.bse[0]
                se_beta = results_ols.bse[1]

                # t-statistics
                t_stat_alpha = results_ols.tvalues[0]
                t_stat_beta = results_ols.tvalues[1]

                # p-values (two-tailed)
                p_value_alpha = results_ols.pvalues[0]
                p_value_beta = results_ols.pvalues[1]

                # Correlation
                correlation = np.corrcoef(X, y)[0, 1]

                # Annualize alpha (multiply monthly alpha by 12)
                alpha_annual = alpha_monthly * 12

                results.append(
                    {
                        "date": window_end_date,
                        "beta": beta,
                        "alpha": alpha_annual,
                        "r_squared": r_squared,
                        "std_error_beta": se_beta,
                        "std_error_alpha": se_alpha * 12,  # Annualized
                        "t_stat_beta": t_stat_beta,
                        "t_stat_alpha": t_stat_alpha,
                        "p_value_beta": p_value_beta,
                        "p_value_alpha": p_value_alpha,
                        "observations": n,
                        "correlation": correlation,
                    }
                )

            except Exception as e:
                self.logger.warning(
                    f"Regression failed for window ending {window_end_date}: {e}"
                )
                continue

        if not results:
            return pd.DataFrame()

        df_results = pd.DataFrame(results)
        return df_results

    def calculate_beta(self, ticker: str, save: bool = True) -> Optional[pd.DataFrame]:
        """
        Calculate market beta and alpha for a ticker

        Args:
            ticker: Stock ticker symbol
            save: Whether to save results to parquet (default: True)

        Returns:
            DataFrame with beta, alpha, and statistics, or None if calculation failed
        """
        self.logger.info(f"Calculating market beta for {ticker}")

        # Load market returns
        try:
            market_returns = self._load_market_returns()
        except Exception as e:
            self.logger.error(f"Failed to load market returns: {e}")
            return None

        # Load ticker returns
        ticker_returns = self._load_ticker_returns(ticker)
        if ticker_returns is None or len(ticker_returns) == 0:
            self.logger.warning(f"No returns data for {ticker}")
            return None

        # Calculate rolling beta
        results = self._calculate_rolling_beta(ticker_returns, market_returns)

        if results.empty:
            self.logger.warning(f"No beta results for {ticker}")
            return None

        self.logger.info(
            f"Calculated {len(results)} beta estimates for {ticker} "
            f"from {results['date'].min()} to {results['date'].max()}"
        )

        # Save results
        if save:
            self._save_beta_results(ticker, results)

        return results

    def _save_beta_results(self, ticker: str, results: pd.DataFrame) -> None:
        """
        Save beta results to parquet file

        Args:
            ticker: Stock ticker symbol
            results: DataFrame with beta and alpha results
        """
        ticker_path = self.universe.get_ticker_path(ticker)
        results_dir = ticker_path / "results" / "betas"
        results_dir.mkdir(parents=True, exist_ok=True)

        output_file = results_dir / "market_beta.parquet"

        # Ensure date is datetime
        results_copy = results.copy()
        results_copy["date"] = pd.to_datetime(results_copy["date"])

        # Save as parquet
        results_copy.to_parquet(output_file, index=False, engine="pyarrow")

        self.logger.info(f"Saved beta results to {output_file}")

    def load_beta(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load saved beta results for a ticker

        Args:
            ticker: Stock ticker symbol

        Returns:
            DataFrame with beta and alpha results, or None if not found
        """
        ticker_path = self.universe.get_ticker_path(ticker)
        results_file = ticker_path / "results" / "betas" / "market_beta.parquet"

        if not results_file.exists():
            self.logger.warning(f"No saved beta results for {ticker}")
            return None

        try:
            df = pd.read_parquet(results_file)
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            self.logger.error(f"Error loading beta results for {ticker}: {e}")
            return None

    def calculate_universe_betas(
        self,
        tickers: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Calculate market betas for multiple tickers

        Args:
            tickers: List of ticker symbols (default: all universe members)
            start_date: Start date for membership filter (YYYY-MM-DD)
            end_date: End date for membership filter (YYYY-MM-DD)

        Returns:
            Dictionary mapping ticker to beta DataFrame
        """
        if tickers is None:
            # Get all historical members
            if start_date and end_date:
                tickers = self.universe.get_all_historical_members(start_date, end_date)
            else:
                # Try to get all members from intervals
                intervals_file = (
                    Path(self.universe.data_root)
                    / "curated"
                    / "membership"
                    / f"universe={self.universe.name}"
                    / "mode=intervals"
                    / f"{self.universe.name}_membership_intervals.parquet"
                )
                if intervals_file.exists():
                    intervals = pd.read_parquet(intervals_file)
                    tickers = sorted(intervals["ticker"].unique().tolist())
                else:
                    raise ValueError(
                        "No tickers specified and no membership data found. "
                        "Please provide tickers or start_date/end_date."
                    )

        self.logger.info(f"Calculating betas for {len(tickers)} tickers")

        results = {}
        for i, ticker in enumerate(tickers, 1):
            self.logger.info(f"[{i}/{len(tickers)}] Processing {ticker}")
            beta_df = self.calculate_beta(ticker, save=True)
            if beta_df is not None and not beta_df.empty:
                results[ticker] = beta_df

        self.logger.info(
            f"Completed: {len(results)}/{len(tickers)} tickers with beta results"
        )

        return results
