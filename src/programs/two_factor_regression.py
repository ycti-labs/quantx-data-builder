"""
Two-Factor OLS Regression: Market + ESG

Performs OLS regression to estimate factor exposures (betas) and alpha:

    R_i,t - RF_t = α_i + β_market * (R_market,t - RF_t) + β_ESG * ESG_factor_t + ε_i,t

Where:
    - R_i,t: Stock return at time t
    - RF_t: Risk-free rate at time t
    - R_market,t: Market return (SPY) at time t
    - ESG_factor_t: ESG factor return at time t
    - α_i: Jensen's alpha (excess return not explained by factors)
    - β_market: Market beta (sensitivity to market movements)
    - β_ESG: ESG beta (sensitivity to ESG factor)
    - ε_i,t: Idiosyncratic return (error term)

Features:
    - Load stock returns, market returns, and ESG factor returns
    - Calculate excess returns (subtract risk-free rate)
    - Perform OLS regression to estimate betas and alpha
    - Statistical significance tests (t-statistics, p-values)
    - Model diagnostics (R², adjusted R², F-statistic)
    - Rolling window regressions for time-varying exposures
    - Save results to parquet for further analysis

Usage:
    # Single stock regression
    python src/programs/two_factor_regression.py --ticker AAPL --start-date 2014-01-01 --end-date 2024-12-31

    # Multiple stocks
    python src/programs/two_factor_regression.py --tickers AAPL MSFT GOOGL TSLA --start-date 2014-01-01 --end-date 2024-12-31

    # Rolling window (60-month)
    python src/programs/two_factor_regression.py --ticker AAPL --start-date 2014-01-01 --end-date 2024-12-31 --rolling --window 60

    # Universe-wide analysis (we use)
    python src/programs/two_factor_regression.py --universe SP500 --start-date 2014-01-01 --end-date 2024-12-31 --continuous-esg-only --rolling --window 36

Academic References:
    - Fama & French (1993): "Common risk factors in the returns on stocks and bonds"
    - Carhart (1997): "On persistence in mutual fund performance" (momentum factor)
    - Pastor, Stambaugh & Taylor (2021): "Sustainable investing in equilibrium"
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from esg import ESGFactorBuilder
from market import PriceManager, RiskFreeRateManager
from universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class TwoFactorRegression:
    """
    Two-factor OLS regression: Market + ESG

    Estimates factor exposures (betas) and alpha using OLS regression:
        Excess_Return = α + β_market * Market_Excess + β_ESG * ESG_Factor + ε
    """

    def __init__(
        self,
        universe: SP500Universe,
        window_months: Optional[int] = None,
        min_observations: int = 36,
        rf_rate_type: str = "3month",
    ):
        """
        Initialize two-factor regression

        Args:
            universe: Universe instance for data access
            window_months: Rolling window size (None = full sample)
            min_observations: Minimum observations required (default: 36)
            rf_rate_type: Risk-free rate type (default: "3month")
        """
        self.universe = universe
        self.window_months = window_months
        self.min_observations = min_observations
        self.rf_rate_type = rf_rate_type
        self.logger = logging.getLogger(__name__)

        self.data_root = Path(universe.data_root)
        self.results_dir = self.data_root / "results" / "two_factor_regression"
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def clean_results(self, ticker: Optional[str] = None) -> None:
        """
        Delete existing result files before generating new ones

        Args:
            ticker: If provided, clean only this ticker's results.
                   If None, clean all results in the directory.
        """
        if ticker:
            ticker_dir = self.results_dir / f"ticker={ticker}"
            if ticker_dir.exists():
                shutil.rmtree(ticker_dir)
                self.logger.info(f"Cleaned existing results for {ticker}")
        else:
            if self.results_dir.exists():
                shutil.rmtree(self.results_dir)
                self.results_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Cleaned all existing results in {self.results_dir}")

        # Initialize RiskFreeRateManager
        rf_data_root = (
            self.data_root
            / "curated"
            / "references"
            / "risk_free_rate"
            / "freq=monthly"
        )
        self.rf_manager = RiskFreeRateManager(
            data_root=str(rf_data_root), default_rate=rf_rate_type
        )

        # Cache
        self._market_excess = None
        self._esg_factors = None

    def _load_risk_free_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Load risk-free rate using RiskFreeRateManager

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with date and RF columns (monthly decimal)
        """
        self.logger.info(f"Loading risk-free rate ({self.rf_rate_type})")

        # Load the cached RF data (FRED format: annual percentage)
        rf_data = self.rf_manager.load_risk_free_rate(
            start_date=start_date,
            end_date=end_date,
            rate_type=self.rf_rate_type,
            frequency="monthly",
        )

        # FRED data is annual percentage (e.g., 5.25 = 5.25% per year)
        # Convert to monthly decimal: annual_pct / 100 / 12
        rf_df = rf_data.copy()
        rf_df["date"] = pd.to_datetime(rf_df["date"])
        rf_df["RF"] = rf_df["rate"] / 100 / 12  # Annual % → monthly decimal

        rf_df = rf_df[["date", "RF"]]

        self.logger.info(
            f"Loaded {len(rf_df)} RF observations from {rf_df['date'].min()} to {rf_df['date'].max()}"
        )
        self.logger.info(
            f"RF mean: {rf_df['RF'].mean():.6f} (monthly decimal) = {rf_df['RF'].mean()*12*100:.2f}% annual"
        )

        return rf_df

    def _load_market_returns(self) -> pd.DataFrame:
        """
        Load market (SPY) returns and convert to excess returns

        Returns:
            DataFrame with date and market_excess columns
        """
        if self._market_excess is not None:
            return self._market_excess

        self.logger.info("Loading market (SPY) returns")

        # Load SPY monthly prices
        market_path = self.data_root / "curated" / "references" / "ticker=SPY"
        price_dir = market_path / "prices" / "freq=monthly"

        if not price_dir.exists():
            raise FileNotFoundError(
                f"SPY data not found: {price_dir}\n"
                f"Please ensure SPY monthly data is available."
            )

        # Load all years
        all_data = []
        for year_dir in sorted(price_dir.glob("year=*")):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                all_data.append(df)

        if not all_data:
            raise ValueError(f"No market data found in {price_dir}")

        # Combine and calculate returns
        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["market_return"] = df["adj_close"].pct_change()

        # Get date range
        start_date = df["date"].min()
        end_date = df["date"].max()

        # Load RF and calculate excess returns
        rf_df = self._load_risk_free_rate(str(start_date.date()), str(end_date.date()))
        market_df = df[["date", "market_return"]].dropna()
        market_df = market_df.merge(rf_df, on="date", how="inner")
        market_df["market_excess"] = market_df["market_return"] - market_df["RF"]

        self._market_excess = market_df[["date", "market_excess"]]
        self.logger.info(
            f"Loaded {len(self._market_excess)} market excess returns from "
            f"{self._market_excess['date'].min()} to {self._market_excess['date'].max()}"
        )

        return self._market_excess

    def _load_esg_factors(self) -> pd.DataFrame:
        """
        Load ESG factor returns

        Returns:
            DataFrame with date and ESG factor columns
        """
        if self._esg_factors is not None:
            return self._esg_factors

        self.logger.info("Loading ESG factors")

        # Load from saved factors
        factors_dir = self.data_root / "results" / "esg_factors"
        factors_file = factors_dir / "esg_factors.parquet"

        if not factors_file.exists():
            raise FileNotFoundError(
                f"ESG factors not found: {factors_file}\n"
                f"Run build_esg_factors.py first."
            )

        df = pd.read_parquet(factors_file)
        df = df.reset_index()
        df.columns = ["date"] + list(df.columns[1:])
        df["date"] = pd.to_datetime(df["date"])

        self._esg_factors = df
        self.logger.info(
            f"Loaded {len(self._esg_factors)} ESG factor observations from "
            f"{self._esg_factors['date'].min()} to {self._esg_factors['date'].max()}"
        )
        self.logger.info(f"Factor columns: {[c for c in df.columns if c != 'date']}")

        return self._esg_factors

    def _load_stock_returns(
        self, ticker: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Load stock returns and convert to excess returns

        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            DataFrame with date and stock_excess columns, or None if failed
        """
        try:
            # Load monthly prices
            ticker_path = (
                self.data_root
                / "curated"
                / "tickers"
                / "exchange=us"
                / f"ticker={ticker}"
            )
            price_dir = ticker_path / "prices" / "freq=monthly"

            if not price_dir.exists():
                self.logger.warning(f"No price data for {ticker}")
                return None

            # Load all years in range
            start_year = pd.to_datetime(start_date).year
            end_year = pd.to_datetime(end_date).year

            all_data = []
            for year in range(start_year, end_year + 1):
                year_dir = price_dir / f"year={year}"
                parquet_file = year_dir / "part-000.parquet"
                if parquet_file.exists():
                    df = pd.read_parquet(parquet_file)
                    all_data.append(df)

            if not all_data:
                self.logger.warning(
                    f"No price data for {ticker} in {start_date} to {end_date}"
                )
                return None

            # Combine and calculate returns
            df = pd.concat(all_data, ignore_index=True)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")
            df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            df["stock_return"] = df["adj_close"].pct_change()

            # Load RF and calculate excess returns
            rf_df = self._load_risk_free_rate(start_date, end_date)
            stock_df = df[["date", "stock_return"]].dropna()
            stock_df = stock_df.merge(rf_df, on="date", how="inner")
            stock_df["stock_excess"] = stock_df["stock_return"] - stock_df["RF"]

            result = stock_df[["date", "stock_excess"]]
            self.logger.info(
                f"Loaded {len(result)} returns for {ticker} from "
                f"{result['date'].min()} to {result['date'].max()}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error loading returns for {ticker}: {e}")
            return None

    def run_regression(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        esg_factor_name: str = "ESG_factor",
    ) -> Optional[pd.DataFrame]:
        """
        Run two-factor OLS regression for a single stock

        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            esg_factor_name: ESG factor column name (default: "ESG_factor")

        Returns:
            DataFrame with regression results, or None if failed
        """
        self.logger.info(f"Running 2-factor regression for {ticker}")

        # Load data
        stock_df = self._load_stock_returns(ticker, start_date, end_date)
        if stock_df is None or len(stock_df) < self.min_observations:
            self.logger.warning(f"Insufficient data for {ticker}")
            return None

        market_df = self._load_market_returns()
        esg_df = self._load_esg_factors()

        # Merge all data on date
        data = stock_df.merge(market_df, on="date", how="inner")
        data = data.merge(esg_df[["date", esg_factor_name]], on="date", how="inner")

        if len(data) < self.min_observations:
            self.logger.warning(
                f"Insufficient aligned data for {ticker}: {len(data)} < {self.min_observations}"
            )
            return None

        # Run regression
        if self.window_months is None:
            # Full sample regression
            results = self._run_single_regression(
                data, ticker, esg_factor_name, end_date
            )
            if results:
                return pd.DataFrame([results])
        else:
            # Rolling window regressions
            return self._run_rolling_regression(data, ticker, esg_factor_name)

    def _run_single_regression(
        self,
        data: pd.DataFrame,
        ticker: str,
        esg_factor_name: str,
        date: str,
    ) -> Optional[Dict]:
        """
        Run single OLS regression with Newey-West (HAC) standard errors

        Uses HAC-robust standard errors to correct for:
        - Autocorrelation in residuals (momentum, mean reversion)
        - Heteroskedasticity (volatility clustering, GARCH effects)

        Args:
            data: DataFrame with stock_excess, market_excess, and ESG factor
            ticker: Stock ticker symbol
            esg_factor_name: ESG factor column name
            date: Date label for this regression

        Returns:
            Dictionary with regression results (all in MONTHLY units), or None if failed

            IMPORTANT: Alpha, betas, and standard errors are in MONTHLY units.
            This ensures consistency and avoids unit mismatch errors.
            Annualization should be done only at display/interpretation time:
            - alpha_annual = alpha_monthly × 12
            - SE_annual = SE_monthly × √12
            - t-statistic remains unchanged (ratio is scale-invariant)

        Note:
            maxlags=12 for monthly data follows Newey & West (1994) rule:
            lag = floor(4 * (T/100)^(2/9)) ≈ 12 for T≈120 months
            This captures annual seasonality in financial data.
        """
        try:
            # Prepare regression data
            y = data["stock_excess"]
            X = data[["market_excess", esg_factor_name]]
            X = sm.add_constant(X)

            # Run OLS with Newey-West (HAC) standard errors
            # maxlags=12 for monthly data (captures annual seasonality)
            # Corrects for autocorrelation and heteroskedasticity
            model = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 12})

            # Extract results (all in MONTHLY units for consistency)
            results = {
                "ticker": ticker,
                "date": pd.to_datetime(date),
                "alpha": model.params["const"],  # Monthly
                "beta_market": model.params["market_excess"],
                "beta_esg": model.params[esg_factor_name],
                "alpha_tstat": model.tvalues["const"],
                "beta_market_tstat": model.tvalues["market_excess"],
                "beta_esg_tstat": model.tvalues[esg_factor_name],
                "alpha_pvalue": model.pvalues["const"],
                "beta_market_pvalue": model.pvalues["market_excess"],
                "beta_esg_pvalue": model.pvalues[esg_factor_name],
                "r_squared": model.rsquared,
                "adj_r_squared": model.rsquared_adj,
                "f_statistic": model.fvalue,
                "f_pvalue": model.f_pvalue,
                "observations": int(model.nobs),
                "std_error_alpha": model.bse["const"],  # Monthly
                "std_error_beta_market": model.bse["market_excess"],
                "std_error_beta_esg": model.bse[esg_factor_name],
            }

            return results

        except Exception as e:
            self.logger.error(f"Regression failed for {ticker}: {e}")
            return None

    def _run_rolling_regression(
        self,
        data: pd.DataFrame,
        ticker: str,
        esg_factor_name: str,
    ) -> pd.DataFrame:
        """
        Run rolling window regressions to generate time-series of betas

        Args:
            data: DataFrame with stock_excess, market_excess, and ESG factor
            ticker: Stock ticker symbol
            esg_factor_name: ESG factor column name

        Returns:
            DataFrame with rolling regression results (time-series)
            Each row represents a window ending at 'date' with estimated betas
        """
        results = []
        num_windows = len(data) - self.window_months + 1

        self.logger.info(
            f"Running {num_windows} rolling {self.window_months}-month windows for {ticker}"
        )

        for i in range(self.window_months, len(data) + 1):
            window_data = data.iloc[i - self.window_months : i]
            end_date = window_data["date"].iloc[-1]

            result = self._run_single_regression(
                window_data, ticker, esg_factor_name, end_date
            )
            if result:
                results.append(result)

        if not results:
            self.logger.warning(f"No rolling regression results for {ticker}")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        self.logger.info(
            f"Generated {len(df)} time-series observations from {df['date'].min()} to {df['date'].max()}"
        )
        return df

    def save_results(self, results: pd.DataFrame, ticker: str) -> None:
        """
        Save regression results to parquet

        Args:
            results: DataFrame with regression results
                    Single row for full-sample regression
                    Multiple rows (time-series) for rolling window regression
            ticker: Stock ticker symbol
        """
        if results.empty:
            self.logger.warning(f"No results to save for {ticker}")
            return

        # Clean existing results for this ticker before saving new ones
        self.clean_results(ticker=ticker)

        output_dir = self.results_dir / f"ticker={ticker}"
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / "two_factor_regression.parquet"
        results.to_parquet(output_file, index=False)

        if len(results) > 1:
            self.logger.info(
                f"Saved {len(results)} time-series observations to {output_file}"
            )
        else:
            self.logger.info(f"Saved results to {output_file}")

    def load_results(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load saved regression results

        Args:
            ticker: Stock ticker symbol

        Returns:
            DataFrame with regression results, or None if not found
        """
        results_file = (
            self.results_dir / f"ticker={ticker}" / "two_factor_regression.parquet"
        )

        if not results_file.exists():
            self.logger.warning(f"No saved results for {ticker}")
            return None

        try:
            df = pd.read_parquet(results_file)
            self.logger.info(
                f"Loaded {len(df)} regression results for {ticker} from "
                f"{df['date'].min()} to {df['date'].max()}"
            )
            return df
        except Exception as e:
            self.logger.error(f"Error loading results for {ticker}: {e}")
            return None

    def display_results(self, results: pd.DataFrame, ticker: str) -> None:
        """
        Display regression results summary

        Args:
            results: DataFrame with regression results
            ticker: Stock ticker symbol
        """
        if results.empty:
            print(f"\nNo results to display for {ticker}")
            return

        print(f"\n{'='*80}")
        print(f"TWO-FACTOR REGRESSION RESULTS: {ticker}")
        print(f"{'='*80}\n")

        if len(results) == 1:
            # Single regression results
            r = results.iloc[0]
            print(f"Period: {r['date']}")
            print(f"Observations: {r['observations']}\n")

            print("Factor Exposures:")
            print(
                f"  Alpha (monthly):     {r['alpha']:>8.6f}  ({r['alpha']*100:>6.2f}%)"
            )
            print(
                f"  Alpha (annualized):  {r['alpha']*12:>8.6f}  ({r['alpha']*12*100:>6.2f}%)"
            )
            print(f"    t-statistic:       {r['alpha_tstat']:>8.2f}")
            print(
                f"    p-value:           {r['alpha_pvalue']:>8.4f}  {'***' if r['alpha_pvalue'] < 0.01 else '**' if r['alpha_pvalue'] < 0.05 else '*' if r['alpha_pvalue'] < 0.10 else ''}"
            )
            print()
            print(f"  Market Beta:         {r['beta_market']:>8.4f}")
            print(f"    t-statistic:       {r['beta_market_tstat']:>8.2f}")
            print(
                f"    p-value:           {r['beta_market_pvalue']:>8.4f}  {'***' if r['beta_market_pvalue'] < 0.01 else '**' if r['beta_market_pvalue'] < 0.05 else '*' if r['beta_market_pvalue'] < 0.10 else ''}"
            )
            print()
            print(f"  ESG Beta:            {r['beta_esg']:>8.4f}")
            print(f"    t-statistic:       {r['beta_esg_tstat']:>8.2f}")
            print(
                f"    p-value:           {r['beta_esg_pvalue']:>8.4f}  {'***' if r['beta_esg_pvalue'] < 0.01 else '**' if r['beta_esg_pvalue'] < 0.05 else '*' if r['beta_esg_pvalue'] < 0.10 else ''}"
            )
            print()
            print(f"Model Fit:")
            print(
                f"  R²:                  {r['r_squared']:>8.4f}  ({r['r_squared']*100:>5.2f}%)"
            )
            print(
                f"  Adjusted R²:         {r['adj_r_squared']:>8.4f}  ({r['adj_r_squared']*100:>5.2f}%)"
            )
            print(f"  F-statistic:         {r['f_statistic']:>8.2f}")
            print(
                f"  F p-value:           {r['f_pvalue']:>8.4f}  {'***' if r['f_pvalue'] < 0.01 else '**' if r['f_pvalue'] < 0.05 else '*' if r['f_pvalue'] < 0.10 else ''}"
            )
            print()
            print("Significance: *** p<0.01, ** p<0.05, * p<0.10")

        else:
            # Rolling regression summary (TIME-SERIES)
            print(f"Rolling Window: {len(results)} time-series observations")
            print(f"Period: {results['date'].min()} to {results['date'].max()}\n")

            summary = pd.DataFrame(
                {
                    "Mean": results[
                        ["alpha", "beta_market", "beta_esg", "r_squared"]
                    ].mean(),
                    "Median": results[
                        ["alpha", "beta_market", "beta_esg", "r_squared"]
                    ].median(),
                    "Std Dev": results[
                        ["alpha", "beta_market", "beta_esg", "r_squared"]
                    ].std(),
                    "Min": results[
                        ["alpha", "beta_market", "beta_esg", "r_squared"]
                    ].min(),
                    "Max": results[
                        ["alpha", "beta_market", "beta_esg", "r_squared"]
                    ].max(),
                }
            )

            print("Time-Series Statistics (across all windows):")
            print(summary.to_string())
            print()

            # Show first, middle, and last estimates to illustrate time variation
            print("Sample Time-Series Estimates:")
            print(f"\n  First ({results.iloc[0]['date'].strftime('%Y-%m-%d')}):")
            print(
                f"    Alpha: {results.iloc[0]['alpha']:>7.4f}  Market β: {results.iloc[0]['beta_market']:>6.3f}  ESG β: {results.iloc[0]['beta_esg']:>6.3f}  R²: {results.iloc[0]['r_squared']*100:>5.1f}%"
            )

            mid_idx = len(results) // 2
            print(f"\n  Middle ({results.iloc[mid_idx]['date'].strftime('%Y-%m-%d')}):")
            print(
                f"    Alpha: {results.iloc[mid_idx]['alpha']:>7.4f}  Market β: {results.iloc[mid_idx]['beta_market']:>6.3f}  ESG β: {results.iloc[mid_idx]['beta_esg']:>6.3f}  R²: {results.iloc[mid_idx]['r_squared']*100:>5.1f}%"
            )

            print(f"\n  Latest ({results.iloc[-1]['date'].strftime('%Y-%m-%d')}):")
            print(
                f"    Alpha: {results.iloc[-1]['alpha']:>7.4f}  Market β: {results.iloc[-1]['beta_market']:>6.3f}  ESG β: {results.iloc[-1]['beta_esg']:>6.3f}  R²: {results.iloc[-1]['r_squared']*100:>5.1f}%"
            )

            print(
                f"\n  → Time-series saved with {len(results)} observations for visualization"
            )


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Two-Factor OLS Regression: Market + ESG",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--ticker",
        type=str,
        help="Single stock ticker symbol",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="Multiple stock ticker symbols",
    )
    parser.add_argument(
        "--universe",
        type=str,
        default="SP500",
        help="Universe for batch processing (default: SP500)",
    )
    parser.add_argument(
        "--continuous-esg-only",
        action="store_true",
        help="Only process tickers with continuous ESG data",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2014-01-01",
        help="Start date (YYYY-MM-DD, default: 2014-01-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-12-31",
        help="End date (YYYY-MM-DD, default: 2024-12-31)",
    )
    parser.add_argument(
        "--esg-factor",
        type=str,
        default="ESG_factor",
        help="ESG factor column name (default: ESG_factor)",
    )
    parser.add_argument(
        "--rolling",
        action="store_true",
        help="Use rolling window regression",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=60,
        help="Rolling window size in months (default: 60)",
    )
    parser.add_argument(
        "--min-obs",
        type=int,
        default=36,
        help="Minimum observations required (default: 36)",
    )
    parser.add_argument(
        "--rf-rate-type",
        type=str,
        default="3month",
        choices=["3month", "1year", "5year", "10year", "30year"],
        help="Risk-free rate type (default: 3month)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        default=True,
        help="Save results to parquet (default: True)",
    )
    parser.add_argument(
        "--no-save",
        action="store_false",
        dest="save",
        help="Don't save results to parquet",
    )

    args = parser.parse_args()

    # Initialize components
    logger.info("Initializing components...")
    config = Config("config/settings.yaml")
    universe = SP500Universe(config.get("storage.local.root_path"))

    # Initialize regression engine
    window = args.window if args.rolling else None
    regression = TwoFactorRegression(
        universe=universe,
        window_months=window,
        min_observations=args.min_obs,
        rf_rate_type=args.rf_rate_type,
    )

    # Determine tickers to process
    tickers = []
    if args.ticker:
        tickers = [args.ticker]
    elif args.tickers:
        tickers = args.tickers
    else:
        # Load universe members
        logger.info(f"Loading {args.universe} universe members...")
        if args.continuous_esg_only:
            # Load tickers with continuous ESG data
            continuous_file = Path("data/continuous_esg_tickers.txt")
            if continuous_file.exists():
                with open(continuous_file) as f:
                    tickers = [line.strip() for line in f if line.strip()]
                logger.info(f"Loaded {len(tickers)} tickers with continuous ESG data")
            else:
                logger.error(f"Continuous ESG file not found: {continuous_file}")
                return
        else:
            # Load all universe members
            membership = universe.get_current_members()
            tickers = membership["ticker"].unique().tolist()
            logger.info(f"Loaded {len(tickers)} universe members")

    # Process tickers
    logger.info(f"Processing {len(tickers)} tickers...")
    for ticker in tqdm(tickers, desc="Running regressions"):
        results = regression.run_regression(
            ticker=ticker,
            start_date=args.start_date,
            end_date=args.end_date,
            esg_factor_name=args.esg_factor,
        )

        if results is not None and not results.empty:
            regression.display_results(results, ticker)

            if args.save:
                regression.save_results(results, ticker)

    logger.info("Completed two-factor regression analysis")


if __name__ == "__main__":
    main()
