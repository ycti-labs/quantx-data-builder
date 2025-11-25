"""
Calculate ESG Beta with Excess Returns

This program calculates ESG beta (sensitivity to ESG factors) for S&P 500 stocks
using excess returns (returns minus risk-free rate). Supports multiple ESG factors
and rolling window analysis.

ESG Beta Formula:
    Excess_Return_Stock = alpha + beta_esg * Excess_Return_ESG_Factor + epsilon

Where:
    - Excess returns = Total returns - Risk-free rate
    - ESG Factor = Composite ESG score or individual E/S/G pillars

Usage:
    # Calculate ESG beta for all S&P 500 stocks
    python -m programs.calculate_esg_beta

    # Use specific ESG factor
    python -m programs.calculate_esg_beta --esg-factor total_score

    # Custom window and period
    python -m programs.calculate_esg_beta --window 36 --start 2014-01-01
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging

from tiingo import TiingoClient

from core.config import Config
from market import ESGManager, PriceManager, RiskFreeRateManager
from universe import SP500Universe


def get_logger(name):
    """Get or create a logger with the given name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = get_logger(__name__)


class ESGBetaCalculator:
    """Calculate ESG beta using excess returns."""

    def __init__(
        self,
        price_manager: PriceManager,
        esg_manager: ESGManager,
        rf_manager: RiskFreeRateManager,
        window_months: int = 60,
    ):
        """
        Initialize ESG beta calculator.

        Args:
            price_manager: PriceManager instance for loading price data
            esg_manager: ESGManager instance for loading ESG data
            rf_manager: RiskFreeRateManager for risk-free rate data
            window_months: Rolling window size in months (default: 60 = 5 years)
        """
        self.price_mgr = price_manager
        self.esg_mgr = esg_manager
        self.rf_mgr = rf_manager
        self.window_months = window_months

    def calculate_esg_beta(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        esg_factor: str = "total_score",
        rolling: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Calculate ESG beta for a single stock.

        Args:
            symbol: Stock ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            esg_factor: ESG factor column to use ('total_score', 'env_score', 'soc_score', 'gov_score')
            rolling: If True, calculate rolling beta; if False, single beta

        Returns:
            DataFrame with ESG beta results, or None if insufficient data
        """
        try:
            # Load stock price data (monthly for ESG matching)
            stock_data = self.price_mgr.load_price_data(
                symbol=symbol,
                frequency="monthly",
                start_date=start_date,
                end_date=end_date,
            )

            if stock_data is None or len(stock_data) == 0:
                logger.warning(f"No price data available for {symbol}")
                return None

            # Load ESG data
            esg_data = self.esg_mgr.load_esg_data(
                ticker=symbol, start_date=start_date, end_date=end_date
            )

            if esg_data.empty or esg_factor not in esg_data.columns:
                logger.warning(
                    f"No ESG data available for {symbol} (factor: {esg_factor})"
                )
                return None

            # Calculate stock returns
            stock_data["date"] = pd.to_datetime(stock_data["date"]).dt.date
            stock_prices = stock_data.set_index("date")["adj_close"]
            stock_returns = stock_prices.pct_change().dropna()

            # Load risk-free rate
            rf_data = self.rf_mgr.load_risk_free_rate(
                start_date=start_date, end_date=end_date, frequency="monthly"
            )

            # Calculate excess returns - convert index to Series of dates
            return_dates = pd.Series(stock_returns.index)
            stock_excess = self.rf_mgr.calculate_excess_returns(
                returns=stock_returns, dates=return_dates, frequency="monthly"
            )

            # Prepare ESG factor data
            esg_data["date"] = pd.to_datetime(esg_data["date"]).dt.date
            esg_factor_data = esg_data.set_index("date")[esg_factor]

            # Calculate ESG factor returns (month-over-month change)
            esg_factor_returns = esg_factor_data.pct_change().dropna()

            # Ensure both series have date-type indices
            stock_excess.index = pd.DatetimeIndex(stock_excess.index)
            esg_factor_returns.index = pd.DatetimeIndex(esg_factor_returns.index)

            # Align all series on dates
            aligned = pd.DataFrame(
                {"stock_excess": stock_excess, "esg_factor": esg_factor_returns}
            ).dropna()

            if len(aligned) < 24:  # Minimum 24 months
                logger.warning(
                    f"Insufficient aligned data for {symbol}: {len(aligned)} months"
                )
                return None

            # Calculate beta
            if rolling:
                results = self._calculate_rolling_esg_beta(aligned, symbol, esg_factor)
            else:
                beta_stats = self._calculate_single_esg_beta(aligned)
                results = pd.DataFrame([beta_stats])
                results["date"] = end_date
                results["symbol"] = symbol
                results["esg_factor"] = esg_factor

            return results

        except Exception as e:
            import traceback

            logger.error(f"Error calculating ESG beta for {symbol}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _calculate_single_esg_beta(self, aligned_data: pd.DataFrame) -> Dict:
        """
        Calculate single ESG beta using OLS regression.

        Args:
            aligned_data: DataFrame with 'stock_excess' and 'esg_factor' columns

        Returns:
            Dictionary with beta statistics
        """
        if len(aligned_data) < 24:
            return {
                "beta_esg": np.nan,
                "alpha": np.nan,
                "r_squared": np.nan,
                "std_error": np.nan,
                "observations": len(aligned_data),
            }

        try:
            # OLS regression: stock_excess = alpha + beta_esg * esg_factor + epsilon
            X = sm.add_constant(aligned_data["esg_factor"])
            y = aligned_data["stock_excess"]

            model = sm.OLS(y, X).fit()

            return {
                "beta_esg": model.params["esg_factor"],
                "alpha": model.params["const"],
                "r_squared": model.rsquared,
                "std_error": model.bse["esg_factor"],
                "observations": len(aligned_data),
            }
        except Exception as e:
            logger.warning(f"Regression failed: {e}")
            return {
                "beta_esg": np.nan,
                "alpha": np.nan,
                "r_squared": np.nan,
                "std_error": np.nan,
                "observations": len(aligned_data),
            }

    def _calculate_rolling_esg_beta(
        self, aligned_data: pd.DataFrame, symbol: str, esg_factor: str
    ) -> pd.DataFrame:
        """
        Calculate rolling ESG beta over time.

        Args:
            aligned_data: DataFrame with 'stock_excess' and 'esg_factor' columns
            symbol: Stock ticker symbol
            esg_factor: ESG factor name

        Returns:
            DataFrame with rolling beta statistics
        """
        rolling_results = []

        for i in range(self.window_months, len(aligned_data) + 1):
            window_data = aligned_data.iloc[i - self.window_months : i]
            end_date = window_data.index[-1]

            beta_stats = self._calculate_single_esg_beta(window_data)
            beta_stats["date"] = end_date
            beta_stats["symbol"] = symbol
            beta_stats["esg_factor"] = esg_factor
            rolling_results.append(beta_stats)

        return pd.DataFrame(rolling_results)

    def calculate_universe_esg_betas(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        esg_factor: str = "total_score",
        rolling: bool = True,
        output_dir: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Calculate ESG betas for multiple stocks.

        Args:
            symbols: List of stock ticker symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            esg_factor: ESG factor to use
            rolling: If True, calculate rolling betas
            output_dir: Optional directory to save results

        Returns:
            DataFrame with all ESG beta results
        """
        all_results = []

        logger.info(
            f"Calculating {'rolling ' if rolling else ''}ESG betas for {len(symbols)} stocks"
        )
        logger.info(f"ESG Factor: {esg_factor}, Window: {self.window_months} months")
        logger.info(f"Period: {start_date} to {end_date}")

        for symbol in tqdm(symbols, desc="Calculating ESG betas"):
            result = self.calculate_esg_beta(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                esg_factor=esg_factor,
                rolling=rolling,
            )

            if result is not None:
                all_results.append(result)

        if not all_results:
            logger.warning("No ESG beta results calculated")
            return pd.DataFrame()

        # Combine all results
        combined_results = pd.concat(all_results, ignore_index=True)
        combined_results = combined_results.sort_values(["symbol", "date"]).reset_index(
            drop=True
        )

        logger.info(
            f"Successfully calculated ESG betas for {len(combined_results['symbol'].unique())} stocks"
        )

        # Save to file if output directory provided
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            window_str = f"{self.window_months}m"
            rolling_str = "rolling" if rolling else "static"
            filename = (
                f"esg_betas_{rolling_str}_{esg_factor}_{window_str}_{timestamp}.parquet"
            )

            output_path = output_dir / filename
            combined_results.to_parquet(output_path, index=False)
            logger.info(f"Saved results to {output_path}")

        return combined_results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Calculate ESG beta for S&P 500 stocks using excess returns"
    )
    parser.add_argument(
        "--window",
        type=int,
        default=60,
        help="Rolling window size in months (default: 60 = 5 years)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2014-01-01",
        help="Start date in YYYY-MM-DD format (default: 2014-01-01)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="2024-12-31",
        help="End date in YYYY-MM-DD format (default: 2024-12-31)",
    )
    parser.add_argument(
        "--esg-factor",
        type=str,
        default="ESG Score",
        help='ESG factor column name (default: "ESG Score"). Common: "ESG Score", "Environmental Pillar Score", "Social Pillar Score", "Governance Pillar Score"',
    )
    parser.add_argument(
        "--rolling",
        action="store_true",
        default=True,
        help="Calculate rolling betas (default: True for time-series)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        help="Specific symbols to calculate (default: all S&P 500 members with ESG data)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/results/esg_betas",
        help="Output directory for results (default: data/results/esg_betas)",
    )

    args = parser.parse_args()

    # Initialize components
    logger.info("Initializing components...")
    config = Config("config/settings.yaml")
    universe = SP500Universe()

    tiingo = TiingoClient(
        {"api_key": config.get("fetcher.tiingo.api_key"), "session": True}
    )

    price_mgr = PriceManager(tiingo=tiingo, universe=universe)
    esg_mgr = ESGManager(universe=universe)

    # Initialize risk-free rate manager
    fred_api_key = config.get("fetcher.fred.api_key")
    if not fred_api_key:
        raise ValueError(
            "FRED API key not found in config/settings.yaml. "
            "Please add: fetcher.fred.api_key"
        )

    rf_mgr = RiskFreeRateManager(fred_api_key=fred_api_key)
    logger.info("Using FRED API for risk-free rates")

    # Create ESG beta calculator
    esg_beta_calc = ESGBetaCalculator(
        price_manager=price_mgr,
        esg_manager=esg_mgr,
        rf_manager=rf_mgr,
        window_months=args.window,
    )

    # Get symbols to process
    if args.symbols:
        symbols = args.symbols
    else:
        # Get all S&P 500 members with ESG data
        logger.info("Fetching S&P 500 members with ESG data...")
        all_members = universe.get_all_historical_members(
            start_date=args.start, end_date=args.end
        )

        # Filter to symbols with ESG data
        symbols_with_esg = []
        for symbol in all_members:
            try:
                esg_data = esg_mgr.load_esg_data(
                    ticker=symbol, start_date=args.start, end_date=args.end
                )
                if not esg_data.empty:
                    symbols_with_esg.append(symbol)
            except:
                pass

        symbols = sorted(symbols_with_esg)
        logger.info(f"Found {len(symbols)} S&P 500 members with ESG data")

    # Calculate ESG betas
    results = esg_beta_calc.calculate_universe_esg_betas(
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
        esg_factor=args.esg_factor,
        rolling=args.rolling,
        output_dir=args.output,
    )

    # Display summary statistics
    if len(results) > 0:
        logger.info("\n" + "=" * 60)
        logger.info("ESG BETA CALCULATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"ESG Factor: {args.esg_factor}")
        logger.info(f"Total stocks processed: {len(results['symbol'].unique())}")
        logger.info(f"Total observations: {len(results)}")
        logger.info(f"\nESG Beta Statistics:")
        logger.info(f"  Mean:   {results['beta_esg'].mean():.4f}")
        logger.info(f"  Median: {results['beta_esg'].median():.4f}")
        logger.info(f"  Std:    {results['beta_esg'].std():.4f}")
        logger.info(f"  Min:    {results['beta_esg'].min():.4f}")
        logger.info(f"  Max:    {results['beta_esg'].max():.4f}")
        logger.info(f"\nR-squared Statistics:")
        logger.info(f"  Mean:   {results['r_squared'].mean():.4f}")
        logger.info(f"  Median: {results['r_squared'].median():.4f}")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
