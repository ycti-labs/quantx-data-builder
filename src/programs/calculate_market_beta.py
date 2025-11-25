"""
Calculate Market Beta for S&P 500 Historical Members

This program calculates rolling market beta for all historical S&P 500 members
using SPY as the market proxy. Uses a 60-month rolling window to generate a
time-series of beta values for each ticker.

Usage:
    # Calculate rolling 60-month betas (time-series) for all S&P 500 members
    python -m programs.calculate_market_beta

    # Specify custom date range (default: 2014-01-01 to 2025-10-31)
    python -m programs.calculate_market_beta --start 2014-01-01 --end 2025-10-31

    # Calculate for specific symbols only
    python -m programs.calculate_market_beta --symbols META AAPL GOOGL

    # Use different window size (default: 60 months = 5 years)
    python -m programs.calculate_market_beta --window 36
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
from market import PriceManager
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


class MarketBetaCalculator:
    """Calculate market beta for stocks using SPY as market proxy."""

    def __init__(self, price_manager: PriceManager, window_months: int = 60):
        """
        Initialize beta calculator.

        Args:
            price_manager: PriceManager instance for loading price data
            window_months: Rolling window size in months (default: 60 = 5 years)
        """
        self.price_mgr = price_manager
        self.window_months = window_months
        self.market_returns = None

    def load_market_returns(
        self, start_date: str, end_date: str, frequency: str = "monthly"
    ) -> pd.Series:
        """
        Load and calculate market returns (SPY).

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            frequency: Data frequency (default: 'monthly')

        Returns:
            Series of market returns indexed by date
        """
        logger.info(
            f"Loading SPY market data from {start_date} to {end_date} ({frequency})"
        )

        spy_data = self.price_mgr.load_price_data(
            symbol="SPY", frequency=frequency, start_date=start_date, end_date=end_date
        )

        if spy_data is None or len(spy_data) == 0:
            raise ValueError("Failed to load SPY market data")

        # Calculate returns
        spy_prices = spy_data.set_index("date")["adj_close"]
        self.market_returns = spy_prices.pct_change().dropna()

        logger.info(f"Loaded {len(self.market_returns)} market return observations")
        return self.market_returns

    def calculate_beta(self, stock_returns: pd.Series) -> Dict[str, float]:
        """
        Calculate beta, alpha, and R-squared for a stock.

        Args:
            stock_returns: Series of stock returns indexed by date

        Returns:
            Dictionary with beta, alpha, r_squared, std_error, observations
        """
        # Align returns on dates
        aligned = pd.DataFrame(
            {"stock": stock_returns, "market": self.market_returns}
        ).dropna()

        if len(aligned) < 24:  # Minimum 24 months for meaningful beta
            return {
                "beta": np.nan,
                "alpha": np.nan,
                "r_squared": np.nan,
                "std_error": np.nan,
                "observations": len(aligned),
            }

        # Run OLS regression: stock_return = alpha + beta * market_return
        X = sm.add_constant(aligned["market"])
        y = aligned["stock"]

        try:
            model = sm.OLS(y, X).fit()

            return {
                "beta": model.params["market"],
                "alpha": model.params["const"],
                "r_squared": model.rsquared,
                "std_error": model.bse["market"],
                "observations": len(aligned),
            }
        except Exception as e:
            logger.warning(f"Regression failed: {e}")
            return {
                "beta": np.nan,
                "alpha": np.nan,
                "r_squared": np.nan,
                "std_error": np.nan,
                "observations": len(aligned),
            }

    def calculate_rolling_beta(self, stock_returns: pd.Series) -> pd.DataFrame:
        """
        Calculate rolling beta over time.

        Args:
            stock_returns: Series of stock returns indexed by date

        Returns:
            DataFrame with date, beta, alpha, r_squared, std_error
        """
        rolling_results = []

        for i in range(self.window_months, len(stock_returns) + 1):
            window_returns = stock_returns.iloc[i - self.window_months : i]
            end_date = window_returns.index[-1]

            beta_stats = self.calculate_beta(window_returns)
            beta_stats["date"] = end_date
            rolling_results.append(beta_stats)

        return pd.DataFrame(rolling_results)

    def calculate_stock_beta(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        frequency: str = "monthly",
        rolling: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Calculate beta for a single stock.

        Args:
            symbol: Stock ticker symbol
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            frequency: Data frequency (default: 'monthly')
            rolling: If True, calculate rolling beta; if False, single beta

        Returns:
            DataFrame with beta results, or None if data unavailable
        """
        try:
            # Load stock data
            stock_data = self.price_mgr.load_price_data(
                symbol=symbol,
                frequency=frequency,
                start_date=start_date,
                end_date=end_date,
            )

            if stock_data is None or len(stock_data) == 0:
                logger.warning(f"No data available for {symbol}")
                return None

            # Calculate returns
            stock_prices = stock_data.set_index("date")["adj_close"]
            stock_returns = stock_prices.pct_change().dropna()

            if rolling:
                results = self.calculate_rolling_beta(stock_returns)
            else:
                beta_stats = self.calculate_beta(stock_returns)
                results = pd.DataFrame([beta_stats])
                results["date"] = end_date

            results["symbol"] = symbol
            return results

        except Exception as e:
            logger.error(f"Error calculating beta for {symbol}: {e}")
            return None

    def calculate_universe_betas(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        frequency: str = "monthly",
        rolling: bool = False,
        output_dir: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Calculate betas for multiple stocks.

        Args:
            symbols: List of stock ticker symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            frequency: Data frequency (default: 'monthly')
            rolling: If True, calculate rolling betas; if False, single betas
            output_dir: Optional directory to save results

        Returns:
            DataFrame with all beta results
        """
        all_results = []

        logger.info(
            f"Calculating {'rolling ' if rolling else ''}betas for {len(symbols)} stocks"
        )
        logger.info(
            f"Window: {self.window_months} months, Period: {start_date} to {end_date}"
        )

        for symbol in tqdm(symbols, desc="Calculating betas"):
            result = self.calculate_stock_beta(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                rolling=rolling,
            )

            if result is not None:
                all_results.append(result)

        if not all_results:
            logger.warning("No beta results calculated")
            return pd.DataFrame()

        # Combine all results
        combined_results = pd.concat(all_results, ignore_index=True)

        # Sort by symbol and date
        combined_results = combined_results.sort_values(["symbol", "date"]).reset_index(
            drop=True
        )

        logger.info(
            f"Successfully calculated betas for {len(combined_results['symbol'].unique())} stocks"
        )

        # Save to file if output directory provided
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            window_str = f"{self.window_months}m"
            rolling_str = "rolling" if rolling else "static"
            filename = f"market_betas_{rolling_str}_{window_str}_{timestamp}.parquet"

            output_path = output_dir / filename
            combined_results.to_parquet(output_path, index=False)
            logger.info(f"Saved results to {output_path}")

        return combined_results


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Calculate market beta for S&P 500 historical members"
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
        default="2025-10-31",
        help="End date in YYYY-MM-DD format (default: 2025-10-31)",
    )
    parser.add_argument(
        "--frequency",
        type=str,
        default="monthly",
        choices=["daily", "weekly", "monthly"],
        help="Data frequency (default: monthly)",
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
        help="Specific symbols to calculate (default: all S&P 500 members)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/results/betas",
        help="Output directory for results (default: data/results/betas)",
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

    # Create beta calculator
    beta_calc = MarketBetaCalculator(price_manager=price_mgr, window_months=args.window)

    # Load market returns
    beta_calc.load_market_returns(
        start_date=args.start, end_date=args.end, frequency=args.frequency
    )

    # Get symbols to process
    if args.symbols:
        symbols = args.symbols
    else:
        # Get all historical S&P 500 members
        logger.info("Fetching S&P 500 historical members...")
        members_data = universe.get_all_historical_members(
            start_date=args.start, end_date=args.end
        )

        # Handle different return types
        if isinstance(members_data, pd.DataFrame):
            df = members_data  # Type hint for checker
            if "symbol" in df.columns:
                symbols = sorted(df["symbol"].unique().tolist())
            else:
                # Try using index
                symbols = sorted(df.index.tolist())
        elif isinstance(members_data, (list, set, tuple)):
            symbols = sorted(list(set(members_data)))
        else:
            raise TypeError(
                f"Unexpected return type from get_all_historical_members: {type(members_data)}"
            )

        # Filter out any invalid symbols
        symbols = [s for s in symbols if s and isinstance(s, str) and len(s) > 0]

        if not symbols:
            raise ValueError("No symbols found in the specified period")

        logger.info(f"Found {len(symbols)} unique S&P 500 members")

    # Calculate betas
    results = beta_calc.calculate_universe_betas(
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
        frequency=args.frequency,
        rolling=args.rolling,
        output_dir=args.output,
    )

    # Display summary statistics
    if len(results) > 0:
        logger.info("\n" + "=" * 60)
        logger.info("BETA CALCULATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total stocks processed: {len(results['symbol'].unique())}")
        logger.info(f"Total observations: {len(results)}")
        logger.info(f"\nBeta Statistics:")
        logger.info(f"  Mean:   {results['beta'].mean():.4f}")
        logger.info(f"  Median: {results['beta'].median():.4f}")
        logger.info(f"  Std:    {results['beta'].std():.4f}")
        logger.info(f"  Min:    {results['beta'].min():.4f}")
        logger.info(f"  Max:    {results['beta'].max():.4f}")
        logger.info(f"\nR-squared Statistics:")
        logger.info(f"  Mean:   {results['r_squared'].mean():.4f}")
        logger.info(f"  Median: {results['r_squared'].median():.4f}")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
