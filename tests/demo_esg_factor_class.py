"""
Demo: ESG Factor Builder Class

Demonstrates the ESGFactorBuilder class for constructing long-short factor portfolios
from ESG signals. Shows both cross-sectional and sector-neutral approaches.

Usage:
    python tests/demo_esg_factor_class.py [--sector-neutral] [--quantile 0.2]
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from esg import ESGFactorBuilder, ESGManager
from market import PriceManager, RiskFreeRateManager
from universe import Universe

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_monthly_prices(universe: Universe, tickers: list) -> pd.DataFrame:
    """Load monthly prices for tickers"""
    logger.info(f"Loading monthly prices for {len(tickers)} tickers")

    price_mgr = PriceManager(universe)
    all_prices = []

    for ticker in tickers:
        prices = price_mgr.load_prices(ticker, freq="monthly")
        if prices is not None and not prices.empty:
            prices = prices[["date", "adj_close"]].copy()
            prices["ticker"] = ticker
            all_prices.append(prices)

    if not all_prices:
        raise ValueError("No price data loaded")

    # Combine and create MultiIndex
    prices_df = pd.concat(all_prices, ignore_index=True)
    prices_df = prices_df.set_index(["date", "ticker"]).sort_index()

    logger.info(f"Loaded prices: {len(prices_df)} observations")
    return prices_df


def main():
    parser = argparse.ArgumentParser(description="Demo ESG Factor Builder")
    parser.add_argument(
        "--sector-neutral", action="store_true", help="Use sector-neutral ranking"
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.2,
        help="Quantile for long/short legs (default: 0.2 = 20%%)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2020-01-01",
        help="Start date for analysis (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-12-31",
        help="End date for analysis (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    # Initialize universe
    data_root = Path(__file__).parent.parent / "data"
    universe = Universe(name="sp500", data_root=str(data_root), exchange="us")

    logger.info("=" * 60)
    logger.info("ESG FACTOR BUILDER DEMO")
    logger.info("=" * 60)
    logger.info(f"Universe: {universe.name}")
    logger.info(f"Sector Neutral: {args.sector_neutral}")
    logger.info(f"Quantile: {args.quantile}")
    logger.info(f"Period: {args.start_date} to {args.end_date}")
    logger.info("")

    # Get universe members with ESG data
    logger.info("Loading universe members with ESG data...")
    continuous_file = data_root / "continuous_esg_tickers.txt"

    if continuous_file.exists():
        with open(continuous_file, "r") as f:
            tickers = [line.strip() for line in f if line.strip()]
        logger.info(f"Found {len(tickers)} tickers with continuous ESG data")
    else:
        logger.warning(
            "No continuous_esg_tickers.txt found, using first 50 universe members"
        )
        tickers = universe.get_all_historical_members(args.start_date, args.end_date)[
            :50
        ]

    # Load ESG data
    logger.info("Loading ESG data...")
    esg_mgr = ESGManager(universe)
    esg_panel = esg_mgr.load_esg_panel(
        tickers=tickers, start_date=args.start_date, end_date=args.end_date
    )

    if esg_panel is None or esg_panel.empty:
        logger.error("No ESG data loaded")
        return

    logger.info(f"ESG data: {len(esg_panel)} observations")

    # Load monthly prices
    prices_df = load_monthly_prices(universe, tickers)

    # Load risk-free rate
    logger.info("Loading risk-free rate...")
    rf_mgr = RiskFreeRateManager(universe)
    rf_df = rf_mgr.load_risk_free_rate()

    if rf_df is not None:
        # Convert to monthly and filter date range
        rf_df = rf_df[rf_df.index.to_series().dt.to_period("M").dt.to_timestamp()]
        rf_df = rf_df[
            (rf_df.index >= pd.to_datetime(args.start_date))
            & (rf_df.index <= pd.to_datetime(args.end_date))
        ]
        logger.info(f"Risk-free rate: {len(rf_df)} observations")
    else:
        logger.warning("No risk-free rate data, using raw returns")

    # Initialize ESG Factor Builder
    logger.info("")
    logger.info("Initializing ESG Factor Builder...")
    factor_builder = ESGFactorBuilder(
        universe=universe,
        quantile=args.quantile,
        sector_neutral=args.sector_neutral,
        lag_signal=1,
    )

    # Build factors
    logger.info("Building ESG factors...")
    logger.info("")

    factor_df = factor_builder.build_factors(
        prices_df=prices_df,
        rf_df=rf_df,
        esg_df=esg_panel,
        weights_df=None,  # Equal-weighted
        sector_map=None,  # TODO: add sector mapping if needed
        save=True,
    )

    # Display results
    logger.info("")
    logger.info("=" * 60)
    logger.info("FACTOR RETURNS SUMMARY")
    logger.info("=" * 60)

    summary = factor_builder.get_factor_summary(factor_df)
    print("\nFactor Statistics (Annualized):")
    print(summary.to_string())

    # Show recent factor returns
    print("\nRecent Factor Returns (last 10 months):")
    print(factor_df.tail(10).to_string())

    # Correlation matrix
    print("\nFactor Correlation Matrix:")
    print(factor_df.corr().to_string())

    # Cumulative returns
    cumulative = (1 + factor_df).cumprod()
    print("\nCumulative Returns:")
    print(f"  Start: {cumulative.iloc[0].to_dict()}")
    print(f"  End:   {cumulative.iloc[-1].to_dict()}")

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ Factor construction complete!")
    logger.info(
        f"   Results saved to: {factor_builder.factors_dir / 'esg_factors.parquet'}"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
