"""
Build ESG Factors

Builds long-short factor portfolios from ESG signals using the ESGFactorBuilder class.
Loads ESG and price data from saved parquet files to construct 5 ESG-based factors.

Features:
- Load ESG data from saved parquet files (automatically date-aligned)
- Load monthly price data from saved parquet files
- Build 5 factors: ESG, E, S, G, ESG Momentum
- Signal lagging to avoid look-ahead bias
- Cross-sectional or sector-neutral ranking
- Equal-weighted or value-weighted portfolios
- Save to data/curated/factors/esg_factors.parquet

Usage:
    # Quick test with specific tickers
    python src/programs/build_esg_factors.py --tickers AAPL MSFT GOOGL

    # Use continuous ESG tickers file
    python src/programs/build_esg_factors.py --continuous-esg-only

    # Custom date range
    python src/programs/build_esg_factors.py \\
        --start-date 2015-01-01 \\
        --end-date 2024-12-31 \\
        --max-tickers 50

    # Sector-neutral with value weighting
    python src/programs/build_esg_factors.py \\
        --sector-neutral \\
        --value-weighted \\
        --quantile 0.3

    # Dry run (show what would be done)
    python src/programs/build_esg_factors.py --dry-run --max-tickers 10
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from esg import ESGFactorBuilder, ESGManager
from market import RiskFreeRateManager
from universe.sp500_universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_continuous_esg_tickers(data_root: Path) -> List[str]:
    """
    Load list of tickers with continuous ESG data (no gaps)
    
    Returns:
        List of ticker symbols
    """
    ticker_file = data_root / "continuous_esg_tickers.txt"
    
    if not ticker_file.exists():
        logger.warning(f"Continuous ESG ticker file not found: {ticker_file}")
        return []
    
    with open(ticker_file, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Loaded {len(tickers)} continuous ESG tickers from {ticker_file}")
    return tickers


def load_esg_panel(
    esg_mgr: ESGManager,
    tickers: List[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Load ESG data for multiple tickers as MultiIndex panel
    
    Args:
        esg_mgr: ESGManager instance
        tickers: List of ticker symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        DataFrame: MultiIndex [date, ticker], columns ['ESG', 'E', 'S', 'G']
    """
    logger.info(f"Loading ESG data for {len(tickers)} tickers...")
    
    all_esg = []
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(tickers)} ({success_count} success, {fail_count} failed)")
        
        try:
            esg_df = esg_mgr.load_esg_data(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date
            )
            
            if esg_df is None or esg_df.empty:
                logger.debug(f"No ESG data for {ticker}")
                fail_count += 1
                continue
            
            # Ensure ticker column exists
            if 'ticker' not in esg_df.columns:
                esg_df['ticker'] = ticker
            
            all_esg.append(esg_df)
            success_count += 1
            
        except Exception as e:
            logger.warning(f"Error loading ESG data for {ticker}: {e}")
            fail_count += 1
            continue
    
    if not all_esg:
        logger.error("No ESG data loaded for any ticker")
        return pd.DataFrame()
    
    # Combine and create MultiIndex
    esg_panel = pd.concat(all_esg, ignore_index=True)
    esg_panel["date"] = pd.to_datetime(esg_panel["date"])
    
    # Note: ESGManager now returns end-of-month dates automatically
    # No date normalization needed - dates already align with price data
    
    esg_panel = esg_panel.set_index(["date", "ticker"]).sort_index()
    
    # Rename columns to match ESGFactorBuilder expectations
    column_mapping = {
        "esg_score": "ESG",
        "environmental_pillar_score": "E",
        "social_pillar_score": "S",
        "governance_pillar_score": "G"
    }
    
    esg_panel = esg_panel.rename(columns=column_mapping)
    
    # Ensure required columns exist
    required_cols = ["ESG", "E", "S", "G"]
    missing_cols = [col for col in required_cols if col not in esg_panel.columns]
    
    if missing_cols:
        logger.error(f"Missing required ESG columns: {missing_cols}")
        return pd.DataFrame()
    
    logger.info(
        f"Loaded ESG data: {len(esg_panel)} observations from "
        f"{success_count} tickers ({fail_count} failed)"
    )
    
    return esg_panel


def load_price_panel(
    data_root: Path,
    tickers: List[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Load monthly price data for multiple tickers as MultiIndex panel
    
    Args:
        data_root: Data root directory
        tickers: List of ticker symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        DataFrame: MultiIndex [date, ticker], column 'adj_close'
    """
    logger.info(f"Loading monthly prices for {len(tickers)} tickers...")
    
    all_prices = []
    success_count = 0
    fail_count = 0
    
    start_pd = pd.to_datetime(start_date)
    end_pd = pd.to_datetime(end_date)
    
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(tickers)} ({success_count} success, {fail_count} failed)")
        
        try:
            ticker_path = data_root / "curated" / "tickers" / "exchange=us" / f"ticker={ticker}"
            price_dir = ticker_path / "prices" / "freq=monthly"
            
            if not price_dir.exists():
                logger.debug(f"No price directory for {ticker}")
                fail_count += 1
                continue
            
            ticker_prices = []
            for year_dir in sorted(price_dir.glob("year=*")):
                parquet_file = year_dir / "part-000.parquet"
                if parquet_file.exists():
                    df = pd.read_parquet(parquet_file)
                    df["date"] = pd.to_datetime(df["date"])
                    df = df[(df["date"] >= start_pd) & (df["date"] <= end_pd)]
                    if not df.empty:
                        ticker_prices.append(df[["date", "adj_close"]])
            
            if not ticker_prices:
                logger.debug(f"No price data for {ticker} in date range")
                fail_count += 1
                continue
            
            # Combine years for this ticker
            ticker_df = pd.concat(ticker_prices, ignore_index=True)
            ticker_df["ticker"] = ticker
            all_prices.append(ticker_df)
            success_count += 1
            
        except Exception as e:
            logger.warning(f"Error loading prices for {ticker}: {e}")
            fail_count += 1
            continue
    
    if not all_prices:
        logger.error("No price data loaded for any ticker")
        return pd.DataFrame()
    
    # Combine and create MultiIndex
    prices_df = pd.concat(all_prices, ignore_index=True)
    prices_df["date"] = pd.to_datetime(prices_df["date"])
    prices_df = prices_df.set_index(["date", "ticker"]).sort_index()
    
    logger.info(
        f"Loaded price data: {len(prices_df)} observations from "
        f"{success_count} tickers ({fail_count} failed)"
    )
    
    return prices_df


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Build ESG factor portfolios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Data selection
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers to process (e.g., AAPL MSFT GOOGL)"
    )
    parser.add_argument(
        "--continuous-esg-only",
        action="store_true",
        help="Use only continuous ESG tickers (from data/continuous_esg_tickers.txt)"
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        help="Maximum number of tickers to process (for testing)"
    )
    
    # Date range
    parser.add_argument(
        "--start-date",
        default="2014-01-01",
        help="Start date (YYYY-MM-DD), default: 2014-01-01"
    )
    parser.add_argument(
        "--end-date",
        default="2024-12-31",
        help="End date (YYYY-MM-DD), default: 2024-12-31"
    )
    
    # Factor construction
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.2,
        help="Quantile for long/short legs (default: 0.2 = top/bottom 20%%)"
    )
    parser.add_argument(
        "--sector-neutral",
        action="store_true",
        help="Use sector-neutral ranking"
    )
    parser.add_argument(
        "--value-weighted",
        action="store_true",
        help="Use value-weighted portfolios (requires market cap data)"
    )
    
    # Execution
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without building factors"
    )
    
    args = parser.parse_args()
    
    # Header
    logger.info("=" * 80)
    logger.info("ESG FACTOR BUILDER")
    logger.info("=" * 80)
    logger.info(f"Date Range: {args.start_date} to {args.end_date}")
    logger.info(f"Quantile: {args.quantile}")
    logger.info(f"Sector Neutral: {args.sector_neutral}")
    logger.info(f"Value Weighted: {args.value_weighted}")
    if args.dry_run:
        logger.info("MODE: DRY RUN")
    logger.info("=" * 80)
    
    # Initialize
    data_root = Path(__file__).parent.parent.parent / "data"
    universe = SP500Universe(data_root=str(data_root))
    esg_mgr = ESGManager(universe)
    rf_mgr = RiskFreeRateManager(universe)
    
    # Determine ticker list
    if args.tickers:
        tickers = args.tickers
        logger.info(f"Using {len(tickers)} specified tickers")
    elif args.continuous_esg_only:
        tickers = load_continuous_esg_tickers(data_root)
        if not tickers:
            logger.error("No continuous ESG tickers found")
            sys.exit(1)
    else:
        # Default: get all available tickers with ESG data
        logger.info("Finding all tickers with ESG data...")
        all_tickers = []
        tickers_dir = data_root / "curated" / "tickers" / "exchange=us"
        if tickers_dir.exists():
            for ticker_dir in sorted(tickers_dir.glob("ticker=*")):
                ticker = ticker_dir.name.split("=")[1]
                esg_dir = ticker_dir / "esg"
                if esg_dir.exists() and list(esg_dir.glob("year=*")):
                    all_tickers.append(ticker)
        tickers = all_tickers
        logger.info(f"Found {len(tickers)} tickers with ESG data")
    
    # Limit tickers if requested
    if args.max_tickers and args.max_tickers < len(tickers):
        tickers = tickers[:args.max_tickers]
        logger.info(f"Limited to {len(tickers)} tickers (--max-tickers {args.max_tickers})")
    
    if not tickers:
        logger.error("No tickers to process")
        sys.exit(1)
    
    if args.dry_run:
        logger.info("")
        logger.info("DRY RUN - Would process the following:")
        logger.info(f"  Tickers: {len(tickers)}")
        logger.info(f"  First 20: {', '.join(tickers[:20])}")
        if len(tickers) > 20:
            logger.info(f"  ... and {len(tickers) - 20} more")
        logger.info("")
        logger.info("Exiting (dry run)")
        sys.exit(0)
    
    # Load data
    logger.info("")
    logger.info("Loading data...")
    logger.info("=" * 80)
    
    # Load ESG data
    esg_panel = load_esg_panel(esg_mgr, tickers, args.start_date, args.end_date)
    if esg_panel.empty:
        logger.error("No ESG data loaded")
        sys.exit(1)
    
    # Get tickers that have ESG data
    esg_tickers = esg_panel.index.get_level_values("ticker").unique().tolist()
    
    # Load price data (only for tickers with ESG data)
    prices_df = load_price_panel(data_root, esg_tickers, args.start_date, args.end_date)
    if prices_df.empty:
        logger.error("No price data loaded")
        sys.exit(1)
    
    # Load risk-free rate
    logger.info("Loading risk-free rate...")
    rf_df = rf_mgr.load_risk_free_rate(start_date=args.start_date, end_date=args.end_date)
    if rf_df is None or rf_df.empty:
        logger.warning("No risk-free rate data, will use raw returns")
        rf_df = None
    else:
        logger.info(f"Loaded {len(rf_df)} months of risk-free rate")
    
    # Data alignment check
    logger.info("")
    logger.info("=" * 80)
    logger.info("DATA ALIGNMENT CHECK")
    logger.info("=" * 80)
    
    price_tickers = set(prices_df.index.get_level_values("ticker").unique())
    esg_tickers_set = set(esg_panel.index.get_level_values("ticker").unique())
    common_tickers = price_tickers & esg_tickers_set
    
    price_dates = set(prices_df.index.get_level_values("date").unique())
    esg_dates = set(esg_panel.index.get_level_values("date").unique())
    common_dates = price_dates & esg_dates
    
    logger.info(f"Common tickers: {len(common_tickers)}")
    logger.info(f"Common dates: {len(common_dates)}")
    logger.info(f"ESG observations: {len(esg_panel)}")
    logger.info(f"Price observations: {len(prices_df)}")
    
    if len(common_dates) == 0:
        logger.error("No common dates between ESG and price data!")
        logger.error("Cannot build factors without aligned dates")
        sys.exit(1)
    
    # Build factors
    logger.info("")
    logger.info("=" * 80)
    logger.info("BUILDING ESG FACTORS")
    logger.info("=" * 80)
    
    factor_builder = ESGFactorBuilder(
        universe=universe,
        quantile=args.quantile,
        sector_neutral=args.sector_neutral,
        lag_signal=1
    )
    
    factor_df = factor_builder.build_factors(
        prices_df=prices_df,
        rf_df=rf_df,
        esg_df=esg_panel,
        save=True
    )
    
    # Display results
    logger.info("")
    logger.info("=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)
    
    if factor_df is None or factor_df.empty:
        logger.error("No factors generated")
        sys.exit(1)
    
    summary = factor_builder.get_factor_summary()
    print("\nFactor Statistics (Annualized):")
    print(summary.to_string())
    
    print(f"\nFactor Returns (last 10 months):")
    print(factor_df.tail(10).to_string())
    
    print("\nFactor Correlations:")
    print(factor_df.corr().to_string())
    
    # Save location
    factors_file = factor_builder.factors_dir / "esg_factors.parquet"
    logger.info("")
    logger.info(f"✅ Saved factors to: {factors_file}")
    logger.info(f"   Total observations: {len(factor_df)}")
    logger.info(f"   Date range: {factor_df.index.min()} to {factor_df.index.max()}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
