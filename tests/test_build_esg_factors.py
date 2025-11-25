"""
Quick Test: Build ESG Factors

Tests the ESG factor builder with a small sample of tickers using existing data.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from esg import ESGFactorBuilder, ESGManager
from market import RiskFreeRateManager
from universe.sp500_universe import SP500Universe

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Test configuration
TEST_TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA",
    "NVDA",
    "META",
    "JPM",
    "JNJ",
    "WMT",
]
START_DATE = "2018-01-01"  # Extended to get more observations
END_DATE = "2024-12-31"

logger.info("=" * 60)
logger.info("ESG FACTOR BUILDER - QUICK TEST")
logger.info("=" * 60)
logger.info(f"Tickers: {len(TEST_TICKERS)}")
logger.info(f"Period: {START_DATE} to {END_DATE}")
logger.info("")

# Initialize
data_root = Path(__file__).parent.parent / "data"
universe = SP500Universe(data_root=str(data_root))

# Load ESG data
logger.info("Loading ESG data...")
esg_mgr = ESGManager(universe)
all_esg = []
for ticker in TEST_TICKERS:
    esg_df = esg_mgr.load_esg_data(ticker, start_date=START_DATE, end_date=END_DATE)
    if esg_df is not None and not esg_df.empty:
        esg_df["ticker"] = ticker
        all_esg.append(esg_df)

if not all_esg:
    logger.error("No ESG data found")
    sys.exit(1)

esg_panel = pd.concat(all_esg, ignore_index=True)
esg_panel["date"] = pd.to_datetime(esg_panel["date"])

# Note: ESGManager now returns end-of-month dates automatically
# No date normalization needed - dates already align with price data

esg_panel = esg_panel.set_index(["date", "ticker"]).sort_index()

# Rename columns to match ESGFactorBuilder expectations
esg_panel = esg_panel.rename(
    columns={
        "esg_score": "ESG",
        "environmental_pillar_score": "E",
        "social_pillar_score": "S",
        "governance_pillar_score": "G",
    }
)

logger.info(
    f"✓ ESG data: {len(esg_panel)} observations from {len(esg_panel.index.get_level_values('ticker').unique())} tickers"
)

# Load monthly prices manually from parquet
logger.info("Loading monthly prices...")
all_prices = []
for ticker in TEST_TICKERS:
    ticker_path = (
        data_root / "curated" / "tickers" / f"exchange=us" / f"ticker={ticker}"
    )
    price_dir = ticker_path / "prices" / "freq=monthly"

    if not price_dir.exists():
        continue

    for year_dir in sorted(price_dir.glob("year=*")):
        parquet_file = year_dir / "part-000.parquet"
        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            df["date"] = pd.to_datetime(df["date"])
            df = df[
                (df["date"] >= pd.to_datetime(START_DATE))
                & (df["date"] <= pd.to_datetime(END_DATE))
            ]
            df = df[["date", "adj_close"]].copy()
            df["ticker"] = ticker
            all_prices.append(df)

if not all_prices:
    logger.error("No price data found")
    sys.exit(1)

prices_df = pd.concat(all_prices, ignore_index=True)
prices_df["date"] = pd.to_datetime(prices_df["date"])
prices_df = prices_df.set_index(["date", "ticker"]).sort_index()

logger.info(
    f"✓ Prices: {len(prices_df)} observations from {len(prices_df.index.get_level_values('ticker').unique())} tickers"
)

# Load risk-free rate
logger.info("Loading risk-free rate...")
rf_mgr = RiskFreeRateManager(universe)
rf_df = rf_mgr.load_risk_free_rate(start_date=START_DATE, end_date=END_DATE)

if rf_df is not None:
    # Already monthly frequency from RiskFreeRateManager
    logger.info(f"✓ Risk-free rate: {len(rf_df)} months")
else:
    logger.warning("No risk-free rate, using raw returns")

# Debug: Check data alignment
logger.info("")
logger.info("=" * 60)
logger.info("DATA ALIGNMENT CHECK")
logger.info("=" * 60)
logger.info(
    f"Prices: {prices_df.shape} - Date range: {prices_df.index.get_level_values(0).min()} to {prices_df.index.get_level_values(0).max()}"
)
logger.info(
    f"ESG: {esg_panel.shape} - Date range: {esg_panel.index.get_level_values(0).min()} to {esg_panel.index.get_level_values(0).max()}"
)
logger.info(
    f"RF: {len(rf_df)} - Date range: {rf_df['date'].min()} to {rf_df['date'].max()}"
)

# Check common tickers
price_tickers = set(prices_df.index.get_level_values("ticker").unique())
esg_tickers = set(esg_panel.index.get_level_values("ticker").unique())
common_tickers = price_tickers & esg_tickers
logger.info(f"Common tickers: {len(common_tickers)} - {sorted(common_tickers)}")

# Check common dates
price_dates = set(prices_df.index.get_level_values("date").unique())
esg_dates = set(esg_panel.index.get_level_values("date").unique())
common_dates = price_dates & esg_dates
logger.info(f"Common dates: {len(common_dates)}")
logger.info(f"Price only dates: {len(price_dates - esg_dates)}")
logger.info(f"ESG only dates: {len(esg_dates - price_dates)}")
logger.info("=" * 60)

# Build factors
logger.info("")
logger.info("Building ESG factors...")
factor_builder = ESGFactorBuilder(universe=universe, quantile=0.2, sector_neutral=False)

factor_df = factor_builder.build_factors(
    prices_df=prices_df, rf_df=rf_df, esg_df=esg_panel, save=True
)

# Display results
logger.info("")
logger.info("=" * 60)
logger.info("RESULTS")
logger.info("=" * 60)

summary = factor_builder.get_factor_summary()
print("\nFactor Statistics (Annualized):")
print(summary.to_string())

print("\nRecent Factor Returns (last 5 months):")
print(factor_df.tail(5).to_string())

print("\nFactor Correlations:")
print(factor_df.corr().to_string())

logger.info("")
logger.info(f"✅ Saved to: {factor_builder.factors_dir / 'esg_factors.parquet'}")
logger.info("=" * 60)
