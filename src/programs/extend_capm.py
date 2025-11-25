"""
Extended CAPM with ESG Factor - Expected Returns Calculator

Calculates expected returns using the Extended CAPM formula:
    E[R_i,t] = RF_t + β_market * λ_market + β_ESG * λ_ESG

Where:
    - RF_t: Risk-free rate at time t (monthly decimal)
    - β_market, β_ESG: Factor exposures from two_factor_regression.py
    - λ_market, λ_ESG: Factor premia (HAC-robust means)

Features:
    - Load pre-computed betas from two_factor_regression results
    - Estimate factor premia using HAC-robust means (Newey-West)
    - Generate time-series of expected returns for each stock
    - Support both rolling and full-sample betas
    - Proper handling of time-varying risk-free rates
    - Compound annualization for realistic return calculations

Statistical Rigor:
    - HAC-robust factor premia correct for autocorrelation and heteroskedasticity
    - Compound annualization: (1 + ER_monthly)^12 - 1
    - Consistent with HAC standard errors in two_factor_regression.py

Usage:
    python src/programs/extend_capm.py --continuous-esg-only --start-date 2016-02-29 --end-date 2024-12-31

Academic References:
    - Sharpe (1964): "Capital asset prices: A theory of market equilibrium"
    - Pastor, Stambaugh & Taylor (2021): "Sustainable investing in equilibrium"
    - Newey & West (1994): "Automatic lag selection in covariance matrix estimation"
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

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import RiskFreeRateManager
from universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------
# 1) Factor premia (lambda) via HAC-robust means
# -------------------------------------------------------


def hac_factor_mean(x: pd.Series, lags: int = 12) -> Tuple[float, float, float]:
    """
    Estimate factor mean using HAC-robust standard errors

    This approach corrects for autocorrelation and heteroskedasticity in
    factor returns, providing more reliable estimates than simple means.

    Args:
        x: Time-series of factor returns (monthly decimal)
        lags: Number of lags for Newey-West correction (default: 12 for monthly data)

    Returns:
        Tuple of (mean, standard_error, t_statistic)
    """
    X = np.ones((len(x), 1))  # Constant only (intercept-only regression)
    model = sm.OLS(x.values, X).fit(cov_type="HAC", cov_kwds={"maxlags": lags})

    mean = float(model.params[0])
    std_err = float(model.bse[0])
    t_stat = float(model.tvalues[0])

    return mean, std_err, t_stat


def estimate_factor_premia(
    market_excess: pd.Series,
    esg_factor: pd.Series,
    shrinkage_weight: float = 0.5,
    historical_market_premium: float = 0.005,
    historical_esg_premium: float = 0.0,
    apply_shrinkage: bool = True,
) -> Tuple[float, float]:
    """
    Estimate factor premia (lambda) using HAC-robust means with optional shrinkage

    Uses Newey-West (HAC) standard errors to correct for:
    - Autocorrelation in factor returns (momentum, mean reversion)
    - Heteroskedasticity (volatility clustering)

    SAMPLE PERIOD BIAS CORRECTION (Option 2):
    Short sample periods (e.g., 2016-2024) can overestimate long-term premia.
    Shrinkage blends sample estimate with historical long-term average:

        λ_adjusted = w × λ_historical + (1-w) × λ_sample

    where w is shrinkage_weight (default 0.5 = equal weighting).

    Historical equity premium: ~6% annual (0.005 monthly) based on long-term data.
    Historical ESG premium: ~0% (assume zero, as ESG factor is recent).

    λ_market = E[MKT_t] (HAC-robust mean of market excess return)
    λ_ESG = E[ESG_t] (HAC-robust mean of ESG factor return)

    Args:
        market_excess: Time-series of market excess returns (monthly decimal)
        esg_factor: Time-series of ESG factor returns (monthly decimal)
        shrinkage_weight: Weight on historical mean (0-1). 0=sample only, 1=historical only
        historical_market_premium: Long-term historical equity premium (monthly decimal)
        historical_esg_premium: Long-term historical ESG premium (monthly decimal)
        apply_shrinkage: Whether to apply shrinkage (False = use raw sample estimates)

    Returns:
        Tuple of (lambda_market, lambda_ESG) in monthly decimals

    Note:
        maxlags=12 follows Newey & West (1994) rule for monthly data,
        capturing annual seasonality and autocorrelation patterns.
    """
    df = pd.concat(
        [market_excess.rename("MKT"), esg_factor.rename("ESG")], axis=1
    ).dropna()

    if df.empty:
        raise ValueError("No overlapping dates for factor premia estimation")

    # Estimate HAC-robust means (sample estimates)
    lambda_market_sample, se_market, t_market = hac_factor_mean(df["MKT"], lags=12)
    lambda_ESG_sample, se_ESG, t_ESG = hac_factor_mean(df["ESG"], lags=12)

    logger.info(f"Factor Premia (Sample, HAC-robust, monthly decimals):")
    logger.info(
        f"  λ_market (sample) = {lambda_market_sample:.6f} ({lambda_market_sample*12*100:.2f}% annualized)"
    )
    logger.info(f"    SE = {se_market:.6f}, t-stat = {t_market:.2f}")
    logger.info(
        f"  λ_ESG (sample)    = {lambda_ESG_sample:.6f} ({lambda_ESG_sample*12*100:.2f}% annualized)"
    )
    logger.info(f"    SE = {se_ESG:.6f}, t-stat = {t_ESG:.2f}")
    logger.info(f"  Based on {len(df)} monthly observations")

    # Apply shrinkage toward historical long-term mean (Option 2)
    if apply_shrinkage:
        w = shrinkage_weight
        lambda_market = w * historical_market_premium + (1 - w) * lambda_market_sample
        lambda_ESG = w * historical_esg_premium + (1 - w) * lambda_ESG_sample

        logger.info(f"\nShrinkage Applied (weight={w:.2f}):")
        logger.info(
            f"  λ_market (adjusted) = {lambda_market:.6f} ({lambda_market*12*100:.2f}% annualized)"
        )
        logger.info(
            f"    Historical: {historical_market_premium:.6f}, Sample: {lambda_market_sample:.6f}"
        )
        logger.info(
            f"  λ_ESG (adjusted)    = {lambda_ESG:.6f} ({lambda_ESG*12*100:.2f}% annualized)"
        )
        logger.info(
            f"    Historical: {historical_esg_premium:.6f}, Sample: {lambda_ESG_sample:.6f}"
        )
    else:
        lambda_market = lambda_market_sample
        lambda_ESG = lambda_ESG_sample
        logger.info("\nNo shrinkage applied (using raw sample estimates)")

    return lambda_market, lambda_ESG


# -------------------------------------------------------------
# 2) Load pre-computed betas from two_factor_regression results
# -------------------------------------------------------------


def load_regression_betas(
    data_root: Path,
    tickers: List[str],
    use_latest: bool = True,
) -> pd.DataFrame:
    """
    Load pre-computed betas from two_factor_regression.py results

    Args:
        data_root: Root data directory
        tickers: List of ticker symbols
        use_latest: If True, use latest beta estimate from rolling window;
                   If False, use all time-series observations

    Returns:
        DataFrame with columns: ticker, date, beta_market, beta_ESG
        If use_latest=True: one row per ticker (latest estimate)
        If use_latest=False: multiple rows per ticker (time-series)
    """
    results_dir = data_root / "results" / "two_factor_regression"

    all_betas = []
    missing_tickers = []

    for ticker in tickers:
        results_file = (
            results_dir / f"ticker={ticker}" / "two_factor_regression.parquet"
        )

        if not results_file.exists():
            missing_tickers.append(ticker)
            continue

        try:
            df = pd.read_parquet(results_file)
            df["date"] = pd.to_datetime(df["date"])

            if use_latest and len(df) > 1:
                # Use latest estimate from rolling window
                latest = df.iloc[[-1]][
                    ["ticker", "date", "beta_market", "beta_esg"]
                ].copy()
                all_betas.append(latest)
            else:
                # Use all observations (full time-series)
                betas = df[["ticker", "date", "beta_market", "beta_esg"]].copy()
                all_betas.append(betas)

        except Exception as e:
            logger.error(f"Error loading betas for {ticker}: {e}")
            missing_tickers.append(ticker)

    if missing_tickers:
        logger.warning(
            f"Missing beta results for {len(missing_tickers)} tickers: {missing_tickers[:10]}..."
        )

    if not all_betas:
        raise ValueError("No beta results found for any ticker")

    betas_df = pd.concat(all_betas, ignore_index=True)
    betas_df = betas_df.rename(columns={"beta_esg": "beta_ESG"})

    logger.info(f"Loaded betas for {len(betas_df['ticker'].unique())} tickers")
    if use_latest:
        logger.info(f"  Using latest beta estimates (1 per ticker)")
    else:
        logger.info(f"  Using full time-series ({len(betas_df)} total observations)")

    return betas_df


# ---------------------------------------------------------
# 3) Expected returns under Extended CAPM with ESG factor
# ---------------------------------------------------------


def calculate_expected_returns(
    betas_df: pd.DataFrame,
    rf_df: pd.DataFrame,
    lambda_market: float,
    lambda_ESG: float,
    start_date: str,
    end_date: str,
    cap_betas: bool = True,
    beta_market_cap: float = 3.0,
    beta_esg_cap: float = 5.0,
) -> pd.DataFrame:
    """
    Calculate expected returns using Extended CAPM formula with optional beta capping

    E[R_i,t] = RF_t + β_market * λ_market + β_ESG * λ_ESG

    EXTREME BETA HANDLING (Option 3):
    Stocks with extreme betas (e.g., β_market=3.2, β_ESG=-7.7) create unrealistic
    expected returns when combined with factor premia. Capping/winsorization prevents
    outlier leverage:

        β_market_capped = clip(β_market, -beta_market_cap, +beta_market_cap)
        β_ESG_capped = clip(β_ESG, -beta_esg_cap, +beta_esg_cap)

    Default caps: β_market ∈ [-3, 3], β_ESG ∈ [-5, 5]
    This retains 95%+ of typical betas while preventing extreme cases.

    Args:
        betas_df: DataFrame with columns [ticker, date, beta_market, beta_ESG]
                 If using latest betas, date represents estimate date
        rf_df: DataFrame with columns [date, RF] (monthly decimal)
        lambda_market: Market factor premium (monthly decimal)
        lambda_ESG: ESG factor premium (monthly decimal)
        start_date: Start date for expected returns (YYYY-MM-DD)
        end_date: End date for expected returns (YYYY-MM-DD)
        cap_betas: Whether to cap extreme betas (default True)
        beta_market_cap: Cap for market beta (default 3.0)
        beta_esg_cap: Cap for ESG beta (default 5.0)

    Returns:
        DataFrame with columns: [ticker, date, beta_market, beta_ESG,
                                 beta_market_capped, beta_ESG_capped,
                                 RF, ER_monthly, ER_annual]
        where ER = Expected Return

        Note:
            ER_annual uses compound annualization: (1 + ER_monthly)^12 - 1
            This properly reflects return compounding, unlike simple ×12 scaling.
            For consistency with volatility scaling (√12), keep internal calculations
            in monthly units and compound only for display/reporting.
    """
    # Filter RF to date range
    rf = rf_df.copy()
    rf["date"] = pd.to_datetime(rf["date"])
    rf = rf[(rf["date"] >= start_date) & (rf["date"] <= end_date)]

    results = []

    for ticker in betas_df["ticker"].unique():
        ticker_betas = betas_df[betas_df["ticker"] == ticker].copy()

        if len(ticker_betas) == 1:
            # Use constant betas across all dates
            beta_m = ticker_betas["beta_market"].iloc[0]
            beta_esg = ticker_betas["beta_ESG"].iloc[0]
            beta_date = ticker_betas["date"].iloc[0]

            # Cap betas to prevent extreme leverage (Option 3)
            beta_m_capped = (
                np.clip(beta_m, -beta_market_cap, beta_market_cap)
                if cap_betas
                else beta_m
            )
            beta_esg_capped = (
                np.clip(beta_esg, -beta_esg_cap, beta_esg_cap)
                if cap_betas
                else beta_esg
            )

            # Apply to all dates in range
            for _, row in rf.iterrows():
                er_monthly = (
                    row["RF"]
                    + beta_m_capped * lambda_market
                    + beta_esg_capped * lambda_ESG
                )
                er_annual = (1 + er_monthly) ** 12 - 1  # Compound annualization

                results.append(
                    {
                        "ticker": ticker,
                        "date": row["date"],
                        "beta_date": beta_date,  # When beta was estimated
                        "beta_market": beta_m,
                        "beta_ESG": beta_esg,
                        "beta_market_capped": beta_m_capped,
                        "beta_ESG_capped": beta_esg_capped,
                        "RF": row["RF"],
                        "ER_monthly": er_monthly,
                        "ER_annual": er_annual,
                    }
                )
        else:
            # Use time-varying betas (align by date)
            ticker_betas = ticker_betas.set_index("date")
            for _, row in rf.iterrows():
                # Find beta estimate at or before this date
                valid_betas = ticker_betas[ticker_betas.index <= row["date"]]
                if len(valid_betas) == 0:
                    continue  # No beta estimate available yet

                # Use most recent beta
                latest_beta = valid_betas.iloc[-1]
                beta_m = latest_beta["beta_market"]
                beta_esg = latest_beta["beta_ESG"]
                beta_date = latest_beta.name

                # Cap betas to prevent extreme leverage (Option 3)
                beta_m_capped = (
                    np.clip(beta_m, -beta_market_cap, beta_market_cap)
                    if cap_betas
                    else beta_m
                )
                beta_esg_capped = (
                    np.clip(beta_esg, -beta_esg_cap, beta_esg_cap)
                    if cap_betas
                    else beta_esg
                )

                er_monthly = (
                    row["RF"]
                    + beta_m_capped * lambda_market
                    + beta_esg_capped * lambda_ESG
                )
                er_annual = (1 + er_monthly) ** 12 - 1  # Compound annualization

                results.append(
                    {
                        "ticker": ticker,
                        "date": row["date"],
                        "beta_date": beta_date,
                        "beta_market": beta_m,
                        "beta_ESG": beta_esg,
                        "beta_market_capped": beta_m_capped,
                        "beta_ESG_capped": beta_esg_capped,
                        "RF": row["RF"],
                        "ER_monthly": er_monthly,
                        "ER_annual": er_annual,
                    }
                )

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(["ticker", "date"])

    logger.info(
        f"Calculated expected returns for {len(results_df['ticker'].unique())} tickers"
    )
    logger.info(
        f"  Date range: {results_df['date'].min()} to {results_df['date'].max()}"
    )
    logger.info(f"  Total observations: {len(results_df)}")

    return results_df


# ------------------------------------------------------
# 4) Main pipeline
# ------------------------------------------------------


class ExpectedReturnsCalculator:
    """Calculate expected returns using Extended CAPM with ESG factor"""

    def __init__(
        self,
        universe: SP500Universe,
        rf_rate_type: str = "3month",
    ):
        """
        Initialize expected returns calculator

        Args:
            universe: Universe instance for data access
            rf_rate_type: Risk-free rate type (default: "3month")
        """
        self.universe = universe
        self.rf_rate_type = rf_rate_type
        self.logger = logging.getLogger(__name__)

        self.data_root = Path(universe.data_root)
        self.results_dir = self.data_root / "results" / "expected_returns"
        self.results_dir.mkdir(parents=True, exist_ok=True)

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

    def clean_results(self) -> None:
        """Delete existing result files before generating new ones"""
        output_file = self.results_dir / "expected_returns.parquet"
        if output_file.exists():
            output_file.unlink()
            self.logger.info(f"Cleaned existing results: {output_file}")

    def _load_risk_free_rate(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Load risk-free rate data"""
        self.logger.info(f"Loading risk-free rate ({self.rf_rate_type})")

        rf_data = self.rf_manager.load_risk_free_rate(
            start_date=start_date,
            end_date=end_date,
            rate_type=self.rf_rate_type,
            frequency="monthly",
        )

        rf_df = rf_data.copy()
        rf_df["date"] = pd.to_datetime(rf_df["date"])
        rf_df["RF"] = rf_df["rate"] / 100 / 12  # Annual % → monthly decimal
        rf_df = rf_df[["date", "RF"]]

        self.logger.info(f"Loaded {len(rf_df)} RF observations")
        return rf_df

    def _load_market_returns(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Load market (SPY) returns"""
        self.logger.info("Loading market (SPY) returns")

        market_path = self.data_root / "curated" / "references" / "ticker=SPY"
        price_dir = market_path / "prices" / "freq=monthly"

        if not price_dir.exists():
            raise FileNotFoundError(f"SPY data not found: {price_dir}")

        # Load all years
        all_data = []
        for year_dir in sorted(price_dir.glob("year=*")):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                all_data.append(df)

        if not all_data:
            raise ValueError(f"No market data found in {price_dir}")

        df = pd.concat(all_data, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        df["market_return"] = df["adj_close"].pct_change()

        # Calculate excess returns
        rf_df = self._load_risk_free_rate(start_date, end_date)
        market_df = df[["date", "market_return"]].dropna()
        market_df = market_df.merge(rf_df, on="date", how="inner")
        market_df["market_excess"] = market_df["market_return"] - market_df["RF"]

        self.logger.info(f"Loaded {len(market_df)} market excess returns")
        return market_df[["date", "market_excess"]].set_index("date")["market_excess"]

    def _load_esg_factors(self) -> pd.DataFrame:
        """Load ESG factor returns"""
        self.logger.info("Loading ESG factors")

        factors_file = (
            self.data_root / "results" / "esg_factors" / "esg_factors.parquet"
        )

        if not factors_file.exists():
            raise FileNotFoundError(
                f"ESG factors not found: {factors_file}\n"
                f"Run build_esg_factors.py first"
            )

        df = pd.read_parquet(factors_file)
        df.index = pd.to_datetime(df.index)

        self.logger.info(f"Loaded {len(df)} ESG factor observations")
        return df["ESG_factor"]

    def calculate(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        use_latest_betas: bool = True,
        apply_shrinkage: bool = True,
        shrinkage_weight: float = 0.5,
        historical_market_premium: float = 0.005,
        historical_esg_premium: float = 0.0,
        cap_betas: bool = True,
        beta_market_cap: float = 3.0,
        beta_esg_cap: float = 5.0,
    ) -> pd.DataFrame:
        """
        Calculate expected returns for list of tickers

        Args:
            tickers: List of ticker symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            use_latest_betas: Use latest beta estimate (True) or time-series (False)
            apply_shrinkage: Apply shrinkage to factor premia (Option 2)
            shrinkage_weight: Weight on historical mean (0-1, default 0.5)
            historical_market_premium: Long-term equity premium (monthly, default 0.005)
            historical_esg_premium: Long-term ESG premium (monthly, default 0.0)
            cap_betas: Cap extreme betas (Option 3)
            beta_market_cap: Cap for market beta (default 3.0)
            beta_esg_cap: Cap for ESG beta (default 5.0)

        Returns:
            DataFrame with expected returns for each ticker and date
        """
        self.logger.info(f"Calculating expected returns for {len(tickers)} tickers")
        self.logger.info(
            f"  Shrinkage: {apply_shrinkage} (weight={shrinkage_weight:.2f})"
        )
        self.logger.info(
            f"  Beta capping: {cap_betas} (market=±{beta_market_cap:.1f}, ESG=±{beta_esg_cap:.1f})"
        )

        # 1. Load factor data
        market_excess = self._load_market_returns(start_date, end_date)
        esg_factor = self._load_esg_factors()
        rf_df = self._load_risk_free_rate(start_date, end_date)

        # 2. Estimate factor premia with shrinkage (Option 2)
        lambda_market, lambda_ESG = estimate_factor_premia(
            market_excess=market_excess,
            esg_factor=esg_factor,
            shrinkage_weight=shrinkage_weight,
            historical_market_premium=historical_market_premium,
            historical_esg_premium=historical_esg_premium,
            apply_shrinkage=apply_shrinkage,
        )

        # 3. Load pre-computed betas
        betas_df = load_regression_betas(
            data_root=self.data_root,
            tickers=tickers,
            use_latest=use_latest_betas,
        )

        # 4. Calculate expected returns with beta capping (Option 3)
        results = calculate_expected_returns(
            betas_df=betas_df,
            rf_df=rf_df,
            lambda_market=lambda_market,
            lambda_ESG=lambda_ESG,
            start_date=start_date,
            end_date=end_date,
            cap_betas=cap_betas,
            beta_market_cap=beta_market_cap,
            beta_esg_cap=beta_esg_cap,
        )

        return results

    def save_results(self, results: pd.DataFrame) -> None:
        """Save expected returns to parquet"""
        # Clean existing results before saving new ones
        self.clean_results()

        output_file = self.results_dir / "expected_returns.parquet"
        results.to_parquet(output_file, index=False)
        self.logger.info(f"Saved expected returns to {output_file}")

    def display_summary(self, results: pd.DataFrame) -> None:
        """Display summary statistics"""
        print("\n" + "=" * 80)
        print("EXPECTED RETURNS SUMMARY (Extended CAPM with ESG)")
        print("=" * 80 + "\n")

        print(f"Tickers: {len(results['ticker'].unique())}")
        print(f"Date range: {results['date'].min()} to {results['date'].max()}")
        print(f"Total observations: {len(results)}")

        # Check if beta capping was applied
        if (
            "beta_market_capped" in results.columns
            and "beta_ESG_capped" in results.columns
        ):
            n_market_capped = (
                results["beta_market"] != results["beta_market_capped"]
            ).sum()
            n_esg_capped = (results["beta_ESG"] != results["beta_ESG_capped"]).sum()

            if n_market_capped > 0 or n_esg_capped > 0:
                print(f"\nBeta Capping Applied:")
                print(f"  Market betas capped: {n_market_capped} observations")
                print(f"  ESG betas capped: {n_esg_capped} observations")

        print("\nExpected Returns Statistics (Annualized %):")
        summary = (
            results.groupby("ticker")["ER_annual"]
            .agg(
                [
                    ("Mean", lambda x: x.mean() * 100),
                    ("Std Dev", lambda x: x.std() * 100),
                    ("Min", lambda x: x.min() * 100),
                    ("Max", lambda x: x.max() * 100),
                ]
            )
            .round(2)
        )

        # Overall statistics
        print(f"\nOverall Statistics:")
        print(f"  Mean ER (annual): {results['ER_annual'].mean() * 100:.2f}%")
        print(f"  Median ER (annual): {results['ER_annual'].median() * 100:.2f}%")
        print(f"  Std ER (annual): {results['ER_annual'].std() * 100:.2f}%")
        print(f"  Min ER (annual): {results['ER_annual'].min() * 100:.2f}%")
        print(f"  Max ER (annual): {results['ER_annual'].max() * 100:.2f}%")

        print("\nSample of Ticker-Level Statistics:")
        print(summary.head(20).to_string())

        print("\nTop 10 Highest Expected Returns (Latest):")
        latest = (
            results.groupby("ticker").last().sort_values("ER_annual", ascending=False)
        )

        if "beta_market_capped" in latest.columns:
            top10 = latest[
                [
                    "beta_market",
                    "beta_market_capped",
                    "beta_ESG",
                    "beta_ESG_capped",
                    "ER_annual",
                ]
            ].head(10)
        else:
            top10 = latest[["beta_market", "beta_ESG", "ER_annual"]].head(10)

        top10["ER_annual"] = top10["ER_annual"] * 100
        print(top10.to_string())


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Extended CAPM Expected Returns Calculator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--universe",
        type=str,
        default="SP500",
        help="Universe for processing (default: SP500)",
    )
    parser.add_argument(
        "--continuous-esg-only",
        action="store_true",
        help="Only process tickers with continuous ESG data",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2016-02-29",
        help="Start date (YYYY-MM-DD, default: 2016-02-29)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2024-12-31",
        help="End date (YYYY-MM-DD, default: 2024-12-31)",
    )
    parser.add_argument(
        "--use-latest-betas",
        action="store_true",
        default=True,
        help="Use latest beta estimates (default: True)",
    )
    parser.add_argument(
        "--use-timeseries-betas",
        action="store_false",
        dest="use_latest_betas",
        help="Use time-series of betas (rolling)",
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
        help="Save results (default: True)",
    )

    args = parser.parse_args()

    # Initialize components
    logger.info("Initializing components...")
    config = Config("config/settings.yaml")
    universe = SP500Universe(config.get("storage.local.root_path"))

    # Load expected returns configuration
    er_config = config.get("expected_returns", {})
    premia_shrinkage = er_config.get("premia_shrinkage", {})
    beta_caps = er_config.get("beta_caps", {})

    # Initialize calculator
    calculator = ExpectedReturnsCalculator(
        universe=universe,
        rf_rate_type=args.rf_rate_type,
    )

    # Determine tickers to process
    logger.info(f"Loading {args.universe} universe members...")
    if args.continuous_esg_only:
        continuous_file = Path("data/continuous_esg_tickers.txt")
        if continuous_file.exists():
            with open(continuous_file) as f:
                tickers = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(tickers)} tickers with continuous ESG data")
        else:
            logger.error(f"Continuous ESG file not found: {continuous_file}")
            sys.exit(1)
    else:
        membership = universe.get_current_members()
        tickers = membership["ticker"].unique().tolist()
        logger.info(f"Loaded {len(tickers)} universe members")

    # Calculate expected returns with shrinkage and capping
    results = calculator.calculate(
        tickers=tickers,
        start_date=args.start_date,
        end_date=args.end_date,
        use_latest_betas=args.use_latest_betas,
        apply_shrinkage=premia_shrinkage.get("enabled", True),
        shrinkage_weight=premia_shrinkage.get("weight", 0.5),
        historical_market_premium=premia_shrinkage.get(
            "historical_market_premium", 0.005
        ),
        historical_esg_premium=premia_shrinkage.get("historical_esg_premium", 0.0),
        cap_betas=beta_caps.get("enabled", True),
        beta_market_cap=beta_caps.get("market_cap", 3.0),
        beta_esg_cap=beta_caps.get("esg_cap", 5.0),
    )

    # Display summary
    calculator.display_summary(results)

    # Save results
    if args.save:
        calculator.save_results(results)

    logger.info("Completed expected returns calculation")


if __name__ == "__main__":
    main()
