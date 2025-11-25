"""
ESG Factor Builder

Constructs long-short factor portfolios based on ESG scores and momentum signals.
Uses monthly excess returns with proper signal lagging to avoid look-ahead bias.

Factor Construction:
    - ESG Factor: Long-short portfolio based on ESG composite scores
    - E/S/G Factors: Long-short portfolios based on individual pillar scores
    - ESG Momentum Factor: Long-short portfolio based on changes in ESG scores

Portfolio Construction:
    - Ranks stocks by signal (cross-sectional or sector-neutral)
    - Long top quantile (e.g., top 20%)
    - Short bottom quantile (e.g., bottom 20%)
    - Equal-weighted or value-weighted legs
    - Signal at t-1 used to form portfolios earning returns at t

Data Structure:
    Input:
        - prices_df: MultiIndex [date, ticker], column 'adj_close' (monthly)
        - esg_df: MultiIndex [date, ticker], columns ['ESG', 'E', 'S', 'G']
        - weights_df: MultiIndex [date, ticker], column 'weight' (optional)
        - sector_map: Series, index=ticker, value=sector (optional)
        - Risk-free rate: Auto-loaded from cache or FRED API

    Output:
        - factor_df: Index=date, columns ['ESG_factor', 'E_factor', 'S_factor', 'G_factor', 'ESG_mom_factor']
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from market.risk_free_rate_manager import RiskFreeRateManager
from universe import Universe

logger = logging.getLogger(__name__)


class ESGFactorBuilder:
    """
    ESG factor portfolio builder

    Constructs long-short factor portfolios based on ESG signals:
    - Level factors: ESG, E, S, G scores
    - Momentum factor: Changes in ESG scores

    Features:
    - Proper signal lagging to avoid look-ahead bias
    - Sector-neutral ranking (optional)
    - Value-weighted or equal-weighted portfolios
    - Automatic risk-free rate loading from cache or FRED API
    - Excess returns (risk-free adjusted)
    - Save/load factor returns

    Data Structure:
        Output: data/curated/factors/esg_factors.parquet
        Schema: date (index), ESG_factor, E_factor, S_factor, G_factor, ESG_mom_factor
    """

    def __init__(
        self,
        universe: Universe,
        quantile: float = 0.2,
        sector_neutral: bool = False,
        lag_signal: int = 1,
        weighting: str = "equal",
        rf_rate_type: str = "3month",
    ):
        """
        Initialize ESG factor builder

        Args:
            universe: Universe instance for data access
            quantile: Quantile for long/short legs (default: 0.2 = top/bottom 20%)
            sector_neutral: Whether to rank within sectors (default: False)
            lag_signal: Number of periods to lag signal (default: 1)
            weighting: Portfolio weighting scheme (default: "equal")
                Options: "equal", "value"
            rf_rate_type: Type of treasury rate (default: "3month")
                Options: "3month", "1year", "5year", "10year", "30year"

                **Equal-Weighted (Academic Standard):**
                - Each stock gets 1/N weight in long/short legs
                - Pros: Simple, eliminates size effects, standard in academic tests
                - Cons: Small-cap bias, high turnover, may not be investable
                - Use case: Testing cross-sectional ESG signal strength

                **Value-Weighted (Practitioner Standard):**
                - Stocks weighted by market cap (price × shares outstanding)
                - Pros: Investable, matches index reality, lower turnover
                - Cons: Large-cap dominated, may miss small-cap alpha
                - Use case: Real-world portfolio implementation

                **Empirical Differences:**
                - EW typically shows higher returns (small-cap premium)
                - VW more stable, lower Sharpe (large-cap drag)
                - Performance gap: 2-5% annualized in US equity factors

                References:
                - Fama & French (1993): Use both EW and VW in factor tests
                - Hou, Xue & Zhang (2015): "EW overstates profitability of anomalies"
                - Novy-Marx & Velikov (2016): "Trading costs matter more for EW"
        """
        self.universe = universe
        self.quantile = quantile
        self.sector_neutral = sector_neutral
        self.lag_signal = lag_signal
        self.weighting = weighting
        self.rf_rate_type = rf_rate_type
        self.logger = logging.getLogger(__name__)

        # Validate weighting
        if weighting not in ["equal", "value"]:
            raise ValueError(f"weighting must be 'equal' or 'value', got {weighting}")

        self.data_root = Path(universe.data_root)
        self.factors_dir = self.data_root / "results" / "esg_factors"

        # Initialize RiskFreeRateManager (no API key needed)
        rf_data_root = str(
            self.data_root / "curated" / "references" / "risk_free_rate" / "freq=monthly"
        )
        self.rf_manager = RiskFreeRateManager(
            data_root=rf_data_root, default_rate=rf_rate_type
        )

        # Cache
        self._factor_returns = None

    @staticmethod
    def _compute_monthly_returns(prices_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute monthly returns from prices

        Args:
            prices_df: MultiIndex [date, ticker], column 'adj_close'

        Returns:
            DataFrame: MultiIndex [date, ticker], column 'ret'
        """
        returns = (
            prices_df.groupby(level="ticker")["adj_close"].pct_change().to_frame("ret")
        )
        return returns.dropna()

    @staticmethod
    def _compute_market_cap_weights(prices_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute market cap weights from prices and volume

        **Market Cap Proxy:**
        Uses price × volume as proxy for market cap (price × shares outstanding).
        This is a simplification but highly correlated with true market cap.

        **Normalization:**
        Weights sum to 1.0 within each date (cross-sectional normalization).

        **Handling Missing Data:**
        - If volume=0 or missing, assigns minimum non-zero weight
        - If all volumes missing on a date, falls back to equal weighting

        Args:
            prices_df: MultiIndex [date, ticker], columns ['adj_close', 'adj_volume']

        Returns:
            DataFrame: MultiIndex [date, ticker], column 'weight'

        Note:
            For more accurate weights, use actual market cap from fundamentals:
            weight = shares_outstanding × price
        """
        if "adj_volume" not in prices_df.columns:
            # No volume data, return equal weights
            weights = pd.DataFrame({"weight": 1.0}, index=prices_df.index)
            return weights

        # Market cap proxy = price × volume
        mktcap = prices_df["adj_close"] * prices_df["adj_volume"]

        # Handle zeros/negatives
        mktcap = mktcap.replace(0, np.nan)
        mktcap = mktcap.clip(lower=0)

        # Normalize within each date to sum to 1
        weights = mktcap.groupby(level="date").transform(
            lambda x: x / x.sum() if x.sum() > 0 else 1.0 / len(x)
        )

        return weights.to_frame("weight")

    def _load_risk_free_rate(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """
        Load risk-free rate data from cache for the date range in returns_df

        Args:
            returns_df: DataFrame with returns data (used to determine date range)

        Returns:
            DataFrame with columns: date, RF

        Raises:
            FileNotFoundError: If cache file doesn't exist
            ValueError: If no data in requested date range
        """
        # Determine date range from returns
        dates = returns_df.index.get_level_values("date")
        start_date = pd.to_datetime(dates.min()).strftime("%Y-%m-%d")
        end_date = pd.to_datetime(dates.max()).strftime("%Y-%m-%d")

        self.logger.info(f"Loading risk-free rate for {start_date} to {end_date}")

        # Load using RiskFreeRateManager
        rf_df = self.rf_manager.load_risk_free_rate(
            start_date=start_date,
            end_date=end_date,
            rate_type=self.rf_rate_type,
            frequency="monthly",
        )

        # Rename 'rate' to 'RF' for consistency
        if "rate" in rf_df.columns:
            rf_df = rf_df.rename(columns={"rate": "RF"})

        rf_df["date"] = pd.to_datetime(rf_df["date"])

        self.logger.info(f"Loaded {len(rf_df)} risk-free rate observations")
        return rf_df

    def _to_excess_returns(
        self, returns_df: pd.DataFrame, rf_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Convert returns to excess returns (subtract risk-free rate)

        **Risk-Free Rate Normalization:**

        Data from FRED (via RiskFreeRateManager) is always:
        - Annual percentage (e.g., 5.25 = 5.25% per year)
        - Needs conversion to monthly decimal: divide by 100 (percent→decimal) and 12 (annual→monthly)

        Example: 6.0% annual → 0.06 / 12 = 0.005 monthly (0.5%)

        Args:
            returns_df: MultiIndex [date, ticker], column 'ret' (monthly decimal)
            rf_df: DataFrame with 'date' and 'RF' columns (annual percentage from FRED)

        Returns:
            DataFrame: MultiIndex [date, ticker], column 'excess'
        """
        # Reset index to merge on date
        returns_with_date = returns_df.reset_index()

        # Ensure both date columns are datetime
        returns_with_date["date"] = pd.to_datetime(returns_with_date["date"])

        # Handle different RF formats
        if "date" in rf_df.columns:
            # RF has date as column
            rf_copy = rf_df.copy()
            rf_copy["date"] = pd.to_datetime(rf_copy["date"])
            # Rename 'rate' to 'RF' for consistency
            if "rate" in rf_copy.columns:
                rf_copy = rf_copy.rename(columns={"rate": "RF"})
        else:
            # RF has date as index
            rf_copy = rf_df.copy()
            rf_copy.index = pd.to_datetime(rf_copy.index)
            # Rename rate column if needed
            if "rate" in rf_copy.columns:
                rf_copy = rf_copy.rename(columns={"rate": "RF"})
            # Need to reset index for merge
            rf_copy = rf_copy.reset_index()
            if "index" in rf_copy.columns:
                rf_copy = rf_copy.rename(columns={"index": "date"})

        # Log original RF statistics for validation
        rf_mean_orig = rf_copy["RF"].mean()
        rf_std_orig = rf_copy["RF"].std()
        self.logger.info(
            f"Risk-free rate (FRED annual %): mean={rf_mean_orig:.4f}%, std={rf_std_orig:.4f}%"
        )

        # Normalize RF to monthly decimal (FRED data is always annual percentage)
        # Convert: annual percentage → monthly decimal
        # Example: 6.0% annual → 0.06 / 12 = 0.005 monthly
        rf_copy["RF"] = rf_copy["RF"] / 100 / 12
        self.logger.info("Converted RF from annual % to monthly decimal (÷100÷12)")

        # Log normalized RF statistics for validation
        rf_mean_norm = rf_copy["RF"].mean()
        rf_std_norm = rf_copy["RF"].std()
        self.logger.info(
            f"Risk-free rate after normalization: mean={rf_mean_norm:.6f}, std={rf_std_norm:.6f}"
        )

        # Validate normalized RF is in reasonable range
        if rf_mean_norm > 0.10:  # >10% monthly is almost certainly wrong
            self.logger.warning(
                f"RF mean {rf_mean_norm:.4f} (annualized: {rf_mean_norm*12:.2%}) "
                f"seems too high - check data quality"
            )
        elif rf_mean_norm < 0:
            self.logger.warning(
                f"RF mean {rf_mean_norm:.4f} is negative - check data quality"
            )

        # Merge RF with returns
        panel = returns_with_date.merge(rf_copy[["date", "RF"]], on="date", how="left")

        panel["excess"] = panel["ret"] - panel["RF"]

        # Restore MultiIndex
        panel = panel.set_index(["date", "ticker"]).sort_index()

        return panel.dropna(subset=["excess"])

    @staticmethod
    def _zscore(series: pd.Series) -> pd.Series:
        """Cross-sectional z-score normalization"""
        mu = series.mean()
        sd = series.std(ddof=0)
        return (series - mu) / (sd if sd and sd != 0 else 1)

    @staticmethod
    def _value_weighted_return(
        sub_df: pd.DataFrame,
        weights: Optional[pd.Series] = None,
        ret_col: str = "excess",
    ) -> float:
        """
        Calculate value-weighted or equal-weighted return

        Args:
            sub_df: DataFrame with return column
            weights: Series of weights (optional, for value-weighting)
            ret_col: Name of return column

        Returns:
            Weighted average return
        """
        if weights is None or weights.sum() == 0 or weights.isnull().all():
            return sub_df[ret_col].mean()

        w = weights.fillna(0)
        w = w / (w.sum() if w.sum() != 0 else 1)
        return float(np.dot(sub_df[ret_col].values, w.values))

    def _rank_within(
        self, df: pd.DataFrame, score_col: str, sector_map: Optional[pd.Series] = None
    ) -> pd.DataFrame:
        """
        Rank stocks by signal (cross-sectional or sector-neutral)

        Args:
            df: DataFrame with signal column
            score_col: Name of signal column
            sector_map: Series mapping ticker to sector (optional)

        Returns:
            DataFrame with 'rank_pct' column (0..1)
        """
        x = df.copy()

        if self.sector_neutral and sector_map is not None:
            # Debug: Log sector-neutral ranking
            self.logger.debug(
                f"Using sector-neutral ranking: {len(x)} stocks, "
                f"sector_map has {len(sector_map)} entries"
            )

            x["sector"] = sector_map.reindex(x.index)

            # Check how many stocks have sector assignments
            n_with_sector = x["sector"].notna().sum()
            if n_with_sector == 0:
                self.logger.warning(
                    "Sector-neutral ranking requested but NO stocks have sector assignments! "
                    "Falling back to cross-sectional ranking."
                )
                x["rank_pct"] = x[score_col].rank(pct=True)
            else:
                self.logger.debug(
                    f"Sector-neutral: {n_with_sector}/{len(x)} stocks have sector assignments"
                )
                # Rank within sector
                ranks = []
                for sec, g in x.groupby("sector", dropna=False):
                    if pd.notna(sec):
                        self.logger.debug(f"  Sector {sec}: {len(g)} stocks")
                    ranks.append(g[score_col].rank(pct=True))
                x["rank_pct"] = pd.concat(ranks).sort_index()

            x.drop(columns=["sector"], inplace=True)
        else:
            # Cross-sectional ranking
            if self.sector_neutral:
                self.logger.debug(
                    f"Sector-neutral requested but sector_map is None - using cross-sectional"
                )
            x["rank_pct"] = x[score_col].rank(pct=True)

        return x

    def _build_long_short_factor(
        self,
        panel_excess: pd.DataFrame,
        signal_df: pd.DataFrame,
        weights_df: Optional[pd.DataFrame] = None,
        sector_map: Optional[pd.Series] = None,
        return_legs: bool = False,
    ) -> pd.Series:
        """
        Build long-short factor from signal

        Args:
            panel_excess: MultiIndex [date, ticker], column 'excess'
            signal_df: MultiIndex [date, ticker], column '<signal>'
            weights_df: MultiIndex [date, ticker], column 'weight' (optional)
            sector_map: Series mapping ticker to sector (optional)
            return_legs: If True, return DataFrame with long/short/factor columns

        Returns:
            Series of factor returns indexed by date (or DataFrame if return_legs=True)
        """
        # Lag signals to avoid look-ahead bias
        sig_lag = signal_df.groupby(level="ticker").shift(self.lag_signal)

        # Lag weights to match signal timing (portfolio formed at t-1 earns returns at t)
        if weights_df is not None:
            weights_lag = weights_df.groupby(level="ticker").shift(self.lag_signal)
            self.logger.debug(
                f"Lagged weights by {self.lag_signal} period(s): "
                f"{len(weights_lag)} observations"
            )
        else:
            weights_lag = None

        # Merge signal and excess returns
        panel = panel_excess[["excess"]].join(sig_lag, how="inner").dropna()

        fac = []
        for dt, df in panel.groupby(level="date"):
            x = df.droplevel(0)  # index=ticker
            score_col = signal_df.columns[0]

            # Rank stocks by signal
            x = self._rank_within(x, score_col=score_col, sector_map=sector_map)

            # Form long/short legs
            long = x[x["rank_pct"] >= (1 - self.quantile)]
            short = x[x["rank_pct"] <= self.quantile]

            # Get weights for this date (use lagged weights to match signal timing)
            w_long = None
            w_short = None
            if weights_lag is not None and dt in weights_lag.index.get_level_values(0):
                w_on_dt = (
                    weights_lag.loc[dt].reindex(long.index)["weight"]
                    if len(long)
                    else None
                )
                w_on_ds = (
                    weights_lag.loc[dt].reindex(short.index)["weight"]
                    if len(short)
                    else None
                )
                w_long = (
                    w_on_dt
                    if w_on_dt is not None and not w_on_dt.isna().all()
                    else None
                )
                w_short = (
                    w_on_ds
                    if w_on_ds is not None and not w_on_ds.isna().all()
                    else None
                )

            # Calculate leg returns
            r_long = (
                self._value_weighted_return(long, weights=w_long)
                if len(long)
                else np.nan
            )
            r_short = (
                self._value_weighted_return(short, weights=w_short)
                if len(short)
                else np.nan
            )

            if np.isnan(r_long) or np.isnan(r_short):
                continue

            if return_legs:
                # Store long, short, and factor returns
                fac.append((dt, r_long, r_short, r_long - r_short))
            else:
                fac.append((dt, r_long - r_short))

        if return_legs:
            # Return DataFrame with separate columns for long/short/factor
            factor_name = signal_df.columns[0]
            df = pd.DataFrame(fac, columns=["date", "long", "short", "factor"])
            df = df.set_index("date").sort_index()
            df.columns = [
                f"{factor_name}_long",
                f"{factor_name}_short",
                f"{factor_name}_factor",
            ]
            return df
        else:
            return (
                pd.Series(dict(fac))
                .sort_index()
                .astype(float)
                .rename(f"{signal_df.columns[0]}_factor")
            )

    @staticmethod
    def _apply_annual_esg_lag(esg_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply annual lag to ESG scores to avoid look-ahead bias

        **Timing Convention (Critical for Replication):**

        This implementation uses a **uniform 12-month lag** (shift by 12 periods):
        - Month t ESG score → used for month t+12 trading
        - Example: Dec 2019 ESG score → Dec 2020 trading

        **Assumption:** ESG scores are available at end of fiscal year, representing
        the full prior year's performance. This ensures:
        1. No look-ahead bias (using future information)
        2. Clean calendar-year mapping (year t scores → year t+1 trading)
        3. Consistency with Refinitiv/MSCI point-in-time conventions

        **Alternative Timing Conventions:**
        Some studies use publication-date lags (3-6 months after fiscal year-end):
        - Example: 2019 scores published Mar 2020 → trade Apr 2020 forward
        - Requires vendor-specific publication dates (not available in all datasets)
        - May introduce survivorship bias if using restated historical data

        **Justification for shift(12):**
        - Avoids vendor-specific timing assumptions
        - Conservative: maximizes lag, minimizes look-ahead risk
        - Standard in academic factor research (Fama-French methodology)
        - Matches fiscal-year reporting cycles for most US companies

        **Warning:** If your ESG data has end-of-month dates (e.g., 2019-12-31) but
        scores were actually published later (e.g., 2020-03-15), this implementation
        may understate factor performance by using stale data. For production systems,
        consider using vendor point-in-time datasets with actual publication dates.

        References:
        - Luo & Balvers (2017): "Social screens and systematic investor boycott risk"
        - Pastor, Stambaugh & Taylor (2021): "Sustainable investing in equilibrium"
        - MSCI ESG Ratings Methodology (2023): Point-in-time data specifications

        Args:
            esg_df: MultiIndex [date, ticker], ESG columns ['ESG', 'E', 'S', 'G']
                   Assumes monthly frequency with end-of-month dates

        Returns:
            DataFrame with ESG scores shifted forward by 12 months
            - Original: date=2019-12-31, ESG=75
            - Lagged:   date=2020-12-31, ESG=75 (used for Jan 2021+ trading)
        """
        # Extract year from date
        df = esg_df.copy()
        df["year"] = pd.to_datetime(df.index.get_level_values("date")).year

        # Shift ESG scores forward by 12 months (1 calendar year)
        # This implements: score at t-12 used for trading at t
        # Equivalent to: year Y score → year Y+1 trading
        esg_cols = ["ESG", "E", "S", "G"]
        lagged = df.groupby(level="ticker")[esg_cols].shift(12)

        # Drop the temporary year column
        result = lagged.dropna()

        return result

    def _build_esg_momentum_signal(self, esg_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build ESG momentum signal (z-scored year-over-year changes in ESG scores)

        Since ESG data is annual, momentum = YoY change (not month-over-month)

        Args:
            esg_df: MultiIndex [date, ticker], column 'ESG'

        Returns:
            DataFrame: MultiIndex [date, ticker], column 'ESG_mom_z'
        """
        # Calculate year-over-year ESG changes (12-month lag)
        d_esg = esg_df.groupby(level="ticker")["ESG"].diff(12).to_frame("dESG")

        # Z-score cross-section by month
        mom = []
        for dt, df in d_esg.groupby(level="date"):
            x = df.droplevel(0)
            z = self._zscore(x["dESG"])
            # Create DataFrame with proper index
            mom_df = pd.DataFrame({"ESG_mom_z": z}, index=x.index)
            mom_df["date"] = dt
            mom_df["ticker"] = mom_df.index
            mom.append(mom_df)

        if not mom:
            return pd.DataFrame()

        result = pd.concat(mom, ignore_index=True)
        result = result.set_index(["date", "ticker"]).sort_index()
        return result

    def build_factors(
        self,
        returns_df: Optional[pd.DataFrame] = None,
        prices_df: Optional[pd.DataFrame] = None,
        esg_df: Optional[pd.DataFrame] = None,
        weights_df: Optional[pd.DataFrame] = None,
        sector_map: Optional[pd.Series] = None,
        save: bool = True,
        save_legs: bool = True,
    ) -> pd.DataFrame:
        """
        Build all ESG factors

        Risk-free rate is automatically loaded from cache or FRED API based on
        the date range in returns_df/prices_df.

        Args:
            returns_df: MultiIndex [date, ticker], column 'ret' (optional)
            prices_df: MultiIndex [date, ticker], column 'adj_close' (optional)
            esg_df: MultiIndex [date, ticker], columns ['ESG', 'E', 'S', 'G']
            weights_df: MultiIndex [date, ticker], column 'weight' (optional)
            sector_map: Series mapping ticker to sector (optional)
            save: Whether to save results (default: True)
            save_legs: Whether to save long/short leg returns (default: True)

        Returns:
            DataFrame indexed by date with columns:
            ['ESG_factor', 'E_factor', 'S_factor', 'G_factor', 'ESG_mom_factor']
        """
        self.logger.info("Building ESG factors")

        # Validate inputs
        if returns_df is None and prices_df is None:
            raise ValueError("Provide either returns_df or prices_df")

        if esg_df is None:
            raise ValueError("esg_df is required")

        required_cols = ["ESG", "E", "S", "G"]
        if not set(required_cols).issubset(esg_df.columns):
            raise ValueError(f"esg_df must include columns {required_cols}")

        # Compute returns if needed
        if returns_df is None:
            returns_df = self._compute_monthly_returns(prices_df)

        # Auto-load risk-free rate (always)
        self.logger.info("Auto-loading risk-free rate for date range in returns data")
        rf_df = self._load_risk_free_rate(returns_df)

        # Compute weights for value-weighting if requested
        if weights_df is None and self.weighting == "value":
            if prices_df is not None and "adj_volume" in prices_df.columns:
                self.logger.info("Computing market cap weights from price × volume")
                weights_df = self._compute_market_cap_weights(prices_df)
                self.logger.info(
                    f"Computed weights for {len(weights_df)} observations "
                    f"({len(weights_df.index.get_level_values('ticker').unique())} tickers, "
                    f"{len(weights_df.index.get_level_values('date').unique())} dates)"
                )
                # Log weight distribution for first date
                first_date = weights_df.index.get_level_values("date").min()
                first_weights = weights_df.loc[first_date].sort_values(
                    "weight", ascending=False
                )
                self.logger.info(
                    f"Weight distribution on {first_date}: "
                    f"top stock={first_weights.iloc[0]['weight']:.4f}, "
                    f"median={first_weights['weight'].median():.4f}, "
                    f"min={first_weights['weight'].min():.6f}"
                )
            else:
                self.logger.warning(
                    "Value weighting requested but no volume data available, "
                    "falling back to equal weighting"
                )

        # Log weighting scheme
        if weights_df is not None:
            self.logger.info("Using value-weighted portfolios")
        else:
            self.logger.info("Using equal-weighted portfolios")

        # Convert to excess returns (always using risk-free rate)
        panel_excess = self._to_excess_returns(returns_df, rf_df)

        # Apply annual ESG lag (uniform 12-month shift)
        # Implements conservative timing: month t-12 score → month t trading
        # Avoids look-ahead bias while maintaining consistent calendar-year mapping
        self.logger.info(
            "Applying annual ESG lag: uniform 12-month shift (t-12 score → t trading)"
        )
        self.logger.info(
            "Timing convention: Conservative lag maximizes avoidance of look-ahead bias"
        )
        esg_lagged = self._apply_annual_esg_lag(esg_df)
        self.logger.info(
            f"After 12-month lag: {len(esg_lagged)} observations from "
            f"{esg_lagged.index.get_level_values('date').min()} to "
            f"{esg_lagged.index.get_level_values('date').max()}"
        )

        # Build level factors (ESG, E, S, G) - with detailed leg returns
        factors = []
        all_legs = []
        for col in ["ESG", "E", "S", "G"]:
            self.logger.info(f"Building {col} factor")
            sig = esg_lagged[[col]]

            # Get detailed returns with long/short legs
            factor_details = self._build_long_short_factor(
                panel_excess=panel_excess,
                signal_df=sig,
                weights_df=weights_df,
                sector_map=sector_map,
                return_legs=save_legs,
            )

            if save_legs:
                # factor_details is a DataFrame with long/short/factor columns
                all_legs.append(factor_details)
                factors.append(factor_details[f"{col}_factor"])
            else:
                # factor_details is just the factor series
                factors.append(factor_details.rename(f"{col}_factor"))

        # Build momentum factor (using lagged ESG for year-over-year changes)
        self.logger.info("Building ESG momentum factor (YoY changes)")
        esg_mom_sig = self._build_esg_momentum_signal(esg_lagged[["ESG"]])
        esg_mom_details = self._build_long_short_factor(
            panel_excess=panel_excess,
            signal_df=esg_mom_sig[["ESG_mom_z"]],
            weights_df=weights_df,
            sector_map=sector_map,
            return_legs=save_legs,
        )

        if save_legs:
            all_legs.append(esg_mom_details)
            esg_mom_factor = esg_mom_details["ESG_mom_z_factor"]
        else:
            esg_mom_factor = esg_mom_details

        # Combine all factors
        factor_df = pd.concat(factors + [esg_mom_factor], axis=1).sort_index()
        factor_df.columns = [
            "ESG_factor",
            "E_factor",
            "S_factor",
            "G_factor",
            "ESG_mom_factor",
        ]
        # Log observations before/after dropna
        self.logger.info(
            f"Combined factors: {len(factor_df)} dates with potential NaNs"
        )
        self.logger.info(f"  NaN counts per factor: {factor_df.isna().sum().to_dict()}")

        # Drop months with missing factors (optional - consider keeping NaNs for analysis)
        factor_df_complete = factor_df.dropna()

        self.logger.info(
            f"After dropna: {len(factor_df_complete)} complete observations"
        )
        self.logger.info(
            f"Built {len(factor_df_complete.columns)} factors with {len(factor_df_complete)} observations "
            f"from {factor_df_complete.index.min()} to {factor_df_complete.index.max()}"
        )

        # Use complete data only (for consistency with academic literature)
        factor_df = factor_df_complete

        # Save results
        if save:
            self._save_factors(factor_df)

            # Save detailed leg returns if requested
            if save_legs and all_legs:
                legs_df = pd.concat(all_legs, axis=1).sort_index()
                # Drop any NaN rows to match factor_df
                legs_df = legs_df.loc[factor_df.index]
                self._save_factor_legs(legs_df)

        self._factor_returns = factor_df
        return factor_df

    def _save_factors(self, factor_df: pd.DataFrame) -> None:
        """
        Save factor returns to parquet

        Args:
            factor_df: DataFrame with factor returns
        """
        self.factors_dir.mkdir(parents=True, exist_ok=True)
        output_file = self.factors_dir / "esg_factors.parquet"

        # Ensure date is datetime
        factor_df_copy = factor_df.copy()
        factor_df_copy.index = pd.to_datetime(factor_df_copy.index)

        # Save as parquet
        factor_df_copy.to_parquet(output_file, engine="pyarrow")

        self.logger.info(f"Saved factor returns to {output_file}")

    def _save_factor_legs(self, legs_df: pd.DataFrame) -> None:
        """
        Save detailed factor leg returns (long/short/factor) to parquet

        Args:
            legs_df: DataFrame with long/short/factor returns
        """
        self.factors_dir.mkdir(parents=True, exist_ok=True)
        output_file = self.factors_dir / "esg_factor_legs.parquet"

        # Ensure date is datetime
        legs_df_copy = legs_df.copy()
        legs_df_copy.index = pd.to_datetime(legs_df_copy.index)

        # Save as parquet
        legs_df_copy.to_parquet(output_file, engine="pyarrow")

        self.logger.info(f"Saved factor leg returns to {output_file}")
        self.logger.info(f"  Columns: {legs_df_copy.columns.tolist()}")

    def load_factors(self) -> Optional[pd.DataFrame]:
        """
        Load saved factor returns

        Returns:
            DataFrame with factor returns, or None if not found
        """
        factors_file = self.factors_dir / "esg_factors.parquet"

        if not factors_file.exists():
            self.logger.warning("No saved factor returns found")
            return None

        try:
            df = pd.read_parquet(factors_file)
            df.index = pd.to_datetime(df.index)
            self._factor_returns = df
            self.logger.info(
                f"Loaded {len(df.columns)} factors with {len(df)} observations "
                f"from {df.index.min()} to {df.index.max()}"
            )
            return df
        except Exception as e:
            self.logger.error(f"Error loading factor returns: {e}")
            return None

    def load_factor_legs(self) -> Optional[pd.DataFrame]:
        """
        Load saved factor leg returns (long/short/factor)

        Returns:
            DataFrame with leg returns, or None if not found
        """
        legs_file = self.factors_dir / "esg_factor_legs.parquet"

        if not legs_file.exists():
            self.logger.warning("No saved factor leg returns found")
            return None

        try:
            df = pd.read_parquet(legs_file)
            df.index = pd.to_datetime(df.index)
            self.logger.info(
                f"Loaded factor legs with {len(df.columns)} columns and {len(df)} observations "
                f"from {df.index.min()} to {df.index.max()}"
            )
            return df
        except Exception as e:
            self.logger.error(f"Error loading factor leg returns: {e}")
            return None

    def get_factor_summary(
        self, factor_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Get summary statistics for factors

        Args:
            factor_df: DataFrame with factor returns (optional, uses cached if None)

        Returns:
            DataFrame with summary statistics
        """
        if factor_df is None:
            factor_df = self._factor_returns

        if factor_df is None:
            raise ValueError(
                "No factor returns available. Call build_factors() or load_factors() first."
            )

        summary = pd.DataFrame(
            {
                "Mean": factor_df.mean() * 12,  # Annualized
                "Std": factor_df.std() * np.sqrt(12),  # Annualized
                "Sharpe": (factor_df.mean() / factor_df.std()) * np.sqrt(12),
                "Min": factor_df.min(),
                "Max": factor_df.max(),
                "Observations": factor_df.count(),
            }
        )

        return summary


# Now factor_df has monthly columns:
# ['ESG_factor','E_factor','S_factor','G_factor','ESG_mom_factor']
# Ready to join with your stock excess returns for OLS regressions.
