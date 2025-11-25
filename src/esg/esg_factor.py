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
        - rf_df: Index=date, column 'RF' (monthly risk-free rate, optional)
        - weights_df: MultiIndex [date, ticker], column 'weight' (optional)
        - sector_map: Series, index=ticker, value=sector (optional)

    Output:
        - factor_df: Index=date, columns ['ESG_factor', 'E_factor', 'S_factor', 'G_factor', 'ESG_mom_factor']
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

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
    - Excess returns (risk-free adjusted) or raw returns
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
    ):
        """
        Initialize ESG factor builder

        Args:
            universe: Universe instance for data access
            quantile: Quantile for long/short legs (default: 0.2 = top/bottom 20%)
            sector_neutral: Whether to rank within sectors (default: False)
            lag_signal: Number of periods to lag signal (default: 1)
        """
        self.universe = universe
        self.quantile = quantile
        self.sector_neutral = sector_neutral
        self.lag_signal = lag_signal
        self.logger = logging.getLogger(__name__)

        self.data_root = Path(universe.data_root)
        self.factors_dir = self.data_root / "curated" / "factors"

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
    def _to_excess_returns(
        returns_df: pd.DataFrame, rf_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Convert returns to excess returns (subtract risk-free rate)

        Args:
            returns_df: MultiIndex [date, ticker], column 'ret' (decimal)
            rf_df: DataFrame with 'date' and 'rate' columns (or Index=date, column 'RF')

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
            # Convert to monthly decimal if needed (from percentage)
            if rf_copy["RF"].mean() > 1:
                rf_copy["RF"] = rf_copy["RF"] / 100 / 12  # Annual % to monthly decimal
            panel = returns_with_date.merge(
                rf_copy[["date", "RF"]], on="date", how="left"
            )
        else:
            # RF has date as index
            rf_copy = rf_df.copy()
            rf_copy.index = pd.to_datetime(rf_copy.index)
            # Rename rate column if needed
            if "rate" in rf_copy.columns:
                rf_copy = rf_copy.rename(columns={"rate": "RF"})
            panel = returns_with_date.merge(
                rf_copy[["RF"]], left_on="date", right_index=True, how="left"
            )

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
            x["sector"] = sector_map.reindex(x.index)
            # Rank within sector
            ranks = []
            for sec, g in x.groupby("sector", dropna=False):
                ranks.append(g[score_col].rank(pct=True))
            x["rank_pct"] = pd.concat(ranks).sort_index()
            x.drop(columns=["sector"], inplace=True)
        else:
            x["rank_pct"] = x[score_col].rank(pct=True)

        return x

    def _build_long_short_factor(
        self,
        panel_excess: pd.DataFrame,
        signal_df: pd.DataFrame,
        weights_df: Optional[pd.DataFrame] = None,
        sector_map: Optional[pd.Series] = None,
    ) -> pd.Series:
        """
        Build long-short factor from signal

        Args:
            panel_excess: MultiIndex [date, ticker], column 'excess'
            signal_df: MultiIndex [date, ticker], column '<signal>'
            weights_df: MultiIndex [date, ticker], column 'weight' (optional)
            sector_map: Series mapping ticker to sector (optional)

        Returns:
            Series of factor returns indexed by date
        """
        # Lag signals to avoid look-ahead bias
        sig_lag = signal_df.groupby(level="ticker").shift(self.lag_signal)

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

            # Get weights for this date
            w_long = None
            w_short = None
            if weights_df is not None and dt in weights_df.index.get_level_values(0):
                w_on_dt = (
                    weights_df.loc[dt].reindex(long.index)["weight"]
                    if len(long)
                    else None
                )
                w_on_ds = (
                    weights_df.loc[dt].reindex(short.index)["weight"]
                    if len(short)
                    else None
                )
                w_long = w_on_dt if w_on_dt is not None else None
                w_short = w_on_ds if w_on_ds is not None else None

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

            fac.append((dt, r_long - r_short))

        return (
            pd.Series(dict(fac))
            .sort_index()
            .astype(float)
            .rename(f"{signal_df.columns[0]}_factor")
        )

    @staticmethod
    def _apply_annual_esg_lag(esg_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply annual lag to ESG scores

        ESG data is published annually. Standard academic approach:
        - 2019 ESG scores (published in early 2020) → used for 2020 trading
        - Shift each year's ESG data forward by 1 year

        Args:
            esg_df: MultiIndex [date, ticker], ESG columns ['ESG', 'E', 'S', 'G']

        Returns:
            DataFrame with ESG scores shifted forward by 1 year
        """
        # Extract year from date
        df = esg_df.copy()
        df["year"] = pd.to_datetime(df.index.get_level_values("date")).year

        # Shift ESG scores forward by 1 year
        # Group by ticker and year, shift by 12 months (1 year)
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
        rf_df: Optional[pd.DataFrame] = None,
        esg_df: Optional[pd.DataFrame] = None,
        weights_df: Optional[pd.DataFrame] = None,
        sector_map: Optional[pd.Series] = None,
        save: bool = True,
    ) -> pd.DataFrame:
        """
        Build all ESG factors

        Args:
            returns_df: MultiIndex [date, ticker], column 'ret' (optional)
            prices_df: MultiIndex [date, ticker], column 'adj_close' (optional)
            rf_df: Index=date, column 'RF' (optional)
            esg_df: MultiIndex [date, ticker], columns ['ESG', 'E', 'S', 'G']
            weights_df: MultiIndex [date, ticker], column 'weight' (optional)
            sector_map: Series mapping ticker to sector (optional)
            save: Whether to save results (default: True)

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

        # Convert to excess returns
        if rf_df is not None:
            panel_excess = self._to_excess_returns(returns_df, rf_df)
        else:
            panel_excess = returns_df.rename(columns={"ret": "excess"}).dropna(
                subset=["excess"]
            )

        # Apply annual ESG lag (standard academic approach)
        # Each year's ESG scores used for FOLLOWING year's trading
        self.logger.info("Applying annual ESG lag (year t ESG → year t+1 returns)")
        esg_lagged = self._apply_annual_esg_lag(esg_df)
        self.logger.info(
            f"After annual lag: {len(esg_lagged)} observations from "
            f"{esg_lagged.index.get_level_values('date').min()} to "
            f"{esg_lagged.index.get_level_values('date').max()}"
        )

        # Build level factors (ESG, E, S, G)
        factors = []
        for col in ["ESG", "E", "S", "G"]:
            self.logger.info(f"Building {col} factor")
            sig = esg_lagged[[col]]
            f = self._build_long_short_factor(
                panel_excess=panel_excess,
                signal_df=sig,
                weights_df=weights_df,
                sector_map=sector_map,
            )
            factors.append(f.rename(f"{col}_factor"))

        # Build momentum factor (using lagged ESG for year-over-year changes)
        self.logger.info("Building ESG momentum factor (YoY changes)")
        esg_mom_sig = self._build_esg_momentum_signal(esg_lagged[["ESG"]])
        esg_mom_factor = self._build_long_short_factor(
            panel_excess=panel_excess,
            signal_df=esg_mom_sig[["ESG_mom_z"]],
            weights_df=weights_df,
            sector_map=sector_map,
        )

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
