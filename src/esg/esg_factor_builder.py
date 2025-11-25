"""
ESG Factor Builder

Constructs ESG factors from raw ESG scores for quantitative analysis and portfolio construction.
Provides three formation methods:
1. ESG Score Formation: Use composite ESG score directly
2. Pillar-Weighted Formation: Combine E/S/G pillars with custom weights
3. ESG Momentum Formation: Rate of ESG improvement over time

Data Structure:
- esg_score: Composite ESG score
- environmental_pillar_score: Environmental pillar score
- social_pillar_score: Social pillar score
- governance_pillar_score: Governance pillar score

Architecture:
- ESGFactorBuilder: Transforms ESG scores → tradeable factors
- ESGManager: Loads/saves ESG data (data I/O only)
- Follows Single Responsibility Principle
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd

from universe import Universe

from .esg_manager import ESGManager

logger = logging.getLogger(__name__)


class ESGFactorBuilder:
    """
    ESG Factor Builder - Constructs quantitative factors from ESG scores

    Three Formation Methods:
    1. ESG Score Formation: Use composite esg_score directly
    2. Pillar-Weighted Formation: Combine E/S/G pillars with custom weights
    3. ESG Momentum Formation: Rate of ESG improvement over time

    Factor Outputs:
    - Cross-sectional: z-scores, percentile ranks, deciles (for each period)
    - Time-series: momentum (% change), trend (slope)
    - Rankings: Long-short portfolio construction signals

    Design Pattern:
    - Uses ESGManager for data access (composition over inheritance)
    - Pure transformation logic - no data fetching
    - Stateless operations - idempotent factor construction
    """

    # Column name mappings for the actual data structure
    PILLAR_COLUMNS = {
        'E': 'environmental_pillar_score',
        'S': 'social_pillar_score',
        'G': 'governance_pillar_score'
    }

    def __init__(self, esg_manager: ESGManager, universe: Universe):
        """
        Initialize ESG Factor Builder

        Args:
            esg_manager: ESGManager instance for data access
            universe: Universe instance for membership and data root
        """
        self.esg_mgr = esg_manager
        self.universe = universe
        self.logger = logging.getLogger(__name__)

    def create_pillar_weighted_score(
        self,
        panel_df: pd.DataFrame,
        weights: Optional[Dict[str, float]] = None
    ) -> pd.DataFrame:
        """
        Formation Method 2: Create pillar-weighted ESG score

        Combines E/S/G pillars with custom weights to create a composite score.
        This allows flexibility in emphasizing different ESG dimensions.

        Args:
            panel_df: Panel DataFrame with pillar scores
            weights: Dict with pillar weights, e.g., {'E': 0.4, 'S': 0.3, 'G': 0.3}
                    If None, uses equal weights (0.333, 0.333, 0.333)

        Returns:
            DataFrame with additional 'pillar_weighted_score' column
        """
        factors = panel_df.copy()

        # Default to equal weights
        if weights is None:
            weights = {'E': 1/3, 'S': 1/3, 'G': 1/3}

        # Validate weights sum to 1
        weight_sum = sum(weights.values())
        if not np.isclose(weight_sum, 1.0, atol=0.01):
            # Normalize weights if they don't sum to 1
            self.logger.warning(f"Weights sum to {weight_sum}, normalizing to 1.0")
            total = sum(weights.values())
            weights = {k: v/total for k, v in weights.items()}

        # Check if pillar columns exist
        required_cols = [self.PILLAR_COLUMNS[k] for k in weights.keys()]
        missing = [col for col in required_cols if col not in factors.columns]
        if missing:
            self.logger.error(f"Missing pillar columns: {missing}")
            return factors

        # Calculate weighted score
        factors['pillar_weighted_score'] = 0.0
        for pillar, weight in weights.items():
            col = self.PILLAR_COLUMNS[pillar]
            # Only add if the column has data
            if col in factors.columns:
                factors['pillar_weighted_score'] += weight * factors[col].fillna(0)

        self.logger.info(
            f"✓ Pillar-weighted score created with weights: "
            f"E={weights.get('E', 0):.2f}, S={weights.get('S', 0):.2f}, G={weights.get('G', 0):.2f}"
        )

        return factors

    def calculate_momentum(
        self,
        panel_df: pd.DataFrame,
        score_column: str = 'esg_score',
        ticker_column: str = 'ticker',
        date_column: str = 'date',
        windows: List[int] = [3, 6, 12]
    ) -> pd.DataFrame:
        """
        Formation Method 3: Calculate ESG momentum factors

        Momentum captures the rate of ESG improvement over time.
        Positive momentum = improving ESG, negative = declining ESG.

        Args:
            panel_df: Panel DataFrame with scores
            score_column: Column to calculate momentum from
            ticker_column: Ticker identifier column
            date_column: Date column for time-series ordering
            windows: List of lookback windows in months (e.g., [3, 6, 12])

        Returns:
            DataFrame with momentum columns: {score_column}_momentum_{N}m
        """
        factors = panel_df.copy()
        factors = factors.sort_values([ticker_column, date_column])

        for window in windows:
            momentum_col = f'{score_column}_momentum_{window}m'

            # Calculate percent change over window
            factors[momentum_col] = (
                factors.groupby(ticker_column)[score_column]
                .pct_change(periods=window) * 100
            )

        self.logger.info(
            f"✓ Momentum calculated for '{score_column}' with windows: {windows} months"
        )

        return factors

    def calculate_cross_sectional_factors(
        self,
        panel_df: pd.DataFrame,
        score_column: str,
        date_column: str = 'date'
    ) -> pd.DataFrame:
        """
        Calculate cross-sectional factors (rankings) for each date

        Normalizes scores within each time period for relative comparison.
        Essential for long-short portfolio construction.

        Args:
            panel_df: Panel DataFrame with scores
            score_column: Column to calculate factors from
            date_column: Date column for grouping

        Returns:
            DataFrame with additional columns:
            - {score_column}_pctrank: Percentile rank (0-100)
            - {score_column}_zscore: Z-score (mean=0, std=1)
            - {score_column}_decile: Decile (1=lowest, 10=highest)
        """
        factors = panel_df.copy()

        if score_column not in factors.columns:
            self.logger.warning(f"Score column '{score_column}' not found, skipping")
            return factors

        if date_column not in factors.columns:
            raise ValueError(f"Date column '{date_column}' not found")

        # Group by date and calculate cross-sectional metrics
        for date in factors[date_column].unique():
            mask = factors[date_column] == date
            scores = factors.loc[mask, score_column]

            # Skip if insufficient data
            valid_scores = scores.dropna()
            if len(valid_scores) < 2:
                self.logger.debug(f"Insufficient data for {date}: {len(valid_scores)} observations")
                continue

            # Percentile rank (0-100)
            factors.loc[mask, f'{score_column}_pctrank'] = scores.rank(pct=True, na_option='keep') * 100

            # Z-score (mean=0, std=1)
            mean = scores.mean()
            std = scores.std()
            if std > 0:
                factors.loc[mask, f'{score_column}_zscore'] = (scores - mean) / std
            else:
                factors.loc[mask, f'{score_column}_zscore'] = 0

            # Decile assignment (1=lowest, 10=highest)
            try:
                factors.loc[mask, f'{score_column}_decile'] = pd.qcut(
                    scores, q=10, labels=range(1, 11), duplicates='drop'
                )
            except ValueError as e:
                # Handle cases with insufficient unique values
                self.logger.debug(f"Could not create deciles for {date}: {e}")
                factors.loc[mask, f'{score_column}_decile'] = pd.NA

        self.logger.info(
            f"✓ Cross-sectional factors calculated for '{score_column}' "
            f"across {factors[date_column].nunique()} periods"
        )

        return factors

    def build_factors_for_universe(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        formation_method: Literal['esg_score', 'pillar_weighted', 'momentum'] = 'esg_score',
        pillar_weights: Optional[Dict[str, float]] = None,
        momentum_windows: List[int] = [3, 6, 12],
        include_rankings: bool = True
    ) -> pd.DataFrame:
        """
        Build ESG factors using the specified formation method

        Main entry point for factor construction. Supports three formation methods:
        1. 'esg_score': Use composite ESG score directly
        2. 'pillar_weighted': Combine E/S/G pillars with custom weights
        3. 'momentum': Focus on ESG improvement rate over time

        Args:
            tickers: List of ticker symbols
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            formation_method: Formation strategy to use
            pillar_weights: Weights for pillar_weighted method, e.g., {'E': 0.5, 'S': 0.3, 'G': 0.2}
            momentum_windows: Lookback windows in months for momentum calculation
            include_rankings: Calculate cross-sectional rankings (z-scores, deciles)

        Returns:
            Panel DataFrame with factors for all tickers
        """
        self.logger.info("=" * 80)
        self.logger.info("ESG FACTOR BUILDER")
        self.logger.info("=" * 80)
        self.logger.info(f"Formation Method: {formation_method.upper()}")
        self.logger.info(f"Tickers: {len(tickers)}")
        self.logger.info(f"Period: {start_date} to {end_date}")
        if formation_method == 'pillar_weighted':
            weights_str = pillar_weights if pillar_weights else {'E': 0.33, 'S': 0.33, 'G': 0.33}
            self.logger.info(f"Pillar Weights: {weights_str}")
        elif formation_method == 'momentum':
            self.logger.info(f"Momentum Windows: {momentum_windows} months")
        self.logger.info("=" * 80)

        # Step 1: Load ESG data
        self.logger.info("\n📂 Loading ESG data...")
        esg_data_list = []

        for ticker in tickers:
            esg_df = self.esg_mgr.load_esg_data(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date
            )

            if not esg_df.empty:
                esg_data_list.append(esg_df)

        if not esg_data_list:
            self.logger.warning("No ESG data found for any ticker")
            return pd.DataFrame()

        # Combine all ESG data
        esg_panel = pd.concat(esg_data_list, ignore_index=True)

        # Ensure date is datetime
        if 'date' in esg_panel.columns:
            esg_panel['date'] = pd.to_datetime(esg_panel['date'], errors='coerce')
            # Remove rows with invalid dates
            esg_panel = esg_panel[esg_panel['date'].notna()]
            # Align to end-of-month
            esg_panel['date'] = esg_panel['date'] + pd.offsets.MonthEnd(0)

        self.logger.info(
            f"✓ Loaded ESG data: {len(esg_panel):,} records for "
            f"{esg_panel['ticker'].nunique()} tickers"
        )

        # Step 2: Apply formation method
        self.logger.info(f"\n🔄 Applying formation method: {formation_method}")

        if formation_method == 'esg_score':
            # Method 1: Use ESG score directly
            score_col = 'esg_score'
            if score_col not in esg_panel.columns:
                self.logger.error(f"Column '{score_col}' not found in data")
                return pd.DataFrame()

            factors = esg_panel.copy()
            self.logger.info(f"✓ Using '{score_col}' as base factor")

        elif formation_method == 'pillar_weighted':
            # Method 2: Create pillar-weighted score
            factors = self.create_pillar_weighted_score(esg_panel, weights=pillar_weights)
            score_col = 'pillar_weighted_score'

            if score_col not in factors.columns:
                self.logger.error("Failed to create pillar-weighted score")
                return pd.DataFrame()

        elif formation_method == 'momentum':
            # Method 3: Calculate momentum on ESG score
            if 'esg_score' not in esg_panel.columns:
                self.logger.error("esg_score not found for momentum calculation")
                return pd.DataFrame()

            factors = self.calculate_momentum(
                esg_panel,
                score_column='esg_score',
                windows=momentum_windows
            )
            # Use the longest window momentum as the primary score
            score_col = f'esg_score_momentum_{max(momentum_windows)}m'
            self.logger.info(f"✓ Using '{score_col}' as primary momentum factor")

        else:
            raise ValueError(f"Unknown formation method: {formation_method}")

        # Step 3: Calculate cross-sectional rankings
        if include_rankings and score_col in factors.columns:
            self.logger.info(f"\n🔄 Calculating cross-sectional rankings for '{score_col}'...")
            factors = self.calculate_cross_sectional_factors(
                factors,
                score_column=score_col,
                date_column='date'
            )

        # Sort by ticker and date
        factors = factors.sort_values(['ticker', 'date']).reset_index(drop=True)

        # Summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("✅ ESG FACTOR CONSTRUCTION COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"Total records: {len(factors):,}")
        self.logger.info(f"Tickers: {factors['ticker'].nunique()}")
        self.logger.info(f"Date range: {factors['date'].min()} to {factors['date'].max()}")
        self.logger.info(f"Primary factor: {score_col}")
        factor_cols = [col for col in factors.columns
                      if any(x in col for x in ['_zscore', '_pctrank', '_decile', '_momentum'])]
        self.logger.info(f"Factor columns: {len(factor_cols)}")
        self.logger.info("=" * 80)

        return factors

    def save_factors(
        self,
        factors_df: pd.DataFrame,
        output_name: Optional[str] = None,
        formation_method: Optional[str] = None
    ) -> Path:
        """
        Save factor dataset to Parquet file

        Args:
            factors_df: DataFrame with calculated factors
            output_name: Optional custom name for output file
            formation_method: Formation method used (esg_score, pillar_weighted, momentum)

        Returns:
            Path to saved file
        """
        output_dir = Path(self.universe.data_root) / "results" / "esg_factors"
        output_dir.mkdir(parents=True, exist_ok=True)

        if output_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            method_suffix = f"_{formation_method}" if formation_method else ""
            output_name = f"esg_factors{method_suffix}_{timestamp}.parquet"

        if not output_name.endswith('.parquet'):
            output_name += '.parquet'

        output_path = output_dir / output_name

        factors_df.to_parquet(output_path, index=False, compression='snappy')

        self.logger.info(f"💾 Factors saved to: {output_path}")

        return output_path

    def load_factors(
        self,
        filename: str
    ) -> pd.DataFrame:
        """
        Load previously saved factor dataset

        Args:
            filename: Name of factor file (with or without .parquet extension)

        Returns:
            DataFrame with factors
        """
        if not filename.endswith('.parquet'):
            filename += '.parquet'

        factors_dir = Path(self.universe.data_root) / "results" / "esg_factors"
        file_path = factors_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Factor file not found: {file_path}")

        factors_df = pd.read_parquet(file_path)

        self.logger.info(
            f"📂 Loaded factors from: {file_path} "
            f"({len(factors_df):,} records, {factors_df['ticker'].nunique()} tickers)"
        )

        return factors_df

    def get_factor_summary(
        self,
        factors_df: pd.DataFrame
    ) -> Dict:
        """
        Get summary statistics for factor dataset

        Args:
            factors_df: DataFrame with calculated factors

        Returns:
            Dict with summary statistics
        """
        factor_cols = [col for col in factors_df.columns
                       if any(x in col for x in ['_zscore', '_pctrank', '_momentum', '_composite'])]

        summary = {
            'total_records': len(factors_df),
            'num_tickers': factors_df['ticker'].nunique(),
            'date_range': {
                'start': str(factors_df['date'].min()),
                'end': str(factors_df['date'].max())
            },
            'factor_columns': factor_cols,
            'factor_statistics': {}
        }

        # Calculate statistics for each factor (skip categorical columns like _decile)
        for col in factor_cols:
            # Skip categorical columns
            if factors_df[col].dtype.name == 'category':
                summary['factor_statistics'][col] = {
                    'mean': None,
                    'median': None,
                    'std': None,
                    'min': None,
                    'max': None,
                    'coverage': float(factors_df[col].notna().mean()),
                    'note': 'categorical column'
                }
            else:
                summary['factor_statistics'][col] = {
                    'mean': float(factors_df[col].mean()),
                    'median': float(factors_df[col].median()),
                    'std': float(factors_df[col].std()),
                    'min': float(factors_df[col].min()),
                    'max': float(factors_df[col].max()),
                    'coverage': float(factors_df[col].notna().mean())
                }

        return summary
