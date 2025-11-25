"""
Expected Return Estimation

Estimates expected returns for factors and assets using various methods:
- Historical mean (sample average)
- EWMA (exponentially weighted moving average)
- Shrinkage estimators (James-Stein, Bayesian)
- Rolling window estimates
- Bootstrap confidence intervals

Features:
- Multiple estimation methods with statistical tests
- Out-of-sample validation
- Confidence intervals and standard errors
- Save estimates for portfolio optimization
- Compare different estimation methods

Usage:
    # Estimate expected returns for ESG factors
    python src/programs/estimate_expected_returns.py --factors-file data/results/esg_factors/esg_factors.parquet --method all --lookback 60

    # Historical mean with bootstrap confidence intervals
    python src/programs/estimate_expected_returns.py --factors-file data/results/esg_factors/esg_factors.parquet --method historical --bootstrap 1000

    # EWMA with custom half-life
    python src/programs/estimate_expected_returns.py \\
        --factors-file data/results/esg_factors/esg_factors.parquet \\
        --method ewma \\
        --halflife 24

    # Out-of-sample validation
    python src/programs/estimate_expected_returns.py \\
        --factors-file data/results/esg_factors/esg_factors.parquet \\
        --method all \\
        --validate \\
        --train-end 2020-12-31
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (14, 10)


class ExpectedReturnEstimator:
    """
    Expected return estimation with multiple methods
    """

    def __init__(self, returns_df: pd.DataFrame, results_dir: Optional[Path] = None):
        """
        Initialize estimator

        Args:
            returns_df: DataFrame with returns (columns=assets/factors, index=dates)
            results_dir: Directory to save results
        """
        self.returns_df = returns_df
        self.results_dir = results_dir or Path("data/results/expected_returns")
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.T = len(returns_df)
        self.N = len(returns_df.columns)

        logger.info(f"Initialized with {self.T} periods and {self.N} assets/factors")

    def historical_mean(
        self, lookback: Optional[int] = None, annualize: bool = True
    ) -> pd.Series:
        """
        Simple historical mean (sample average)

        Args:
            lookback: Number of periods to use (None = all data)
            annualize: Whether to annualize (default: True)

        Returns:
            Series of expected returns
        """
        if lookback:
            data = self.returns_df.iloc[-lookback:]
        else:
            data = self.returns_df

        mu = data.mean()

        if annualize:
            mu = mu * 12  # Assume monthly data

        return mu

    def ewma(self, halflife: int = 36, annualize: bool = True) -> pd.Series:
        """
        Exponentially weighted moving average

        Gives more weight to recent observations using exponential decay.
        Half-life = number of periods for weight to decay to 50%

        Args:
            halflife: Half-life in periods (default: 36 months = 3 years)
            annualize: Whether to annualize

        Returns:
            Series of expected returns
        """
        # EWMA with pandas (span parameter)
        # span = 2*halflife - 1 for consistency with halflife definition
        span = 2 * halflife - 1

        mu = self.returns_df.ewm(span=span, min_periods=halflife).mean().iloc[-1]

        if annualize:
            mu = mu * 12

        return mu

    def james_stein_shrinkage(
        self, lookback: Optional[int] = None, annualize: bool = True
    ) -> pd.Series:
        """
        James-Stein shrinkage estimator

        Shrinks sample means toward grand mean. Dominates sample mean
        in terms of mean squared error (Stein's paradox).

        Shrinkage intensity depends on estimation uncertainty.

        Args:
            lookback: Number of periods to use
            annualize: Whether to annualize

        Returns:
            Series of shrunk expected returns
        """
        if lookback:
            data = self.returns_df.iloc[-lookback:]
        else:
            data = self.returns_df

        T = len(data)
        N = len(data.columns)

        # Sample means
        mu_sample = data.mean().values

        # Grand mean (equal-weighted average of all assets)
        mu_grand = mu_sample.mean()

        # Sample covariance matrix
        Sigma = data.cov().values

        # Shrinkage intensity (optimal for minimizing MSE)
        # λ = (N - 2) / [T * (μ - μ_grand)' Σ^(-1) (μ - μ_grand)]
        diff = mu_sample - mu_grand

        try:
            Sigma_inv = np.linalg.inv(Sigma)
            denominator = T * diff @ Sigma_inv @ diff

            if denominator > 0:
                shrinkage = min((N - 2) / denominator, 1.0)  # Cap at 1.0
            else:
                shrinkage = 0.0
        except np.linalg.LinAlgError:
            logger.warning("Singular covariance matrix - using shrinkage = 0.5")
            shrinkage = 0.5

        # Shrunk estimates
        mu_shrunk = (1 - shrinkage) * mu_sample + shrinkage * mu_grand

        logger.info(f"James-Stein shrinkage intensity: {shrinkage:.4f}")

        mu = pd.Series(mu_shrunk, index=self.returns_df.columns)

        if annualize:
            mu = mu * 12

        return mu

    def bayesian_shrinkage(
        self,
        lookback: Optional[int] = None,
        prior_strength: float = 0.3,
        annualize: bool = True,
    ) -> pd.Series:
        """
        Bayesian shrinkage toward equal-weighted portfolio

        Combines sample mean with prior belief (grand mean) using
        confidence-weighted average.

        Args:
            lookback: Number of periods to use
            prior_strength: Weight on prior (0=sample mean, 1=grand mean)
            annualize: Whether to annualize

        Returns:
            Series of Bayesian expected returns
        """
        if lookback:
            data = self.returns_df.iloc[-lookback:]
        else:
            data = self.returns_df

        # Sample means
        mu_sample = data.mean()

        # Prior (grand mean)
        mu_prior = mu_sample.mean()

        # Bayesian combination
        mu = (1 - prior_strength) * mu_sample + prior_strength * mu_prior

        if annualize:
            mu = mu * 12

        return mu

    def bootstrap_ci(
        self,
        n_bootstrap: int = 1000,
        confidence: float = 0.95,
        lookback: Optional[int] = None,
        annualize: bool = True,
    ) -> pd.DataFrame:
        """
        Bootstrap confidence intervals for expected returns

        Args:
            n_bootstrap: Number of bootstrap samples
            confidence: Confidence level (default: 0.95)
            lookback: Number of periods to use
            annualize: Whether to annualize

        Returns:
            DataFrame with columns: mean, lower_ci, upper_ci, std_error
        """
        if lookback:
            data = self.returns_df.iloc[-lookback:]
        else:
            data = self.returns_df

        T = len(data)
        bootstrap_means = []

        logger.info(f"Running {n_bootstrap} bootstrap samples...")

        for i in range(n_bootstrap):
            # Resample with replacement
            sample_idx = np.random.choice(T, size=T, replace=True)
            sample = data.iloc[sample_idx]

            mu = sample.mean()
            if annualize:
                mu = mu * 12

            bootstrap_means.append(mu)

        bootstrap_df = pd.DataFrame(bootstrap_means)

        # Calculate statistics
        alpha = 1 - confidence
        results = pd.DataFrame(
            {
                "mean": bootstrap_df.mean(),
                "std_error": bootstrap_df.std(),
                "lower_ci": bootstrap_df.quantile(alpha / 2),
                "upper_ci": bootstrap_df.quantile(1 - alpha / 2),
            }
        )

        return results

    def rolling_estimates(
        self, window: int = 60, method: str = "historical", annualize: bool = True
    ) -> pd.DataFrame:
        """
        Calculate rolling expected return estimates

        Args:
            window: Rolling window size
            method: Estimation method ('historical', 'ewma')
            annualize: Whether to annualize

        Returns:
            DataFrame with rolling estimates (index=date, columns=assets)
        """
        logger.info(f"Calculating {window}-period rolling {method} estimates...")

        rolling_estimates = []

        for i in range(window - 1, len(self.returns_df)):
            window_data = self.returns_df.iloc[i - window + 1 : i + 1]

            if method == "historical":
                mu = window_data.mean()
            elif method == "ewma":
                mu = window_data.ewm(span=window // 2).mean().iloc[-1]
            else:
                raise ValueError(f"Unknown method: {method}")

            if annualize:
                mu = mu * 12

            rolling_estimates.append({"date": self.returns_df.index[i], **mu.to_dict()})

        result = pd.DataFrame(rolling_estimates).set_index("date")

        logger.info(f"Generated {len(result)} rolling estimates")

        return result

    def out_of_sample_validation(
        self,
        train_end: str,
        methods: List[str] = ["historical", "ewma", "james_stein"],
        lookback: int = 60,
    ) -> pd.DataFrame:
        """
        Out-of-sample validation of estimation methods

        Train on data up to train_end, test on subsequent period.
        Compare estimated vs realized returns.

        Args:
            train_end: End date for training period (YYYY-MM-DD)
            methods: List of methods to compare
            lookback: Lookback period for estimation

        Returns:
            DataFrame with comparison results
        """
        train_end_dt = pd.to_datetime(train_end)

        # Split data
        train = self.returns_df[self.returns_df.index <= train_end_dt]
        test = self.returns_df[self.returns_df.index > train_end_dt]

        logger.info(f"Train: {len(train)} periods (up to {train_end})")
        logger.info(f"Test: {len(test)} periods (after {train_end})")

        if len(test) == 0:
            raise ValueError("No test data available")

        # Estimate expected returns on training data
        estimator_train = ExpectedReturnEstimator(train)

        estimates = {}
        for method in methods:
            if method == "historical":
                est = estimator_train.historical_mean(lookback=lookback)
            elif method == "ewma":
                est = estimator_train.ewma(halflife=lookback // 2)
            elif method == "james_stein":
                est = estimator_train.james_stein_shrinkage(lookback=lookback)
            else:
                logger.warning(f"Unknown method: {method}")
                continue

            estimates[method] = est

        # Realized returns in test period
        realized = test.mean() * 12  # Annualized

        # Calculate errors
        results = []
        for method, est in estimates.items():
            error = est - realized
            mse = (error**2).mean()
            mae = error.abs().mean()

            results.append(
                {"method": method, "mse": mse, "mae": mae, "rmse": np.sqrt(mse)}
            )

        results_df = pd.DataFrame(results).set_index("method")

        # Also return detailed comparison
        comparison = pd.DataFrame(
            {
                "realized": realized,
                **{f"{method}_estimate": estimates[method] for method in methods},
            }
        )

        return results_df, comparison

    def plot_estimates_comparison(
        self, estimates_dict: Dict[str, pd.Series], save_path: Optional[Path] = None
    ):
        """
        Plot comparison of different estimation methods

        Args:
            estimates_dict: Dict of {method_name: estimates_series}
            save_path: Path to save plot
        """
        df = pd.DataFrame(estimates_dict)

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Bar plot
        df.plot(kind="bar", ax=ax1, width=0.8)
        ax1.set_title("Expected Returns by Method", fontsize=14, fontweight="bold")
        ax1.set_xlabel("Asset/Factor")
        ax1.set_ylabel("Expected Return (Annualized)")
        ax1.axhline(y=0, color="black", linestyle="--", alpha=0.3)
        ax1.legend(title="Method", bbox_to_anchor=(1.05, 1), loc="upper left")
        ax1.grid(True, alpha=0.3)
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

        # Heatmap of differences from mean
        mean_estimate = df.mean(axis=1)
        diff_from_mean = df.sub(mean_estimate, axis=0)

        sns.heatmap(
            diff_from_mean.T,
            annot=True,
            fmt=".4f",
            cmap="RdYlGn",
            center=0,
            ax=ax2,
            cbar_kws={"label": "Deviation from Mean"},
        )
        ax2.set_title(
            "Deviations from Average Estimate", fontsize=14, fontweight="bold"
        )
        ax2.set_xlabel("Asset/Factor")
        ax2.set_ylabel("Method")

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            logger.info(f"Saved comparison plot to {save_path}")
        else:
            plt.savefig(
                self.results_dir / "estimates_comparison.png",
                dpi=300,
                bbox_inches="tight",
            )

        plt.close()

    def plot_rolling_estimates(
        self, rolling_df: pd.DataFrame, save_path: Optional[Path] = None
    ):
        """
        Plot rolling expected return estimates

        Args:
            rolling_df: DataFrame with rolling estimates
            save_path: Path to save plot
        """
        n_assets = len(rolling_df.columns)
        n_cols = min(3, n_assets)
        n_rows = (n_assets + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(14, 4 * n_rows))
        if n_assets == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        for i, col in enumerate(rolling_df.columns):
            ax = axes[i]
            ax.plot(rolling_df.index, rolling_df[col], lw=2)
            ax.axhline(
                y=rolling_df[col].mean(),
                color="r",
                linestyle="--",
                label=f"Mean: {rolling_df[col].mean():.4f}",
            )
            ax.fill_between(
                rolling_df.index,
                rolling_df[col].mean() - rolling_df[col].std(),
                rolling_df[col].mean() + rolling_df[col].std(),
                alpha=0.2,
            )
            ax.set_title(f"{col} - Rolling Expected Return", fontweight="bold")
            ax.set_xlabel("Date")
            ax.set_ylabel("Expected Return (Annualized)")
            ax.legend()
            ax.grid(True, alpha=0.3)

        # Hide unused subplots
        for i in range(n_assets, len(axes)):
            axes[i].axis("off")

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            logger.info(f"Saved rolling estimates plot to {save_path}")
        else:
            plt.savefig(
                self.results_dir / "rolling_estimates.png", dpi=300, bbox_inches="tight"
            )

        plt.close()


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Estimate expected returns for factors/assets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Data input
    parser.add_argument(
        "--factors-file", required=True, help="Path to factors/returns parquet file"
    )

    # Estimation method
    parser.add_argument(
        "--method",
        choices=["historical", "ewma", "james_stein", "bayesian", "all"],
        default="all",
        help="Estimation method (default: all)",
    )

    # Parameters
    parser.add_argument(
        "--lookback",
        type=int,
        default=60,
        help="Lookback period in months (default: 60)",
    )
    parser.add_argument(
        "--halflife",
        type=int,
        default=36,
        help="EWMA half-life in months (default: 36)",
    )
    parser.add_argument(
        "--prior-strength",
        type=float,
        default=0.3,
        help="Bayesian prior strength 0-1 (default: 0.3)",
    )

    # Bootstrap
    parser.add_argument(
        "--bootstrap",
        type=int,
        help="Number of bootstrap samples for confidence intervals",
    )

    # Rolling analysis
    parser.add_argument(
        "--rolling-window", type=int, help="Window size for rolling estimates"
    )

    # Out-of-sample validation
    parser.add_argument(
        "--validate", action="store_true", help="Run out-of-sample validation"
    )
    parser.add_argument(
        "--train-end",
        default="2020-12-31",
        help="End date for training period (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    # Header
    logger.info("=" * 80)
    logger.info("EXPECTED RETURN ESTIMATION")
    logger.info("=" * 80)

    # Load data
    factors_file = Path(args.factors_file)
    if not factors_file.exists():
        logger.error(f"File not found: {factors_file}")
        sys.exit(1)

    logger.info(f"Loading data from {factors_file}")
    returns_df = pd.read_parquet(factors_file)
    returns_df.index = pd.to_datetime(returns_df.index)

    logger.info(
        f"Loaded {len(returns_df)} periods and {len(returns_df.columns)} assets/factors"
    )
    logger.info(f"Date range: {returns_df.index.min()} to {returns_df.index.max()}")
    logger.info("=" * 80)

    # Initialize estimator
    estimator = ExpectedReturnEstimator(returns_df)

    # Estimate expected returns
    print("\n" + "=" * 80)
    print("EXPECTED RETURN ESTIMATES (Annualized)")
    print("=" * 80)

    estimates = {}

    if args.method in ["historical", "all"]:
        logger.info(f"\nHistorical mean (lookback={args.lookback} months)")
        hist = estimator.historical_mean(lookback=args.lookback)
        estimates["Historical"] = hist
        print(f"\nHistorical Mean:")
        print(hist.to_string())

    if args.method in ["ewma", "all"]:
        logger.info(f"\nEWMA (half-life={args.halflife} months)")
        ewma = estimator.ewma(halflife=args.halflife)
        estimates["EWMA"] = ewma
        print(f"\nEWMA:")
        print(ewma.to_string())

    if args.method in ["james_stein", "all"]:
        logger.info(f"\nJames-Stein shrinkage (lookback={args.lookback} months)")
        js = estimator.james_stein_shrinkage(lookback=args.lookback)
        estimates["James-Stein"] = js
        print(f"\nJames-Stein Shrinkage:")
        print(js.to_string())

    if args.method in ["bayesian", "all"]:
        logger.info(f"\nBayesian shrinkage (prior_strength={args.prior_strength})")
        bayes = estimator.bayesian_shrinkage(
            lookback=args.lookback, prior_strength=args.prior_strength
        )
        estimates["Bayesian"] = bayes
        print(f"\nBayesian Shrinkage:")
        print(bayes.to_string())

    # Comparison table
    if len(estimates) > 1:
        print("\n" + "=" * 80)
        print("COMPARISON ACROSS METHODS")
        print("=" * 80)
        comparison = pd.DataFrame(estimates)
        print(comparison.to_string())

        # Plot comparison
        logger.info("\nGenerating comparison plots...")
        estimator.plot_estimates_comparison(estimates)

    # Bootstrap confidence intervals
    if args.bootstrap:
        logger.info(f"\nBootstrap confidence intervals (n={args.bootstrap})")
        print("\n" + "=" * 80)
        print(f"BOOTSTRAP CONFIDENCE INTERVALS ({args.bootstrap} samples)")
        print("=" * 80)

        bootstrap_results = estimator.bootstrap_ci(
            n_bootstrap=args.bootstrap, lookback=args.lookback
        )
        print(bootstrap_results.to_string())

        # Save bootstrap results
        bootstrap_path = estimator.results_dir / "bootstrap_ci.parquet"
        bootstrap_results.to_parquet(bootstrap_path)
        logger.info(f"Saved bootstrap results to {bootstrap_path}")

    # Rolling estimates
    if args.rolling_window:
        logger.info(f"\nRolling estimates (window={args.rolling_window} months)")
        rolling_df = estimator.rolling_estimates(
            window=args.rolling_window, method="historical"
        )

        print("\n" + "=" * 80)
        print(f"ROLLING ESTIMATES (Last 10 periods)")
        print("=" * 80)
        print(rolling_df.tail(10).to_string())

        # Plot rolling estimates
        logger.info("\nGenerating rolling estimates plots...")
        estimator.plot_rolling_estimates(rolling_df)

        # Save rolling estimates
        rolling_path = estimator.results_dir / "rolling_estimates.parquet"
        rolling_df.to_parquet(rolling_path)
        logger.info(f"Saved rolling estimates to {rolling_path}")

    # Out-of-sample validation
    if args.validate:
        logger.info(f"\nOut-of-sample validation (train until {args.train_end})")
        print("\n" + "=" * 80)
        print("OUT-OF-SAMPLE VALIDATION")
        print("=" * 80)

        methods = ["historical", "ewma", "james_stein"]
        errors, comparison = estimator.out_of_sample_validation(
            train_end=args.train_end, methods=methods, lookback=args.lookback
        )

        print("\nForecast Errors:")
        print(errors.to_string())

        print("\nEstimates vs Realized:")
        print(comparison.to_string())

        # Save validation results
        errors_path = estimator.results_dir / "oos_validation_errors.parquet"
        comparison_path = estimator.results_dir / "oos_validation_comparison.parquet"
        errors.to_parquet(errors_path)
        comparison.to_parquet(comparison_path)
        logger.info(f"Saved validation results to {estimator.results_dir}")

    # Save main estimates
    if estimates:
        estimates_df = pd.DataFrame(estimates)
        estimates_path = estimator.results_dir / "expected_returns.parquet"
        estimates_df.to_parquet(estimates_path)

        print("\n" + "=" * 80)
        logger.info(f"✅ Saved expected returns to {estimates_path}")
        logger.info(f"   Methods: {', '.join(estimates.keys())}")
        logger.info(f"   Assets: {len(estimates_df)}")
        print("=" * 80)


if __name__ == "__main__":
    main()
