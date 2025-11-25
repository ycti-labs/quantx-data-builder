"""
Single Factor Regression Analysis

Performs single factor regression analysis using ESG factors as explanatory variables.
Tests whether ESG factors explain cross-sectional stock returns or portfolio returns.

Two modes:
1. Portfolio Returns: Regress a portfolio/stock return against ESG factors (time-series)
2. Cross-Sectional: Regress individual stock returns against their ESG characteristics (cross-sectional)

Features:
- Load ESG factors from saved parquet files
- Load portfolio or stock returns
- Run OLS regression with statistical tests
- Calculate alpha, beta, R-squared, t-stats, p-values
- Newey-West HAC standard errors for robustness
- Rolling window regression for stability analysis
- Factor performance attribution
- Save regression results and diagnostic plots

Usage:
    # Regress SPY returns against ESG factor
    python src/programs/esg_factor_regression.py \\
        --target SPY \\
        --factor ESG_factor \\
        --start-date 2016-01-01 \\
        --end-date 2024-12-31

    # Regress PEP against all ESG factors
    python src/programs/esg_factor_regression.py \\
        --target PEP \\
        --factor ESG_factor E_factor S_factor G_factor \\
        --start-date 2016-01-01

    # Use a custom portfolio returns file
    python src/programs/esg_factor_regression.py \\
        --portfolio-file data/my_portfolio_returns.parquet \\
        --factor ESG_factor \\
        --rolling-window 36
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from statsmodels.regression.linear_model import OLS
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.stattools import durbin_watson
from statsmodels.tools.tools import add_constant

sys.path.insert(0, str(Path(__file__).parent.parent))

from tiingo import TiingoClient

from core.config import Config
from market import PriceManager
from universe.sp500_universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)


class SingleFactorRegression:
    """
    Single factor regression analyzer for ESG factors
    """

    def __init__(self, esg_factors_path: Path, results_dir: Optional[Path] = None):
        """
        Initialize regression analyzer

        Args:
            esg_factors_path: Path to ESG factors parquet file
            results_dir: Directory to save results (default: data/results/regression)
        """
        self.esg_factors_path = esg_factors_path
        self.results_dir = results_dir or Path("data/results/regression")
        self.results_dir.mkdir(parents=True, exist_ok=True)

        # Load ESG factors
        logger.info(f"Loading ESG factors from {esg_factors_path}")
        self.esg_factors = pd.read_parquet(esg_factors_path)
        self.esg_factors.index = pd.to_datetime(self.esg_factors.index)
        logger.info(f"Loaded {len(self.esg_factors)} months of ESG factors")
        logger.info(f"Available factors: {list(self.esg_factors.columns)}")

    def load_target_returns(
        self, ticker: str, start_date: str, end_date: str, frequency: str = "monthly"
    ) -> pd.Series:
        """
        Load target returns (dependent variable)

        Args:
            ticker: Ticker symbol (e.g., 'SPY', 'PEP')
            start_date: Start date
            end_date: End date
            frequency: Data frequency (default: monthly)

        Returns:
            Series of returns with DatetimeIndex
        """
        logger.info(f"Loading {frequency} returns for {ticker}...")

        # Initialize data managers
        project_root = Path(__file__).parent.parent.parent
        config = Config(str(project_root / "config/settings.yaml"))
        universe = SP500Universe(data_root=str(project_root / "data"))
        tiingo = TiingoClient(
            {"api_key": config.get("fetcher.tiingo.api_key"), "session": True}
        )
        price_mgr = PriceManager(tiingo=tiingo, universe=universe)

        # Load price data
        if ticker.upper() == "SPY":
            price_df = price_mgr.load_market_etf_data(
                frequency=frequency, start_date=start_date, end_date=end_date
            )
        else:
            price_df = price_mgr.load_price_data(
                symbol=ticker,
                frequency=frequency,
                start_date=start_date,
                end_date=end_date,
            )

        if price_df is None or price_df.empty:
            raise ValueError(f"No price data found for {ticker}")

        # Calculate returns
        price_df = price_df.set_index("date")
        returns = price_df["adj_close"].pct_change().dropna()

        logger.info(f"Loaded {len(returns)} {frequency} returns for {ticker}")
        logger.info(f"Date range: {returns.index.min()} to {returns.index.max()}")

        return returns

    def load_portfolio_returns(self, portfolio_file: Path) -> pd.Series:
        """
        Load portfolio returns from file

        Args:
            portfolio_file: Path to portfolio returns parquet/csv file

        Returns:
            Series of returns with DatetimeIndex
        """
        logger.info(f"Loading portfolio returns from {portfolio_file}")

        if portfolio_file.suffix == ".parquet":
            df = pd.read_parquet(portfolio_file)
        elif portfolio_file.suffix == ".csv":
            df = pd.read_csv(portfolio_file, index_col=0, parse_dates=True)
        else:
            raise ValueError(f"Unsupported file format: {portfolio_file.suffix}")

        # Assume first column is returns if multiple columns
        if isinstance(df, pd.DataFrame):
            returns = df.iloc[:, 0]
        else:
            returns = df

        returns.index = pd.to_datetime(returns.index)

        logger.info(f"Loaded {len(returns)} periods of portfolio returns")

        return returns

    def run_regression(
        self,
        y: pd.Series,
        factor_names: List[str],
        use_hac: bool = True,
        hac_lags: int = 12,
    ) -> Tuple[OLS, pd.DataFrame]:
        """
        Run single/multi-factor regression

        Args:
            y: Dependent variable (returns)
            factor_names: List of factor names to use as independent variables
            use_hac: Use Newey-West HAC standard errors
            hac_lags: Number of lags for HAC (default: 12 months)

        Returns:
            Tuple of (fitted model, results DataFrame)
        """
        # Align data
        X_factors = self.esg_factors[factor_names].copy()

        # Merge on date index
        data = pd.DataFrame({"y": y})
        for factor in factor_names:
            data[factor] = X_factors[factor]

        # Drop missing values
        data = data.dropna()

        logger.info(f"Regression data: {len(data)} observations")
        logger.info(f"Date range: {data.index.min()} to {data.index.max()}")

        if len(data) < 30:
            logger.warning(f"Only {len(data)} observations - results may be unreliable")

        # Prepare regression
        y_reg = data["y"].values
        X_reg = data[factor_names].values
        X_reg = add_constant(X_reg)  # Add intercept

        # Run OLS
        model = OLS(y_reg, X_reg).fit()

        # Use HAC standard errors if requested
        if use_hac:
            model = OLS(y_reg, X_reg).fit(
                cov_type="HAC", cov_kwds={"maxlags": hac_lags}
            )

        # Extract results
        results = pd.DataFrame(
            {
                "coefficient": model.params,
                "std_error": model.bse,
                "t_statistic": model.tvalues,
                "p_value": model.pvalues,
            },
            index=["alpha"] + factor_names,
        )

        # Add significance stars
        results["significance"] = results["p_value"].apply(
            lambda p: (
                "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            )
        )

        return model, results

    def rolling_regression(
        self, y: pd.Series, factor_name: str, window: int = 36, min_periods: int = 24
    ) -> pd.DataFrame:
        """
        Run rolling window regression

        Args:
            y: Dependent variable
            factor_name: Single factor name
            window: Rolling window size (months)
            min_periods: Minimum periods required

        Returns:
            DataFrame with rolling alpha, beta, R-squared
        """
        logger.info(f"Running {window}-month rolling regression...")

        # Align data
        X_factor = self.esg_factors[factor_name]
        data = pd.DataFrame({"y": y, "x": X_factor}).dropna()

        rolling_results = []

        for i in range(window - 1, len(data)):
            window_data = data.iloc[i - window + 1 : i + 1]

            if len(window_data) < min_periods:
                continue

            y_window = window_data["y"].values
            X_window = add_constant(window_data["x"].values)

            try:
                model = OLS(y_window, X_window).fit()

                rolling_results.append(
                    {
                        "date": data.index[i],
                        "alpha": model.params[0],
                        "beta": model.params[1],
                        "r_squared": model.rsquared,
                        "alpha_pvalue": model.pvalues[0],
                        "beta_pvalue": model.pvalues[1],
                    }
                )
            except:
                continue

        rolling_df = pd.DataFrame(rolling_results).set_index("date")

        logger.info(f"Generated {len(rolling_df)} rolling estimates")

        return rolling_df

    def diagnostic_tests(
        self, model: OLS, y: pd.Series, factor_names: List[str]
    ) -> dict:
        """
        Run diagnostic tests on regression

        Args:
            model: Fitted OLS model
            y: Dependent variable
            factor_names: List of factor names

        Returns:
            Dictionary of diagnostic statistics
        """
        # Align data for tests
        X_factors = self.esg_factors[factor_names]
        data = pd.DataFrame({"y": y})
        for factor in factor_names:
            data[factor] = X_factors[factor]
        data = data.dropna()

        X_reg = add_constant(data[factor_names].values)

        diagnostics = {}

        # Durbin-Watson test for autocorrelation
        dw = durbin_watson(model.resid)
        diagnostics["durbin_watson"] = dw
        diagnostics["autocorrelation"] = "Yes" if (dw < 1.5 or dw > 2.5) else "No"

        # Breusch-Pagan test for heteroskedasticity
        _, bp_pvalue, _, _ = het_breuschpagan(model.resid, X_reg)
        diagnostics["breusch_pagan_pvalue"] = bp_pvalue
        diagnostics["heteroskedasticity"] = "Yes" if bp_pvalue < 0.05 else "No"

        # Jarque-Bera test for normality (use scipy instead)
        from scipy import stats

        jb_stat, jb_pvalue = stats.jarque_bera(model.resid)
        diagnostics["jarque_bera"] = jb_stat
        diagnostics["jb_pvalue"] = jb_pvalue
        diagnostics["normal_residuals"] = "Yes" if jb_pvalue > 0.05 else "No"

        return diagnostics

    def plot_regression_diagnostics(
        self,
        model: OLS,
        y: pd.Series,
        factor_names: List[str],
        target_name: str,
        save_path: Optional[Path] = None,
    ):
        """
        Create diagnostic plots

        Args:
            model: Fitted OLS model
            y: Dependent variable
            factor_names: Factor names
            target_name: Name of target (for title)
            save_path: Path to save plot
        """
        # Align data
        X_factors = self.esg_factors[factor_names]
        data = pd.DataFrame({"y": y})
        for factor in factor_names:
            data[factor] = X_factors[factor]
        data = data.dropna()

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(
            f'Regression Diagnostics: {target_name} vs {", ".join(factor_names)}',
            fontsize=14,
            fontweight="bold",
        )

        # 1. Actual vs Fitted
        axes[0, 0].scatter(model.fittedvalues, data["y"].values, alpha=0.6)
        axes[0, 0].plot(
            [data["y"].min(), data["y"].max()],
            [data["y"].min(), data["y"].max()],
            "r--",
            lw=2,
        )
        axes[0, 0].set_xlabel("Fitted Values")
        axes[0, 0].set_ylabel("Actual Returns")
        axes[0, 0].set_title(f"Actual vs Fitted (R² = {model.rsquared:.4f})")
        axes[0, 0].grid(True, alpha=0.3)

        # 2. Residuals vs Fitted
        axes[0, 1].scatter(model.fittedvalues, model.resid, alpha=0.6)
        axes[0, 1].axhline(y=0, color="r", linestyle="--", lw=2)
        axes[0, 1].set_xlabel("Fitted Values")
        axes[0, 1].set_ylabel("Residuals")
        axes[0, 1].set_title("Residuals vs Fitted")
        axes[0, 1].grid(True, alpha=0.3)

        # 3. Residual histogram with normal curve
        axes[1, 0].hist(
            model.resid, bins=30, density=True, alpha=0.7, edgecolor="black"
        )
        mu, sigma = model.resid.mean(), model.resid.std()
        x = np.linspace(model.resid.min(), model.resid.max(), 100)
        axes[1, 0].plot(
            x,
            1
            / (sigma * np.sqrt(2 * np.pi))
            * np.exp(-((x - mu) ** 2) / (2 * sigma**2)),
            "r-",
            lw=2,
            label="Normal",
        )
        axes[1, 0].set_xlabel("Residuals")
        axes[1, 0].set_ylabel("Density")
        axes[1, 0].set_title("Residual Distribution")
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Q-Q plot
        from scipy import stats

        stats.probplot(model.resid, dist="norm", plot=axes[1, 1])
        axes[1, 1].set_title("Q-Q Plot")
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            logger.info(f"Saved diagnostic plot to {save_path}")
        else:
            plt.savefig(
                self.results_dir / f"diagnostics_{target_name}.png",
                dpi=300,
                bbox_inches="tight",
            )

        plt.close()

    def plot_rolling_regression(
        self,
        rolling_df: pd.DataFrame,
        target_name: str,
        factor_name: str,
        save_path: Optional[Path] = None,
    ):
        """
        Plot rolling regression results

        Args:
            rolling_df: Rolling regression results
            target_name: Target name
            factor_name: Factor name
            save_path: Path to save plot
        """
        fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
        fig.suptitle(
            f"Rolling Regression: {target_name} vs {factor_name}",
            fontsize=14,
            fontweight="bold",
        )

        # Alpha
        axes[0].plot(rolling_df.index, rolling_df["alpha"], lw=2)
        axes[0].axhline(y=0, color="r", linestyle="--", alpha=0.5)
        axes[0].fill_between(
            rolling_df.index,
            rolling_df["alpha"],
            0,
            where=(rolling_df["alpha_pvalue"] < 0.05),
            alpha=0.3,
            label="Significant (p<0.05)",
        )
        axes[0].set_ylabel("Alpha")
        axes[0].set_title("Rolling Alpha (Intercept)")
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)

        # Beta
        axes[1].plot(rolling_df.index, rolling_df["beta"], lw=2, color="green")
        axes[1].axhline(y=0, color="r", linestyle="--", alpha=0.5)
        axes[1].fill_between(
            rolling_df.index,
            rolling_df["beta"] - 2 * rolling_df["beta"].std(),
            rolling_df["beta"] + 2 * rolling_df["beta"].std(),
            alpha=0.2,
        )
        axes[1].set_ylabel("Beta")
        axes[1].set_title("Rolling Beta (Factor Loading)")
        axes[1].grid(True, alpha=0.3)

        # R-squared
        axes[2].plot(rolling_df.index, rolling_df["r_squared"], lw=2, color="purple")
        axes[2].set_ylabel("R-squared")
        axes[2].set_xlabel("Date")
        axes[2].set_title("Rolling R-squared (Explanatory Power)")
        axes[2].grid(True, alpha=0.3)

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            logger.info(f"Saved rolling regression plot to {save_path}")
        else:
            plt.savefig(
                self.results_dir / f"rolling_{target_name}_{factor_name}.png",
                dpi=300,
                bbox_inches="tight",
            )

        plt.close()


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(
        description="Single factor regression using ESG factors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Target selection
    parser.add_argument("--target", help="Target ticker symbol (e.g., SPY, PEP, AAPL)")
    parser.add_argument(
        "--portfolio-file", help="Path to portfolio returns file (parquet or csv)"
    )

    # Factor selection
    parser.add_argument(
        "--factor",
        nargs="+",
        default=["ESG_factor"],
        help="Factor name(s) to use (default: ESG_factor)",
    )

    # Date range
    parser.add_argument(
        "--start-date",
        default="2016-01-01",
        help="Start date (YYYY-MM-DD), default: 2016-01-01",
    )
    parser.add_argument(
        "--end-date",
        default="2024-12-31",
        help="End date (YYYY-MM-DD), default: 2024-12-31",
    )

    # Analysis options
    parser.add_argument(
        "--rolling-window",
        type=int,
        help="Rolling window size for stability analysis (e.g., 36 months)",
    )
    parser.add_argument(
        "--no-hac", action="store_true", help="Don't use Newey-West HAC standard errors"
    )
    parser.add_argument(
        "--hac-lags",
        type=int,
        default=12,
        help="Number of lags for HAC standard errors (default: 12)",
    )

    args = parser.parse_args()

    # Validation
    if not args.target and not args.portfolio_file:
        parser.error("Must specify either --target or --portfolio-file")

    # Header
    logger.info("=" * 80)
    logger.info("SINGLE FACTOR REGRESSION ANALYSIS")
    logger.info("=" * 80)

    # Initialize
    project_root = Path(__file__).parent.parent.parent
    esg_factors_path = (
        project_root / "data/results/esg_factors/esg_factors.parquet"
    )

    if not esg_factors_path.exists():
        logger.error(f"ESG factors file not found: {esg_factors_path}")
        logger.error("Please run build_esg_factors.py first")
        sys.exit(1)

    analyzer = SingleFactorRegression(esg_factors_path)

    # Load target returns
    if args.target:
        target_name = args.target
        y = analyzer.load_target_returns(
            ticker=args.target, start_date=args.start_date, end_date=args.end_date
        )
    else:
        portfolio_path = Path(args.portfolio_file)
        target_name = portfolio_path.stem
        y = analyzer.load_portfolio_returns(portfolio_path)

    logger.info("")
    logger.info(f"Target: {target_name}")
    logger.info(f"Factors: {', '.join(args.factor)}")
    logger.info(f"Observations: {len(y)}")
    logger.info("=" * 80)

    # Run regression
    logger.info("")
    logger.info("Running regression...")
    model, results = analyzer.run_regression(
        y=y, factor_names=args.factor, use_hac=not args.no_hac, hac_lags=args.hac_lags
    )

    # Display results
    print("\n" + "=" * 80)
    print("REGRESSION RESULTS")
    print("=" * 80)
    print(f"\nTarget: {target_name}")
    print(f"Factors: {', '.join(args.factor)}")
    print(f"Observations: {model.nobs:.0f}")

    # Handle both datetime and date objects
    min_date = y.index.min()
    max_date = y.index.max()
    if hasattr(min_date, "date"):
        min_date = min_date.date()
    if hasattr(max_date, "date"):
        max_date = max_date.date()
    print(f"Date Range: {min_date} to {max_date}")

    if not args.no_hac:
        print(f"Standard Errors: Newey-West HAC (lags={args.hac_lags})")
    print("\n" + "-" * 80)
    print("\nCoefficients:")
    print(results.to_string())

    print("\n" + "-" * 80)
    print(f"\nModel Statistics:")
    print(f"  R-squared:     {model.rsquared:.4f}")
    print(f"  Adj R-squared: {model.rsquared_adj:.4f}")
    print(f"  F-statistic:   {model.fvalue:.4f}")
    print(f"  Prob(F-stat):  {model.f_pvalue:.6f}")
    print(f"  AIC:           {model.aic:.2f}")
    print(f"  BIC:           {model.bic:.2f}")

    # Annualized metrics
    alpha_monthly = results.loc["alpha", "coefficient"]
    alpha_annual = (1 + alpha_monthly) ** 12 - 1
    print("\n" + "-" * 80)
    print(f"\nAnnualized Alpha: {alpha_annual:.4f} ({alpha_annual*100:.2f}%)")

    # Diagnostic tests
    logger.info("\nRunning diagnostic tests...")
    diagnostics = analyzer.diagnostic_tests(model, y, args.factor)

    print("\n" + "-" * 80)
    print("\nDiagnostic Tests:")
    print(
        f"  Durbin-Watson:        {diagnostics['durbin_watson']:.4f} "
        f"(Autocorrelation: {diagnostics['autocorrelation']})"
    )
    print(
        f"  Breusch-Pagan p-val:  {diagnostics['breusch_pagan_pvalue']:.4f} "
        f"(Heteroskedasticity: {diagnostics['heteroskedasticity']})"
    )
    print(
        f"  Jarque-Bera p-val:    {diagnostics['jb_pvalue']:.4f} "
        f"(Normal Residuals: {diagnostics['normal_residuals']})"
    )

    # Create diagnostic plots
    logger.info("\nGenerating diagnostic plots...")
    analyzer.plot_regression_diagnostics(
        model=model, y=y, factor_names=args.factor, target_name=target_name
    )

    # Rolling regression if requested
    if args.rolling_window and len(args.factor) == 1:
        logger.info(f"\nRunning {args.rolling_window}-month rolling regression...")
        rolling_df = analyzer.rolling_regression(
            y=y, factor_name=args.factor[0], window=args.rolling_window
        )

        print("\n" + "-" * 80)
        print(f"\nRolling Regression Statistics ({args.rolling_window}-month window):")
        print(
            f"  Alpha - Mean: {rolling_df['alpha'].mean():.6f}, "
            f"Std: {rolling_df['alpha'].std():.6f}"
        )
        print(
            f"  Beta  - Mean: {rolling_df['beta'].mean():.4f}, "
            f"Std: {rolling_df['beta'].std():.4f}"
        )
        print(
            f"  R²    - Mean: {rolling_df['r_squared'].mean():.4f}, "
            f"Std: {rolling_df['r_squared'].std():.4f}"
        )

        # Plot rolling results
        analyzer.plot_rolling_regression(
            rolling_df=rolling_df, target_name=target_name, factor_name=args.factor[0]
        )

        # Save rolling results
        rolling_path = (
            analyzer.results_dir / f"rolling_{target_name}_{args.factor[0]}.parquet"
        )
        rolling_df.to_parquet(rolling_path)
        logger.info(f"Saved rolling results to {rolling_path}")

    # Save regression results
    results_path = analyzer.results_dir / f"regression_{target_name}.parquet"
    results.to_parquet(results_path)

    summary_dict = {
        "target": target_name,
        "factors": ", ".join(args.factor),
        "observations": int(model.nobs),
        "r_squared": model.rsquared,
        "adj_r_squared": model.rsquared_adj,
        "f_statistic": model.fvalue,
        "f_pvalue": model.f_pvalue,
        "alpha": results.loc["alpha", "coefficient"],
        "alpha_pvalue": results.loc["alpha", "p_value"],
        "alpha_annual": alpha_annual,
        **{
            f"beta_{factor}": results.loc[factor, "coefficient"]
            for factor in args.factor
        },
        **{
            f"beta_{factor}_pvalue": results.loc[factor, "p_value"]
            for factor in args.factor
        },
        **diagnostics,
    }

    summary_df = pd.DataFrame([summary_dict])
    summary_path = analyzer.results_dir / f"summary_{target_name}.parquet"
    summary_df.to_parquet(summary_path)

    print("\n" + "=" * 80)
    logger.info(f"✅ Saved results to:")
    logger.info(f"   - {results_path}")
    logger.info(f"   - {summary_path}")
    logger.info(f"   - {analyzer.results_dir / f'diagnostics_{target_name}.png'}")
    if args.rolling_window and len(args.factor) == 1:
        logger.info(f"   - {rolling_path}")
        logger.info(
            f"   - {analyzer.results_dir / f'rolling_{target_name}_{args.factor[0]}.png'}"
        )
    print("=" * 80)


if __name__ == "__main__":
    main()
