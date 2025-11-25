"""
Portfolio Optimization with Markowitz Mean-Variance and ESG Control

Optimizes portfolios using Expected CAPM returns with constraints:
- Mean-variance optimization (Markowitz)
- ESG exposure controls (beta-based)
- Sector concentration limits
- Turnover penalties
- Position size limits

Usage:
    python src/programs/optimize_portfolio.py --gamma 4.0 --esg-neutral --save

References:
    - Markowitz (1952): Portfolio Selection
    - Ledoit & Wolf (2003): Improved covariance estimation via shrinkage
    - Pastor, Stambaugh & Taylor (2021): Sustainable investing
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Try CVXPY first; fallback to SciPy if unavailable
try:
    import cvxpy as cp

    HAS_CVXPY = True
except Exception:
    from scipy.optimize import minimize

    HAS_CVXPY = False

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import RiskFreeRateManager
from universe import SP500Universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# -----------------------------
# 1) Data loading utilities
# -----------------------------


def load_expected_returns(data_root: Path, date: str = None) -> pd.Series:
    """
    Load expected returns from Extended CAPM results

    Args:
        data_root: Root data directory
        date: Specific date (YYYY-MM-DD) or None for latest

    Returns:
        Series with ticker -> expected monthly return (decimal)
    """
    er_file = data_root / "results" / "expected_returns" / "expected_returns.parquet"

    if not er_file.exists():
        raise FileNotFoundError(
            f"Expected returns not found: {er_file}\n" f"Run extend_capm.py first"
        )

    df = pd.read_parquet(er_file)
    df["date"] = pd.to_datetime(df["date"])

    if date is not None:
        target_date = pd.to_datetime(date)
        df = df[df["date"] == target_date]
        if df.empty:
            raise ValueError(f"No expected returns for date: {date}")
    else:
        # Use latest date
        df = df[df["date"] == df["date"].max()]

    er = df.set_index("ticker")["ER_monthly"]
    logger.info(
        f"Loaded expected returns for {len(er)} tickers (date: {df['date'].iloc[0]})"
    )
    return er


def load_esg_betas(data_root: Path, tickers: List[str]) -> pd.Series:
    """
    Load ESG betas from two-factor regression results

    Args:
        data_root: Root data directory
        tickers: List of ticker symbols

    Returns:
        Series with ticker -> ESG beta
    """
    results_dir = data_root / "results" / "two_factor_regression"

    betas = {}
    for ticker in tickers:
        results_file = (
            results_dir / f"ticker={ticker}" / "two_factor_regression.parquet"
        )
        if results_file.exists():
            df = pd.read_parquet(results_file)
            # Use latest beta estimate
            betas[ticker] = df.iloc[-1]["beta_esg"]

    beta_series = pd.Series(betas)
    logger.info(f"Loaded ESG betas for {len(beta_series)} tickers")
    return beta_series


def load_returns_panel(
    data_root: Path,
    tickers: List[str],
    start_date: str,
    end_date: str,
    frequency: str = "monthly",
) -> pd.DataFrame:
    """
    Load historical returns panel for covariance estimation

    Args:
        data_root: Root data directory
        tickers: List of ticker symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        frequency: 'monthly' or 'weekly'

    Returns:
        DataFrame with MultiIndex [date, ticker] and columns [return, excess]
    """
    # Load risk-free rate
    rf_path = data_root / "curated" / "references" / "risk_free_rate" / "freq=monthly"
    rf_manager = RiskFreeRateManager(data_root=str(rf_path), default_rate="3month")
    rf_data = rf_manager.load_risk_free_rate(
        start_date=start_date,
        end_date=end_date,
        rate_type="3month",
        frequency=frequency,
    )
    rf_data["date"] = pd.to_datetime(rf_data["date"])
    rf_data["RF"] = rf_data["rate"] / 100
    if frequency == "monthly":
        rf_data["RF"] = rf_data["RF"] / 12
    elif frequency == "weekly":
        rf_data["RF"] = rf_data["RF"] / 52
    rf_dict = rf_data.set_index("date")["RF"].to_dict()

    # Load price data and calculate returns
    all_returns = []

    for ticker in tickers:
        ticker_path = (
            data_root
            / "curated"
            / "tickers"
            / "exchange=us"
            / f"ticker={ticker}"
            / "prices"
            / f"freq={frequency}"
        )

        if not ticker_path.exists():
            continue

        # Load all years
        dfs = []
        for year_dir in sorted(ticker_path.glob("year=*")):
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                dfs.append(pd.read_parquet(parquet_file))

        if not dfs:
            continue

        df = pd.concat(dfs, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]

        if len(df) < 10:  # Need minimum observations
            continue

        # Calculate returns
        df["return"] = df["adj_close"].pct_change()
        df["ticker"] = ticker

        # Add RF and calculate excess returns
        df["RF"] = df["date"].map(rf_dict)
        df["excess"] = df["return"] - df["RF"]

        df = df[["date", "ticker", "return", "excess"]].dropna()
        all_returns.append(df)

    if not all_returns:
        raise ValueError("No returns data found for any ticker")

    returns_panel = pd.concat(all_returns, ignore_index=True)
    returns_panel = returns_panel.set_index(["date", "ticker"])

    logger.info(f"Loaded returns panel: {len(returns_panel)} observations")
    logger.info(
        f"  Tickers: {returns_panel.index.get_level_values('ticker').nunique()}"
    )
    logger.info(
        f"  Date range: {returns_panel.index.get_level_values('date').min()} to {returns_panel.index.get_level_values('date').max()}"
    )

    return returns_panel


def load_sector_mapping(data_root: Path, tickers: List[str]) -> pd.Series:
    """
    Load sector mapping from universe metadata

    Args:
        data_root: Root data directory
        tickers: List of ticker symbols

    Returns:
        Series with ticker -> sector (GICS sector)
    """
    metadata_file = data_root / "curated" / "metadata" / "symbols.parquet"

    if not metadata_file.exists():
        logger.warning(f"Sector metadata not found: {metadata_file}")
        return pd.Series(dtype=str)

    df = pd.read_parquet(metadata_file)
    df = df[df["ticker"].isin(tickers)]

    if "sector" not in df.columns:
        logger.warning("Sector column not found in metadata")
        return pd.Series(dtype=str)

    sector_map = df.set_index("ticker")["sector"]
    logger.info(f"Loaded sector mapping for {len(sector_map)} tickers")
    return sector_map


def get_current_rf_monthly(data_root: Path) -> float:
    """
    Get the most recent monthly risk-free rate (as decimal)

    Args:
        data_root: Root data directory

    Returns:
        Risk-free rate (monthly decimal, e.g., 0.003 for 0.3%)
    """
    rf_file = (
        data_root
        / "curated"
        / "references"
        / "risk_free_rate"
        / "freq=monthly"
        / "3month_monthly.parquet"
    )

    if not rf_file.exists():
        logger.warning(f"Risk-free rate file not found: {rf_file}, using RF=0")
        return 0.0

    df = pd.read_parquet(rf_file)
    latest_rf_annual = df.iloc[-1]["rate"]  # Annual percentage (e.g., 4.37)
    rf_monthly = latest_rf_annual / 12 / 100  # Convert to monthly decimal

    logger.info(
        f"Latest risk-free rate: {latest_rf_annual:.2f}% annual = {rf_monthly:.6f} monthly"
    )
    return rf_monthly


# -----------------------------
# 2) Covariance estimation
# -----------------------------


def build_shrinkage_cov(
    returns_panel: pd.DataFrame, col: str = "excess", shrink: float = 0.25
) -> pd.DataFrame:
    """
    Build a shrinkage covariance matrix (Ledoit-Wolf style) for the cross-section of assets.

    Args:
        returns_panel: MultiIndex [date, ticker], contains column 'excess' (monthly decimal returns)
        col: Column name for returns (default: 'excess')
        shrink: Shrinkage intensity [0, 1]; mix between sample cov (1-shrink) and diagonal target (shrink)

    Returns:
        Covariance matrix as DataFrame (ticker x ticker)
    """
    X = returns_panel[col].unstack("ticker").dropna(how="any")  # T x N
    S = np.cov(X.values, rowvar=False)  # N x N
    S = pd.DataFrame(S, index=X.columns, columns=X.columns)

    # Diagonal target (variances only)
    F = pd.DataFrame(np.diag(np.diag(S.values)), index=S.index, columns=S.columns)
    Sigma = (1.0 - shrink) * S + shrink * F

    # Ensure Sigma is symmetric and numerically PSD
    eigvals = np.linalg.eigvalsh(Sigma.values)
    if eigvals.min() < 0:
        # Shift by small epsilon to enforce PSD
        eps = 1e-8 - eigvals.min()
        Sigma.values[:] = Sigma.values + np.eye(Sigma.shape[0]) * eps

    logger.info(
        f"Built shrinkage covariance matrix: {Sigma.shape[0]} assets, shrinkage={shrink:.2f}"
    )
    logger.info(f"  Min eigenvalue: {eigvals.min():.6f}, Max: {eigvals.max():.6f}")

    return Sigma


# -----------------------------
# 3) Validation utilities
# -----------------------------


def check_units_expected(exp_ret: pd.Series):
    """
    Sanity-check expected returns (should be monthly decimals, e.g., 0.01 = 1%).
    """
    mn, mx = exp_ret.min(), exp_ret.max()
    if mn < -1.0 or mx > 1.0:
        raise ValueError(
            f"exp_ret out of bounds [-1, +1] monthly. min={mn:.3f}, max={mx:.3f}. "
            "Likely a percent vs decimal issue."
        )


# -----------------------------
# 4) CVXPY-based optimization
# -----------------------------


def optimize_markowitz_esg_cvxpy(
    exp_ret: pd.Series,  # μ: expected monthly returns (decimal)
    Sigma: pd.DataFrame,  # Σ: covariance matrix (monthly)
    esg_beta: pd.Series,  # β^ESG per ticker
    gamma: float = 4.0,  # risk aversion (higher => more conservative)
    long_only: bool = True,
    w_max: float = 0.10,  # per-name cap
    esg_bounds: tuple = None,  # (L_ESG, U_ESG), e.g., (-0.05, 0.05) for neutrality
    sector_map: pd.Series = None,  # ticker -> sector
    sector_caps: dict = None,  # {sector_name: cap}
    prev_w: pd.Series = None,  # previous weights for turnover penalty
    lambda_tc: float = 0.0,  # turnover penalty strength on L1 ||w - prev||
    solver: str = "ECOS",  # "ECOS" | "OSQP" | "SCS"
):
    """
    Markowitz mean-variance optimization with ESG constraints using CVXPY

    Objective: Maximize μ'w - 0.5*γ*w'Σw - λ_tc*||w - w_prev||_1

    Subject to:
        - Budget: sum(w) = 1
        - Long-only: w >= 0 (if enabled)
        - Position limits: w <= w_max
        - ESG exposure: L_ESG <= β_ESG'w <= U_ESG
        - Sector limits: sum(w[sector]) <= cap
    """
    tickers = exp_ret.index.tolist()
    n = len(tickers)

    # Align all inputs
    Sigma = Sigma.loc[tickers, tickers]
    beta_esg = esg_beta.reindex(tickers).fillna(0.0).values

    mu = exp_ret.values
    Sigma_mat = Sigma.values

    # Decision variable
    w = cp.Variable(n)

    # Objective: μ' w - 0.5 * γ * w' Σ w  - λ_tc * ||w - prev||_1
    quad = cp.quad_form(w, Sigma_mat)
    objective = mu @ w - 0.5 * gamma * quad
    constraints = [cp.sum(w) == 1]

    if long_only:
        constraints += [w >= 0]
    if w_max is not None:
        constraints += [w <= w_max]

    if esg_bounds is not None:
        L_esg, U_esg = esg_bounds
        constraints += [beta_esg @ w >= L_esg, beta_esg @ w <= U_esg]

    if sector_map is not None and sector_caps is not None:
        sec = sector_map.reindex(tickers)
        for s, cap in sector_caps.items():
            idx = np.where(sec.values == s)[0]
            if len(idx) > 0:
                constraints += [cp.sum(w[idx]) <= cap]

    if prev_w is not None and lambda_tc > 0.0:
        prev = prev_w.reindex(tickers).fillna(0.0).values
        objective = objective - lambda_tc * cp.norm1(w - prev)

    prob = cp.Problem(cp.Maximize(objective), constraints)
    prob.solve(solver=solver, verbose=False)

    if w.value is None:
        raise RuntimeError("Optimization failed; check constraints and covariance PSD.")

    weights = pd.Series(np.array(w.value).ravel(), index=tickers)

    logger.info(f"CVXPY optimization successful (solver: {solver})")
    logger.info(f"  Objective value: {prob.value:.6f}")
    logger.info(f"  Active positions: {(weights.abs() > 1e-4).sum()}")

    return weights


# ---------------------------------
# 5) SciPy fallback optimization
# ---------------------------------


def optimize_markowitz_esg_scipy(
    exp_ret: pd.Series,
    Sigma: pd.DataFrame,
    esg_beta: pd.Series,
    gamma: float = 4.0,
    long_only: bool = True,
    w_max: float = 0.10,
    esg_bounds: tuple = None,
    sector_map: pd.Series = None,
    sector_caps: dict = None,
    prev_w: pd.Series = None,
    lambda_tc: float = 0.0,
):
    """
    SciPy SLSQP fallback: maximize μ' w - 0.5 γ w' Σ w - λ_tc ||w - prev||_1 subject to constraints.
    We convert to minimization by flipping the sign of the objective.
    Note: L1 turnover handled via smooth approximation (Huber-like).
    """
    from scipy.optimize import minimize

    tickers = exp_ret.index.tolist()
    n = len(tickers)
    Sigma = Sigma.loc[tickers, tickers]
    mu = exp_ret.values
    beta_esg = esg_beta.reindex(tickers).fillna(0.0).values
    Sigma_mat = Sigma.values

    # Smooth L1 proxy for SciPy (Huber-like)
    def l1_smooth(x, epsilon=1e-4):
        return np.sum(np.sqrt(x**2 + epsilon))

    prev = None
    if prev_w is not None:
        prev = prev_w.reindex(tickers).fillna(0.0).values

    def neg_objective(w):
        risk = 0.5 * gamma * np.dot(w, Sigma_mat @ w)
        ret = np.dot(mu, w)
        turn = 0.0
        if prev is not None and lambda_tc > 0.0:
            turn = lambda_tc * l1_smooth(w - prev)
        # Max ret - risk - turnover  => minimize negative
        return -(ret - risk - turn)

    # Constraints
    cons = []
    # Budget
    cons.append({"type": "eq", "fun": lambda w: np.sum(w) - 1.0})

    # ESG bounds
    if esg_bounds is not None:
        L_esg, U_esg = esg_bounds
        cons.append(
            {"type": "ineq", "fun": lambda w: np.dot(beta_esg, w) - L_esg}
        )  # >= 0
        cons.append(
            {"type": "ineq", "fun": lambda w: U_esg - np.dot(beta_esg, w)}
        )  # >= 0

    # Sector caps
    if sector_map is not None and sector_caps is not None:
        sec = sector_map.reindex(tickers)
        for s, cap in sector_caps.items():
            idx = np.where(sec.values == s)[0]
            if len(idx) > 0:
                cons.append(
                    {
                        "type": "ineq",
                        "fun": lambda w, idx=idx, cap=cap: cap - np.sum(w[idx]),
                    }
                )

    # Bounds
    bounds = None
    if long_only:
        if w_max is None:
            bounds = [(0.0, 1.0) for _ in range(n)]
        else:
            bounds = [(0.0, w_max) for _ in range(n)]
    else:
        if w_max is None:
            bounds = [(-1.0, 1.0) for _ in range(n)]
        else:
            bounds = [(-w_max, w_max) for _ in range(n)]

    # Initial guess: equal-weight within bounds
    w0 = np.ones(n) / n
    if long_only and w_max is not None and (1.0 / n) > w_max:
        w0 = np.clip(w0, 0.0, w_max)
        w0 = w0 / w0.sum()

    res = minimize(
        neg_objective,
        w0,
        method="SLSQP",
        bounds=bounds,
        constraints=cons,
        options={"maxiter": 500},
    )
    if not res.success:
        raise RuntimeError(f"SciPy optimization failed: {res.message}")

    logger.info(f"SciPy SLSQP optimization successful")
    logger.info(f"  Objective value: {-res.fun:.6f}")
    logger.info(f"  Active positions: {(np.abs(res.x) > 1e-4).sum()}")

    return pd.Series(res.x, index=tickers)


# -----------------------------
# 6) Driver + evaluation
# -----------------------------


def optimize_markowitz_esg(
    exp_ret: pd.Series,
    Sigma: pd.DataFrame,
    esg_beta: pd.Series,
    gamma: float = 4.0,
    long_only: bool = True,
    w_max: float = 0.10,
    esg_bounds: tuple = None,
    sector_map: pd.Series = None,
    sector_caps: dict = None,
    prev_w: pd.Series = None,
    lambda_tc: float = 0.0,
    solver: str = "ECOS",
):
    """
    Unified interface: uses CVXPY if available; otherwise SciPy fallback.
    """
    check_units_expected(exp_ret)
    if HAS_CVXPY:
        return optimize_markowitz_esg_cvxpy(
            exp_ret,
            Sigma,
            esg_beta,
            gamma,
            long_only,
            w_max,
            esg_bounds,
            sector_map,
            sector_caps,
            prev_w,
            lambda_tc,
            solver,
        )
    else:
        return optimize_markowitz_esg_scipy(
            exp_ret,
            Sigma,
            esg_beta,
            gamma,
            long_only,
            w_max,
            esg_bounds,
            sector_map,
            sector_caps,
            prev_w,
            lambda_tc,
        )


def evaluate_portfolio(
    weights: pd.Series,
    exp_ret: pd.Series,
    Sigma: pd.DataFrame,
    esg_beta: pd.Series,
    rf_month: float = 0.0,
    benchmark_w: pd.Series = None,
) -> dict:
    """
    Compute expected return, volatility, Sharpe, ESG exposure, (optional) TE vs benchmark.
    rf_month: mean monthly risk-free rate (decimal), for Sharpe interpretation.
    """
    tickers = weights.index
    Sigma = Sigma.loc[tickers, tickers]
    mu = exp_ret.reindex(tickers).values
    beta_esg = esg_beta.reindex(tickers).fillna(0.0).values
    w = weights.values

    er_month = float(mu @ w)  # expected monthly return
    var_month = float(w.T @ Sigma.values @ w)  # variance
    vol_month = var_month**0.5
    sharpe = (er_month - rf_month) / vol_month if vol_month > 0 else np.nan
    esg_exposure = float(beta_esg @ w)

    out = {
        "expected_return_month": er_month,
        "expected_return_ann": er_month * 12,
        "vol_month": vol_month,
        "vol_ann": vol_month * np.sqrt(12),
        "sharpe_month": sharpe,
        "esg_exposure": esg_exposure,
    }

    # Optional tracking error vs benchmark
    if benchmark_w is not None:
        b = benchmark_w.reindex(tickers).fillna(0.0).values
        diff = w - b
        te_var = float(diff.T @ Sigma.values @ diff)
        out["tracking_error_month"] = te_var**0.5
        out["tracking_error_ann"] = out["tracking_error_month"] * np.sqrt(12)
        out["active_return_month"] = er_month - float(
            exp_ret.reindex(tickers).values @ b
        )

    return out


# --------------------------------
# 5) Efficient frontier (optional)
# --------------------------------


def efficient_frontier_esg(
    exp_ret: pd.Series,
    Sigma: pd.DataFrame,
    esg_beta: pd.Series,
    gammas: list,
    long_only: bool = True,
    w_max: float = 0.10,
    esg_bounds: tuple = None,
    sector_map: pd.Series = None,
    sector_caps: dict = None,
    prev_w: pd.Series = None,
    lambda_tc: float = 0.0,
    solver: str = "ECOS",
    rf_month: float = 0.0,
):
    """
    Solve the optimizer for a list of risk-aversion levels γ to trace a frontier.
    Returns DataFrame with weights & summary stats per γ.
    """
    rows, w_dict = [], {}
    for g in gammas:
        w = optimize_markowitz_esg(
            exp_ret,
            Sigma,
            esg_beta,
            gamma=g,
            long_only=long_only,
            w_max=w_max,
            esg_bounds=esg_bounds,
            sector_map=sector_map,
            sector_caps=sector_caps,
            prev_w=prev_w,
            lambda_tc=lambda_tc,
            solver=solver,
        )
        stats = evaluate_portfolio(w, exp_ret, Sigma, esg_beta, rf_month=rf_month)
        rows.append({"gamma": g, **stats})
        w_dict[g] = w
    return pd.DataFrame(rows).set_index("gamma"), w_dict


# ---------------------------------
# 7) Main execution
# ---------------------------------


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Portfolio Optimization with Markowitz Mean-Variance and ESG Control",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--gamma",
        type=float,
        default=4.0,
        help="Risk aversion parameter (higher = more conservative, default: 4.0)",
    )
    parser.add_argument(
        "--w-max",
        type=float,
        default=0.10,
        help="Maximum position size per ticker (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--esg-neutral",
        action="store_true",
        help="Enforce ESG-neutral portfolio (exposure in [-0.05, 0.05])",
    )
    parser.add_argument(
        "--esg-lower",
        type=float,
        default=None,
        help="Custom ESG lower bound (overrides --esg-neutral)",
    )
    parser.add_argument(
        "--esg-upper",
        type=float,
        default=None,
        help="Custom ESG upper bound (overrides --esg-neutral)",
    )
    parser.add_argument(
        "--sector-caps",
        action="store_true",
        help="Apply GICS sector concentration limits",
    )
    parser.add_argument(
        "--lookback-months",
        type=int,
        default=36,
        help="Lookback period for covariance estimation (default: 36 months)",
    )
    parser.add_argument(
        "--shrinkage",
        type=float,
        default=0.25,
        help="Covariance shrinkage intensity [0, 1] (default: 0.25)",
    )
    parser.add_argument(
        "--continuous-esg-only",
        action="store_true",
        help="Only use tickers with continuous ESG data",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date for portfolio (YYYY-MM-DD, default: latest)",
    )
    parser.add_argument(
        "--solver",
        type=str,
        default="ECOS",
        choices=["ECOS", "OSQP", "SCS"],
        help="CVXPY solver (default: ECOS)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        default=True,
        help="Save optimization results (default: True)",
    )
    parser.add_argument(
        "--compute-frontier",
        action="store_true",
        help="Compute efficient frontier for multiple gamma values",
    )

    args = parser.parse_args()

    # Initialize
    logger.info("=" * 80)
    logger.info("PORTFOLIO OPTIMIZATION WITH ESG CONTROL")
    logger.info("=" * 80)

    config = Config("config/settings.yaml")
    data_root = Path(config.get("storage.local.root_path"))

    # Load expected returns
    logger.info("\n1. Loading expected returns...")
    exp_ret = load_expected_returns(data_root, date=args.date)
    tickers = exp_ret.index.tolist()

    # Filter to continuous ESG if requested
    if args.continuous_esg_only:
        continuous_file = Path("data/continuous_esg_tickers.txt")
        if continuous_file.exists():
            with open(continuous_file) as f:
                continuous_tickers = [line.strip() for line in f]
            tickers = [t for t in tickers if t in continuous_tickers]
            exp_ret = exp_ret.loc[tickers]
            logger.info(f"  Filtered to {len(tickers)} continuous ESG tickers")

    # Load ESG betas
    logger.info("\n2. Loading ESG betas...")
    esg_beta = load_esg_betas(data_root, tickers)

    # Align tickers (keep only those with both exp_ret and esg_beta)
    common_tickers = exp_ret.index.intersection(esg_beta.index).tolist()
    exp_ret = exp_ret.loc[common_tickers]
    esg_beta = esg_beta.loc[common_tickers]
    logger.info(f"  Common universe: {len(common_tickers)} tickers")

    # Load risk-free rate
    logger.info("\n3. Loading risk-free rate...")
    rf_monthly = get_current_rf_monthly(data_root)

    # Load returns panel for covariance
    logger.info("\n4. Loading historical returns for covariance estimation...")
    end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    start_date = (
        pd.Timestamp.today() - pd.DateOffset(months=args.lookback_months)
    ).strftime("%Y-%m-%d")

    returns_panel = load_returns_panel(
        data_root=data_root,
        tickers=common_tickers,
        start_date=start_date,
        end_date=end_date,
        frequency="monthly",
    )

    # Build covariance matrix
    logger.info("\n5. Building covariance matrix...")
    Sigma = build_shrinkage_cov(returns_panel, col="excess", shrink=args.shrinkage)

    # Align all inputs to common ticker set
    final_tickers = exp_ret.index.intersection(Sigma.index).tolist()
    exp_ret = exp_ret.loc[final_tickers]
    esg_beta = esg_beta.reindex(final_tickers).fillna(0.0)
    Sigma = Sigma.loc[final_tickers, final_tickers]

    logger.info(f"  Final universe: {len(final_tickers)} tickers")

    # Load sector mapping if needed
    sector_map = None
    sector_caps_dict = None
    if args.sector_caps:
        logger.info("\n5. Loading sector mapping...")
        sector_map = load_sector_mapping(data_root, final_tickers)
        # Default GICS sector caps (30% max per sector)
        sector_caps_dict = {
            sector: 0.30 for sector in sector_map.unique() if pd.notna(sector)
        }
        logger.info(f"  Applying sector caps: {len(sector_caps_dict)} sectors")

    # ESG bounds
    esg_bounds = None
    if args.esg_lower is not None and args.esg_upper is not None:
        esg_bounds = (args.esg_lower, args.esg_upper)
        logger.info(f"\n6. ESG bounds: [{args.esg_lower:.3f}, {args.esg_upper:.3f}]")
    elif args.esg_neutral:
        esg_bounds = (-0.05, 0.05)
        logger.info(f"\n6. ESG neutral bounds: [-0.05, 0.05]")

    # Optimize portfolio
    if args.compute_frontier:
        logger.info("\n7. Computing efficient frontier...")
        gammas = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
        frontier_df, weights_map = efficient_frontier_esg(
            exp_ret=exp_ret,
            Sigma=Sigma,
            esg_beta=esg_beta,
            gammas=gammas,
            long_only=True,
            w_max=args.w_max,
            esg_bounds=esg_bounds,
            sector_map=sector_map,
            sector_caps=sector_caps_dict,
            solver=args.solver,
            rf_month=rf_monthly,
        )

        print("\n" + "=" * 80)
        print("EFFICIENT FRONTIER")
        print("=" * 80)
        print(frontier_df.to_string())

        if args.save:
            results_dir = data_root / "results" / "portfolio_optimization"
            results_dir.mkdir(parents=True, exist_ok=True)

            # Clean existing results before saving new ones
            logger.info("Cleaning existing portfolio optimization results...")
            for old_file in results_dir.glob("*"):
                if old_file.is_file():
                    old_file.unlink()
                    logger.debug(f"Deleted: {old_file.name}")

            frontier_df.to_csv(results_dir / "efficient_frontier.csv")

            # Save weights for each gamma
            for gamma, weights in weights_map.items():
                weights_file = results_dir / f"weights_gamma_{gamma:.1f}.csv"
                weights[weights.abs() > 1e-6].sort_values(ascending=False).to_csv(
                    weights_file
                )

            logger.info(f"\nSaved frontier results to {results_dir}")

    else:
        logger.info(f"\n8. Optimizing portfolio (gamma={args.gamma})...")
        weights = optimize_markowitz_esg(
            exp_ret=exp_ret,
            Sigma=Sigma,
            esg_beta=esg_beta,
            gamma=args.gamma,
            long_only=True,
            w_max=args.w_max,
            esg_bounds=esg_bounds,
            sector_map=sector_map,
            sector_caps=sector_caps_dict,
            solver=args.solver,
        )

        # Evaluate portfolio
        logger.info("\n9. Evaluating portfolio...")
        stats = evaluate_portfolio(
            weights, exp_ret, Sigma, esg_beta, rf_month=rf_monthly
        )

        # Display results
        print("\n" + "=" * 80)
        print("OPTIMAL PORTFOLIO")
        print("=" * 80)
        print(f"\nPortfolio Statistics:")
        print(
            f"  Expected Return (monthly):  {stats['expected_return_month']:.4f} ({stats['expected_return_month']*100:.2f}%)"
        )
        print(
            f"  Expected Return (annual):   {stats['expected_return_ann']:.4f} ({stats['expected_return_ann']*100:.2f}%)"
        )
        print(
            f"  Volatility (monthly):       {stats['vol_month']:.4f} ({stats['vol_month']*100:.2f}%)"
        )
        print(
            f"  Volatility (annual):        {stats['vol_ann']:.4f} ({stats['vol_ann']*100:.2f}%)"
        )
        print(f"  Sharpe Ratio (monthly):     {stats['sharpe_month']:.4f}")
        print(f"  ESG Exposure (β_ESG):       {stats['esg_exposure']:.4f}")

        print(f"\nTop 20 Holdings:")
        top20 = weights[weights > 1e-6].sort_values(ascending=False).head(20)
        for ticker, weight in top20.items():
            print(f"  {ticker:6s}  {weight*100:6.2f}%")

        print(f"\nActive Positions: {(weights.abs() > 1e-6).sum()}")
        print(f"Concentration (top 10): {weights.nlargest(10).sum()*100:.2f}%")

        # Save results
        if args.save:
            results_dir = data_root / "results" / "portfolio_optimization"
            results_dir.mkdir(parents=True, exist_ok=True)

            # Save weights
            weights_df = pd.DataFrame(
                {
                    "ticker": weights.index,
                    "weight": weights.values,
                    "exp_return_monthly": exp_ret.values,
                    "esg_beta": esg_beta.values,
                }
            )
            weights_df = weights_df[weights_df["weight"].abs() > 1e-6].sort_values(
                "weight", ascending=False
            )
            weights_file = results_dir / f"optimal_weights_gamma_{args.gamma:.1f}.csv"
            weights_df.to_csv(weights_file, index=False)

            # Save stats
            stats_df = pd.DataFrame([stats])
            stats_file = results_dir / f"portfolio_stats_gamma_{args.gamma:.1f}.csv"
            stats_df.to_csv(stats_file, index=False)

            logger.info(f"\nSaved results to {results_dir}")

    logger.info("\n" + "=" * 80)
    logger.info("OPTIMIZATION COMPLETE")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
