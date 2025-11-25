#!/usr/bin/env python3
"""
Analyze Market Beta Results

Load and analyze saved beta results to understand risk characteristics
and time-varying behavior.
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import MarketBetaManager
from universe import SP500Universe

config = Config("config/settings.yaml")
data_root = config.get("storage.local.root_path")


def analyze_beta_stability(ticker: str):
    """Analyze how stable beta is over time"""
    sp500_universe = SP500Universe(data_root)
    beta_manager = MarketBetaManager(universe=sp500_universe)

    # Load beta
    beta_df = beta_manager.load_beta(ticker)

    if beta_df is None:
        print(f"No beta data for {ticker}. Run demo_market_beta.py first.")
        return

    print(f"Beta Stability Analysis: {ticker}")
    print("=" * 60)

    # Overall statistics
    print(f"\nOverall Period: {beta_df['date'].min()} to {beta_df['date'].max()}")
    print(f"Observations: {len(beta_df)}")
    print(f"\nBeta Statistics:")
    print(f"  Mean:     {beta_df['beta'].mean():.4f}")
    print(f"  Std Dev:  {beta_df['beta'].std():.4f}")
    print(
        f"  Min:      {beta_df['beta'].min():.4f} on {beta_df.loc[beta_df['beta'].idxmin(), 'date']}"
    )
    print(
        f"  Max:      {beta_df['beta'].max():.4f} on {beta_df.loc[beta_df['beta'].idxmax(), 'date']}"
    )

    # Coefficient of variation (relative stability)
    cv = beta_df["beta"].std() / beta_df["beta"].mean()
    print(f"  CV:       {cv:.4f} ", end="")
    if cv < 0.1:
        print("(Very Stable)")
    elif cv < 0.2:
        print("(Stable)")
    elif cv < 0.3:
        print("(Moderately Stable)")
    else:
        print("(Unstable)")

    # Trend analysis
    from scipy import stats

    x = range(len(beta_df))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, beta_df["beta"])

    print(f"\nBeta Trend:")
    print(f"  Slope:    {slope:.6f} per month")
    print(f"  R²:       {r_value**2:.4f}")
    print(f"  p-value:  {p_value:.4f} ", end="")
    if p_value < 0.05:
        if slope > 0:
            print("(Significantly Increasing)")
        else:
            print("(Significantly Decreasing)")
    else:
        print("(No Significant Trend)")

    # Alpha statistics
    print(f"\nAlpha Statistics (Annualized):")
    print(
        f"  Mean:     {beta_df['alpha'].mean():.4f} ({beta_df['alpha'].mean()*100:.2f}%)"
    )
    print(
        f"  Median:   {beta_df['alpha'].median():.4f} ({beta_df['alpha'].median()*100:.2f}%)"
    )
    print(f"  Std Dev:  {beta_df['alpha'].std():.4f}")

    # Significance
    sig_betas = (beta_df["p_value_beta"] < 0.05).sum()
    sig_alphas = (beta_df["p_value_alpha"] < 0.05).sum()
    print(f"\nStatistical Significance:")
    print(
        f"  Beta significant (p<0.05):  {sig_betas}/{len(beta_df)} ({sig_betas/len(beta_df)*100:.1f}%)"
    )
    print(
        f"  Alpha significant (p<0.05): {sig_alphas}/{len(beta_df)} ({sig_alphas/len(beta_df)*100:.1f}%)"
    )

    # Recent vs historical
    recent = beta_df.tail(12)
    historical = beta_df.head(len(beta_df) - 12)

    print(f"\nRecent vs Historical (Last 12 months):")
    print(f"  Historical Beta: {historical['beta'].mean():.4f}")
    print(f"  Recent Beta:     {recent['beta'].mean():.4f}")
    print(
        f"  Change:          {(recent['beta'].mean() - historical['beta'].mean()):.4f}"
    )


def compare_tickers(tickers: list):
    """Compare beta characteristics across multiple tickers"""
    sp500_universe = SP500Universe(data_root)
    beta_manager = MarketBetaManager(universe=sp500_universe)

    print("Beta Comparison")
    print("=" * 80)

    results = []

    for ticker in tickers:
        beta_df = beta_manager.load_beta(ticker)
        if beta_df is not None:
            latest = beta_df.iloc[-1]
            results.append(
                {
                    "Ticker": ticker,
                    "Beta": latest["beta"],
                    "Alpha": latest["alpha"],
                    "R²": latest["r_squared"],
                    "Corr": latest["correlation"],
                    "Mean Beta": beta_df["beta"].mean(),
                    "Std Beta": beta_df["beta"].std(),
                    "CV": beta_df["beta"].std() / beta_df["beta"].mean(),
                }
            )

    if not results:
        print("No data available. Run demo_market_beta.py first.")
        return

    df = pd.DataFrame(results)

    print("\nLatest Beta Estimates:")
    print(df[["Ticker", "Beta", "Alpha", "R²", "Corr"]].to_string(index=False))

    print("\n\nBeta Stability:")
    print(df[["Ticker", "Mean Beta", "Std Beta", "CV"]].to_string(index=False))

    print("\n\nRisk Categories:")
    for _, row in df.iterrows():
        ticker = row["Ticker"]
        beta = row["Beta"]
        cv = row["CV"]

        risk_type = (
            "Defensive" if beta < 0.9 else "Neutral" if beta <= 1.1 else "Aggressive"
        )
        stability = "Stable" if cv < 0.2 else "Moderate" if cv < 0.3 else "Unstable"

        print(
            f"{ticker:6s}: {risk_type:10s} (β={beta:.3f}), {stability:8s} (CV={cv:.3f})"
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Stability analysis:  python tests/analyze_beta.py AAPL")
        print(
            "  Compare tickers:     python tests/analyze_beta.py AAPL MSFT GOOGL TSLA"
        )
        sys.exit(1)

    if len(sys.argv) == 2:
        analyze_beta_stability(sys.argv[1])
    else:
        compare_tickers(sys.argv[1:])
