#!/usr/bin/env python3
"""
Demo: Market Beta and Alpha Calculation

Demonstrates the MarketBetaManager for calculating rolling market beta and alpha.
Uses 60-month rolling windows with monthly return data.

Example Usage:
    # Calculate beta for a single ticker
    python tests/demo_market_beta.py AAPL

    # Calculate beta for multiple tickers
    python tests/demo_market_beta.py AAPL MSFT GOOGL

    # Calculate beta for all continuous ESG tickers
    python tests/demo_market_beta.py --all-continuous
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import MarketBetaManager
from universe import SP500Universe

# Configuration
config = Config("config/settings.yaml")
data_root = config.get("storage.local.root_path")


def demo_single_ticker(ticker: str):
    """Demonstrate beta calculation for a single ticker"""
    print("=" * 80)
    print(f"MARKET BETA CALCULATION: {ticker}")
    print("=" * 80)
    print()

    # Initialize universe and beta manager
    sp500_universe = SP500Universe(data_root)
    beta_manager = MarketBetaManager(
        universe=sp500_universe, window_months=60, min_observations=36
    )

    # Calculate beta
    print(f"📊 Calculating 60-month rolling beta for {ticker}...")
    beta_df = beta_manager.calculate_beta(ticker, save=True)

    if beta_df is None or beta_df.empty:
        print(f"❌ No beta results for {ticker}")
        return

    print(f"✅ Calculated {len(beta_df)} beta estimates")
    print()

    # Display summary statistics
    print("BETA STATISTICS")
    print("-" * 80)
    print(
        f"Period:           {beta_df['date'].min().strftime('%Y-%m-%d')} to {beta_df['date'].max().strftime('%Y-%m-%d')}"
    )
    print(f"Observations:     {len(beta_df)}")
    print()
    print(f"Beta:")
    print(f"  Mean:           {beta_df['beta'].mean():.4f}")
    print(f"  Median:         {beta_df['beta'].median():.4f}")
    print(f"  Std Dev:        {beta_df['beta'].std():.4f}")
    print(f"  Min:            {beta_df['beta'].min():.4f}")
    print(f"  Max:            {beta_df['beta'].max():.4f}")
    print()
    print(f"Alpha (Annualized):")
    print(
        f"  Mean:           {beta_df['alpha'].mean():.4f} ({beta_df['alpha'].mean()*100:.2f}%)"
    )
    print(
        f"  Median:         {beta_df['alpha'].median():.4f} ({beta_df['alpha'].median()*100:.2f}%)"
    )
    print(f"  Std Dev:        {beta_df['alpha'].std():.4f}")
    print()
    print(f"R-Squared:")
    print(f"  Mean:           {beta_df['r_squared'].mean():.4f}")
    print(f"  Median:         {beta_df['r_squared'].median():.4f}")
    print()
    print(f"Correlation:")
    print(f"  Mean:           {beta_df['correlation'].mean():.4f}")
    print()

    # Display recent values
    print("RECENT BETA ESTIMATES (Last 12 months)")
    print("-" * 80)
    recent = beta_df.tail(12)
    print(f"{'Date':<12} {'Beta':>8} {'Alpha':>8} {'R²':>8} {'Corr':>8} {'Sig':>8}")
    print("-" * 80)
    for _, row in recent.iterrows():
        sig = (
            "***"
            if row["p_value_beta"] < 0.01
            else (
                "**"
                if row["p_value_beta"] < 0.05
                else ("*" if row["p_value_beta"] < 0.10 else "")
            )
        )
        print(
            f"{row['date'].strftime('%Y-%m-%d'):<12} "
            f"{row['beta']:>8.4f} "
            f"{row['alpha']:>8.4f} "
            f"{row['r_squared']:>8.4f} "
            f"{row['correlation']:>8.4f} "
            f"{sig:>8}"
        )
    print()
    print("Significance: *** p<0.01, ** p<0.05, * p<0.10")
    print()

    # Interpretation
    latest = beta_df.iloc[-1]
    print("INTERPRETATION (Latest Estimate)")
    print("-" * 80)
    print(f"Date:            {latest['date'].strftime('%Y-%m-%d')}")
    print(f"Beta:            {latest['beta']:.4f}")

    if latest["beta"] > 1.1:
        print(f"  → {ticker} is MORE volatile than the market (aggressive stock)")
    elif latest["beta"] < 0.9:
        print(f"  → {ticker} is LESS volatile than the market (defensive stock)")
    else:
        print(f"  → {ticker} moves WITH the market (neutral)")

    print()
    print(
        f"Alpha:           {latest['alpha']:.4f} ({latest['alpha']*100:.2f}% annualized)"
    )

    if abs(latest["alpha"]) < 0.01:
        print(f"  → Performance in line with market expectations")
    elif latest["alpha"] > 0:
        print(
            f"  → Outperforming by {latest['alpha']*100:.2f}% per year (after risk adjustment)"
        )
    else:
        print(
            f"  → Underperforming by {abs(latest['alpha'])*100:.2f}% per year (after risk adjustment)"
        )

    print()
    print(
        f"R-Squared:       {latest['r_squared']:.4f} ({latest['r_squared']*100:.1f}% of variance explained)"
    )
    print(f"Correlation:     {latest['correlation']:.4f}")
    print()

    # Save location
    ticker_path = sp500_universe.get_ticker_path(ticker)
    results_file = ticker_path / "results" / "betas" / "market_beta.parquet"
    print(f"💾 Results saved to: {results_file}")
    print()


def demo_multiple_tickers(tickers: list):
    """Demonstrate beta calculation for multiple tickers"""
    print("=" * 80)
    print(f"MARKET BETA CALCULATION: {len(tickers)} TICKERS")
    print("=" * 80)
    print()

    # Initialize universe and beta manager
    sp500_universe = SP500Universe(data_root)
    beta_manager = MarketBetaManager(
        universe=sp500_universe, window_months=60, min_observations=36
    )

    # Calculate betas
    results = {}
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Processing {ticker}...")
        beta_df = beta_manager.calculate_beta(ticker, save=True)
        if beta_df is not None and not beta_df.empty:
            results[ticker] = beta_df

    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Successful:      {len(results)}/{len(tickers)}")
    print()

    if results:
        # Create comparison table
        print("LATEST BETA ESTIMATES")
        print("-" * 80)
        print(
            f"{'Ticker':<8} {'Beta':>8} {'Alpha':>8} {'R²':>8} {'Corr':>8} {'Period':<20}"
        )
        print("-" * 80)

        for ticker, beta_df in sorted(results.items()):
            latest = beta_df.iloc[-1]
            period = f"{beta_df['date'].min().strftime('%Y-%m')} to {beta_df['date'].max().strftime('%Y-%m')}"
            print(
                f"{ticker:<8} "
                f"{latest['beta']:>8.4f} "
                f"{latest['alpha']:>8.4f} "
                f"{latest['r_squared']:>8.4f} "
                f"{latest['correlation']:>8.4f} "
                f"{period:<20}"
            )
        print()

        # Average statistics
        all_latest_betas = [df.iloc[-1]["beta"] for df in results.values()]
        all_latest_alphas = [df.iloc[-1]["alpha"] for df in results.values()]
        print(f"Average Beta:    {sum(all_latest_betas)/len(all_latest_betas):.4f}")
        print(f"Average Alpha:   {sum(all_latest_alphas)/len(all_latest_alphas):.4f}")
        print()


def demo_continuous_esg_tickers():
    """Calculate beta for all continuous ESG tickers"""
    print("=" * 80)
    print("MARKET BETA CALCULATION: ALL CONTINUOUS ESG TICKERS")
    print("=" * 80)
    print()

    # Load continuous ESG tickers
    continuous_file = Path(data_root) / "continuous_esg_tickers.txt"
    if not continuous_file.exists():
        print(f"❌ Continuous ESG tickers file not found: {continuous_file}")
        print("Please run check_esg_continuity.py first.")
        return

    with open(continuous_file, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]

    print(f"📋 Loaded {len(tickers)} continuous ESG tickers")
    print()

    # Initialize universe and beta manager
    sp500_universe = SP500Universe(data_root)
    beta_manager = MarketBetaManager(
        universe=sp500_universe, window_months=60, min_observations=36
    )

    # Calculate betas
    print("🚀 Starting beta calculation...")
    print()

    success_count = 0
    fail_count = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i:3d}/{len(tickers)}] {ticker:6s} ... ", end="", flush=True)
        beta_df = beta_manager.calculate_beta(ticker, save=True)

        if beta_df is not None and not beta_df.empty:
            latest_beta = beta_df.iloc[-1]["beta"]
            latest_alpha = beta_df.iloc[-1]["alpha"]
            print(
                f"✅ β={latest_beta:.3f}, α={latest_alpha:.4f} ({len(beta_df)} estimates)"
            )
            success_count += 1
        else:
            print(f"❌ No data")
            fail_count += 1

    print()
    print("=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print(f"Total tickers:   {len(tickers)}")
    print(f"Successful:      {success_count} ({success_count/len(tickers)*100:.1f}%)")
    print(f"Failed:          {fail_count} ({fail_count/len(tickers)*100:.1f}%)")
    print()
    print(
        f"💾 Results saved to: data/curated/tickers/exchange=us/ticker=*/results/betas/market_beta.parquet"
    )
    print()


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Single ticker:     python tests/demo_market_beta.py AAPL")
        print("  Multiple tickers:  python tests/demo_market_beta.py AAPL MSFT GOOGL")
        print("  All continuous:    python tests/demo_market_beta.py --all-continuous")
        sys.exit(1)

    if sys.argv[1] == "--all-continuous":
        demo_continuous_esg_tickers()
    elif len(sys.argv) == 2:
        demo_single_ticker(sys.argv[1])
    else:
        demo_multiple_tickers(sys.argv[1:])


if __name__ == "__main__":
    main()
