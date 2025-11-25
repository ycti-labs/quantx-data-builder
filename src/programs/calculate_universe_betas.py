#!/usr/bin/env python3
"""
Calculate Market Beta for All Universe Members

Calculates 60-month rolling market beta and alpha for all S&P 500 universe members.
Uses monthly return data and saves results to ticker-specific results directories.

Usage:
    # Calculate for all historical members
    python src/programs/calculate_universe_betas.py

    # Calculate for specific date range
    python src/programs/calculate_universe_betas.py --start-date 2014-01-01 --end-date 2024-12-31

    # Calculate only for continuous ESG tickers
    python src/programs/calculate_universe_betas.py --continuous-esg-only

    # Skip already calculated tickers
    python src/programs/calculate_universe_betas.py --skip-existing

    # Dry run to see what would be processed
    python src/programs/calculate_universe_betas.py --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import MarketBetaManager
from universe import SP500Universe

config = Config("config/settings.yaml")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Calculate market beta for S&P 500 universe members",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate for all universe members
  python src/programs/calculate_universe_betas.py

  # Calculate for specific period
  python src/programs/calculate_universe_betas.py --start-date 2014-01-01 --end-date 2024-12-31

  # Only continuous ESG tickers
  python src/programs/calculate_universe_betas.py --continuous-esg-only

  # Skip already calculated
  python src/programs/calculate_universe_betas.py --skip-existing

  # Preview without calculating
  python src/programs/calculate_universe_betas.py --dry-run
        """,
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date for universe membership filter (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date for universe membership filter (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--continuous-esg-only",
        action="store_true",
        help="Only calculate for tickers with continuous ESG data",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers that already have beta results",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be calculated without actually calculating",
    )

    parser.add_argument(
        "--window-months",
        type=int,
        default=60,
        help="Rolling window size in months (default: 60)",
    )

    parser.add_argument(
        "--min-observations",
        type=int,
        default=36,
        help="Minimum observations required (default: 36)",
    )

    return parser.parse_args()


def load_continuous_esg_tickers(data_root: Path) -> list:
    """Load list of tickers with continuous ESG data"""
    continuous_file = data_root / "continuous_esg_tickers.txt"

    if not continuous_file.exists():
        print(f"❌ Continuous ESG tickers file not found: {continuous_file}")
        print("Please run: python src/programs/check_esg_continuity.py")
        sys.exit(1)

    with open(continuous_file, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]

    return tickers


def get_existing_beta_tickers(universe: SP500Universe) -> set:
    """Get set of tickers that already have beta results"""
    existing = set()
    tickers_dir = Path(universe.data_root) / "curated" / "tickers" / "exchange=us"

    if not tickers_dir.exists():
        return existing

    for ticker_dir in tickers_dir.glob("ticker=*"):
        ticker = ticker_dir.name.replace("ticker=", "")
        beta_file = ticker_dir / "results" / "betas" / "market_beta.parquet"
        if beta_file.exists():
            existing.add(ticker)

    return existing


def main():
    args = parse_args()

    print("=" * 80)
    print("MARKET BETA CALCULATION FOR UNIVERSE")
    print("=" * 80)
    print()

    # Initialize universe
    data_root = Path(config.get("storage.local.root_path"))
    sp500_universe = SP500Universe(data_root)

    # Get date range
    start_date = args.start_date or config.get("universe.sp500.start_date")
    end_date = args.end_date or config.get("universe.sp500.end_date")

    print(f"📊 Universe:      S&P 500")
    print(f"📅 Period:        {start_date} to {end_date}")
    print(f"🪟  Window:        {args.window_months} months")
    print(f"📏 Min Obs:       {args.min_observations} observations")
    print()

    # Get ticker list
    if args.continuous_esg_only:
        print("🎯 Loading continuous ESG tickers...")
        tickers = load_continuous_esg_tickers(data_root)
        print(f"   Found {len(tickers)} tickers with continuous ESG data")
    else:
        print("🎯 Loading all historical universe members...")
        tickers = sp500_universe.get_all_historical_members(start_date, end_date)
        print(f"   Found {len(tickers)} unique tickers")

    print()

    # Filter existing if requested
    if args.skip_existing:
        existing = get_existing_beta_tickers(sp500_universe)
        before_count = len(tickers)
        tickers = [t for t in tickers if t not in existing]
        print(
            f"⏭️  Skip Existing: Filtered out {before_count - len(tickers)} tickers with existing results"
        )
        print(f"   Remaining: {len(tickers)} tickers to process")
        print()

    # Sort for consistent ordering
    tickers = sorted(tickers)

    # Dry run check
    if args.dry_run:
        print("🔍 DRY RUN MODE - Preview Only")
        print("=" * 80)
        print(f"Would calculate beta for {len(tickers)} tickers:")
        print()

        # Show first 20 tickers
        for i, ticker in enumerate(tickers[:20], 1):
            print(f"  {i:3d}. {ticker}")

        if len(tickers) > 20:
            print(f"  ... and {len(tickers) - 20} more")

        print()
        print("Run without --dry-run to execute calculation")
        return

    # Initialize beta manager
    print("🚀 Initializing Market Beta Manager...")
    beta_manager = MarketBetaManager(
        universe=sp500_universe,
        window_months=args.window_months,
        min_observations=args.min_observations,
    )
    print()

    # Calculate betas
    print("=" * 80)
    print("CALCULATING BETAS")
    print("=" * 80)
    print()

    success_count = 0
    fail_count = 0
    no_data_count = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"[{i:4d}/{len(tickers)}] {ticker:6s} ... ", end="", flush=True)

        try:
            beta_df = beta_manager.calculate_beta(ticker, save=True)

            if beta_df is not None and not beta_df.empty:
                latest = beta_df.iloc[-1]
                beta_val = latest["beta"]
                alpha_val = latest["alpha"]
                r2_val = latest["r_squared"]

                print(
                    f"✅ β={beta_val:6.3f}, α={alpha_val:7.4f}, R²={r2_val:.3f} ({len(beta_df):3d} estimates)"
                )
                success_count += 1
            else:
                print(f"⚠️  No data (insufficient observations)")
                no_data_count += 1

        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
            fail_count += 1

    # Summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total tickers:    {len(tickers)}")
    print(f"✅ Successful:    {success_count} ({success_count/len(tickers)*100:.1f}%)")
    print(f"⚠️  No data:       {no_data_count} ({no_data_count/len(tickers)*100:.1f}%)")
    print(f"❌ Failed:        {fail_count} ({fail_count/len(tickers)*100:.1f}%)")
    print()

    if success_count > 0:
        print(
            f"💾 Results saved to: data/curated/tickers/exchange=us/ticker=*/results/betas/market_beta.parquet"
        )
        print()

    # Calculate summary statistics
    if success_count > 0:
        print("=" * 80)
        print("UNIVERSE BETA STATISTICS")
        print("=" * 80)

        all_betas = []
        all_alphas = []
        all_r2 = []

        for ticker in tickers:
            try:
                beta_df = beta_manager.load_beta(ticker)
                if beta_df is not None and not beta_df.empty:
                    latest = beta_df.iloc[-1]
                    all_betas.append(latest["beta"])
                    all_alphas.append(latest["alpha"])
                    all_r2.append(latest["r_squared"])
            except:
                pass

        if all_betas:
            import numpy as np

            print(f"\nLatest Beta Estimates (n={len(all_betas)}):")
            print(f"  Mean:             {np.mean(all_betas):.4f}")
            print(f"  Median:           {np.median(all_betas):.4f}")
            print(f"  Std Dev:          {np.std(all_betas):.4f}")
            print(f"  Min:              {np.min(all_betas):.4f}")
            print(f"  Max:              {np.max(all_betas):.4f}")
            print(f"  25th percentile:  {np.percentile(all_betas, 25):.4f}")
            print(f"  75th percentile:  {np.percentile(all_betas, 75):.4f}")

            print(f"\nLatest Alpha Estimates (Annualized):")
            print(
                f"  Mean:             {np.mean(all_alphas):.4f} ({np.mean(all_alphas)*100:.2f}%)"
            )
            print(
                f"  Median:           {np.median(all_alphas):.4f} ({np.median(all_alphas)*100:.2f}%)"
            )
            print(f"  Std Dev:          {np.std(all_alphas):.4f}")

            print(f"\nLatest R-Squared:")
            print(f"  Mean:             {np.mean(all_r2):.4f}")
            print(f"  Median:           {np.median(all_r2):.4f}")

            # Risk categories
            defensive = sum(1 for b in all_betas if b < 0.9)
            neutral = sum(1 for b in all_betas if 0.9 <= b <= 1.1)
            aggressive = sum(1 for b in all_betas if b > 1.1)

            print(f"\nRisk Categories:")
            print(
                f"  Defensive (β<0.9):  {defensive:3d} ({defensive/len(all_betas)*100:.1f}%)"
            )
            print(
                f"  Neutral (0.9-1.1):  {neutral:3d} ({neutral/len(all_betas)*100:.1f}%)"
            )
            print(
                f"  Aggressive (β>1.1): {aggressive:3d} ({aggressive/len(all_betas)*100:.1f}%)"
            )
            print()


if __name__ == "__main__":
    main()
