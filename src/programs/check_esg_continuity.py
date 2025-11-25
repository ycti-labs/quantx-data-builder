#!/usr/bin/env python3
"""
Check ESG data continuity across all tickers.
Identifies gaps in the monthly time series for each ticker.
"""

import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import ESGManager
from universe import SP500Universe

config = Config("config/settings.yaml")


def check_esg_continuity(ticker, analysis_start_date="2014-01-01"):
    """
    Check if ESG data is continuous for a given ticker from analysis_start_date onwards.
    Returns gap information if discontinuous.

    Args:
        ticker: Ticker symbol
        analysis_start_date: Start date for gap analysis (default: 2014-01-01)
    """
    sp500_universe = SP500Universe(config.get("storage.local.root_path"))
    esg_dir = sp500_universe.get_ticker_path(ticker) / "esg"

    if not esg_dir.exists():
        return None, "No ESG data directory"

    # Load all years
    all_data = []
    for year_dir in sorted(esg_dir.glob("year=*")):
        parquet_file = year_dir / "part-000.parquet"
        if parquet_file.exists():
            try:
                df = pd.read_parquet(parquet_file)
                all_data.append(df)
            except Exception as e:
                print(f"⚠️  Error reading {parquet_file}: {e}")

    if not all_data:
        return None, "No readable parquet files"

    # Combine all data
    df = pd.concat(all_data, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Filter to analysis period (2014-01-01 onwards)
    analysis_start = pd.to_datetime(analysis_start_date)
    df_analysis = df[df["date"] >= analysis_start].copy()

    # Store full date range info
    full_first_date = df["date"].min()
    full_last_date = df["date"].max()

    if len(df_analysis) == 0:
        return None, f"No data after {analysis_start_date}"

    # Check for gaps in analysis period only
    date_range = pd.date_range(
        start=df_analysis["date"].min(), end=df_analysis["date"].max(), freq="MS"
    )
    actual_dates = set(df_analysis["date"].dt.to_period("M"))
    expected_dates = set(date_range.to_period("M"))

    missing_dates = sorted(expected_dates - actual_dates)

    info = {
        "ticker": ticker,
        "full_first_date": full_first_date,
        "full_last_date": full_last_date,
        "analysis_first_date": df_analysis["date"].min(),
        "analysis_last_date": df_analysis["date"].max(),
        "total_records": len(df_analysis),
        "expected_records": len(expected_dates),
        "missing_records": len(missing_dates),
        "missing_dates": missing_dates[:10] if missing_dates else [],  # First 10 gaps
        "is_continuous": len(missing_dates) == 0,
    }

    return info, None


def main():
    print("=" * 80)
    print("ESG DATA CONTINUITY CHECKER")
    print("=" * 80)
    print()

    # Analysis parameters
    analysis_start_date = "2014-01-01"
    print(f"📅 Analysis Period: {analysis_start_date} onwards")
    print(f"   (Ignoring gaps before {analysis_start_date})")
    print()

    # Initialize universe to get research period
    sp500_universe = SP500Universe(config.get("storage.local.root_path"))
    research_start = config.get("universe.sp500.start_date")
    research_end = config.get("universe.sp500.end_date")

    print(f"🌍 Universe: S&P 500")
    print(f"📊 Research Period: {research_start} to {research_end}")
    print()

    # Get all historical members during research period
    print("Loading S&P 500 historical members...")
    universe_members = sp500_universe.get_all_historical_members(
        research_start, research_end
    )
    print(f"Total universe members: {len(universe_members)}")

    # Check which members have ESG data
    data_root = Path(config.get("storage.local.root_path"))
    esg_base = data_root / "curated/tickers/exchange=us"

    tickers_with_esg = []
    tickers_without_esg = []

    for ticker in sorted(universe_members):
        esg_dir = esg_base / f"ticker={ticker}" / "esg"
        if esg_dir.exists() and any(esg_dir.glob("year=*/part-*.parquet")):
            tickers_with_esg.append(ticker)
        else:
            tickers_without_esg.append(ticker)

    print(
        f"Tickers with ESG data:    {len(tickers_with_esg)} ({len(tickers_with_esg)/len(universe_members)*100:.1f}%)"
    )
    print(
        f"Tickers without ESG data: {len(tickers_without_esg)} ({len(tickers_without_esg)/len(universe_members)*100:.1f}%)"
    )
    print()

    tickers = tickers_with_esg

    # Check each ticker
    continuous_count = 0
    discontinuous_count = 0
    error_count = 0

    continuous_tickers = []
    discontinuous_tickers = []
    all_latest_dates = []

    for i, ticker in enumerate(tickers, 1):
        info, error = check_esg_continuity(ticker, analysis_start_date)

        if error:
            error_count += 1
            if "No data after" not in error:  # Only print real errors
                print(f"[{i:3d}/{len(tickers)}] ❌ {ticker:6s} - {error}")
        elif info:
            all_latest_dates.append(info["analysis_last_date"])

            if info["is_continuous"]:
                continuous_count += 1
                continuous_tickers.append(ticker)
                print(
                    f"[{i:3d}/{len(tickers)}] ✅ {ticker:6s} - Continuous ({info['analysis_first_date'].strftime('%Y-%m')} to {info['analysis_last_date'].strftime('%Y-%m')}, {info['total_records']} records)"
                )
            else:
                discontinuous_count += 1
                gap_pct = (info["missing_records"] / info["expected_records"]) * 100
                print(
                    f"[{i:3d}/{len(tickers)}] ⚠️  {ticker:6s} - Gaps: {info['missing_records']}/{info['expected_records']} ({gap_pct:.1f}%) missing"
                )

                if info["missing_dates"]:
                    sample_gaps = [str(d) for d in info["missing_dates"][:5]]
                    print(f"             First gaps: {', '.join(sample_gaps)}")

                discontinuous_tickers.append(info)

    # Summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total universe members:        {len(universe_members)}")
    print(
        f"Tickers with ESG data:         {len(tickers_with_esg)} ({len(tickers_with_esg)/len(universe_members)*100:.1f}%)"
    )
    print(
        f"Tickers without ESG data:      {len(tickers_without_esg)} ({len(tickers_without_esg)/len(universe_members)*100:.1f}%)"
    )
    print()
    total_analyzed = continuous_count + discontinuous_count
    print(f"Among tickers with ESG data:")
    if total_analyzed > 0:
        print(
            f"  ✅ Continuous data:    {continuous_count} ({continuous_count/total_analyzed*100:.1f}%)"
        )
        print(
            f"  ⚠️  Discontinuous data: {discontinuous_count} ({discontinuous_count/total_analyzed*100:.1f}%)"
        )
    else:
        print(f"  ✅ Continuous data:    {continuous_count}")
        print(f"  ⚠️  Discontinuous data: {discontinuous_count}")
    print(f"  ❌ Errors/No data:     {error_count}")
    print()

    # Latest date coverage analysis
    if all_latest_dates:
        latest_dates_df = pd.DataFrame({"date": all_latest_dates})
        latest_date = latest_dates_df["date"].max()
        min_latest_date = latest_dates_df["date"].min()
        median_latest_date = latest_dates_df["date"].median()

        # Count tickers by latest date
        date_counts = latest_dates_df["date"].value_counts().sort_index(ascending=False)

        print("=" * 80)
        print("LATEST DATE COVERAGE")
        print("=" * 80)
        print(f"Most recent ESG data date:     {latest_date.strftime('%Y-%m-%d')}")
        print(f"Oldest 'latest' date:          {min_latest_date.strftime('%Y-%m-%d')}")
        print(
            f"Median latest date:            {median_latest_date.strftime('%Y-%m-%d')}"
        )
        print()
        print("Distribution of latest dates (Top 10):")
        print(f"{'Date':<15} {'Count':<10} {'Percentage':<12}")
        print("-" * 40)
        for date, count in date_counts.head(10).items():
            pct = (count / len(all_latest_dates)) * 100
            print(f"{date.strftime('%Y-%m-%d'):<15} {count:<10} {pct:>6.1f}%")

        print()
        print(
            f"📊 RECOMMENDATION: Use analysis period 2014-01-01 to {latest_date.strftime('%Y-%m-%d')}"
        )
        print()

    if discontinuous_tickers:
        print("=" * 80)
        print(f"TICKERS WITH GAPS (After {analysis_start_date})")
        print("=" * 80)

        # Sort by gap percentage
        discontinuous_tickers.sort(
            key=lambda x: x["missing_records"] / x["expected_records"], reverse=True
        )

        print(
            f"{'Ticker':<8} {'First Date':<12} {'Last Date':<12} {'Records':<10} {'Gaps':<10} {'Gap %':<10}"
        )
        print("-" * 80)

        for info in discontinuous_tickers[:20]:
            gap_pct = (info["missing_records"] / info["expected_records"]) * 100
            print(
                f"{info['ticker']:<8} "
                f"{info['analysis_first_date'].strftime('%Y-%m'):<12} "
                f"{info['analysis_last_date'].strftime('%Y-%m'):<12} "
                f"{info['total_records']:<10} "
                f"{info['missing_records']:<10} "
                f"{gap_pct:>6.1f}%"
            )

    # Save detailed report
    if discontinuous_tickers:
        report_file = (
            data_root
            / f"esg_continuity_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        report_data = []
        for info in discontinuous_tickers:
            report_data.append(
                {
                    "ticker": info["ticker"],
                    "full_first_date": info["full_first_date"].strftime("%Y-%m-%d"),
                    "full_last_date": info["full_last_date"].strftime("%Y-%m-%d"),
                    "analysis_first_date": info["analysis_first_date"].strftime(
                        "%Y-%m-%d"
                    ),
                    "analysis_last_date": info["analysis_last_date"].strftime(
                        "%Y-%m-%d"
                    ),
                    "total_records": info["total_records"],
                    "expected_records": info["expected_records"],
                    "missing_records": info["missing_records"],
                    "gap_percentage": (
                        info["missing_records"] / info["expected_records"]
                    )
                    * 100,
                    "sample_gaps": ", ".join(
                        [str(d) for d in info["missing_dates"][:10]]
                    ),
                }
            )

        report_df = pd.DataFrame(report_data)
        report_df.to_csv(report_file, index=False)

        print()
        print(f"📄 Detailed report saved to: {report_file}")

    # Show sample of tickers without ESG data
    if tickers_without_esg:
        print()
        print("=" * 80)
        print("SAMPLE TICKERS WITHOUT ESG DATA (showing first 50):")
        print("=" * 80)
        sample_missing = tickers_without_esg[:50]
        # Print in columns for readability
        cols = 5
        for i in range(0, len(sample_missing), cols):
            row = sample_missing[i : i + cols]
            print("  " + "  ".join(f"{t:8s}" for t in row))

        if len(tickers_without_esg) > 50:
            print(f"  ... and {len(tickers_without_esg) - 50} more")
        print()

    # Save continuous tickers list
    if continuous_tickers:
        continuous_file = data_root / "continuous_esg_tickers.txt"
        with open(continuous_file, "w") as f:
            for ticker in sorted(continuous_tickers):
                f.write(f"{ticker}\n")
        print(
            f"✅ Saved {len(continuous_tickers)} continuous ESG tickers to: {continuous_file}"
        )
        print()


if __name__ == "__main__":
    main()
