"""
Check missing data for entire SP500 universe with overlap-aware logic

Uses historical scope to include all symbols that were ever in SP500,
and applies overlap detection to only check periods within both membership
and research period.

Outputs detailed report to timestamped log file.
"""

import sys
from datetime import datetime
from pathlib import Path

from tiingo import TiingoClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import Config
from market import ESGManager, PriceManager
from universe import SP500Universe


def main():
    """Check missing data for entire SP500 universe"""

    # Create output log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(f"missing_data_report_{timestamp}.txt")

    # Check multiple frequencies
    frequencies = ["daily", "weekly", "monthly"]

    def log_print(message, file_handle=None):
        """Print to console and write to file"""
        print(message)
        if file_handle:
            file_handle.write(message + "\n")

    print("=" * 80)
    print("SP500 Universe Missing Data Check - Overlap-Aware (Price + ESG)")
    print("=" * 80)
    print(f"\nLog file: {log_file}")

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo_config = {"api_key": config.get("fetcher.tiingo.api_key"), "session": True}
    tiingo = TiingoClient(tiingo_config)
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)
    checker = price_mgr.get_missing_data_checker()
    esg_mgr = ESGManager(universe=universe)

    # Research period - typical backtest window
    research_start = config.get("universe.sp500.start_date")
    research_end = config.get("universe.sp500.end_date")

    # Open log file for writing
    with open(log_file, "w") as f:
        log_print("=" * 80, f)
        log_print(
            "SP500 Universe Missing Data Check - Multi-Frequency Overlap-Aware (Price + ESG)",
            f,
        )
        log_print("=" * 80, f)
        log_print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", f)
        log_print(f"Research Period: {research_start} to {research_end}", f)
        log_print("Universe Scope: Historical (all symbols ever in SP500)", f)
        log_print(f"Frequencies: {', '.join(frequencies)}", f)
        log_print(
            "Note: Frequency-aware alignment applied (monthly=end-of-month, weekly=end-of-week)",
            f,
        )
        log_print("=" * 80, f)

        # Get all historical members during the research period
        log_print("\nLoading historical universe members...", f)
        members = universe.get_all_historical_members(research_start, research_end)
        log_print(f"Total symbols: {len(members)}", f)

        # Categorize results by frequency
        results_by_frequency = {}
        for freq in frequencies:
            results_by_frequency[freq] = {
                "complete": [],  # All data present
                "no_overlap": [],  # No overlap with research period
                "missing_simple": [],  # Missing data (single period)
                "missing_gaps": [],  # Missing data (multiple periods)
                "errors": [],  # Processing errors
            }

        # Check each symbol for all frequencies
        log_print("\nChecking missing data for all symbols (all frequencies)...", f)
        log_print("─" * 80, f)

        for i, symbol in enumerate(members, 1):
            # Print progress every 50 symbols
            if i % 50 == 0 or i == 1:
                progress_msg = f"Progress: {i}/{len(members)} ({i*100//len(members)}%) - Current: {symbol}"
                log_print(progress_msg, f)

            # Check each frequency
            for freq in frequencies:
                try:
                    # Auto-calculate tolerance based on frequency
                    from programs.check_missing_data import get_tolerance_for_frequency

                    tolerance = get_tolerance_for_frequency(freq)

                    result = checker.check_missing_data(
                        symbol,
                        research_start,
                        research_end,
                        frequency=freq,
                        tolerance_days=tolerance,
                    )

                    # Categorize result for this frequency
                    results = results_by_frequency[freq]
                    if result["status"] == "complete":
                        if result.get("reason") == "no_overlap":
                            results["no_overlap"].append(
                                {
                                    "symbol": symbol,
                                    "reason": "No overlap with research period",
                                }
                            )
                        else:
                            results["complete"].append(
                                {
                                    "symbol": symbol,
                                    "checked_periods": result.get(
                                        "checked_periods", []
                                    ),
                                }
                            )
                    elif result["status"] == "missing":
                        if result.get("summary", {}).get("has_gaps"):
                            # Multiple periods with gaps
                            results["missing_gaps"].append(
                                {
                                    "symbol": symbol,
                                    "checked_periods": result.get(
                                        "checked_periods", []
                                    ),
                                    "intervals": result.get("intervals", []),
                                    "summary": result.get("summary", {}),
                                }
                            )
                        else:
                            # Single period
                            results["missing_simple"].append(
                                {
                                    "symbol": symbol,
                                    "checked_periods": result.get(
                                        "checked_periods", []
                                    ),
                                    "missing_start": result.get("missing_start"),
                                    "missing_end": result.get("missing_end"),
                                    "expected_days": result.get("expected_days"),
                                    "actual_days": result.get("actual_days"),
                                }
                            )

                except Exception as e:
                    results_by_frequency[freq]["errors"].append(
                        {"symbol": symbol, "error": str(e)}
                    )
                    error_msg = f"  ⚠️  Error checking {symbol} ({freq}): {e}"
                    log_print(error_msg, f)

        # ESG Data Checking
        log_print("\n" + "=" * 80, f)
        log_print("ESG DATA COVERAGE CHECK", f)
        log_print("=" * 80, f)
        log_print("\nChecking ESG data coverage for all symbols...", f)

        esg_results = {"has_data": [], "no_data": [], "errors": []}

        for i, symbol in enumerate(members, 1):
            if i % 50 == 0 or i == 1:
                progress_msg = f"ESG Progress: {i}/{len(members)} ({i*100//len(members)}%) - Current: {symbol}"
                log_print(progress_msg, f)

            try:
                # Check if ESG data exists for this symbol
                esg_df = esg_mgr.load_esg_data(
                    ticker=symbol, start_date=research_start, end_date=research_end
                )

                if not esg_df.empty:
                    esg_results["has_data"].append(
                        {
                            "symbol": symbol,
                            "records": len(esg_df),
                            "years": (
                                sorted(esg_df["year"].unique().tolist())
                                if "year" in esg_df.columns
                                else []
                            ),
                            "date_range": (
                                (esg_df["date"].min(), esg_df["date"].max())
                                if "date" in esg_df.columns
                                else None
                            ),
                        }
                    )
                else:
                    esg_results["no_data"].append(symbol)

            except FileNotFoundError:
                esg_results["no_data"].append(symbol)
            except Exception as e:
                esg_results["errors"].append({"symbol": symbol, "error": str(e)})
                log_print(f"  ⚠️  Error checking ESG for {symbol}: {e}", f)

        # Print summary for each frequency
        log_print("\n" + "=" * 80, f)
        log_print("SUMMARY BY FREQUENCY (PRICE DATA)", f)
        log_print("=" * 80, f)

        total_symbols = len(members)
        log_print(f"\nTotal Symbols Checked: {total_symbols}", f)

        for freq in frequencies:
            results = results_by_frequency[freq]
            log_print("\n" + "─" * 80, f)
            log_print(f"Frequency: {freq.upper()}", f)
            log_print("─" * 80, f)

            # Complete data
            complete_count = len(results["complete"])
            complete_pct = (
                complete_count * 100 / total_symbols if total_symbols > 0 else 0
            )
            log_print(
                f"✅ Complete Data:           {complete_count:4d} ({complete_pct:5.1f}%)",
                f,
            )

            # No overlap
            no_overlap_count = len(results["no_overlap"])
            no_overlap_pct = (
                no_overlap_count * 100 / total_symbols if total_symbols > 0 else 0
            )
            log_print(
                f"⊘  No Overlap:              {no_overlap_count:4d} ({no_overlap_pct:5.1f}%)",
                f,
            )

            # Missing data - simple
            missing_simple_count = len(results["missing_simple"])
            missing_simple_pct = (
                missing_simple_count * 100 / total_symbols if total_symbols > 0 else 0
            )
            log_print(
                f"⚠️  Missing (Simple):        {missing_simple_count:4d} ({missing_simple_pct:5.1f}%)",
                f,
            )

            # Missing data - with gaps
            missing_gaps_count = len(results["missing_gaps"])
            missing_gaps_pct = (
                missing_gaps_count * 100 / total_symbols if total_symbols > 0 else 0
            )
            log_print(
                f"⚠️  Missing (With Gaps):     {missing_gaps_count:4d} ({missing_gaps_pct:5.1f}%)",
                f,
            )

            # Errors
            errors_count = len(results["errors"])
            errors_pct = errors_count * 100 / total_symbols if total_symbols > 0 else 0
            log_print(
                f"❌ Errors:                  {errors_count:4d} ({errors_pct:5.1f}%)", f
            )

            # Total needing attention
            total_missing = missing_simple_count + missing_gaps_count
            total_missing_pct = (
                total_missing * 100 / total_symbols if total_symbols > 0 else 0
            )
            log_print(
                f"📊 Total Needing Fetch:     {total_missing:4d} ({total_missing_pct:5.1f}%)",
                f,
            )

        # Show details for missing data by frequency
        for freq in frequencies:
            results = results_by_frequency[freq]
            missing_simple_count = len(results["missing_simple"])
            missing_gaps_count = len(results["missing_gaps"])
            no_overlap_count = len(results["no_overlap"])
            errors_count = len(results["errors"])

            if missing_simple_count > 0:
                log_print("\n" + "=" * 80, f)
                log_print(
                    f"MISSING DATA - SIMPLE [{freq.upper()}] (All {missing_simple_count} symbols)",
                    f,
                )
                log_print("=" * 80, f)
                for item in results["missing_simple"]:
                    symbol = item["symbol"]
                    periods = item["checked_periods"]
                    missing_start = item.get("missing_start", "N/A")
                    missing_end = item.get("missing_end", "N/A")
                    expected = item.get("expected_days", 0)
                    actual = item.get("actual_days", 0)

                    # Handle None values
                    if expected is None:
                        expected = 0
                    if actual is None:
                        actual = 0

                    missing_days = expected - actual
                    missing_pct = missing_days * 100 / expected if expected > 0 else 0

                    log_print(f"\n{symbol}:", f)
                    log_print(f"  Checked periods: {periods}", f)
                    log_print(f"  Missing range: {missing_start} to {missing_end}", f)
                    log_print(f"  Expected days: {expected}", f)
                    log_print(f"  Actual days: {actual}", f)
                    log_print(f"  Missing days: {missing_days} ({missing_pct:.1f}%)", f)

            if missing_gaps_count > 0:
                log_print("\n" + "=" * 80, f)
                log_print(
                    f"MISSING DATA - WITH GAPS [{freq.upper()}] (All {missing_gaps_count} symbols)",
                    f,
                )
                log_print("=" * 80, f)
                for item in results["missing_gaps"]:
                    symbol = item["symbol"]
                    periods = item["checked_periods"]
                    intervals = item["intervals"]
                    summary = item["summary"]

                    log_print(f"\n{symbol}:", f)
                    log_print(
                        f"  Checked periods: {len(periods)} discontinuous segments", f
                    )
                    for period in periods:
                        log_print(f"    - {period[0]} to {period[1]}", f)

                    log_print(f"  Per-period status:", f)
                    for interval in intervals:
                        period_num = interval["period"]
                        status = interval["status"]
                        period_start = interval.get("period_start")
                        period_end = interval.get("period_end")

                        if status == "complete":
                            log_print(
                                f"    Period {period_num} ({period_start} to {period_end}): ✅ Complete",
                                f,
                            )
                        else:
                            missing_start = interval.get("missing_start", "N/A")
                            missing_end = interval.get("missing_end", "N/A")
                            expected = interval.get("expected_days", 0)
                            actual = interval.get("actual_days", 0)

                            # Handle None values
                            if expected is None:
                                expected = 0
                            if actual is None:
                                actual = 0

                            missing_days = expected - actual
                            log_print(
                                f"    Period {period_num} ({period_start} to {period_end}): ⚠️  Missing {missing_days}/{expected} days ({missing_start} to {missing_end})",
                                f,
                            )

            # Show no-overlap examples
            if no_overlap_count > 0:
                log_print("\n" + "=" * 80, f)
                log_print(
                    f"NO OVERLAP WITH RESEARCH PERIOD [{freq.upper()}] (All {no_overlap_count} symbols)",
                    f,
                )
                log_print("=" * 80, f)
                log_print(
                    "These symbols were members, but their membership periods don't", f
                )
                log_print(
                    "overlap with the research period 2014-2024. No data needs fetching.",
                    f,
                )
                for item in results["no_overlap"]:
                    log_print(f"  - {item['symbol']}", f)

            # Show errors
            if errors_count > 0:
                log_print("\n" + "=" * 80, f)
                log_print(f"ERRORS [{freq.upper()}] (All {errors_count} symbols)", f)
                log_print("=" * 80, f)
                for item in results["errors"]:
                    log_print(f"  {item['symbol']}: {item['error']}", f)

        # ESG Summary
        log_print("\n" + "=" * 80, f)
        log_print("ESG DATA COVERAGE", f)
        log_print("=" * 80, f)

        esg_has_data_count = len(esg_results["has_data"])
        esg_no_data_count = len(esg_results["no_data"])
        esg_errors_count = len(esg_results["errors"])
        esg_has_data_pct = (
            esg_has_data_count * 100 / total_symbols if total_symbols > 0 else 0
        )
        esg_no_data_pct = (
            esg_no_data_count * 100 / total_symbols if total_symbols > 0 else 0
        )

        log_print(
            f"\n✓ Has ESG Data:            {esg_has_data_count:4d} ({esg_has_data_pct:5.1f}%)",
            f,
        )
        log_print(
            f"⚠️  No ESG Data:             {esg_no_data_count:4d} ({esg_no_data_pct:5.1f}%)",
            f,
        )
        log_print(f"❌ Errors:                  {esg_errors_count:4d}", f)

        if esg_has_data_count > 0:
            log_print("\n" + "=" * 80, f)
            log_print(f"ESG DATA AVAILABLE ({esg_has_data_count} symbols)", f)
            log_print("=" * 80, f)
            for item in esg_results["has_data"][:20]:  # Show first 20
                symbol = item["symbol"]
                records = item["records"]
                years = item["years"]
                if years:
                    log_print(
                        f"  {symbol}: {records} records, years {min(years)}-{max(years)}",
                        f,
                    )
                else:
                    log_print(f"  {symbol}: {records} records", f)
            if esg_has_data_count > 20:
                log_print(f"  ... and {esg_has_data_count - 20} more", f)

        if esg_no_data_count > 0:
            log_print("\n" + "=" * 80, f)
            log_print(f"NO ESG DATA ({esg_no_data_count} symbols)", f)
            log_print("=" * 80, f)
            for symbol in esg_results["no_data"][:20]:  # Show first 20
                log_print(f"  {symbol}", f)
            if esg_no_data_count > 20:
                log_print(f"  ... and {esg_no_data_count - 20} more", f)

        # Print formatted insights
        log_print("\n" + "=" * 80, f)
        log_print("INSIGHTS & RECOMMENDATIONS", f)
        log_print("=" * 80, f)

        log_print("\n📊 1. MULTI-FREQUENCY CHECKING", f)
        log_print("   " + "─" * 76, f)
        log_print("   ✓ Checked daily, weekly, and monthly data for all symbols", f)
        log_print("   ✓ Frequency-aware alignment applied:", f)
        log_print(
            "     • Monthly: End-of-month alignment (e.g., 2014-01-01 → 2014-01-31)", f
        )
        log_print(
            "     • Weekly:  End-of-week alignment  (e.g., 2014-01-01 → 2014-01-03)", f
        )
        log_print("     • Daily:   Direct date matching   (no alignment needed)", f)
        log_print("   ✓ Eliminates false 'PARTIAL' warnings for monthly/weekly data", f)

        log_print("\n🎯 2. OVERLAP-AWARE LOGIC", f)
        log_print("   " + "─" * 76, f)
        log_print(
            "   Only checked periods within BOTH membership AND research window:", f
        )
        for freq in frequencies:
            results = results_by_frequency[freq]
            no_overlap_count = len(results["no_overlap"])
            if no_overlap_count > 0:
                log_print(
                    f"   • {freq.capitalize():8s}: {no_overlap_count:3d} symbols had no overlap",
                    f,
                )
            else:
                log_print(f"   • {freq.capitalize():8s}: All symbols have overlap ✓", f)

        log_print("\n📈 3. DATA COMPLETENESS BY FREQUENCY", f)
        log_print("   " + "─" * 76, f)
        for freq in frequencies:
            results = results_by_frequency[freq]
            complete_count = len(results["complete"])
            complete_pct = (
                complete_count * 100 / total_symbols if total_symbols > 0 else 0
            )
            total_missing = len(results["missing_simple"]) + len(
                results["missing_gaps"]
            )
            total_missing_pct = (
                total_missing * 100 / total_symbols if total_symbols > 0 else 0
            )

            status_icon = "✅" if total_missing == 0 else "⚠️"
            log_print(
                f"   {status_icon} {freq.capitalize():8s}: {complete_count:3d} complete ({complete_pct:4.1f}%)  |  {total_missing:3d} missing ({total_missing_pct:4.1f}%)",
                f,
            )

        # Explain any discrepancies in counts
        daily_checked = (
            len(results_by_frequency["daily"]["complete"])
            + len(results_by_frequency["daily"]["missing_simple"])
            + len(results_by_frequency["daily"]["missing_gaps"])
        )
        weekly_checked = (
            len(results_by_frequency["weekly"]["complete"])
            + len(results_by_frequency["weekly"]["missing_simple"])
            + len(results_by_frequency["weekly"]["missing_gaps"])
        )
        monthly_checked = (
            len(results_by_frequency["monthly"]["complete"])
            + len(results_by_frequency["monthly"]["missing_simple"])
            + len(results_by_frequency["monthly"]["missing_gaps"])
        )

        if (
            daily_checked != weekly_checked
            or daily_checked != monthly_checked
            or weekly_checked != monthly_checked
        ):
            log_print(f"\n   💡 Note: Different counts across frequencies:", f)
            log_print(f"      Daily checked:   {daily_checked} tickers", f)
            log_print(f"      Weekly checked:  {weekly_checked} tickers", f)
            log_print(f"      Monthly checked: {monthly_checked} tickers", f)
            log_print(f"      ", f)
            log_print(f"      This occurs due to frequency-aware date alignment:", f)
            log_print(
                f"      • Monthly alignment captures month-ends (may include more short-term members)",
                f,
            )
            log_print(
                f"      • Daily/weekly alignment may exclude very short memberships", f
            )
            log_print(
                f"      • Tickers with <30 days membership can appear in one frequency but not others",
                f,
            )

        log_print("\n🌱 4. ESG DATA COVERAGE", f)
        log_print("   " + "─" * 76, f)
        log_print(
            f"   ✓ {esg_has_data_count:3d} symbols ({esg_has_data_pct:4.1f}%) have ESG data",
            f,
        )
        log_print(
            f"   ⚠ {esg_no_data_count:3d} symbols ({esg_no_data_pct:4.1f}%) missing ESG data",
            f,
        )
        if esg_has_data_count > 0:
            total_esg_records = sum(item["records"] for item in esg_results["has_data"])
            avg_records = total_esg_records / esg_has_data_count
            log_print(
                f"   📊 Average {avg_records:.0f} ESG records per symbol with data", f
            )

        log_print("\n🚀 5. RECOMMENDED ACTIONS", f)
        log_print("   " + "─" * 76, f)

        # Price data actions
        has_price_missing = False
        for freq in frequencies:
            results = results_by_frequency[freq]
            total_missing = len(results["missing_simple"]) + len(
                results["missing_gaps"]
            )
            missing_gaps_count = len(results["missing_gaps"])
            if total_missing > 0:
                if not has_price_missing:
                    log_print("   📥 PRICE DATA:", f)
                    has_price_missing = True
                log_print(
                    f"      • Fetch {freq:8s} data for {total_missing:3d} symbols", f
                )
                if missing_gaps_count > 0:
                    log_print(
                        f"        ({missing_gaps_count} have discontinuous membership - use period-aware fetch)",
                        f,
                    )

        if not has_price_missing:
            log_print("   ✅ PRICE DATA: All frequencies complete!", f)

        # ESG data actions
        if esg_no_data_count > 0:
            log_print(f"\n   🌱 ESG DATA:", f)
            log_print(f"      • Process ESG data for {esg_no_data_count:3d} symbols", f)
            log_print(
                f"      • Command: python src/programs/process_esg_universe.py", f
            )
        else:
            log_print(f"\n   ✅ ESG DATA: Full coverage achieved!", f)

        # Next steps
        if has_price_missing or esg_no_data_count > 0:
            log_print(f"\n   💡 SUGGESTED WORKFLOW:", f)
            if has_price_missing:
                log_print(f"      1. Fetch missing price data:", f)
                log_print(
                    f"         python src/programs/fetch_research_universe.py --skip-existing",
                    f,
                )
            if esg_no_data_count > 0:
                log_print(f"      2. Process ESG data:", f)
                log_print(f"         python src/programs/process_esg_universe.py", f)
            log_print(f"      3. Verify completeness:", f)
            log_print(f"         python tests/check_universe_overlap_aware.py", f)

        log_print("\n" + "=" * 80, f)
        log_print("Report saved to: " + str(log_file), f)
        log_print("=" * 80, f)


if __name__ == "__main__":
    main()
