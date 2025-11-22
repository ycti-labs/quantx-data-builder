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

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tiingo import TiingoClient

from core.config import Config
from market import PriceManager
from universe import SP500Universe


def main():
    """Check missing data for entire SP500 universe"""

    # Create output log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(f"missing_data_report_{timestamp}.txt")

    def log_print(message, file_handle=None):
        """Print to console and write to file"""
        print(message)
        if file_handle:
            file_handle.write(message + '\n')

    print("=" * 80)
    print("SP500 Universe Missing Data Check - Overlap-Aware")
    print("=" * 80)
    print(f"\nLog file: {log_file}")

    # Initialize components
    config = Config("config/settings.yaml")
    tiingo_config = {
        'api_key': config.get('fetcher.tiingo.api_key'),
        'session': True
    }
    tiingo = TiingoClient(tiingo_config)
    universe = SP500Universe()
    price_mgr = PriceManager(tiingo=tiingo, universe=universe)
    checker = price_mgr.get_missing_data_checker()

    # Research period - typical backtest window
    research_start = '2014-01-01'
    research_end = '2024-12-31'

    # Open log file for writing
    with open(log_file, 'w') as f:
        log_print("=" * 80, f)
        log_print("SP500 Universe Missing Data Check - Overlap-Aware", f)
        log_print("=" * 80, f)
        log_print(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", f)
        log_print(f"Research Period: {research_start} to {research_end}", f)
        log_print("Universe Scope: Historical (all symbols ever in SP500)", f)
        log_print("=" * 80, f)

        # Get all historical members during the research period
        log_print("\nLoading historical universe members...", f)
        members = universe.get_all_historical_members(research_start, research_end)
        log_print(f"Total symbols: {len(members)}", f)

        # Categorize results
        results = {
            'complete': [],          # All data present
            'no_overlap': [],        # No overlap with research period
            'missing_simple': [],    # Missing data (single period)
            'missing_gaps': [],      # Missing data (multiple periods)
            'errors': []             # Processing errors
        }

        # Check each symbol
        log_print("\nChecking missing data for all symbols...", f)
        log_print("─" * 80, f)

        for i, symbol in enumerate(members, 1):
            try:
                # Print progress every 50 symbols
                if i % 50 == 0 or i == 1:
                    progress_msg = f"Progress: {i}/{len(members)} ({i*100//len(members)}%) - Current: {symbol}"
                    log_print(progress_msg, f)

                result = checker.check_missing_data(
                    symbol,
                    research_start,
                    research_end,
                    tolerance_days=2
                )

                # Categorize result
                if result['status'] == 'complete':
                    if result.get('reason') == 'no_overlap':
                        results['no_overlap'].append({
                            'symbol': symbol,
                            'reason': 'No overlap with research period'
                        })
                    else:
                        results['complete'].append({
                            'symbol': symbol,
                            'checked_periods': result.get('checked_periods', [])
                        })
                elif result['status'] == 'missing':
                    if result.get('summary', {}).get('has_gaps'):
                        # Multiple periods with gaps
                        results['missing_gaps'].append({
                            'symbol': symbol,
                            'checked_periods': result.get('checked_periods', []),
                            'intervals': result.get('intervals', []),
                            'summary': result.get('summary', {})
                        })
                    else:
                        # Single period
                        results['missing_simple'].append({
                            'symbol': symbol,
                            'checked_periods': result.get('checked_periods', []),
                            'missing_start': result.get('missing_start'),
                            'missing_end': result.get('missing_end'),
                            'expected_days': result.get('expected_days'),
                            'actual_days': result.get('actual_days')
                        })

            except Exception as e:
                results['errors'].append({
                    'symbol': symbol,
                    'error': str(e)
                })
                error_msg = f"  ⚠️  Error checking {symbol}: {e}"
                log_print(error_msg, f)

        # Print summary
        log_print("\n" + "=" * 80, f)
        log_print("SUMMARY", f)
        log_print("=" * 80, f)

        total_symbols = len(members)
        log_print(f"\nTotal Symbols Checked: {total_symbols}", f)
        log_print("─" * 80, f)

        # Complete data
        complete_count = len(results['complete'])
        complete_pct = complete_count * 100 / total_symbols if total_symbols > 0 else 0
        log_print(f"✅ Complete Data:           {complete_count:4d} ({complete_pct:5.1f}%)", f)

        # No overlap
        no_overlap_count = len(results['no_overlap'])
        no_overlap_pct = no_overlap_count * 100 / total_symbols if total_symbols > 0 else 0
        log_print(f"⊘  No Overlap:              {no_overlap_count:4d} ({no_overlap_pct:5.1f}%)", f)

        # Missing data - simple
        missing_simple_count = len(results['missing_simple'])
        missing_simple_pct = missing_simple_count * 100 / total_symbols if total_symbols > 0 else 0
        log_print(f"⚠️  Missing (Simple):        {missing_simple_count:4d} ({missing_simple_pct:5.1f}%)", f)

        # Missing data - with gaps
        missing_gaps_count = len(results['missing_gaps'])
        missing_gaps_pct = missing_gaps_count * 100 / total_symbols if total_symbols > 0 else 0
        log_print(f"⚠️  Missing (With Gaps):     {missing_gaps_count:4d} ({missing_gaps_pct:5.1f}%)", f)

        # Errors
        errors_count = len(results['errors'])
        errors_pct = errors_count * 100 / total_symbols if total_symbols > 0 else 0
        log_print(f"❌ Errors:                  {errors_count:4d} ({errors_pct:5.1f}%)", f)

        # Total needing attention
        total_missing = missing_simple_count + missing_gaps_count
        total_missing_pct = total_missing * 100 / total_symbols if total_symbols > 0 else 0
        log_print("─" * 80, f)
        log_print(f"📊 Total Needing Fetch:     {total_missing:4d} ({total_missing_pct:5.1f}%)", f)

        # Show details for missing data
        if missing_simple_count > 0:
            log_print("\n" + "=" * 80, f)
            log_print(f"MISSING DATA - SIMPLE (All {missing_simple_count} symbols)", f)
            log_print("=" * 80, f)
            for item in results['missing_simple']:
                symbol = item['symbol']
                periods = item['checked_periods']
                missing_start = item.get('missing_start', 'N/A')
                missing_end = item.get('missing_end', 'N/A')
                expected = item.get('expected_days', 0)
                actual = item.get('actual_days', 0)

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
            log_print(f"MISSING DATA - WITH GAPS (All {missing_gaps_count} symbols)", f)
            log_print("=" * 80, f)
            for item in results['missing_gaps']:
                symbol = item['symbol']
                periods = item['checked_periods']
                intervals = item['intervals']
                summary = item['summary']

                log_print(f"\n{symbol}:", f)
                log_print(f"  Checked periods: {len(periods)} discontinuous segments", f)
                for period in periods:
                    log_print(f"    - {period[0]} to {period[1]}", f)

                log_print(f"  Per-period status:", f)
                for interval in intervals:
                    period_num = interval['period']
                    status = interval['status']
                    period_start = interval.get('period_start')
                    period_end = interval.get('period_end')

                    if status == 'complete':
                        log_print(f"    Period {period_num} ({period_start} to {period_end}): ✅ Complete", f)
                    else:
                        missing_start = interval.get('missing_start', 'N/A')
                        missing_end = interval.get('missing_end', 'N/A')
                        expected = interval.get('expected_days', 0)
                        actual = interval.get('actual_days', 0)

                        # Handle None values
                        if expected is None:
                            expected = 0
                        if actual is None:
                            actual = 0

                        missing_days = expected - actual
                        log_print(f"    Period {period_num} ({period_start} to {period_end}): ⚠️  Missing {missing_days}/{expected} days ({missing_start} to {missing_end})", f)

        # Show no-overlap examples
        if no_overlap_count > 0:
            log_print("\n" + "=" * 80, f)
            log_print(f"NO OVERLAP WITH RESEARCH PERIOD (All {no_overlap_count} symbols)", f)
            log_print("=" * 80, f)
            log_print("These symbols were members, but their membership periods don't", f)
            log_print("overlap with the research period 2014-2024. No data needs fetching.", f)
            for item in results['no_overlap']:
                log_print(f"  - {item['symbol']}", f)

        # Show errors
        if errors_count > 0:
            log_print("\n" + "=" * 80, f)
            log_print(f"ERRORS (All {errors_count} symbols)", f)
            log_print("=" * 80, f)
            for item in results['errors']:
                log_print(f"  {item['symbol']}: {item['error']}", f)

        log_print("\n" + "=" * 80, f)
        log_print("INSIGHTS", f)
        log_print("=" * 80, f)
        log_print(f"""
1. Overlap-Aware Logic Applied:
   - Only checked data for periods within BOTH membership AND research window
   - {no_overlap_count} symbols had no overlap (pre-2014 or post-2024 membership)
   - These {no_overlap_count} symbols require NO data fetching

2. Data Completeness:
   - {complete_count} symbols ({complete_pct:.1f}%) have complete data in checked periods
   - {total_missing} symbols ({total_missing_pct:.1f}%) need data fetching
   - {missing_gaps_count} of these have discontinuous membership (gaps)

3. Efficiency Gain:
   - Without overlap logic: Would check all {total_symbols} symbols
   - With overlap logic: Only {total_symbols - no_overlap_count} symbols need checking
   - Savings: {no_overlap_count} symbols ({no_overlap_pct:.1f}%) skipped

4. Next Steps:
   - Fetch missing data for {total_missing} symbols
   - Focus on {missing_gaps_count} symbols with gaps (requires period-aware fetch)
   - {errors_count} symbols need investigation
    """, f)

        log_print("\n" + "=" * 80, f)
        log_print("Report saved to: " + str(log_file), f)
        log_print("=" * 80, f)

if __name__ == "__main__":
    main()
