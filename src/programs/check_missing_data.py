"""
Missing Data Checker

Utility program for checking and analyzing missing price data across universes.
Provides comprehensive missing data detection and reporting functionality with
proper handling of discontinuous membership periods.
"""

import logging
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.market.price_manager import align_start_date_to_frequency

logger = logging.getLogger(__name__)

TOLERANCE_DAYS_DEFAULT = 2


def get_tolerance_for_frequency(frequency: str) -> int:
    """
    Calculate appropriate tolerance in days based on data frequency

    For daily data: Allow ±2 days tolerance (weekends, holidays)
    For weekly data: Allow ±6 days tolerance (data might be on any weekday)
    For monthly data: Allow ±3 days tolerance (month-end vs first/last trading day)

    Args:
        frequency: Data frequency ('daily', 'weekly', 'monthly')

    Returns:
        Tolerance in days

    Examples:
        >>> get_tolerance_for_frequency('daily')
        2
        >>> get_tolerance_for_frequency('weekly')
        6
        >>> get_tolerance_for_frequency('monthly')
        3
    """
    frequency = frequency.lower()

    if frequency == 'daily':
        # Allow ±2 days for weekends/holidays
        return 2
    elif frequency == 'weekly':
        # Allow ±6 days since weekly data can be any weekday (Monday-Friday)
        # If required start is Wednesday but data starts Monday, that's 2 days
        # If required end is Tuesday but data ends Friday, that's 3 days
        # Maximum possible gap is ~6 days (within same week)
        return 6
    elif frequency == 'monthly':
        # Allow ±3 days for month-end adjustments
        # Monthly data typically uses last trading day of month
        # which can be 1-3 days before calendar month-end
        return 3
    else:
        # Unknown frequency, use daily default
        logger.warning(f"Unknown frequency '{frequency}', using daily tolerance")
        return 2


class MissingDataChecker:
    """
    Utility class for checking missing price data

    Provides comprehensive missing data analysis including:
    - Single symbol checks with gap awareness
    - Universe-wide missing data reports
    - Membership-aware validation
    - Support for symbols with discontinuous membership
    - Proper handling of research period overlaps
    """

    def __init__(self, price_manager):
        """
        Initialize missing data checker

        Args:
            price_manager: PriceManager instance for data access
        """
        from market import PriceManager
        self.price_manager: PriceManager = price_manager
        self.universe = price_manager.universe
        self.logger = logging.getLogger(__name__)

    def check_missing_data(
        self,
        symbol: str,
        required_start: str,
        required_end: str,
        frequency: str,
        tolerance_days: Optional[int] = None,
        handle_gaps: bool = True
    ) -> Dict:
        """
        Check what data is missing for a symbol during a required research period

        Only checks data within the intersection of membership intervals and the research period.
        For symbols with multiple membership intervals (removed/re-added), checks each
        overlapping segment separately.

        Example 1: Stock A
            Membership: [2007-03-01 to 2012-08-17], [2017-06-23 to current]
            Research: 2014-01-01 to 2024-12-31
            Check: Only [2017-06-23 to 2024-12-31] (second interval overlaps)

        Example 2: Stock B
            Membership: [2007-03-01 to 2017-08-17], [2022-06-23 to current]
            Research: 2014-01-01 to 2024-12-31
            Check: [2014-01-01 to 2017-08-17] AND [2022-06-23 to 2024-12-31] (both overlap)

        Args:
            symbol: Ticker symbol
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format
            frequency: Data frequency ('daily', 'weekly', 'monthly') - default: 'daily'
            tolerance_days: Ignore gaps of this many days or less. If None, auto-calculated
                          based on frequency (daily: 2, weekly: 6, monthly: 3)
            handle_gaps: If True, check each period separately (default: True)
                        If False, use simple span check

        Returns:
            Dictionary with:
                - status: 'complete', 'partial', or 'missing'
                - actual_start: Actual start date (or None)
                - actual_end: Actual end date (or None)
                - missing_start_days: Days missing at start
                - missing_end_days: Days missing at end
                - fetch_start: Recommended start date for fetching
                - fetch_end: Recommended end date for fetching
                - membership_start: Start of overall membership span (or None)
                - membership_end: End of overall membership span (or None)
                - checked_periods: List of (start, end) periods actually checked
                - intervals: List of per-period results (if handle_gaps=True and multiple periods)
                - summary: Overall summary (if handle_gaps=True and multiple periods)
        """
        # Auto-calculate tolerance if not provided
        if tolerance_days is None:
            tolerance_days = get_tolerance_for_frequency(frequency)
            self.logger.debug(
                f"Auto-calculated tolerance for {frequency} frequency: {tolerance_days} days"
            )

        req_start = pd.to_datetime(required_start).date()
        req_end = pd.to_datetime(required_end).date()

        # Get all membership intervals for this symbol
        membership_intervals = self.universe.get_membership_intervals(symbol)

        # Calculate overlapping periods between membership and research period
        checked_periods = self._get_overlapping_periods(
            membership_intervals, req_start, req_end
        )

        # If no overlap, data is complete (nothing to check)
        if not checked_periods:
            self.logger.info(
                f"✅ {symbol}: No membership overlap with research period | "
                f"Research: ({req_start} to {req_end})"
            )
            return {
                'status': 'complete',
                'actual_start': None,
                'actual_end': None,
                'missing_start_days': 0,
                'missing_end_days': 0,
                'fetch_start': None,
                'fetch_end': None,
                'membership_start': None,
                'membership_end': None,
                'checked_periods': [],
                'reason': 'no_overlap'
            }

        # Determine if we should use gap-aware checking
        use_gap_aware = handle_gaps and len(checked_periods) > 1

        if use_gap_aware:
            # Gap-aware checking for multiple discontinuous periods
            return self._check_missing_data_with_gaps(
                symbol, frequency, req_start, req_end, tolerance_days,
                membership_intervals, checked_periods
            )
        else:
            # Simple checking for single continuous period
            return self._check_missing_data_simple(
                symbol, frequency, req_start, req_end, tolerance_days,
                membership_intervals, checked_periods
            )

    def _get_overlapping_periods(
        self,
        membership_intervals: List[Tuple[date, date]],
        req_start: date,
        req_end: date
    ) -> List[Tuple[date, date]]:
        """
        Calculate periods that overlap between membership intervals and research period

        Args:
            membership_intervals: List of (start, end) membership periods
            req_start: Research period start
            req_end: Research period end

        Returns:
            List of (start, end) tuples representing overlapping periods, sorted chronologically

        Example:
            Membership: [(2007-03-01, 2017-08-17), (2022-06-23, 2025-07-09)]
            Research: (2014-01-01, 2024-12-31)
            Returns: [(2014-01-01, 2017-08-17), (2022-06-23, 2024-12-31)]
        """
        overlapping_periods = []

        for member_start, member_end in membership_intervals:
            # Calculate intersection
            overlap_start = max(req_start, member_start)
            overlap_end = min(req_end, member_end)

            # Only include if there's actual overlap
            if overlap_start <= overlap_end:
                overlapping_periods.append((overlap_start, overlap_end))

        # Return sorted by start date
        return sorted(overlapping_periods, key=lambda x: x[0])

    def _check_missing_data_simple(
        self,
        symbol: str,
        frequency: str,
        req_start: date,
        req_end: date,
        tolerance_days: int,
        membership_intervals: List[Tuple[date, date]],
        checked_periods: List[Tuple[date, date]]
    ) -> Dict:
        """
        Simple missing data check for single continuous period

        Internal method - use check_missing_data() instead
        """
        # Use the single checked period
        if len(checked_periods) == 1:
            effective_start, effective_end = checked_periods[0]
        else:
            # Multiple periods but handle_gaps=False, use span
            effective_start = min(start for start, _ in checked_periods)
            effective_end = max(end for _, end in checked_periods)

        # Get overall membership span
        if membership_intervals:
            member_start = min(start for start, _ in membership_intervals)
            member_end = max(end for _, end in membership_intervals)
        else:
            member_start = None
            member_end = None

        # Get existing date range
        existing_range = self.price_manager.get_existing_date_range(symbol, frequency=frequency)

        if existing_range is None:
            # No data exists - need to fetch entire checked period
            total_gap_days = (effective_end - effective_start).days
            self.logger.info(
                f"📭 {symbol}: No existing data | "
                f"Need to fetch period ({effective_start} to {effective_end})"
            )
            return {
                'status': 'missing',
                'actual_start': None,
                'actual_end': None,
                'missing_start_days': total_gap_days,
                'missing_end_days': 0,
                'fetch_start': str(effective_start),
                'fetch_end': str(effective_end),
                'membership_start': member_start,
                'membership_end': member_end,
                'checked_periods': checked_periods
            }

        actual_start, actual_end = existing_range

        # Apply frequency-aware alignment to avoid false gaps
        # For monthly: 2014-01-01 → 2014-01-31 (end of month)
        # For weekly: 2014-01-01 → 2014-01-03 (end of week/Friday)
        # For daily: No change
        aligned_start = align_start_date_to_frequency(effective_start, frequency)

        # Calculate gaps within checked period using aligned start
        start_gap_days = max(0, (actual_start - aligned_start).days)
        end_gap_days = max(0, (effective_end - actual_end).days)

        # Determine status with tolerance
        if start_gap_days <= tolerance_days and end_gap_days <= tolerance_days:
            status = 'complete'
            fetch_start = None
            fetch_end = None
            tolerance_note = f" (±{tolerance_days}d)" if start_gap_days > 0 or end_gap_days > 0 else ""
            self.logger.info(
                f"✅ {symbol}: Existing data COMPLETE{tolerance_note} | "
                f"({actual_start} to {actual_end}) | Checked: ({effective_start} to {effective_end})"
            )
        elif start_gap_days > 0 or end_gap_days > 0:
            status = 'partial'
            fetch_start = str(effective_start) if start_gap_days > tolerance_days else None
            fetch_end = str(effective_end) if end_gap_days > tolerance_days else None
            self.logger.warning(
                f"⚠️  {symbol}: Existing data PARTIAL | "
                f"({actual_start} to {actual_end}) | "
                f"Missing: {start_gap_days}d at start, {end_gap_days}d at end "
                f"(tolerance: ±{tolerance_days}d)"
            )
        else:
            status = 'complete'
            fetch_start = None
            fetch_end = None
            self.logger.info(
                f"✅ {symbol}: Existing data COMPLETE | "
                f"({actual_start} to {actual_end})"
            )

        return {
            'status': status,
            'actual_start': actual_start,
            'actual_end': actual_end,
            'missing_start_days': start_gap_days,
            'missing_end_days': end_gap_days,
            'fetch_start': fetch_start,
            'fetch_end': fetch_end,
            'membership_start': member_start,
            'membership_end': member_end,
            'checked_periods': checked_periods
        }

    def _check_missing_data_with_gaps(
        self,
        symbol: str,
        frequency: str,
        req_start: date,
        req_end: date,
        tolerance_days: int,
        membership_intervals: List[Tuple[date, date]],
        checked_periods: List[Tuple[date, date]]
    ) -> Dict:
        """
        Gap-aware missing data check for multiple discontinuous periods

        Internal method - use check_missing_data() instead
        """
        # Get existing data range once
        existing_range = self.price_manager.get_existing_date_range(symbol, frequency=frequency)

        # Check each period separately
        period_results = []
        total_missing_days = 0
        all_complete = True
        any_missing = False

        for period_idx, (period_start, period_end) in enumerate(checked_periods, 1):
            # Check this period
            if existing_range is None:
                # No data at all
                missing_days = (period_end - period_start).days
                period_results.append({
                    'period': period_idx,
                    'period_start': period_start,
                    'period_end': period_end,
                    'status': 'missing',
                    'missing_days': missing_days,
                    'actual_start': None,
                    'actual_end': None
                })
                total_missing_days += missing_days
                any_missing = True
                all_complete = False
            else:
                actual_start, actual_end = existing_range

                # Apply frequency-aware alignment to avoid false gaps
                # For monthly: 2014-01-01 → 2014-01-31 (end of month)
                # For weekly: 2014-01-01 → 2014-01-03 (end of week/Friday)
                # For daily: No change
                aligned_period_start = align_start_date_to_frequency(period_start, frequency)

                # Calculate gaps within this period using aligned start
                start_gap = max(0, (actual_start - aligned_period_start).days)
                end_gap = max(0, (period_end - actual_end).days)

                # Check if complete within tolerance
                is_complete = (start_gap <= tolerance_days and end_gap <= tolerance_days)

                if not is_complete:
                    all_complete = False

                period_results.append({
                    'period': period_idx,
                    'period_start': period_start,
                    'period_end': period_end,
                    'status': 'complete' if is_complete else 'partial',
                    'missing_start_days': start_gap,
                    'missing_end_days': end_gap,
                    'actual_start': actual_start,
                    'actual_end': actual_end
                })
                total_missing_days += max(0, start_gap - tolerance_days) + max(0, end_gap - tolerance_days)

        # Determine overall status
        if any_missing:
            overall_status = 'missing'
        elif all_complete:
            overall_status = 'complete'
        else:
            overall_status = 'partial'

        # Log summary
        self.logger.info(
            f"{'✅' if overall_status == 'complete' else '⚠️'} {symbol}: "
            f"{len(checked_periods)} checked period(s) | "
            f"Status: {overall_status.upper()} | "
            f"Total missing: {total_missing_days} days"
        )

        # Get overall membership span
        member_start = min(start for start, _ in membership_intervals) if membership_intervals else None
        member_end = max(end for _, end in membership_intervals) if membership_intervals else None
        actual_start = existing_range[0] if existing_range else None
        actual_end = existing_range[1] if existing_range else None

        return {
            'status': overall_status,
            'actual_start': actual_start,
            'actual_end': actual_end,
            'missing_start_days': period_results[0].get('missing_start_days', 0) if period_results else 0,
            'missing_end_days': period_results[-1].get('missing_end_days', 0) if period_results else 0,
            'fetch_start': str(checked_periods[0][0]),
            'fetch_end': str(checked_periods[-1][1]),
            'membership_start': member_start,
            'membership_end': member_end,
            'checked_periods': checked_periods,
            'intervals': period_results,
            'summary': {
                'total_periods': len(checked_periods),
                'checked_periods': len(period_results),
                'total_missing_days': total_missing_days,
                'has_gaps': len(checked_periods) > 1
            },
            'membership_intervals': membership_intervals
        }

    def check_universe_missing_data(
        self,
        frequency: str,
        required_start: str,
        required_end: str,
        tolerance_days: int = TOLERANCE_DAYS_DEFAULT,
        scope: str = "current"
    ) -> Dict:
        """
        Check missing data for all symbols in the universe

        Args:
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format
            tolerance_days: Ignore gaps of this many days or less (default: TOLERANCE_DAYS_DEFAULT)
            scope: Scope of members to check ('current' or 'historical')
            frequency: Data frequency ('daily', 'weekly', 'monthly')

        Returns:
            Dictionary with:
                - summary: Overall statistics
                - symbols: Dictionary mapping symbol -> check_missing_data result
                - by_status: Dictionary grouping symbols by status
        """
        self.logger.info(f"Checking missing data for universe '{self.universe.name}'")
        self.logger.info(f"Period: {required_start} to {required_end} (tolerance: ±{tolerance_days}d)")

        # Get symbols to check
        if scope == "current":
            symbols = self.universe.get_current_members()
            self.logger.info(f"Checking {len(symbols)} current members")
        else:
            symbols = self.universe.get_all_historical_members(required_start, required_end)
            self.logger.info(f"Checking {len(symbols)} historical members")

        # Check each symbol
        results = {}
        complete_symbols = []
        partial_symbols = []
        missing_symbols = []

        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                self.logger.info(f"Progress: {i}/{len(symbols)} symbols checked")

            result = self.check_missing_data(
                symbol=symbol,
                required_start=required_start,
                required_end=required_end,
                tolerance_days=tolerance_days
            )
            results[symbol] = result

            # Categorize by status
            if result['status'] == 'complete':
                complete_symbols.append(symbol)
            elif result['status'] == 'partial':
                partial_symbols.append(symbol)
            else:  # missing
                missing_symbols.append(symbol)

        # Calculate summary statistics
        total = len(symbols)
        complete_count = len(complete_symbols)
        partial_count = len(partial_symbols)
        missing_count = len(missing_symbols)
        complete_pct = (complete_count / total * 100) if total > 0 else 0

        summary = {
            'total_symbols': total,
            'complete': complete_count,
            'partial': partial_count,
            'missing': missing_count,
            'complete_pct': complete_pct
        }

        # Log summary
        self.logger.info("=" * 80)
        self.logger.info("Universe Missing Data Summary")
        self.logger.info("=" * 80)
        self.logger.info(f"Total symbols: {total}")
        self.logger.info(f"✅ Complete: {complete_count} ({complete_pct:.1f}%)")
        self.logger.info(f"⚠️  Partial: {partial_count} ({partial_count/total*100:.1f}%)")
        self.logger.info(f"📭 Missing: {missing_count} ({missing_count/total*100:.1f}%)")
        self.logger.info("=" * 80)

        # Show some examples of partial/missing if they exist
        if partial_symbols:
            self.logger.info(f"\nPartial data examples (first 10): {partial_symbols[:10]}")
        if missing_symbols:
            self.logger.info(f"\nMissing data examples (first 10): {missing_symbols[:10]}")

        return {
            'summary': summary,
            'symbols': results,
            'by_status': {
                'complete': complete_symbols,
                'partial': partial_symbols,
                'missing': missing_symbols
            }
        }

    def fetch_universe_missing_data(
        self,
        frequency: str,
        required_start: str,
        required_end: str,
        tolerance_days: int = TOLERANCE_DAYS_DEFAULT,
        scope: str = "current",
        skip_complete: bool = True,
        skip_errors: bool = True
    ) -> Dict:
        """
        Fetch missing data for all symbols in the universe

        This method first checks what data is missing, then fetches only the
        gaps for symbols with partial or missing data.

        Args:
            required_start: Required start date in 'YYYY-MM-DD' format
            required_end: Required end date in 'YYYY-MM-DD' format
            tolerance_days: Ignore gaps of this many days or less (default: TOLERANCE_DAYS_DEFAULT)
            scope: Scope of members to check ('current' or 'historical')
            frequency: Data frequency ('daily', 'weekly', 'monthly')
            skip_complete: If True, skip symbols with complete data (default: True)
            skip_errors: If True, continue on errors (default: True)

        Returns:
            Dictionary with:
                - check_result: Result from check_universe_missing_data
                - fetch_summary: Statistics about fetch operations
                - fetch_results: Dictionary mapping symbol -> fetch result
        """
        self.logger.info("=" * 80)
        self.logger.info("Fetching Missing Data for Universe")
        self.logger.info("=" * 80)

        # First, check what's missing
        check_result = self.check_universe_missing_data(
            required_start=required_start,
            required_end=required_end,
            tolerance_days=tolerance_days,
            scope=scope,
            frequency=frequency
        )

        by_status = check_result['by_status']
        symbols_details = check_result['symbols']

        # Determine which symbols to fetch
        symbols_to_fetch = []
        if not skip_complete:
            symbols_to_fetch.extend(by_status['complete'])
        symbols_to_fetch.extend(by_status['partial'])
        symbols_to_fetch.extend(by_status['missing'])

        self.logger.info(f"\nSymbols to fetch: {len(symbols_to_fetch)}")
        if skip_complete:
            self.logger.info(f"Skipping {len(by_status['complete'])} complete symbols")

        # Fetch data for each symbol based on its specific gaps
        fetch_results = {}
        symbols_fetched = 0
        symbols_failed = 0

        for i, symbol in enumerate(symbols_to_fetch, 1):
            if i % 50 == 0:
                self.logger.info(f"Fetch progress: {i}/{len(symbols_to_fetch)} symbols")

            details = symbols_details[symbol]

            # Determine what dates to fetch
            if details['status'] == 'complete':
                # Fetch the entire required period (if not skipped)
                fetch_start = required_start
                fetch_end = required_end
            else:
                # Use the recommended fetch dates (respects membership overlap)
                fetch_start = details['fetch_start'] or required_start
                fetch_end = details['fetch_end'] or required_end

            try:
                # Fetch the missing data
                df = self.price_manager.fetch_eod(
                    symbol=symbol,
                    start_date=fetch_start,
                    end_date=fetch_end,
                    frequency=frequency,
                    save=True
                )

                fetch_results[symbol] = {
                    'status': 'success',
                    'rows': len(df),
                    'fetch_start': fetch_start,
                    'fetch_end': fetch_end
                }
                symbols_fetched += 1

            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"Failed to fetch {symbol}: {error_msg}")
                fetch_results[symbol] = {
                    'status': 'failed',
                    'error': error_msg,
                    'fetch_start': fetch_start,
                    'fetch_end': fetch_end
                }
                symbols_failed += 1

                if not skip_errors:
                    raise

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("Fetch Operation Complete")
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Successfully fetched: {symbols_fetched}")
        self.logger.info(f"❌ Failed: {symbols_failed}")
        self.logger.info(f"⏭️  Skipped (complete): {len(by_status['complete']) if skip_complete else 0}")

        return {
            'check_result': check_result,
            'fetch_summary': {
                'symbols_fetched': symbols_fetched,
                'symbols_skipped': len(by_status['complete']) if skip_complete else 0,
                'symbols_failed': symbols_failed
            },
            'fetch_results': fetch_results
        }
