"""
Test membership intervals with span_mode parameter

Demonstrates the difference between span_mode=False (default) and span_mode=True
"""

import sys
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from universe import SP500Universe


def main():
    """Test membership intervals with different modes"""

    print("=" * 80)
    print("Testing get_membership_intervals with span_mode parameter")
    print("=" * 80)

    universe = SP500Universe()

    # Test symbols with multiple intervals
    test_symbols = [
        ('AMD', 'Removed 2013, re-added 2017'),
        ('AAPL', 'Continuous member')
    ]

    for symbol, description in test_symbols:
        print(f"\n{'─' * 80}")
        print(f"Symbol: {symbol} ({description})")
        print(f"{'─' * 80}")

        # Default mode (span_mode=False): all individual intervals
        intervals_full = universe.get_membership_intervals(symbol, span_mode=False)
        print(f"\nspan_mode=False (default - all intervals):")
        print(f"  Count: {len(intervals_full)} interval(s)")
        for idx, (start, end) in enumerate(intervals_full, 1):
            duration = (end - start).days / 365.25
            print(f"  Interval {idx}: {start} to {end} ({duration:.1f} years)")

        # Span mode (span_mode=True): single interval spanning full period
        intervals_span = universe.get_membership_intervals(symbol, span_mode=True)
        print(f"\nspan_mode=True (span - earliest to latest):")
        print(f"  Count: {len(intervals_span)} interval(s)")
        for start, end in intervals_span:
            duration = (end - start).days / 365.25
            print(f"  Span: {start} to {end} ({duration:.1f} years)")

    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print("""
Key Differences:

1. span_mode=False (default):
   - Returns all individual membership intervals
   - Preserves gaps between memberships
   - Use for: Gap-aware data checking, accurate historical analysis

2. span_mode=True:
   - Returns single interval from earliest start to latest end
   - Treats gaps as continuous membership (span)
   - Use for: Simple range checking, backward compatibility

Example Results:
- AMD with span_mode=False: 2 intervals (2000-2013, 2017-2025)
- AMD with span_mode=True: 1 interval (2000-2025)
- AAPL: Same result in both modes (continuous member)
    """)

if __name__ == "__main__":
    main()
