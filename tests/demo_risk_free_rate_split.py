"""
Demo: Risk-Free Rate Builder and Manager Split

Shows how to use the two separate classes:
- RiskFreeRateBuilder: Fetch and save data (requires API key)
- RiskFreeRateManager: Load cached data (no API key needed)
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import Config
from market.risk_free_rate_manager import RiskFreeRateBuilder, RiskFreeRateManager


def demo_builder():
    """Demo: Use RiskFreeRateBuilder to fetch and save data"""
    print("\n" + "=" * 80)
    print("DEMO 1: RiskFreeRateBuilder - Fetch and Save Data")
    print("=" * 80)

    # Load config
    config = Config("config/settings.yaml")
    fred_api_key = config.get("fred.api_key")

    if not fred_api_key:
        print("⚠️  No FRED API key found in config")
        print("   Set fred.api_key in config/settings.yaml")
        return

    # Create builder (requires API key)
    builder = RiskFreeRateBuilder(
        fred_api_key=fred_api_key,
        data_root="data/curated/references/risk_free_rate/freq=monthly",
        default_rate="3month",
    )

    print(f"\n✓ Created RiskFreeRateBuilder")
    print(f"  Default rate: 3-Month Treasury")
    print(f"  Data root: data/curated/references/risk_free_rate/freq=monthly")

    # Fetch and save data
    print("\n📡 Fetching data from FRED...")
    rf_df = builder.build_and_save(
        start_date="2020-01-01",
        end_date="2024-12-31",
        rate_type="3month",
        frequency="monthly",
    )

    print(f"\n✓ Fetched and saved {len(rf_df)} observations")
    print(f"\nFirst 5 rows:")
    print(rf_df.head())
    print(f"\nLast 5 rows:")
    print(rf_df.tail())
    print(f"\nSummary:")
    print(f"  Date range: {rf_df['date'].min()} to {rf_df['date'].max()}")
    print(f"  Mean rate: {rf_df['rate'].mean():.2f}%")
    print(f"  Min rate: {rf_df['rate'].min():.2f}%")
    print(f"  Max rate: {rf_df['rate'].max():.2f}%")


def demo_manager():
    """Demo: Use RiskFreeRateManager to load cached data"""
    print("\n" + "=" * 80)
    print("DEMO 2: RiskFreeRateManager - Load Cached Data (No API Key)")
    print("=" * 80)

    # Create manager (no API key needed!)
    manager = RiskFreeRateManager(
        data_root="data/curated/references/risk_free_rate/freq=monthly",
        default_rate="3month",
    )

    print(f"\n✓ Created RiskFreeRateManager")
    print(f"  Default rate: 3-Month Treasury")
    print(f"  No API key required!")

    # Load cached data
    print("\n📂 Loading cached data...")
    try:
        rf_df = manager.load_risk_free_rate(
            start_date="2022-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )

        print(f"\n✓ Loaded {len(rf_df)} observations from cache")
        print(f"\nFirst 5 rows:")
        print(rf_df.head())
        print(f"\nLast 5 rows:")
        print(rf_df.tail())

        # Calculate statistics
        print("\n📊 Summary Statistics:")
        stats = manager.get_summary_statistics(
            start_date="2022-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )
        for key, value in stats.items():
            print(f"  {key}: {value}")

    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\n💡 Solution: Run demo_builder() first to fetch and cache data")


def demo_usage_patterns():
    """Demo: Common usage patterns"""
    print("\n" + "=" * 80)
    print("DEMO 3: Common Usage Patterns")
    print("=" * 80)

    print("\n📝 Pattern 1: First-time setup (fetch and cache data)")
    print("─" * 80)
    print(
        """
from market.risk_free_rate_manager import RiskFreeRateBuilder

# One-time setup with API key
builder = RiskFreeRateBuilder(
    fred_api_key="your_api_key_here",
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)

# Fetch and save 20 years of data
rf_df = builder.build_and_save(
    start_date="2004-01-01",
    end_date="2024-12-31",
    rate_type="3month",
    frequency="monthly"
)
"""
    )

    print("\n📝 Pattern 2: Regular usage (load from cache, no API key needed)")
    print("─" * 80)
    print(
        """
from market.risk_free_rate_manager import RiskFreeRateManager

# Create manager (no API key!)
manager = RiskFreeRateManager(
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)

# Load cached data instantly
rf_df = manager.load_risk_free_rate(
    start_date="2020-01-01",
    end_date="2024-12-31"
)
"""
    )

    print("\n📝 Pattern 3: ESGFactorBuilder integration")
    print("─" * 80)
    print(
        """
from esg.esg_factor import ESGFactorBuilder

# Without API key (uses cached data only)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month",
    # No fred_api_key parameter!
)

# With API key (auto-fetches if needed)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month",
    fred_api_key=config.get("fred.api_key")
)
"""
    )

    print("\n✅ Benefits of this design:")
    print("  1. No API key needed for regular usage (loading)")
    print("  2. API key only required for fetching new data")
    print("  3. Clear separation: Builder (fetch) vs Manager (load)")
    print("  4. Simpler API: RiskFreeRateManager constructor has 2 params instead of 3")


def main():
    """Run all demos"""
    print("\n" + "=" * 80)
    print("Risk-Free Rate: Builder & Manager Demo")
    print("=" * 80)

    # Demo 1: Fetch and save (requires API key)
    demo_builder()

    # Demo 2: Load from cache (no API key)
    demo_manager()

    # Demo 3: Usage patterns
    demo_usage_patterns()

    print("\n" + "=" * 80)
    print("✓ Demo Complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
