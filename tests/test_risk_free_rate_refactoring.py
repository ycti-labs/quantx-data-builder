"""
Test Program for Risk-Free Rate Manager Refactoring

Tests the split between RiskFreeRateBuilder and RiskFreeRateManager,
and verifies ESGFactorBuilder integration.

Usage:
    python tests/test_risk_free_rate_refactoring.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from core.config import Config
from market.risk_free_rate_builder import RiskFreeRateBuilder
from market.risk_free_rate_manager import RiskFreeRateManager

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TestResults:
    """Track test results"""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.tests = []

    def add(self, name: str, passed: bool, message: str = ""):
        self.tests.append((name, passed, message))
        if passed:
            self.passed += 1
            logger.info(f"✅ PASS: {name}")
        else:
            self.failed += 1
            logger.error(f"❌ FAIL: {name} - {message}")

    def summary(self):
        total = self.passed + self.failed
        logger.info("\n" + "=" * 80)
        logger.info(f"TEST SUMMARY: {self.passed}/{total} tests passed")
        logger.info("=" * 80)

        if self.failed > 0:
            logger.info("\nFailed Tests:")
            for name, passed, message in self.tests:
                if not passed:
                    logger.info(f"  ❌ {name}: {message}")

        return self.failed == 0


def test_risk_free_rate_builder(config: Config, results: TestResults):
    """Test RiskFreeRateBuilder functionality"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUITE 1: RiskFreeRateBuilder")
    logger.info("=" * 80)

    fred_api_key = config.get("fetcher.fred.api_key")
    if not fred_api_key:
        results.add("RiskFreeRateBuilder", False, "No FRED API key in config")
        logger.warning("⚠️  Skipping builder tests - no API key")
        return

    try:
        # Test 1: Create builder
        builder = RiskFreeRateBuilder(
            fred_api_key=fred_api_key,
            data_root="data/curated/references/risk_free_rate/freq=monthly",
            default_rate="3month",
        )
        results.add("Builder Creation", True)
    except Exception as e:
        results.add("Builder Creation", False, str(e))
        return

    try:
        # Test 2: Fetch small date range
        logger.info("\nTest: Fetching 1 year of data from FRED...")
        rf_df = builder.fetch_risk_free_rate(
            start_date="2023-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )

        # Validate structure
        assert isinstance(rf_df, pd.DataFrame), "Result should be DataFrame"
        assert "date" in rf_df.columns, "Should have 'date' column"
        assert "rate" in rf_df.columns, "Should have 'rate' column"
        assert len(rf_df) > 0, "Should have data"
        assert len(rf_df) <= 12, "Should have ~12 months of data"

        results.add("Fetch from FRED", True)
        logger.info(f"  Fetched {len(rf_df)} observations")
        logger.info(f"  Date range: {rf_df['date'].min()} to {rf_df['date'].max()}")
        logger.info(
            f"  Rate stats: mean={rf_df['rate'].mean():.2f}%, std={rf_df['rate'].std():.2f}%"
        )

    except Exception as e:
        results.add("Fetch from FRED", False, str(e))

    try:
        # Test 3: Build and save
        logger.info("\nTest: Building and saving to cache...")
        rf_df = builder.build_and_save(
            start_date="2022-01-01",
            end_date="2024-12-31",
            rate_type="3month",
            frequency="monthly",
            merge_existing=True,
        )

        # Validate
        assert len(rf_df) > 12, "Should have multiple years of data"

        # Check file exists
        cache_path = builder.get_cache_path("3month", "monthly")
        assert cache_path.exists(), f"Cache file should exist at {cache_path}"

        results.add("Build and Save", True)
        logger.info(f"  Saved {len(rf_df)} observations to cache")
        logger.info(f"  Cache location: {cache_path}")

    except Exception as e:
        results.add("Build and Save", False, str(e))

    try:
        # Test 4: Test different rate types
        logger.info("\nTest: Fetching different rate types...")
        for rate_type in ["1year", "5year", "10year"]:
            rf_df = builder.fetch_risk_free_rate(
                start_date="2023-01-01",
                end_date="2023-12-31",
                rate_type=rate_type,
                frequency="monthly",
            )
            assert len(rf_df) > 0, f"Should have data for {rate_type}"
            logger.info(
                f"  {rate_type}: {len(rf_df)} obs, mean={rf_df['rate'].mean():.2f}%"
            )

        results.add("Multiple Rate Types", True)

    except Exception as e:
        results.add("Multiple Rate Types", False, str(e))


def test_risk_free_rate_manager(results: TestResults):
    """Test RiskFreeRateManager functionality"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUITE 2: RiskFreeRateManager")
    logger.info("=" * 80)

    try:
        # Test 1: Create manager (no API key needed!)
        manager = RiskFreeRateManager(
            data_root="data/curated/references/risk_free_rate/freq=monthly",
            default_rate="3month",
        )
        results.add("Manager Creation (No API Key)", True)
        logger.info("  ✅ Created manager without API key")

    except Exception as e:
        results.add("Manager Creation (No API Key)", False, str(e))
        return

    try:
        # Test 2: Load from cache
        logger.info("\nTest: Loading from cache...")
        rf_df = manager.load_risk_free_rate(
            start_date="2023-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )

        # Validate
        assert isinstance(rf_df, pd.DataFrame), "Result should be DataFrame"
        assert "date" in rf_df.columns, "Should have 'date' column"
        assert "rate" in rf_df.columns, "Should have 'rate' column"
        assert len(rf_df) > 0, "Should have data"

        results.add("Load from Cache", True)
        logger.info(f"  Loaded {len(rf_df)} observations")
        logger.info(f"  Date range: {rf_df['date'].min()} to {rf_df['date'].max()}")

    except Exception as e:
        results.add("Load from Cache", False, str(e))

    try:
        # Test 3: Calculate risk-free returns
        logger.info("\nTest: Calculating risk-free returns...")

        # Create sample dates
        dates = pd.date_range("2023-01-01", "2023-12-31", freq="ME")
        dates_series = pd.Series([d.date() for d in dates])

        rf_returns = manager.calculate_risk_free_returns(
            dates=dates_series, rate_type="3month", frequency="monthly"
        )

        # Validate
        assert isinstance(rf_returns, pd.Series), "Should return Series"
        assert len(rf_returns) == len(dates_series), "Should match input length"
        assert rf_returns.mean() > 0, "Average RF return should be positive"
        assert rf_returns.mean() < 0.05, "Average monthly return should be < 5%"

        results.add("Calculate RF Returns", True)
        logger.info(
            f"  Mean monthly return: {rf_returns.mean():.6f} ({rf_returns.mean()*12:.4f} annualized)"
        )
        logger.info(f"  Min: {rf_returns.min():.6f}, Max: {rf_returns.max():.6f}")

    except Exception as e:
        results.add("Calculate RF Returns", False, str(e))

    try:
        # Test 4: Calculate excess returns
        logger.info("\nTest: Calculating excess returns...")

        # Create sample stock returns
        stock_returns = pd.Series(np.random.normal(0.01, 0.05, len(dates_series)))

        excess_returns = manager.calculate_excess_returns(
            returns=stock_returns,
            dates=dates_series,
            rate_type="3month",
            frequency="monthly",
        )

        # Validate
        assert isinstance(excess_returns, pd.Series), "Should return Series"
        assert len(excess_returns) == len(stock_returns), "Should match input length"

        results.add("Calculate Excess Returns", True)
        logger.info(f"  Mean excess return: {excess_returns.mean():.6f}")
        logger.info(
            f"  Stock return: {stock_returns.mean():.6f}, RF: {rf_returns.mean():.6f}"
        )

    except Exception as e:
        results.add("Calculate Excess Returns", False, str(e))

    try:
        # Test 5: Get summary statistics
        logger.info("\nTest: Getting summary statistics...")

        stats = manager.get_summary_statistics(
            start_date="2023-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )

        # Validate
        assert isinstance(stats, dict), "Should return dict"
        assert "mean_rate" in stats, "Should have mean_rate"
        assert "observations" in stats, "Should have observations count"
        assert stats["observations"] > 0, "Should have observations"

        results.add("Summary Statistics", True)
        logger.info(f"  Statistics: {stats}")

    except Exception as e:
        results.add("Summary Statistics", False, str(e))

    try:
        # Test 6: Error handling - missing cache
        logger.info("\nTest: Error handling for missing cache...")

        try:
            manager.load_risk_free_rate(
                start_date="2030-01-01",  # Future date not in cache
                end_date="2030-12-31",
                rate_type="3month",
                frequency="monthly",
            )
            results.add("Missing Cache Error", False, "Should have raised ValueError")
        except ValueError as e:
            assert "Use RiskFreeRateBuilder" in str(
                e
            ), "Error should mention RiskFreeRateBuilder"
            results.add("Missing Cache Error", True)
            logger.info(f"  ✅ Correctly raised ValueError with helpful message")

    except Exception as e:
        results.add("Missing Cache Error", False, str(e))


def test_esg_factor_builder_integration(results: TestResults):
    """Test ESGFactorBuilder integration with new RF loading"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUITE 3: ESGFactorBuilder Integration")
    logger.info("=" * 80)

    try:
        from esg.esg_factor import ESGFactorBuilder
        from universe import SP500Universe

        # Test 1: Create builder without API key
        logger.info("\nTest: Creating ESGFactorBuilder without API key...")

        universe = SP500Universe(data_root="data/curated")
        builder = ESGFactorBuilder(
            universe=universe,
            rf_rate_type="3month",
            # No fred_api_key parameter!
        )

        results.add("ESGFactorBuilder Creation (No API Key)", True)
        logger.info("  ✅ Created ESGFactorBuilder without API key")

        # Verify it has RiskFreeRateManager
        assert hasattr(builder, "rf_manager"), "Should have rf_manager"
        assert not hasattr(builder, "rf_builder"), "Should NOT have rf_builder"

        results.add("ESGFactorBuilder Structure", True)
        logger.info("  ✅ Has rf_manager, no rf_builder")

    except Exception as e:
        results.add("ESGFactorBuilder Integration", False, str(e))

    try:
        # Test 2: Test RF loading through ESGFactorBuilder
        logger.info("\nTest: Loading RF through ESGFactorBuilder...")

        # Create fake returns data
        dates = pd.date_range("2023-01-01", "2023-12-31", freq="ME")
        tickers = ["AAPL", "MSFT", "GOOGL"]

        returns_data = []
        for date in dates:
            for ticker in tickers:
                returns_data.append(
                    {
                        "date": date,
                        "ticker": ticker,
                        "ret": np.random.normal(0.01, 0.05),
                    }
                )

        returns_df = pd.DataFrame(returns_data)
        returns_df = returns_df.set_index(["date", "ticker"]).sort_index()

        # Load RF using internal method
        rf_df = builder._load_risk_free_rate(returns_df)

        # Validate
        assert isinstance(rf_df, pd.DataFrame), "Should return DataFrame"
        assert "RF" in rf_df.columns, "Should have RF column (not 'rate')"
        assert len(rf_df) > 0, "Should have data"

        results.add("ESGFactorBuilder RF Loading", True)
        logger.info(f"  Loaded {len(rf_df)} RF observations")
        logger.info(f"  RF column name: {rf_df.columns.tolist()}")

    except Exception as e:
        results.add("ESGFactorBuilder RF Loading", False, str(e))

    try:
        # Test 3: Test excess returns conversion
        logger.info("\nTest: Converting to excess returns...")

        panel_excess = builder._to_excess_returns(returns_df, rf_df)

        # Validate
        assert isinstance(panel_excess, pd.DataFrame), "Should return DataFrame"
        assert "excess" in panel_excess.columns, "Should have 'excess' column"
        assert len(panel_excess) > 0, "Should have data"

        # Check normalization
        excess_mean = panel_excess["excess"].mean()
        assert -0.05 < excess_mean < 0.05, "Excess return mean should be reasonable"

        results.add("ESGFactorBuilder Excess Returns", True)
        logger.info(f"  Created {len(panel_excess)} excess return observations")
        logger.info(f"  Mean excess return: {excess_mean:.6f}")

    except Exception as e:
        results.add("ESGFactorBuilder Excess Returns", False, str(e))


def test_data_format_validation(results: TestResults):
    """Test data format and normalization"""
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUITE 4: Data Format Validation")
    logger.info("=" * 80)

    try:
        manager = RiskFreeRateManager(
            data_root="data/curated/references/risk_free_rate/freq=monthly"
        )

        # Load data
        rf_df = manager.load_risk_free_rate(
            start_date="2023-01-01",
            end_date="2023-12-31",
            rate_type="3month",
            frequency="monthly",
        )

        # Test 1: Validate FRED format (annual percentage)
        logger.info("\nTest: Validating FRED data format...")

        mean_rate = rf_df["rate"].mean()

        # FRED rates should be in percentage (e.g., 5.25 = 5.25%)
        assert 0 < mean_rate < 20, "Rate should be in percentage format (0-20%)"
        assert mean_rate > 0.01, "Rate should not be in decimal format"

        results.add("FRED Format Validation", True)
        logger.info(
            f"  ✅ Rate format correct: mean={mean_rate:.2f}% (annual percentage)"
        )

    except Exception as e:
        results.add("FRED Format Validation", False, str(e))

    try:
        # Test 2: Validate conversion to monthly decimal
        logger.info("\nTest: Validating conversion to monthly decimal...")

        # Convert as ESGFactorBuilder does
        monthly_decimal = rf_df["rate"] / 100 / 12
        mean_monthly = monthly_decimal.mean()

        # Monthly decimal should be in range 0.001-0.01 (0.1%-1%)
        assert 0.0001 < mean_monthly < 0.02, "Monthly rate should be small decimal"

        results.add("Monthly Decimal Conversion", True)
        logger.info(
            f"  ✅ Monthly decimal: mean={mean_monthly:.6f} ({mean_monthly*100:.4f}%)"
        )
        logger.info(f"  Annualized: {mean_monthly*12:.4f} ({mean_monthly*12*100:.2f}%)")

    except Exception as e:
        results.add("Monthly Decimal Conversion", False, str(e))


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("RISK-FREE RATE REFACTORING TEST SUITE")
    print("=" * 80)

    results = TestResults()

    try:
        # Load config
        config = Config("config/settings.yaml")
        logger.info("✅ Loaded configuration")
    except Exception as e:
        logger.error(f"❌ Failed to load config: {e}")
        return 1

    # Run test suites
    test_risk_free_rate_builder(config, results)
    test_risk_free_rate_manager(results)
    test_esg_factor_builder_integration(results)
    test_data_format_validation(results)

    # Print summary
    success = results.summary()

    if success:
        print("\n" + "=" * 80)
        print("🎉 ALL TESTS PASSED!")
        print("=" * 80)
        return 0
    else:
        print("\n" + "=" * 80)
        print(f"❌ {results.failed} TEST(S) FAILED")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
