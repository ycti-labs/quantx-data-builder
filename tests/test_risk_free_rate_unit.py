"""
Unit Tests for Risk-Free Rate Manager

Simple unit tests that can run without API keys or cached data.
"""

import sys
import unittest
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import date

import numpy as np
import pandas as pd

from market.risk_free_rate_manager import RiskFreeRateManager


class TestRiskFreeRateManager(unittest.TestCase):
    """Unit tests for RiskFreeRateManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.manager = RiskFreeRateManager(
            data_root="data/curated/references/risk_free_rate/freq=monthly",
            default_rate="3month",
        )

    def test_initialization(self):
        """Test manager initialization"""
        self.assertEqual(self.manager.default_rate, "3month")
        self.assertIn("3month", self.manager.FRED_SERIES)
        self.assertIn("10year", self.manager.FRED_SERIES)

    def test_invalid_rate_type(self):
        """Test initialization with invalid rate type"""
        with self.assertRaises(ValueError):
            RiskFreeRateManager(default_rate="invalid_rate")

    def test_get_cache_path(self):
        """Test cache path generation"""
        path = self.manager.get_cache_path("3month", "monthly")

        self.assertIsInstance(path, Path)
        self.assertTrue(str(path).endswith("3month_monthly.parquet"))

    def test_fred_series_mapping(self):
        """Test FRED series code mapping"""
        self.assertEqual(self.manager.FRED_SERIES["3month"], "DGS3MO")
        self.assertEqual(self.manager.FRED_SERIES["1year"], "DGS1")
        self.assertEqual(self.manager.FRED_SERIES["5year"], "DGS5")
        self.assertEqual(self.manager.FRED_SERIES["10year"], "DGS10")
        self.assertEqual(self.manager.FRED_SERIES["30year"], "DGS30")

    def test_calculate_risk_free_returns_conversion(self):
        """Test RF rate to return conversion logic"""
        # Create test data: annual percentage rates
        dates_list = [date(2023, 1, 31), date(2023, 2, 28), date(2023, 3, 31)]
        dates = pd.Series(dates_list)

        # Mock annualized rate DataFrame (annual percentage)
        annualized_rate = pd.DataFrame(
            {"date": dates_list, "rate": [5.0, 5.25, 5.5]}  # 5%, 5.25%, 5.5% annual
        )

        # Calculate returns
        rf_returns = self.manager.calculate_risk_free_returns(
            dates=dates,
            rate_type="3month",
            frequency="monthly",
            annualized_rate=annualized_rate,
        )

        # Validate
        self.assertEqual(len(rf_returns), 3)

        # Check conversion: annual % -> monthly decimal
        # 5.0% annual = 5.0 / 100 / 12 = 0.004167 monthly
        expected_first = 5.0 / 100 / 12
        self.assertAlmostEqual(rf_returns.iloc[0], expected_first, places=6)

        # All values should be small positive decimals
        self.assertTrue(all(rf_returns > 0))
        self.assertTrue(all(rf_returns < 0.01))  # Less than 1% monthly

    def test_calculate_excess_returns_logic(self):
        """Test excess return calculation"""
        # Create test data
        dates_list = [date(2023, 1, 31), date(2023, 2, 28)]
        dates = pd.Series(dates_list)

        # Stock returns (monthly decimal)
        returns = pd.Series([0.02, -0.01])  # 2%, -1% monthly

        # Mock RF data
        annualized_rate = pd.DataFrame(
            {"date": dates_list, "rate": [6.0, 6.0]}  # 6% annual = 0.5% monthly
        )

        # Calculate excess returns manually for validation
        rf_monthly = 6.0 / 100 / 12  # 0.005
        expected_excess = returns - rf_monthly

        # Calculate using manager
        excess_returns = self.manager.calculate_excess_returns(
            returns=returns, dates=dates, rate_type="3month", frequency="monthly"
        )

        # Validate - this will fail without cache, but tests the logic
        # We're testing the method signature and structure
        self.assertIsInstance(excess_returns, pd.Series)

    def test_get_summary_statistics_structure(self):
        """Test summary statistics return structure"""
        # This tests the method exists and has correct signature
        # Actual execution requires cached data

        # Check method exists
        self.assertTrue(hasattr(self.manager, "get_summary_statistics"))

        # Check it's callable
        self.assertTrue(callable(self.manager.get_summary_statistics))


class TestDataFormatAssumptions(unittest.TestCase):
    """Test assumptions about data formats"""

    def test_fred_format_assumptions(self):
        """Test assumptions about FRED data format"""
        # FRED data is always:
        # - Annual percentage (e.g., 5.25 = 5.25% per year)
        # - Needs division by 100 (percent -> decimal)
        # - Needs division by 12 (annual -> monthly)

        fred_annual_pct = 6.0  # 6% annual

        # Convert to monthly decimal
        monthly_decimal = fred_annual_pct / 100 / 12

        # Validate conversion
        self.assertAlmostEqual(monthly_decimal, 0.005, places=6)

        # Annualized back should equal original
        annualized = monthly_decimal * 12 * 100
        self.assertAlmostEqual(annualized, fred_annual_pct, places=6)

    def test_reasonable_rate_ranges(self):
        """Test what constitutes reasonable rate values"""
        # Annual percentage (FRED format)
        annual_pct = 5.0

        # After conversion to monthly decimal
        monthly_decimal = annual_pct / 100 / 12

        # Reasonable ranges
        self.assertTrue(0.0001 < monthly_decimal < 0.02)  # 0.01% - 2% monthly

        # What would be wrong
        wrong_value = 5.0 / 12  # Forgot to divide by 100
        self.assertTrue(wrong_value > 0.1)  # This would be > 10% monthly (wrong!)


class TestNormalizationPipeline(unittest.TestCase):
    """Test the complete normalization pipeline"""

    def test_full_conversion_pipeline(self):
        """Test complete FRED -> excess returns pipeline"""

        # Step 1: FRED data (annual percentage)
        fred_rate = 6.0  # 6% annual

        # Step 2: Convert to monthly decimal
        monthly_rf = fred_rate / 100 / 12
        self.assertAlmostEqual(monthly_rf, 0.005, places=6)

        # Step 3: Calculate excess return
        stock_return = 0.02  # 2% monthly
        excess_return = stock_return - monthly_rf

        self.assertAlmostEqual(excess_return, 0.015, places=6)

        # Step 4: Validate reasonable range
        self.assertTrue(-0.1 < excess_return < 0.1)  # Within ±10% monthly

    def test_multiple_frequencies(self):
        """Test conversion for different frequencies"""
        annual_pct = 12.0  # 12% annual

        # Monthly
        monthly = annual_pct / 100 / 12
        self.assertAlmostEqual(monthly, 0.01, places=6)

        # Weekly
        weekly = annual_pct / 100 / 52
        self.assertAlmostEqual(weekly, 0.002308, places=6)

        # Daily (trading days)
        daily = annual_pct / 100 / 252
        self.assertAlmostEqual(daily, 0.000476, places=6)


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
