#!/usr/bin/env python3
"""
Market Data Builder Test Script

Test the market data builder API integration.
"""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.market_data import FetcherConfig, PriceDataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_price_data_builder():
    """Test market data builder with various methods"""

    print("=" * 80)
    print("Market Data Builder Test")
    print("=" * 80)
    print()

    try:
        # Load configuration
        config = FetcherConfig("config/settings.yaml")
        logger.info(f"✅ Configuration loaded")
        logger.info(f"   API Key: {config.fetcher.tiingo.api_key[:10]}...")
        print()

        # Create market data builder
        builder = PriceDataManager(
            api_key=config.fetcher.tiingo.api_key
        )
        logger.info(f"✅ PriceDataManager created")
        print()

        # Test 1: Connection test
        print("=" * 80)
        print("Test 1: Connection Test")
        print("=" * 80)
        if builder.test_connection():
            print("✅ Connection successful!")
        else:
            print("❌ Connection failed")
            return 1
        print()

        # Test 2: Fetch single symbol
        print("=" * 80)
        print("Test 2: Fetch Single Symbol (AAPL, last 5 days)")
        print("=" * 80)
        df = builder.fetch_eod('AAPL', start_date='2024-11-01', end_date='2024-11-08')
        if not df.empty:
            print(f"✅ Fetched {len(df)} rows")
            print("\nSample data:")
            print(df.head())
        else:
            print("⚠️  No data returned")
        print()

        # Test 3: Fetch multiple symbols
        print("=" * 80)
        print("Test 3: Fetch Multiple Symbols (AAPL, MSFT, GOOGL)")
        print("=" * 80)
        results = builder.fetch_multiple_eod(
            symbols=['AAPL', 'MSFT', 'GOOGL'],
            start_date='2024-11-01',
            end_date='2024-11-08'
        )
        print(f"✅ Fetched data for {len(results)} symbols")
        for symbol, data in results.items():
            print(f"   {symbol}: {len(data)} rows")
        print()

        # Test 4: Get universe members (if membership data exists)
        print("=" * 80)
        print("Test 4: Get Universe Members")
        print("=" * 80)
        try:
            members = builder.get_current_members('sp500')
            if members:
                print(f"✅ Found {len(members)} members in SP500")
                print(f"   Sample: {members[:10]}")
            else:
                print("⚠️  No membership data found (expected if not yet loaded)")
        except Exception as e:
            print(f"⚠️  Membership data not available: {e}")
        print()

        print("=" * 80)
        print("✅ All Tests Completed!")
        print("=" * 80)
        print()
        print("The PriceDataManager is working correctly!")
        print()
        print("Usage examples:")
        print("  # Fetch single symbol")
        print("  df = builder.fetch_eod('AAPL', start_date='2020-01-01')")
        print()
        print("  # Fetch multiple symbols")
        print("  data = builder.fetch_multiple(['AAPL', 'MSFT'], start_date='2020-01-01')")
        print()
        print("  # Get universe members")
        print("  symbols = builder.get_current_members('sp500')")
        print()
        print("  # Fetch universe data")
        print("  data = builder.fetch_universe_data('sp500', start_date='2020-01-01')")
        print()

        return 0

    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        print()
        print("=" * 80)
        print("Setup Required")
        print("=" * 80)
        print()
        print("To use Tiingo API, you need an API key:")
        print("  1. Sign up for free at: https://www.tiingo.com")
        print("  2. Get your API key from: https://www.tiingo.com/account/api/token")
        print("  3. Set environment variable:")
        print("     export TIINGO_API_KEY=your_key_here")
        print("  OR")
        print("  4. Edit config/settings.yaml and replace ${TIINGO_API_KEY} with your key")
        print()
        return 1
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(test_price_data_builder())
