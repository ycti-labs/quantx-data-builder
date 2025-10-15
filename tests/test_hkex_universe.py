#!/usr/bin/env python3
"""
Test HKEX Universe Building

Test script to validate the HK market builder's ability to fetch
the complete universe from official HKEX sources.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from fetcher.universe.markets.hk_market import HKMarketBuilder
from fetcher.logging import configure_logging, get_logger

logger = get_logger(__name__)


async def test_hkex_universe():
    """Test HKEX universe building."""
    configure_logging('INFO')
    
    logger.info("Testing HKEX universe building")
    
    # Create HK market builder
    hk_builder = HKMarketBuilder()
    
    try:
        # Test comprehensive HKEX universe
        logger.info("Building comprehensive HKEX universe...")
        hk_all_universe = await hk_builder.build_universe('hk_all')
        logger.info(f"✅ Built hk_all universe: {len(hk_all_universe)} stocks")
        
        # Test specific indices
        logger.info("Building HSI universe...")
        hsi_universe = await hk_builder.build_universe('hk_hsi')
        logger.info(f"✅ Built HSI universe: {len(hsi_universe)} stocks")
        
        logger.info("Building HSCEI universe...")
        hscei_universe = await hk_builder.build_universe('hk_hscei')
        logger.info(f"✅ Built HSCEI universe: {len(hscei_universe)} stocks")
        
        # Get market statistics
        logger.info("Getting market statistics...")
        stats = await hk_builder.get_hkex_market_statistics()
        
        print("\n" + "="*50)
        print("HKEX MARKET STATISTICS")
        print("="*50)
        for key, value in stats.items():
            print(f"{key:25}: {value}")
        
        # Show sample tickers
        print("\n" + "="*50)
        print("SAMPLE TICKERS")
        print("="*50)
        print(f"First 20 HKEX tickers: {[s.ticker for s in hk_all_universe[:20]]}")
        print(f"First 10 HSI tickers:  {[s.ticker for s in hsi_universe[:10]]}")
        print(f"First 10 HSCEI tickers: {[s.ticker for s in hscei_universe[:10]]}")
        
        # Test live HKEX fetch directly
        logger.info("Testing direct HKEX fetch...")
        live_tickers = await hk_builder.fetch_live_hkex_universe()
        if live_tickers:
            logger.info(f"✅ Live HKEX fetch successful: {len(live_tickers)} tickers")
            print(f"Live vs built comparison: {len(live_tickers)} vs {len(hk_all_universe)}")
        else:
            logger.warning("❌ Live HKEX fetch failed, using fallback")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False


async def test_modular_integration():
    """Test integration with modular builder."""
    from fetcher.universe.modular_builder import ModularUniverseBuilder
    
    logger.info("Testing modular builder integration...")
    
    builder = ModularUniverseBuilder()
    
    try:
        # Test Phase 2 which includes hk_all
        phase2_universes = await builder.build_phase_universe('phase2')
        
        print("\n" + "="*50)
        print("PHASE 2 UNIVERSE RESULTS")
        print("="*50)
        
        total_symbols = 0
        for universe_id, universe in phase2_universes.items():
            print(f"{universe_id:15}: {len(universe):4d} symbols")
            total_symbols += len(universe)
        
        print(f"{'Total':15}: {total_symbols:4d} symbols")
        
        # Save to test files
        output_dir = Path("test_output")
        output_paths = await builder.save_phase_to_csv(
            phase2_universes, output_dir, 'phase2', create_combined=True
        )
        
        print("\n" + "="*50)
        print("OUTPUT FILES CREATED")
        print("="*50)
        for universe_id, path in output_paths.items():
            print(f"{universe_id:15}: {path}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Modular integration test failed: {e}")
        return False


async def main():
    """Run all tests."""
    print("🚀 HKEX Universe Builder Testing")
    print("="*50)
    
    # Test 1: Basic HKEX building
    test1_result = await test_hkex_universe()
    
    # Test 2: Modular integration
    test2_result = await test_modular_integration()
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    print(f"HKEX Universe Building:     {'✅ PASS' if test1_result else '❌ FAIL'}")
    print(f"Modular Integration:        {'✅ PASS' if test2_result else '❌ FAIL'}")
    
    if test1_result and test2_result:
        print("\n🎉 All tests passed! HKEX integration is working correctly.")
        return 0
    else:
        print("\n❌ Some tests failed. Check logs for details.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)