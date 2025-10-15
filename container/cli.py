"""
Container Apps CLI Entry Point
Uses the SAME shared core logic from src/ module as Azure Functions
Provides CLI commands for backfill and other heavy operations
"""

import click
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import src module
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.data_operations import DataOperations
from src.storage.azure_storage import AzureBlobStorage
from src.storage.local_storage import LocalStorage

# Configure logging
logging.basicConfig(
    level=os.getenv("FETCHER_LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _get_storage(use_azure: bool = True):
    """Helper to initialize storage based on environment"""
    if use_azure:
        return AzureBlobStorage(
            account_name=os.getenv("FETCHER_AZURE_STORAGE_ACCOUNT", "stfinsightdata"),
            container_name=os.getenv("FETCHER_AZURE_CONTAINER_NAME", "finsight-data"),
            use_managed_identity=os.getenv("USE_MANAGED_IDENTITY", "true").lower() == "true"
        )
    else:
        return LocalStorage(
            root_path=os.getenv("LOCAL_STORAGE_ROOT", "./data")
        )


@click.group()
@click.version_option(version="2.0.0")
def cli():
    """
    QuantX Data Builder CLI
    
    Enterprise-grade financial data pipeline for stock market data.
    Supports multiple markets (US, HK, JP, EU) with historical backfill
    and incremental updates.
    """
    pass


@cli.command()
@click.option('--start', required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end', required=True, help='End date (YYYY-MM-DD)')
@click.option('--universe', default='meta/universe_phase_1.csv', help='Universe CSV file path')
@click.option('--phase', help='Universe phase (e.g., phase_1) - builds from config if universe file not found')
@click.option('--max-workers', default=20, type=int, help='Max concurrent workers')
@click.option('--chunk-size', default=100, type=int, help='Symbols per chunk')
@click.option('--use-local', is_flag=True, help='Use local storage instead of Azure')
def backfill(start, end, universe, phase, max_workers, chunk_size, use_local):
    """
    Run historical backfill operation using shared core logic
    
    This is the SAME logic as Azure Functions, just different entry point.
    Optimized for long-running operations with high parallelism.
    
    Example:
        python cli.py backfill --start 2020-01-01 --end 2024-12-31 --max-workers 20
    """
    logger.info(f"🚀 Starting backfill: {start} to {end}")
    logger.info(f"⚙️  Config: max_workers={max_workers}, chunk_size={chunk_size}")

    try:
        storage = _get_storage(use_azure=not use_local)
        ops = DataOperations(storage, max_workers=max_workers)

        # Load or build universe
        try:
            symbols = storage.load_universe(universe)
            logger.info(f"📊 Loaded {len(symbols)} symbols from {universe}")
        except Exception as e:
            if phase:
                logger.info(f"Universe file not found. Building from config phase: {phase}")
                symbols = ops.refresh_universe(phase=phase)
            else:
                raise ValueError(
                    f"Universe file not found: {universe}. "
                    "Provide --phase to build from config."
                )

        # Execute backfill using SHARED logic
        results = ops.fetch_backfill(
            symbols=symbols,
            start_date=start,
            end_date=end,
            chunk_size=chunk_size
        )

        logger.info(
            f"✅ Backfill complete: "
            f"{results['success']} succeeded, "
            f"{results['failed']} failed, "
            f"{results.get('skipped', 0)} skipped"
        )

        # Save report
        report = {
            "completed_at": datetime.utcnow().isoformat(),
            "operation": "backfill",
            "start_date": start,
            "end_date": end,
            "universe": universe,
            "total_symbols": len(symbols),
            "results": results,
            "config": {
                "max_workers": max_workers,
                "chunk_size": chunk_size
            }
        }
        storage.save_metadata(
            f"reports/backfill_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            report
        )
        logger.info("📄 Saved execution report")

        # Exit with error code if too many failures
        failure_rate = results['failed'] / len(symbols) if symbols else 0
        if failure_rate > 0.1:  # More than 10% failed
            logger.error(f"⚠️  High failure rate: {failure_rate:.1%}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Backfill failed: {str(e)}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option('--universe', default='meta/universe_phase_1.csv', help='Universe CSV file path')
@click.option('--phase', help='Universe phase - builds from config if universe file not found')
@click.option('--lookback-days', default=5, type=int, help='Days to look back')
@click.option('--max-workers', default=10, type=int, help='Max concurrent workers')
@click.option('--use-local', is_flag=True, help='Use local storage instead of Azure')
def update_daily(universe, phase, lookback_days, max_workers, use_local):
    """
    Run daily incremental update using shared core logic
    
    Fetches recent data (last N days) for all symbols in universe.
    Typically runs as a scheduled job for T-1 data updates.
    
    Example:
        python cli.py update-daily --lookback-days 5
    """
    logger.info(f"📅 Starting daily update (lookback: {lookback_days} days)")

    try:
        storage = _get_storage(use_azure=not use_local)
        ops = DataOperations(storage, max_workers=max_workers)

        # Load or build universe
        try:
            symbols = storage.load_universe(universe)
            logger.info(f"📊 Loaded {len(symbols)} symbols from {universe}")
        except Exception as e:
            if phase:
                logger.info(f"Universe file not found. Building from config phase: {phase}")
                symbols = ops.refresh_universe(phase=phase)
            else:
                raise ValueError(
                    f"Universe file not found: {universe}. "
                    "Provide --phase to build from config."
                )

        # Execute update using SHARED logic
        results = ops.fetch_daily_incremental(symbols, lookback_days=lookback_days)

        logger.info(
            f"✅ Daily update complete: "
            f"{results['success']} succeeded, "
            f"{results['failed']} failed, "
            f"{results.get('skipped', 0)} skipped"
        )

        # Save report
        report = {
            "completed_at": datetime.utcnow().isoformat(),
            "operation": "daily_update",
            "lookback_days": lookback_days,
            "universe": universe,
            "total_symbols": len(symbols),
            "results": results
        }
        storage.save_metadata(
            f"reports/daily_update_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            report
        )

    except Exception as e:
        logger.error(f"❌ Daily update failed: {str(e)}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option('--phase', default='phase_1', help='Universe phase (e.g., phase_1)')
@click.option('--output', default='meta', help='Output directory for universe files')
@click.option('--use-local', is_flag=True, help='Use local storage instead of Azure')
def refresh_universe(phase, output, use_local):
    """
    Refresh universe using shared core logic
    
    Downloads latest stock lists from configured sources and saves to storage.
    
    Example:
        python cli.py refresh-universe --phase phase_1
    """
    logger.info(f"🔄 Refreshing universe: {phase}")

    try:
        storage = _get_storage(use_azure=not use_local)
        ops = DataOperations(storage)

        # Use SAME refresh logic as Functions
        symbols = ops.refresh_universe(phase=phase)

        logger.info(f"✅ Universe refreshed: {len(symbols)} total symbols")

    except Exception as e:
        logger.error(f"❌ Universe refresh failed: {str(e)}", exc_info=True)
        sys.exit(1)


@cli.command()
@click.option('--manifest', default='meta/symbols_manifest.csv', help='Manifest file path')
@click.option('--use-local', is_flag=True, help='Use local storage instead of Azure')
def show_manifest(manifest, use_local):
    """
    Display symbols manifest with last update information
    
    Shows tracking information for all symbols including last fetch date
    and backfill completion status.
    """
    try:
        storage = _get_storage(use_azure=not use_local)
        
        if storage.exists(manifest):
            df = storage.load_dataframe(manifest)
            
            logger.info(f"\n📋 Symbols Manifest ({len(df)} symbols)")
            logger.info("=" * 80)
            print(df.to_string())
            logger.info("=" * 80)
            
            # Summary statistics
            completed = df['backfill_complete'].sum() if 'backfill_complete' in df.columns else 0
            logger.info(f"\n✅ Backfill complete: {completed}/{len(df)}")
        else:
            logger.warning(f"Manifest not found: {manifest}")

    except Exception as e:
        logger.error(f"❌ Failed to load manifest: {str(e)}", exc_info=True)
        sys.exit(1)


@cli.command()
def version():
    """Display version information"""
    click.echo("QuantX Data Builder CLI v2.0.0")
    click.echo("Shared Core Architecture - Azure Functions + Container Apps")


if __name__ == '__main__':
    cli()
