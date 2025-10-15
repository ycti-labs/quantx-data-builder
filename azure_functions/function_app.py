"""
Azure Functions Entry Point
Uses shared core logic from src/ module
Provides timer-triggered functions for scheduled data updates
"""

import azure.functions as func
import logging
import os
import sys
from datetime import datetime
import json

# Add parent directory to path to import src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.data_operations import DataOperations
from src.storage.azure_storage import AzureBlobStorage

app = func.FunctionApp()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _get_storage():
    """Helper to initialize storage with managed identity"""
    return AzureBlobStorage(
        account_name=os.getenv("AZURE_STORAGE_ACCOUNT", "stfinsightdata"),
        container_name=os.getenv("AZURE_CONTAINER_NAME", "finsight-data"),
        use_managed_identity=True
    )


@app.schedule(
    schedule="0 6 * * 1-5",  # Weekdays at 6 AM UTC (after Asian markets close)
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def daily_data_update(timer: func.TimerRequest) -> None:
    """
    Daily incremental data fetch using shared core logic
    Runs on weekdays at 6 AM UTC to fetch T-1 data
    """
    logger.info(f"🚀 Daily data update started at {datetime.utcnow()}")

    try:
        storage = _get_storage()
        ops = DataOperations(
            storage,
            max_workers=int(os.getenv("MAX_WORKERS", "10"))
        )

        # Load universe for phase 1
        universe_path = os.getenv("UNIVERSE_PATH", "meta/universe_phase_1.csv")
        
        try:
            symbols = storage.load_universe(universe_path)
            logger.info(f"📊 Loaded {len(symbols)} symbols from {universe_path}")
        except Exception as e:
            logger.warning(f"Failed to load universe from {universe_path}: {e}")
            logger.info("Building universe from config...")
            symbols = ops.refresh_universe(phase="phase_1")

        # Fetch last 5 days (handles weekends and holidays)
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "5"))
        results = ops.fetch_daily_incremental(symbols, lookback_days=lookback_days)

        # Log results
        logger.info(
            f"✅ Daily update complete: "
            f"{results['success']} succeeded, "
            f"{results['failed']} failed, "
            f"{results.get('skipped', 0)} skipped"
        )

        # Save execution report
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "type": "daily_update",
            "symbols_processed": len(symbols),
            "results": results
        }
        storage.save_metadata(
            f"reports/daily_update_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            report
        )

    except Exception as e:
        logger.error(f"❌ Daily update failed: {str(e)}", exc_info=True)
        raise


@app.schedule(
    schedule="0 2 * * 6",  # Saturdays at 2 AM UTC
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def weekly_universe_refresh(timer: func.TimerRequest) -> None:
    """
    Weekly universe refresh using shared core logic
    Runs on Saturdays at 2 AM UTC to update stock lists
    """
    logger.info(f"🔄 Weekly universe refresh started at {datetime.utcnow()}")

    try:
        storage = _get_storage()
        ops = DataOperations(storage)

        # Refresh universe using shared logic
        phase = os.getenv("UNIVERSE_PHASE", "phase_1")
        symbols = ops.refresh_universe(phase=phase)

        logger.info(f"✅ Universe refreshed: {len(symbols)} total symbols in {phase}")

        # Save execution report
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "type": "universe_refresh",
            "phase": phase,
            "total_symbols": len(symbols)
        }
        storage.save_metadata(
            f"reports/universe_refresh_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
            report
        )

    except Exception as e:
        logger.error(f"❌ Universe refresh failed: {str(e)}", exc_info=True)
        raise


@app.route(
    route="health",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint
    Returns service status and version information
    """
    health_data = {
        "status": "healthy",
        "service": "quantx-data-builder-functions",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }

    return func.HttpResponse(
        json.dumps(health_data, indent=2),
        mimetype="application/json",
        status_code=200
    )


@app.route(
    route="trigger-update",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def manual_trigger_update(req: func.HttpRequest) -> func.HttpResponse:
    """
    Manual trigger for data update
    Allows on-demand execution of daily update logic
    
    POST /api/trigger-update
    Body: {
        "lookback_days": 5  // Optional
    }
    """
    logger.info("🔧 Manual trigger received for data update")

    try:
        # Parse request body
        try:
            req_body = req.get_json()
            lookback_days = req_body.get('lookback_days', 5)
        except ValueError:
            lookback_days = 5

        storage = _get_storage()
        ops = DataOperations(
            storage,
            max_workers=int(os.getenv("MAX_WORKERS", "10"))
        )

        # Load universe
        universe_path = os.getenv("UNIVERSE_PATH", "meta/universe_phase_1.csv")
        symbols = storage.load_universe(universe_path)

        # Execute update
        results = ops.fetch_daily_incremental(symbols, lookback_days=lookback_days)

        response_data = {
            "status": "success",
            "symbols_processed": len(symbols),
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        }

        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logger.error(f"❌ Manual trigger failed: {str(e)}", exc_info=True)
        error_data = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_data, indent=2),
            mimetype="application/json",
            status_code=500
        )
