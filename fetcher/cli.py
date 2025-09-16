"""
FinSight Data Fetcher CLI

Typer-based command-line interface for the financial data fetching pipeline
with separate commands for backfill, daily updates, and ESG import.
"""

from __future__ import annotations

import asyncio
import csv
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from fetcher.config import (
    DataFetcherSettings, LogLevel, OperationMode, ExchangeCode, 
    UniverseSymbol, StorageBackend, settings
)
from fetcher.logging import configure_logging, get_logger, set_correlation_id, Timer
from fetcher.providers.yfinance_provider import YFinanceProvider
from fetcher.storage.local_storage import LocalStorageBackend
from fetcher.storage.azure_storage import AzureBlobStorageBackend
from fetcher.manifest.manager import ManifestManager

# Create Typer app
app = typer.Typer(
    name="fetcher",
    help="FinSight Data Fetcher - Enterprise-grade financial data pipeline",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True  # Show help when no command is provided
)

console = Console()
logger = get_logger(__name__)


def setup_logging(log_level: LogLevel, use_json: bool = False) -> None:
    """Setup logging configuration."""
    configure_logging(
        log_level=log_level,
        use_json=use_json,
        use_rich=not use_json
    )


def create_storage_backend(backend_type: StorageBackend) -> LocalStorageBackend | AzureBlobStorageBackend:
    """Create appropriate storage backend."""
    if backend_type == StorageBackend.LOCAL:
        return LocalStorageBackend(settings.out_root)
    elif backend_type == StorageBackend.AZURE_BLOB:
        if not settings.azure_storage_account:
            raise typer.BadParameter("Azure storage account must be specified for azure-blob backend")
        
        from azure.identity import DefaultAzureCredential
        return AzureBlobStorageBackend(
            storage_account=settings.azure_storage_account,
            container_name=settings.azure_container_name,
            credential=DefaultAzureCredential()
        )
    else:
        raise typer.BadParameter(f"Unsupported storage backend: {backend_type}")


async def load_universe(universe_path: Path) -> List[UniverseSymbol]:
    """Load universe symbols from CSV file."""
    with Timer(logger, "load_universe", universe_path=str(universe_path)):
        try:
            if not universe_path.exists():
                raise typer.BadParameter(f"Universe file not found: {universe_path}")
            
            symbols = []
            with open(universe_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = UniverseSymbol(
                        ticker=row['ticker'],
                        exchange=ExchangeCode(row['exchange'].lower()),
                        name=row.get('name'),
                        sector=row.get('sector'),
                        market_cap=float(row['market_cap']) if row.get('market_cap') else None,
                        active=row.get('active', 'true').lower() == 'true'
                    )
                    symbols.append(symbol)
            
            logger.info(f"Loaded {len(symbols)} symbols from universe", 
                       active_symbols=len([s for s in symbols if s.active]))
            return symbols
            
        except Exception as e:
            logger.error(f"Failed to load universe: {str(e)}")
            raise typer.Exit(1)


async def fetch_symbol_data(
    provider: YFinanceProvider,
    storage: LocalStorageBackend | AzureBlobStorageBackend,
    manifest: ManifestManager,
    symbol: UniverseSymbol,
    start_date: date,
    end_date: date,
    fetch_actions: bool = False
) -> bool:
    """
    Fetch data for a single symbol.
    
    Returns:
        True if successful, False otherwise
    """
    correlation_id = set_correlation_id()
    
    try:
        with Timer(
            logger, 
            "fetch_symbol_data", 
            ticker=symbol.ticker,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        ):
            # Fetch data from provider
            ohlcv_data, actions_data = await provider.fetch_ohlcv(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                include_actions=fetch_actions
            )
            
            if ohlcv_data is None or ohlcv_data.empty:
                logger.warning("No data fetched", ticker=symbol.ticker)
                manifest.update_entry(
                    symbol=symbol,
                    error_message="No data available"
                )
                return False
            
            # Write OHLCV data to storage
            await storage.write_ohlcv_data(
                data=ohlcv_data,
                symbol=symbol,
                append=True
            )
            
            # Write actions data if available
            if actions_data is not None and not actions_data.empty:
                await storage.write_actions_data(
                    data=actions_data,
                    symbol=symbol,
                    append=True
                )
            
            # Update manifest
            last_date = ohlcv_data.index.max().strftime('%Y-%m-%d')
            manifest.update_entry(
                symbol=symbol,
                last_date=last_date,
                total_records=len(ohlcv_data)
            )
            
            logger.info(
                "Successfully processed symbol",
                ticker=symbol.ticker,
                records=len(ohlcv_data),
                last_date=last_date
            )
            
            return True
            
    except Exception as e:
        logger.error(
            f"Failed to fetch data for {symbol.ticker}: {str(e)}",
            ticker=symbol.ticker,
            error=str(e)
        )
        
        manifest.update_entry(
            symbol=symbol,
            error_message=str(e)
        )
        
        return False


@app.command()
def backfill(
    universe: Annotated[Path, typer.Option(
        "--universe", "-u", 
        help="Path to universe CSV file"
    )] = Path("meta/universe_v1.csv"),
    
    out_root: Annotated[Path, typer.Option(
        "--out-root", "-o",
        help="Root directory for data output"
    )] = Path("./data"),
    
    manifest: Annotated[Path, typer.Option(
        "--manifest", "-m",
        help="Path to symbols manifest file"
    )] = Path("meta/symbols_manifest.csv"),
    
    start: Annotated[str, typer.Option(
        "--start", "-s",
        help="Start date (YYYY-MM-DD)"
    )] = "2014-01-01",
    
    end: Annotated[str, typer.Option(
        "--end", "-e",
        help="End date (YYYY-MM-DD)"
    )] = "2024-12-31",
    
    max_workers: Annotated[int, typer.Option(
        "--max-workers", "-w",
        help="Maximum number of concurrent workers",
        min=1, max=20
    )] = 10,
    
    fetch_actions: Annotated[bool, typer.Option(
        "--fetch-actions/--no-actions",
        help="Fetch dividend and split actions"
    )] = False,
    
    log_level: Annotated[LogLevel, typer.Option(
        "--log-level", "-l",
        help="Logging level"
    )] = LogLevel.INFO,
    
    storage_backend: Annotated[str, typer.Option(
        "--storage", 
        help="Storage backend (local or azure-blob)"
    )] = "local",
    
    azure_account: Annotated[Optional[str], typer.Option(
        "--azure-account",
        help="Azure Storage Account (required for azure-blob backend)"
    )] = None,
    
    azure_container: Annotated[str, typer.Option(
        "--azure-container",
        help="Azure Blob Container name"
    )] = "finsight-data"
):
    """
    Perform historical backfill for all symbols in the universe.
    
    Fetches historical OHLCV data for the specified date range with
    resumable operations and comprehensive error handling.
    """
    # Setup
    setup_logging(log_level)
    set_correlation_id()
    
    # Convert string to enum
    storage_backend_enum = StorageBackend(storage_backend)
    
    logger.info(
        "Starting historical backfill",
        universe_path=str(universe),
        out_root=str(out_root),
        start_date=start,
        end_date=end,
        max_workers=max_workers,
        storage_backend=storage_backend_enum.value
    )
    
    # Override settings
    settings.out_root = out_root
    settings.max_workers = max_workers
    settings.fetch_actions = fetch_actions
    settings.storage_backend = storage_backend_enum
    if azure_account:
        settings.azure_storage_account = azure_account
    settings.azure_container_name = azure_container
    
    asyncio.run(_run_backfill(
        universe_path=universe,
        manifest_path=manifest,
        start_date=datetime.strptime(start, "%Y-%m-%d").date(),
        end_date=datetime.strptime(end, "%Y-%m-%d").date(),
        max_workers=max_workers,
        fetch_actions=fetch_actions,
        storage_backend=storage_backend_enum
    ))


@app.command()
def update_daily(
    universe: Annotated[Path, typer.Option(
        "--universe", "-u", 
        help="Path to universe CSV file"
    )] = Path("meta/universe_v1.csv"),
    
    out_root: Annotated[Path, typer.Option(
        "--out-root", "-o",
        help="Root directory for data output"
    )] = Path("./data"),
    
    manifest: Annotated[Path, typer.Option(
        "--manifest", "-m",
        help="Path to symbols manifest file"
    )] = Path("meta/symbols_manifest.csv"),
    
    max_workers: Annotated[int, typer.Option(
        "--max-workers", "-w",
        help="Maximum number of concurrent workers",
        min=1, max=20
    )] = 5,
    
    fetch_actions: Annotated[bool, typer.Option(
        "--fetch-actions/--no-actions",
        help="Fetch dividend and split actions"
    )] = False,
    
    log_level: Annotated[LogLevel, typer.Option(
        "--log-level", "-l",
        help="Logging level"
    )] = LogLevel.INFO,
    
    storage_backend: Annotated[str, typer.Option(
        "--storage", 
        help="Storage backend (local or azure-blob)"
    )] = "local",
    
    azure_account: Annotated[Optional[str], typer.Option(
        "--azure-account",
        help="Azure Storage Account (required for azure-blob backend)"
    )] = None,
    
    azure_container: Annotated[str, typer.Option(
        "--azure-container",
        help="Azure Blob Container name"
    )] = "finsight-data",
    
    target_date: Annotated[Optional[str], typer.Option(
        "--target-date",
        help="Target date for update (YYYY-MM-DD), defaults to T-1"
    )] = None
):
    """
    Perform daily incremental updates for symbols with completed backfill.
    
    Fetches incremental data from the last available date to the target date
    (defaults to T-1 business day).
    """
    # Setup
    setup_logging(log_level)
    set_correlation_id()
    
    # Convert string to enum
    storage_backend_enum = StorageBackend(storage_backend)
    
    # Determine target date (T-1 by default)
    if target_date:
        end_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        # Use previous business day
        today = datetime.now().date()
        if today.weekday() == 0:  # Monday
            end_date = today - timedelta(days=3)  # Previous Friday
        else:
            end_date = today - timedelta(days=1)  # Previous day
    
    logger.info(
        "Starting daily incremental update",
        universe_path=str(universe),
        out_root=str(out_root),
        target_date=end_date.isoformat(),
        max_workers=max_workers,
        storage_backend=storage_backend_enum.value
    )
    
    # Override settings
    settings.out_root = out_root
    settings.max_workers = max_workers
    settings.fetch_actions = fetch_actions
    settings.storage_backend = storage_backend_enum
    if azure_account:
        settings.azure_storage_account = azure_account
    settings.azure_container_name = azure_container
    
    asyncio.run(_run_daily_update(
        universe_path=universe,
        manifest_path=manifest,
        end_date=end_date,
        max_workers=max_workers,
        fetch_actions=fetch_actions,
        storage_backend=storage_backend_enum
    ))


@app.command()
def import_esg(
    esg_data_path: Annotated[Path, typer.Option(
        "--esg-data-path",
        help="Path to ESG data CSV file"
    )],
    
    out_root: Annotated[Path, typer.Option(
        "--out-root", "-o",
        help="Root directory for data output"
    )] = Path("./data"),
    
    manifest: Annotated[Path, typer.Option(
        "--manifest", "-m",
        help="Path to symbols manifest file"
    )] = Path("meta/symbols_manifest.csv"),
    
    log_level: Annotated[LogLevel, typer.Option(
        "--log-level", "-l",
        help="Logging level"
    )] = LogLevel.INFO
):
    """
    Import ESG data from professor-provided CSV file.
    
    [FUTURE IMPLEMENTATION]
    This command will be implemented when ESG data format is defined.
    """
    setup_logging(log_level)
    
    console.print(
        "[yellow]ESG data import functionality is reserved for future implementation.[/yellow]\n"
        "[blue]This will be developed once the ESG data format is defined by the professor.[/blue]"
    )
    
    logger.info(
        "ESG import placeholder executed",
        esg_data_path=str(esg_data_path),
        out_root=str(out_root)
    )


@app.command()
def status(
    manifest: Annotated[Path, typer.Option(
        "--manifest", "-m",
        help="Path to symbols manifest file"
    )] = Path("meta/symbols_manifest.csv"),
    
    storage_backend: Annotated[str, typer.Option(
        "--storage", 
        help="Storage backend (local or azure-blob)"
    )] = "local",
    
    out_root: Annotated[Path, typer.Option(
        "--out-root", "-o",
        help="Root directory for data output"
    )] = Path("./data"),
    
    azure_account: Annotated[Optional[str], typer.Option(
        "--azure-account",
        help="Azure Storage Account (required for azure-blob backend)"
    )] = None,
    
    azure_container: Annotated[str, typer.Option(
        "--azure-container",
        help="Azure Blob Container name"
    )] = "finsight-data"
):
    """Display status information about the data fetcher."""
    setup_logging(LogLevel.INFO)
    
    # Convert string to enum
    storage_backend_enum = StorageBackend(storage_backend)
    
    # Override settings
    settings.out_root = out_root
    settings.storage_backend = storage_backend_enum
    if azure_account:
        settings.azure_storage_account = azure_account
    settings.azure_container_name = azure_container
    
    asyncio.run(_show_status(manifest, storage_backend_enum))


async def _run_backfill(
    universe_path: Path,
    manifest_path: Path,
    start_date: date,
    end_date: date,
    max_workers: int,
    fetch_actions: bool,
    storage_backend: StorageBackend
) -> None:
    """Execute historical backfill operation."""
    try:
        # Initialize components
        universe_symbols = await load_universe(universe_path)
        provider = YFinanceProvider(
            rate_limit_delay=settings.rate_limit_delay,
            max_retries=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor
        )
        storage = create_storage_backend(storage_backend)
        manifest_manager = ManifestManager(manifest_path)
        
        # Load existing manifest
        await manifest_manager.load_manifest()
        
        # Get symbols that need backfill
        symbols_for_backfill = manifest_manager.get_symbols_for_backfill(universe_symbols)
        
        if not symbols_for_backfill:
            console.print("[green]All symbols have completed backfill![/green]")
            return
        
        console.print(f"[blue]Starting backfill for {len(symbols_for_backfill)} symbols[/blue]")
        
        # Process symbols with progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Processing symbols...", total=len(symbols_for_backfill))
            
            # Use semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(max_workers)
            
            async def process_symbol(symbol: UniverseSymbol):
                async with semaphore:
                    success = await fetch_symbol_data(
                        provider=provider,
                        storage=storage,
                        manifest=manifest_manager,
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        fetch_actions=fetch_actions
                    )
                    
                    if success:
                        # Mark backfill as complete
                        manifest_manager.update_entry(
                            symbol=symbol,
                            backfill_complete=True
                        )
                    
                    progress.advance(task)
            
            # Process all symbols concurrently
            await asyncio.gather(*[
                process_symbol(symbol) for symbol in symbols_for_backfill
            ], return_exceptions=True)
        
        # Save updated manifest
        await manifest_manager.save_manifest()
        
        # Show summary
        summary = manifest_manager.get_manifest_summary()
        console.print(f"\n[green]Backfill completed![/green]")
        console.print(f"Symbols processed: {len(symbols_for_backfill)}")
        console.print(f"Backfill complete: {summary['backfill_complete']}/{summary['total_symbols']}")
        console.print(f"Total records: {summary['total_records']:,}")
        
    except Exception as e:
        logger.error(f"Backfill failed: {str(e)}")
        raise typer.Exit(1)


async def _run_daily_update(
    universe_path: Path,
    manifest_path: Path,
    end_date: date,
    max_workers: int,
    fetch_actions: bool,
    storage_backend: StorageBackend
) -> None:
    """Execute daily incremental update operation."""
    try:
        # Initialize components
        universe_symbols = await load_universe(universe_path)
        provider = YFinanceProvider(
            rate_limit_delay=settings.rate_limit_delay,
            max_retries=settings.max_retries,
            backoff_factor=settings.retry_backoff_factor
        )
        storage = create_storage_backend(storage_backend)
        manifest_manager = ManifestManager(manifest_path)
        
        # Load existing manifest
        await manifest_manager.load_manifest()
        
        # Get symbols that need updates
        symbols_for_update = manifest_manager.get_symbols_for_update(universe_symbols)
        
        if not symbols_for_update:
            console.print("[green]No symbols need daily updates![/green]")
            return
        
        console.print(f"[blue]Starting daily update for {len(symbols_for_update)} symbols[/blue]")
        
        # Process symbols with progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Updating symbols...", total=len(symbols_for_update))
            
            # Use semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(max_workers)
            
            async def update_symbol(symbol: UniverseSymbol):
                async with semaphore:
                    # Determine start date for incremental update
                    last_date = manifest_manager.get_last_date(symbol)
                    if last_date:
                        start_date = (last_date + timedelta(days=1)).date()
                    else:
                        # Fallback to recent data if no history
                        start_date = end_date - timedelta(days=30)
                    
                    if start_date <= end_date:
                        success = await fetch_symbol_data(
                            provider=provider,
                            storage=storage,
                            manifest=manifest_manager,
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            fetch_actions=fetch_actions
                        )
                    else:
                        logger.debug(f"No update needed for {symbol.ticker}, data is current")
                    
                    progress.advance(task)
            
            # Process all symbols concurrently
            await asyncio.gather(*[
                update_symbol(symbol) for symbol in symbols_for_update
            ], return_exceptions=True)
        
        # Save updated manifest
        await manifest_manager.save_manifest()
        
        # Show summary
        summary = manifest_manager.get_manifest_summary()
        console.print(f"\n[green]Daily update completed![/green]")
        console.print(f"Symbols updated: {len(symbols_for_update)}")
        console.print(f"Total records: {summary['total_records']:,}")
        
    except Exception as e:
        logger.error(f"Daily update failed: {str(e)}")
        raise typer.Exit(1)


async def _show_status(manifest_path: Path, storage_backend: StorageBackend) -> None:
    """Show status information."""
    try:
        # Load manifest
        manifest_manager = ManifestManager(manifest_path)
        await manifest_manager.load_manifest()
        
        # Get manifest summary
        summary = manifest_manager.get_manifest_summary()
        
        # Get storage info
        storage = create_storage_backend(storage_backend)
        storage_info = await storage.get_storage_info()
        
        # Display status table
        table = Table(title="FinSight Data Fetcher Status")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Symbols", str(summary['total_symbols']))
        table.add_row("Backfill Complete", f"{summary['backfill_complete']}/{summary['total_symbols']}")
        table.add_row("Symbols with Data", str(summary['symbols_with_data']))
        table.add_row("Symbols with Errors", str(summary['symbols_with_errors']))
        table.add_row("Total Records", f"{summary['total_records']:,}")
        
        if summary['last_updated']:
            table.add_row("Last Updated", summary['last_updated'])
        
        table.add_row("Storage Backend", storage_info['backend'])
        if 'total_size_mb' in storage_info:
            table.add_row("Storage Size", f"{storage_info['total_size_mb']:.2f} MB")
        if 'file_count' in storage_info:
            table.add_row("File Count", str(storage_info['file_count']))
        
        console.print(table)
        
        # Show symbols by exchange
        symbols_by_exchange = manifest_manager.get_symbols_by_exchange()
        if symbols_by_exchange:
            console.print("\n[bold]Symbols by Exchange:[/bold]")
            for exchange, tickers in symbols_by_exchange.items():
                console.print(f"  {exchange.upper()}: {len(tickers)} symbols")
        
    except Exception as e:
        logger.error(f"Failed to show status: {str(e)}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()