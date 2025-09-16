"""
Local Filesystem Storage Backend

Implementation of StorageBackend for local filesystem storage with
partitioned Parquet files and append capabilities.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

from . import StorageBackend, StorageError, deduplicate_data, partition_data_by_year, get_parquet_engine
from ..config import ExchangeCode, UniverseSymbol, settings
from ..logging import get_logger, Timer

logger = get_logger(__name__)


class LocalStorageBackend(StorageBackend):
    """
    Local filesystem implementation of StorageBackend.
    
    Stores data in partitioned Parquet files on the local filesystem
    with the directory structure: /data/exchange=<us|hk>/ticker=<SYMBOL>/year=YYYY/
    """
    
    def __init__(self, root_path: Path):
        """
        Initialize local storage backend.
        
        Args:
            root_path: Root directory for data storage
        """
        self.root_path = Path(root_path).resolve()
        self.engine = get_parquet_engine()
        
        # Create root directory if it doesn't exist
        self.root_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "Initialized local storage backend",
            root_path=str(self.root_path),
            parquet_engine=self.engine
        )
    
    def _get_ohlcv_partition_path(
        self, 
        symbol: UniverseSymbol, 
        year: int
    ) -> Path:
        """Get the partition path for OHLCV data."""
        return self.root_path / f"exchange={symbol.exchange.value}" / f"ticker={symbol.ticker}" / f"year={year}"
    
    def _get_actions_path(self, symbol: UniverseSymbol) -> Path:
        """Get the path for actions data."""
        return self.root_path / "actions" / f"exchange={symbol.exchange.value}" / f"ticker={symbol.ticker}"
    
    def _get_parquet_file_path(self, partition_path: Path, prefix: str = "part") -> Path:
        """Get the Parquet file path within a partition."""
        return partition_path / f"{prefix}-000.parquet"
    
    async def write_ohlcv_data(
        self,
        data: pd.DataFrame,
        symbol: UniverseSymbol,
        append: bool = True
    ) -> None:
        """
        Write OHLCV data with partitioning by year.
        
        Args:
            data: OHLCV data to write
            symbol: Symbol metadata for partitioning
            append: Whether to append to existing data or overwrite
        """
        if data.empty:
            logger.warning("Attempted to write empty data", ticker=symbol.ticker)
            return
        
        with Timer(
            logger,
            "write_ohlcv_data",
            ticker=symbol.ticker,
            records=len(data),
            append=append
        ):
            try:
                # Partition data by year
                year_partitions = partition_data_by_year(data)
                
                for year, year_data in year_partitions.items():
                    partition_path = self._get_ohlcv_partition_path(symbol, year)
                    file_path = self._get_parquet_file_path(partition_path)
                    
                    # Create partition directory
                    partition_path.mkdir(parents=True, exist_ok=True)
                    
                    if append and file_path.exists():
                        # Read existing data and merge
                        existing_data = pd.read_parquet(
                            file_path,
                            engine=self.engine
                        )
                        
                        # Ensure Date is the index
                        if 'Date' in existing_data.columns:
                            existing_data = existing_data.set_index('Date')
                        
                        # Deduplicate and combine
                        combined_data = deduplicate_data(existing_data, year_data)
                    else:
                        combined_data = year_data
                    
                    # Write to Parquet with compression
                    combined_data.to_parquet(
                        file_path,
                        engine=self.engine,
                        compression=settings.parquet_compression,
                        index=True  # Keep Date as index
                    )
                    
                    logger.debug(
                        "Wrote partition",
                        ticker=symbol.ticker,
                        year=year,
                        records=len(combined_data),
                        file_path=str(file_path)
                    )
                
                logger.info(
                    "Successfully wrote OHLCV data",
                    ticker=symbol.ticker,
                    total_records=len(data),
                    partitions=len(year_partitions)
                )
                
            except Exception as e:
                error_msg = f"Failed to write OHLCV data for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "local", "write_ohlcv_data")
    
    async def read_ohlcv_data(
        self,
        symbol: UniverseSymbol,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:
        """
        Read OHLCV data for a symbol with optional date filtering.
        
        Args:
            symbol: Symbol to read data for
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            OHLCV data or None if not found
        """
        with Timer(
            logger,
            "read_ohlcv_data",
            ticker=symbol.ticker,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None
        ):
            try:
                data_frames = []
                
                # Determine which years to read
                years_to_read = []
                if start_date and end_date:
                    years_to_read = list(range(start_date.year, end_date.year + 1))
                else:
                    # Read all available years
                    symbol_base_path = self.root_path / f"exchange={symbol.exchange.value}" / f"ticker={symbol.ticker}"
                    if symbol_base_path.exists():
                        for year_dir in symbol_base_path.iterdir():
                            if year_dir.is_dir() and year_dir.name.startswith("year="):
                                try:
                                    year = int(year_dir.name.split("=")[1])
                                    years_to_read.append(year)
                                except ValueError:
                                    continue
                
                # Read data from each year partition
                for year in sorted(years_to_read):
                    partition_path = self._get_ohlcv_partition_path(symbol, year)
                    file_path = self._get_parquet_file_path(partition_path)
                    
                    if file_path.exists():
                        year_data = pd.read_parquet(file_path, engine=self.engine)
                        
                        # Ensure Date is the index
                        if 'Date' in year_data.columns:
                            year_data = year_data.set_index('Date')
                        
                        data_frames.append(year_data)
                
                if not data_frames:
                    logger.debug("No data found", ticker=symbol.ticker)
                    return None
                
                # Combine all data
                combined_data = pd.concat(data_frames, axis=0)
                combined_data = combined_data.sort_index()
                
                # Apply date filtering if specified
                if start_date:
                    combined_data = combined_data[combined_data.index >= start_date]
                if end_date:
                    combined_data = combined_data[combined_data.index <= end_date]
                
                logger.debug(
                    "Read OHLCV data",
                    ticker=symbol.ticker,
                    records=len(combined_data),
                    date_range=f"{combined_data.index.min()} to {combined_data.index.max()}"
                )
                
                return combined_data
                
            except Exception as e:
                error_msg = f"Failed to read OHLCV data for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "local", "read_ohlcv_data")
    
    async def write_actions_data(
        self,
        data: pd.DataFrame,
        symbol: UniverseSymbol,
        append: bool = True
    ) -> None:
        """
        Write dividend/split actions data.
        
        Args:
            data: Actions data to write
            symbol: Symbol metadata
            append: Whether to append to existing data or overwrite
        """
        if data.empty:
            logger.debug("No actions data to write", ticker=symbol.ticker)
            return
        
        with Timer(logger, "write_actions_data", ticker=symbol.ticker, records=len(data)):
            try:
                actions_path = self._get_actions_path(symbol)
                actions_path.mkdir(parents=True, exist_ok=True)
                
                file_path = actions_path / "actions.parquet"
                
                if append and file_path.exists():
                    # Read existing data and merge
                    existing_data = pd.read_parquet(file_path, engine=self.engine)
                    
                    # Combine and deduplicate
                    combined_data = pd.concat([existing_data, data])
                    combined_data = combined_data.drop_duplicates(subset=['Date'], keep='last')
                else:
                    combined_data = data
                
                # Write to Parquet
                combined_data.to_parquet(
                    file_path,
                    engine=self.engine,
                    compression=settings.parquet_compression,
                    index=False
                )
                
                logger.info(
                    "Wrote actions data",
                    ticker=symbol.ticker,
                    records=len(combined_data),
                    file_path=str(file_path)
                )
                
            except Exception as e:
                error_msg = f"Failed to write actions data for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "local", "write_actions_data")
    
    async def get_last_date(self, symbol: UniverseSymbol) -> Optional[datetime]:
        """
        Get the last available date for a symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            Last available date or None if no data
        """
        try:
            data = await self.read_ohlcv_data(symbol)
            if data is not None and not data.empty:
                return data.index.max().to_pydatetime()
            return None
            
        except Exception as e:
            logger.debug(
                f"Failed to get last date for {symbol.ticker}: {str(e)}",
                ticker=symbol.ticker,
                error=str(e)
            )
            return None
    
    async def list_symbols(self, exchange: Optional[ExchangeCode] = None) -> List[str]:
        """
        List all symbols with data in storage.
        
        Args:
            exchange: Optional exchange filter
            
        Returns:
            List of symbol tickers
        """
        symbols = []
        
        try:
            if exchange:
                # List symbols for specific exchange
                exchange_path = self.root_path / f"exchange={exchange.value}"
                if exchange_path.exists():
                    for ticker_dir in exchange_path.iterdir():
                        if ticker_dir.is_dir() and ticker_dir.name.startswith("ticker="):
                            ticker = ticker_dir.name.split("=")[1]
                            symbols.append(ticker)
            else:
                # List symbols for all exchanges
                for exchange_dir in self.root_path.iterdir():
                    if exchange_dir.is_dir() and exchange_dir.name.startswith("exchange="):
                        for ticker_dir in exchange_dir.iterdir():
                            if ticker_dir.is_dir() and ticker_dir.name.startswith("ticker="):
                                ticker = ticker_dir.name.split("=")[1]
                                symbols.append(ticker)
            
            return sorted(list(set(symbols)))
            
        except Exception as e:
            logger.error(f"Failed to list symbols: {str(e)}", error=str(e))
            return []
    
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Get information about storage usage and configuration.
        
        Returns:
            Storage metadata
        """
        try:
            # Calculate storage usage
            total_size = 0
            file_count = 0
            
            for root, dirs, files in os.walk(self.root_path):
                for file in files:
                    file_path = Path(root) / file
                    if file_path.suffix == '.parquet':
                        total_size += file_path.stat().st_size
                        file_count += 1
            
            # Get available space
            statvfs = os.statvfs(self.root_path)
            available_space = statvfs.f_frsize * statvfs.f_bavail
            
            return {
                "backend": "local",
                "root_path": str(self.root_path),
                "parquet_engine": self.engine,
                "compression": settings.parquet_compression,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "file_count": file_count,
                "available_space_bytes": available_space,
                "available_space_gb": round(available_space / (1024 * 1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage info: {str(e)}", error=str(e))
            return {
                "backend": "local",
                "root_path": str(self.root_path),
                "error": str(e)
            }