"""
FinSight Data Fetcher Storage Module

Abstraction layer for data storage with support for local filesystem
and Azure Blob Storage with partitioned Parquet files.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd

from ..config import ExchangeCode, UniverseSymbol
from ..logging import get_logger

logger = get_logger(__name__)


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    Defines the interface for storing and retrieving partitioned data
    with support for append operations and deduplication.
    """
    
    @abstractmethod
    async def write_ohlcv_data(
        self,
        data: pd.DataFrame,
        symbol: UniverseSymbol,
        append: bool = True
    ) -> None:
        """
        Write OHLCV data with proper partitioning.
        
        Args:
            data: OHLCV data to write
            symbol: Symbol metadata for partitioning
            append: Whether to append to existing data or overwrite
        """
        pass
    
    @abstractmethod
    async def read_ohlcv_data(
        self,
        symbol: UniverseSymbol,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:
        """
        Read OHLCV data for a symbol within date range.
        
        Args:
            symbol: Symbol to read data for
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            OHLCV data or None if not found
        """
        pass
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    async def get_last_date(self, symbol: UniverseSymbol) -> Optional[datetime]:
        """
        Get the last available date for a symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            Last available date or None if no data
        """
        pass
    
    @abstractmethod
    async def list_symbols(self, exchange: Optional[ExchangeCode] = None) -> List[str]:
        """
        List all symbols with data in storage.
        
        Args:
            exchange: Optional exchange filter
            
        Returns:
            List of symbol tickers
        """
        pass
    
    @abstractmethod
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Get information about storage usage and configuration.
        
        Returns:
            Storage metadata
        """
        pass


class StorageError(Exception):
    """Exception raised when storage operations fail."""
    
    def __init__(self, message: str, backend: str, operation: Optional[str] = None):
        self.message = message
        self.backend = backend
        self.operation = operation
        super().__init__(f"[{backend}] {message}" + (f" during {operation}" if operation else ""))


def deduplicate_data(
    existing_data: pd.DataFrame,
    new_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Deduplicate data by merging existing and new data.
    
    Args:
        existing_data: Existing data with Date index
        new_data: New data with Date index
        
    Returns:
        Deduplicated combined data
    """
    if existing_data.empty:
        return new_data
    
    if new_data.empty:
        return existing_data
    
    # Combine and deduplicate by index (Date)
    combined = pd.concat([existing_data, new_data])
    
    # Remove duplicates, keeping the last occurrence (newest data)
    deduplicated = combined[~combined.index.duplicated(keep='last')]
    
    # Sort by date
    deduplicated = deduplicated.sort_index()
    
    logger.debug(
        "Deduplicated data",
        existing_records=len(existing_data),
        new_records=len(new_data),
        final_records=len(deduplicated)
    )
    
    return deduplicated


def partition_data_by_year(data: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """
    Partition data by year for efficient storage.
    
    Args:
        data: Data with Date index
        
    Returns:
        Dictionary mapping year to data
    """
    if data.empty:
        return {}
    
    # Group by year
    partitions = {}
    for year, year_data in data.groupby(data.index.year):
        partitions[year] = year_data
    
    logger.debug(
        "Partitioned data by year",
        total_records=len(data),
        years=list(partitions.keys()),
        date_range=f"{data.index.min()} to {data.index.max()}"
    )
    
    return partitions


def get_parquet_engine() -> str:
    """
    Get the best available Parquet engine.
    
    Returns:
        Parquet engine name
    """
    try:
        import pyarrow
        return "pyarrow"
    except ImportError:
        logger.warning("PyArrow not available, falling back to fastparquet")
        return "fastparquet"