"""
Azure Blob Storage Backend

Implementation of StorageBackend for Azure Blob Storage with
partitioned Parquet files and append capabilities.
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

from . import StorageBackend, StorageError, deduplicate_data, partition_data_by_year, get_parquet_engine
from ..config import ExchangeCode, UniverseSymbol, settings
from ..logging import get_logger, Timer

logger = get_logger(__name__)


class AzureBlobStorageBackend(StorageBackend):
    """
    Azure Blob Storage implementation of StorageBackend.
    
    Stores data in partitioned Parquet files in Azure Blob Storage
    with the directory structure: /data/exchange=<us|hk>/ticker=<SYMBOL>/year=YYYY/
    """
    
    def __init__(
        self,
        storage_account: str,
        container_name: str,
        credential: Optional[DefaultAzureCredential] = None
    ):
        """
        Initialize Azure Blob Storage backend.
        
        Args:
            storage_account: Azure Storage Account name
            container_name: Blob container name
            credential: Azure credential (uses DefaultAzureCredential if None)
        """
        self.storage_account = storage_account
        self.container_name = container_name
        self.credential = credential or DefaultAzureCredential()
        self.engine = get_parquet_engine()
        
        # Initialize blob service client
        account_url = f"https://{storage_account}.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(
            account_url=account_url,
            credential=self.credential
        )
        
        # Ensure container exists
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            container_client.get_container_properties()
        except ResourceNotFoundError:
            logger.info(f"Creating container: {container_name}")
            self.blob_service_client.create_container(container_name)
        
        logger.info(
            "Initialized Azure Blob Storage backend",
            storage_account=storage_account,
            container_name=container_name,
            parquet_engine=self.engine
        )
    
    def _get_ohlcv_blob_path(
        self, 
        symbol: UniverseSymbol, 
        year: int
    ) -> str:
        """Get the blob path for OHLCV data."""
        return f"exchange={symbol.exchange.value}/ticker={symbol.ticker}/year={year}/part-000.parquet"
    
    def _get_actions_blob_path(self, symbol: UniverseSymbol) -> str:
        """Get the blob path for actions data."""
        return f"actions/exchange={symbol.exchange.value}/ticker={symbol.ticker}/actions.parquet"
    
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
            "write_ohlcv_data_azure",
            ticker=symbol.ticker,
            records=len(data),
            append=append
        ):
            try:
                # Partition data by year
                year_partitions = partition_data_by_year(data)
                
                for year, year_data in year_partitions.items():
                    blob_path = self._get_ohlcv_blob_path(symbol, year)
                    
                    if append:
                        # Try to read existing data
                        existing_data = await self._read_parquet_blob(blob_path)
                        if existing_data is not None:
                            # Ensure Date is the index
                            if 'Date' in existing_data.columns:
                                existing_data = existing_data.set_index('Date')
                            
                            # Deduplicate and combine
                            combined_data = deduplicate_data(existing_data, year_data)
                        else:
                            combined_data = year_data
                    else:
                        combined_data = year_data
                    
                    # Write to blob
                    await self._write_parquet_blob(blob_path, combined_data)
                    
                    logger.debug(
                        "Wrote partition to blob",
                        ticker=symbol.ticker,
                        year=year,
                        records=len(combined_data),
                        blob_path=blob_path
                    )
                
                logger.info(
                    "Successfully wrote OHLCV data to Azure Blob",
                    ticker=symbol.ticker,
                    total_records=len(data),
                    partitions=len(year_partitions)
                )
                
            except Exception as e:
                error_msg = f"Failed to write OHLCV data to Azure Blob for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "azure-blob", "write_ohlcv_data")
    
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
            "read_ohlcv_data_azure",
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
                    # List available years by looking for blobs
                    years_to_read = await self._list_available_years(symbol)
                
                # Read data from each year partition
                for year in sorted(years_to_read):
                    blob_path = self._get_ohlcv_blob_path(symbol, year)
                    year_data = await self._read_parquet_blob(blob_path)
                    
                    if year_data is not None:
                        # Ensure Date is the index
                        if 'Date' in year_data.columns:
                            year_data = year_data.set_index('Date')
                        
                        data_frames.append(year_data)
                
                if not data_frames:
                    logger.debug("No data found in Azure Blob", ticker=symbol.ticker)
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
                    "Read OHLCV data from Azure Blob",
                    ticker=symbol.ticker,
                    records=len(combined_data),
                    date_range=f"{combined_data.index.min()} to {combined_data.index.max()}"
                )
                
                return combined_data
                
            except Exception as e:
                error_msg = f"Failed to read OHLCV data from Azure Blob for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "azure-blob", "read_ohlcv_data")
    
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
        
        with Timer(logger, "write_actions_data_azure", ticker=symbol.ticker, records=len(data)):
            try:
                blob_path = self._get_actions_blob_path(symbol)
                
                if append:
                    # Try to read existing data
                    existing_data = await self._read_parquet_blob(blob_path)
                    if existing_data is not None:
                        # Combine and deduplicate
                        combined_data = pd.concat([existing_data, data])
                        combined_data = combined_data.drop_duplicates(subset=['Date'], keep='last')
                    else:
                        combined_data = data
                else:
                    combined_data = data
                
                # Write to blob
                await self._write_parquet_blob(blob_path, combined_data, index=False)
                
                logger.info(
                    "Wrote actions data to Azure Blob",
                    ticker=symbol.ticker,
                    records=len(combined_data),
                    blob_path=blob_path
                )
                
            except Exception as e:
                error_msg = f"Failed to write actions data to Azure Blob for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise StorageError(error_msg, "azure-blob", "write_actions_data")
    
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
                f"Failed to get last date from Azure Blob for {symbol.ticker}: {str(e)}",
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
        symbols = set()
        
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            if exchange:
                prefix = f"exchange={exchange.value}/"
            else:
                prefix = "exchange="
            
            # List blobs with the specified prefix
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blob_list:
                # Parse ticker from blob path
                # Expected format: exchange=us/ticker=AAPL/year=2024/part-000.parquet
                path_parts = blob.name.split('/')
                for part in path_parts:
                    if part.startswith('ticker='):
                        ticker = part.split('=')[1]
                        symbols.add(ticker)
                        break
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Failed to list symbols from Azure Blob: {str(e)}", error=str(e))
            return []
    
    async def get_storage_info(self) -> Dict[str, Any]:
        """
        Get information about storage usage and configuration.
        
        Returns:
            Storage metadata
        """
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            # Count blobs and calculate total size
            blob_count = 0
            total_size = 0
            
            blob_list = container_client.list_blobs()
            for blob in blob_list:
                if blob.name.endswith('.parquet'):
                    blob_count += 1
                    total_size += blob.size or 0
            
            return {
                "backend": "azure-blob",
                "storage_account": self.storage_account,
                "container_name": self.container_name,
                "parquet_engine": self.engine,
                "compression": settings.parquet_compression,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "blob_count": blob_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get Azure Blob storage info: {str(e)}", error=str(e))
            return {
                "backend": "azure-blob",
                "storage_account": self.storage_account,
                "container_name": self.container_name,
                "error": str(e)
            }
    
    async def _read_parquet_blob(self, blob_path: str) -> Optional[pd.DataFrame]:
        """
        Read a Parquet file from blob storage.
        
        Args:
            blob_path: Path to the blob
            
        Returns:
            DataFrame or None if blob doesn't exist
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            # Download blob data
            blob_data = blob_client.download_blob().readall()
            
            # Read Parquet from bytes
            return pd.read_parquet(io.BytesIO(blob_data), engine=self.engine)
            
        except ResourceNotFoundError:
            return None
        except Exception as e:
            logger.error(
                f"Failed to read Parquet blob {blob_path}: {str(e)}",
                blob_path=blob_path,
                error=str(e)
            )
            raise
    
    async def _write_parquet_blob(
        self, 
        blob_path: str, 
        data: pd.DataFrame, 
        index: bool = True
    ) -> None:
        """
        Write a DataFrame as Parquet to blob storage.
        
        Args:
            blob_path: Path to the blob
            data: Data to write
            index: Whether to include the index
        """
        try:
            # Convert DataFrame to Parquet bytes
            buffer = io.BytesIO()
            data.to_parquet(
                buffer,
                engine=self.engine,
                compression=settings.parquet_compression,
                index=index
            )
            buffer.seek(0)
            
            # Upload to blob
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            blob_client.upload_blob(
                buffer.getvalue(),
                overwrite=True,
                content_settings={'content_type': 'application/octet-stream'}
            )
            
        except Exception as e:
            logger.error(
                f"Failed to write Parquet blob {blob_path}: {str(e)}",
                blob_path=blob_path,
                error=str(e)
            )
            raise
    
    async def _list_available_years(self, symbol: UniverseSymbol) -> List[int]:
        """
        List available years for a symbol.
        
        Args:
            symbol: Symbol to check
            
        Returns:
            List of available years
        """
        years = set()
        
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            prefix = f"exchange={symbol.exchange.value}/ticker={symbol.ticker}/"
            
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            
            for blob in blob_list:
                # Parse year from blob path
                # Expected format: exchange=us/ticker=AAPL/year=2024/part-000.parquet
                path_parts = blob.name.split('/')
                for part in path_parts:
                    if part.startswith('year='):
                        try:
                            year = int(part.split('=')[1])
                            years.add(year)
                        except ValueError:
                            continue
                        break
            
            return sorted(list(years))
            
        except Exception as e:
            logger.debug(
                f"Failed to list available years for {symbol.ticker}: {str(e)}",
                ticker=symbol.ticker,
                error=str(e)
            )
            return []