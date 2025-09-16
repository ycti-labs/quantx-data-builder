"""
FinSight Data Fetcher

Enterprise-grade financial data pipeline for downloading historical daily OHLCV data
with partitioned Parquet storage and manifest tracking for incremental updates.
"""

__version__ = "1.0.0"
__author__ = "FinSight Team"
__email__ = "frank@ycti.com"

from .config import DataFetcherSettings, LogLevel, OperationMode, ExchangeCode
from .providers import DataProvider
from .providers.yfinance_provider import YFinanceProvider
from .storage import StorageBackend
from .storage.local_storage import LocalStorageBackend
from .storage.azure_storage import AzureBlobStorageBackend
from .manifest import ManifestManager

__all__ = [
    "DataFetcherSettings",
    "LogLevel", 
    "OperationMode",
    "ExchangeCode",
    "DataProvider",
    "YFinanceProvider",
    "StorageBackend",
    "LocalStorageBackend", 
    "AzureBlobStorageBackend",
    "ManifestManager"
]