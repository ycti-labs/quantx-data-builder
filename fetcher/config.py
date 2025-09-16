"""
FinSight Data Fetcher Configuration Module

Pydantic-based configuration management with environment variable overrides
and comprehensive validation for the financial data fetching pipeline.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(str, Enum):
    """Supported log levels for structured logging."""
    
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class OperationMode(str, Enum):
    """Supported operation modes for the data fetcher."""
    
    BACKFILL = "backfill"
    DAILY = "daily"
    ESG_IMPORT = "esg-import"


class ExchangeCode(str, Enum):
    """Supported exchange codes for partitioning."""
    
    US = "us"
    HK = "hk"
    JP = "jp"  # Future support for Nikkei


class StorageBackend(str, Enum):
    """Supported storage backends."""
    
    LOCAL = "local"
    AZURE_BLOB = "azure-blob"


class DataFetcherSettings(BaseSettings):
    """
    Main configuration class for the FinSight Data Fetcher.
    
    Supports environment variable overrides with the FETCHER_ prefix.
    """
    
    model_config = SettingsConfigDict(
        env_prefix="FETCHER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )
    
    # Logging Configuration
    log_level: LogLevel = Field(
        default=LogLevel.INFO,
        description="Logging level for structured logs"
    )
    
    # Operation Configuration
    operation_mode: OperationMode = Field(
        default=OperationMode.DAILY,
        description="Operation mode: backfill, daily, or esg-import"
    )
    
    max_workers: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of concurrent workers for data fetching"
    )
    
    # Storage Configuration
    storage_backend: StorageBackend = Field(
        default=StorageBackend.LOCAL,
        description="Storage backend: local or azure-blob"
    )
    
    out_root: Path = Field(
        default=Path("./data"),
        description="Root directory for data output"
    )
    
    # Azure Storage Configuration (when using azure-blob backend)
    azure_storage_account: Optional[str] = Field(
        default=None,
        description="Azure Storage Account name"
    )
    
    azure_container_name: str = Field(
        default="finsight-data",
        description="Azure Blob Storage container name"
    )
    
    # Universe and Manifest Configuration
    universe_path: Path = Field(
        default=Path("meta/universe_v1.csv"),
        description="Path to the universe CSV file"
    )
    
    manifest_path: Path = Field(
        default=Path("meta/symbols_manifest.csv"),
        description="Path to the symbols manifest file"
    )
    
    # Data Provider Configuration
    fetch_actions: bool = Field(
        default=False,
        description="Whether to fetch dividend and split actions data"
    )
    
    rate_limit_delay: float = Field(
        default=0.1,
        ge=0.0,
        le=10.0,
        description="Delay between API requests in seconds"
    )
    
    # Retry Configuration
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of retry attempts for failed requests"
    )
    
    retry_backoff_factor: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Exponential backoff factor for retries"
    )
    
    # Data Quality Configuration
    min_data_points: int = Field(
        default=100,
        ge=1,
        description="Minimum data points required for a valid symbol"
    )
    
    # Parquet Configuration
    parquet_compression: str = Field(
        default="zstd",
        description="Compression algorithm for Parquet files"
    )
    
    parquet_row_group_size: int = Field(
        default=50000,
        ge=1000,
        description="Row group size for Parquet files"
    )
    
    @field_validator("out_root", "universe_path", "manifest_path")
    @classmethod
    def validate_paths(cls, v: Path) -> Path:
        """Ensure paths are absolute and expanded."""
        return Path(v).expanduser().resolve()
    
    @field_validator("azure_storage_account")
    @classmethod
    def validate_azure_config(cls, v: Optional[str], info) -> Optional[str]:
        """Validate Azure configuration when using azure-blob backend."""
        if info.data.get("storage_backend") == StorageBackend.AZURE_BLOB:
            if not v:
                raise ValueError(
                    "azure_storage_account is required when using azure-blob backend"
                )
        return v
    
    def get_partition_path(
        self, 
        exchange: ExchangeCode, 
        ticker: str, 
        year: int
    ) -> Path:
        """
        Generate partition path for a given exchange, ticker, and year.
        
        Format: /data/exchange=<us|hk>/ticker=<SYMBOL>/year=YYYY/
        """
        return self.out_root / f"exchange={exchange.value}" / f"ticker={ticker}" / f"year={year}"
    
    def get_esg_path(self) -> Path:
        """Get the reserved path for ESG data."""
        return self.out_root / "esg"
    
    def get_actions_path(
        self, 
        exchange: ExchangeCode, 
        ticker: str
    ) -> Path:
        """Get the path for dividends and splits data."""
        return self.out_root / "actions" / f"exchange={exchange.value}" / f"ticker={ticker}"


class UniverseSymbol(BaseModel):
    """
    Model for a symbol in the trading universe.
    
    Represents a single stock symbol with its exchange and metadata.
    """
    
    ticker: str = Field(
        ...,
        min_length=1,
        max_length=20,
        description="Stock ticker symbol"
    )
    
    exchange: ExchangeCode = Field(
        ...,
        description="Exchange code where the symbol is traded"
    )
    
    name: Optional[str] = Field(
        default=None,
        description="Company or instrument name"
    )
    
    sector: Optional[str] = Field(
        default=None,
        description="Sector classification"
    )
    
    market_cap: Optional[float] = Field(
        default=None,
        ge=0,
        description="Market capitalization in USD"
    )
    
    active: bool = Field(
        default=True,
        description="Whether the symbol is actively traded"
    )
    
    @field_validator("ticker")
    @classmethod
    def validate_ticker(cls, v: str) -> str:
        """Normalize ticker symbol."""
        return v.upper().strip()


class ManifestEntry(BaseModel):
    """
    Model for a manifest entry tracking symbol fetch status.
    
    Used to enable incremental updates and resume capability.
    """
    
    ticker: str = Field(
        ...,
        description="Stock ticker symbol"
    )
    
    exchange: ExchangeCode = Field(
        ...,
        description="Exchange code"
    )
    
    last_date: Optional[str] = Field(
        default=None,
        description="Last successfully fetched date (YYYY-MM-DD)"
    )
    
    backfill_complete: bool = Field(
        default=False,
        description="Whether historical backfill is complete"
    )
    
    total_records: int = Field(
        default=0,
        ge=0,
        description="Total number of records fetched"
    )
    
    last_updated: Optional[str] = Field(
        default=None,
        description="Timestamp of last update (ISO format)"
    )
    
    error_count: int = Field(
        default=0,
        ge=0,
        description="Number of consecutive errors"
    )
    
    last_error: Optional[str] = Field(
        default=None,
        description="Last error message"
    )


# Global settings instance
settings = DataFetcherSettings()