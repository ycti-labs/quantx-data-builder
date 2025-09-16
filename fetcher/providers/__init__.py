"""
FinSight Data Fetcher Providers Module

Abstract provider interface and concrete implementations for financial data sources.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Optional, Tuple, Dict, Any
import pandas as pd

from ..config import ExchangeCode, UniverseSymbol
from ..logging import get_logger

logger = get_logger(__name__)


class DataProvider(ABC):
    """
    Abstract base class for financial data providers.
    
    Defines the interface that all data providers must implement for
    consistent data fetching across different sources.
    """
    
    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol: UniverseSymbol,
        start_date: date,
        end_date: date,
        include_actions: bool = False
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Fetch OHLCV data for a symbol within the specified date range.
        
        Args:
            symbol: The symbol to fetch data for
            start_date: Start date for data fetch
            end_date: End date for data fetch
            include_actions: Whether to fetch dividend/split actions
            
        Returns:
            Tuple of (ohlcv_data, actions_data) where each can be None if no data
            
        Raises:
            ProviderError: If data fetching fails
        """
        pass
    
    @abstractmethod
    async def validate_symbol(self, symbol: UniverseSymbol) -> bool:
        """
        Validate that a symbol exists and is tradeable.
        
        Args:
            symbol: The symbol to validate
            
        Returns:
            True if symbol is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def get_provider_info(self) -> Dict[str, Any]:
        """
        Get information about this provider.
        
        Returns:
            Dictionary containing provider metadata
        """
        pass
    
    def normalize_ticker(self, ticker: str, exchange: ExchangeCode) -> str:
        """
        Normalize ticker symbol for the specific exchange.
        
        Args:
            ticker: Raw ticker symbol
            exchange: Exchange code
            
        Returns:
            Normalized ticker symbol for the provider
        """
        # Default implementation - subclasses can override
        ticker = ticker.upper().strip()
        
        # Add exchange-specific suffixes if needed
        if exchange == ExchangeCode.HK:
            # Hong Kong stocks need .HK suffix for yfinance
            if not ticker.endswith('.HK'):
                ticker = f"{ticker}.HK"
        elif exchange == ExchangeCode.JP:
            # Japanese stocks need .T suffix for yfinance
            if not ticker.endswith('.T'):
                ticker = f"{ticker}.T"
        
        return ticker


class ProviderError(Exception):
    """Exception raised when provider operations fail."""
    
    def __init__(self, message: str, provider: str, symbol: Optional[str] = None):
        self.message = message
        self.provider = provider
        self.symbol = symbol
        super().__init__(f"[{provider}] {message}" + (f" for {symbol}" if symbol else ""))


class DataQualityError(ProviderError):
    """Exception raised when fetched data doesn't meet quality standards."""
    pass


class RateLimitError(ProviderError):
    """Exception raised when provider rate limits are exceeded."""
    pass