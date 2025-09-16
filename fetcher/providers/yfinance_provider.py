"""
YFinance Provider Implementation

Concrete implementation of the DataProvider interface using the yfinance library
for fetching financial data with robust error handling and retry logic.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, date
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import yfinance as yf
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)

from . import DataProvider, ProviderError, DataQualityError, RateLimitError
from ..config import ExchangeCode, UniverseSymbol, settings
from ..logging import get_logger, Timer

logger = get_logger(__name__)


class YFinanceProvider(DataProvider):
    """
    YFinance implementation of the DataProvider interface.
    
    Provides robust data fetching with retry logic, rate limiting,
    and comprehensive error handling.
    """
    
    def __init__(
        self,
        rate_limit_delay: float = 0.1,
        max_retries: int = 3,
        backoff_factor: float = 2.0
    ):
        """
        Initialize the YFinance provider.
        
        Args:
            rate_limit_delay: Delay between requests in seconds
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff factor for retries
        """
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.last_request_time = 0.0
        
        # Suppress yfinance's noisy error logging
        import logging
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)
        
        logger.info(
            "Initialized YFinance provider",
            rate_limit_delay=rate_limit_delay,
            max_retries=max_retries,
            backoff_factor=backoff_factor
        )
    
    async def _respect_rate_limit(self) -> None:
        """Ensure we don't exceed rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, RateLimitError)),
        before_sleep=before_sleep_log(logger, "WARNING"),
        after=after_log(logger, "INFO")
    )
    async def fetch_ohlcv(
        self,
        symbol: UniverseSymbol,
        start_date: date,
        end_date: date,
        include_actions: bool = False
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Fetch OHLCV data from YFinance with retry logic.
        
        Args:
            symbol: The symbol to fetch data for
            start_date: Start date for data fetch
            end_date: End date for data fetch
            include_actions: Whether to fetch dividend/split actions
            
        Returns:
            Tuple of (ohlcv_data, actions_data)
            
        Raises:
            ProviderError: If data fetching fails after retries
        """
        await self._respect_rate_limit()
        
        normalized_ticker = self.normalize_ticker(symbol.ticker, symbol.exchange)
        
        with Timer(
            logger, 
            "yfinance_fetch",
            ticker=normalized_ticker,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        ):
            try:
                # Use yf.download() instead of Ticker().history() as it's more reliable
                hist_data = yf.download(
                    normalized_ticker,
                    start=start_date,  # Pass date object directly
                    end=end_date,      # Pass date object directly
                    auto_adjust=False,  # We want both Close and Adj Close
                    back_adjust=False,
                    repair=True,  # Fix data issues
                    keepna=False,  # Remove NaN values
                    actions=False,  # We'll fetch actions separately if needed
                    progress=False  # Disable progress bar
                )
                
                # Debug: Log what we got from yfinance
                logger.debug(
                    "Raw yfinance download result",
                    ticker=normalized_ticker,
                    data_shape=hist_data.shape if not hist_data.empty else "empty",
                    columns=list(hist_data.columns) if not hist_data.empty else "none",
                    index_type=type(hist_data.index).__name__ if not hist_data.empty else "none"
                )
                
                if hist_data.empty:
                    logger.warning(
                        "No historical data found",
                        ticker=normalized_ticker,
                        start_date=start_date.isoformat(),
                        end_date=end_date.isoformat()
                    )
                    return None, None
                
                # Debug: Log the raw data we got
                logger.debug(
                    "Raw yfinance data received",
                    ticker=normalized_ticker,
                    records=len(hist_data),
                    columns=list(hist_data.columns),
                    date_range=f"{hist_data.index.min()} to {hist_data.index.max()}" if not hist_data.empty else "empty"
                )
                
                # Validate data quality
                ohlcv_data = self._process_ohlcv_data(hist_data, symbol)
                
                # Fetch actions data if requested
                actions_data = None
                if include_actions:
                    actions_data = await self._fetch_actions_data(normalized_ticker, symbol)
                
                logger.info(
                    "Successfully fetched data",
                    ticker=normalized_ticker,
                    records=len(ohlcv_data),
                    date_range=f"{ohlcv_data.index.min()} to {ohlcv_data.index.max()}"
                )
                
                return ohlcv_data, actions_data
                
            except Exception as e:
                error_str = str(e).lower()
                error_msg = f"Failed to fetch data for {normalized_ticker}: {str(e)}"
                
                # Enhanced error classification for better handling
                if "429" in error_str or "rate limit" in error_str:
                    raise RateLimitError(error_msg, "yfinance", normalized_ticker)
                elif "timeout" in error_str:
                    raise TimeoutError(error_msg)
                elif "expecting value" in error_str and "char 0" in error_str:
                    # JSON parsing error - likely temporary API issue
                    logger.warning(
                        "JSON parsing error from yfinance API",
                        ticker=normalized_ticker,
                        error=str(e)
                    )
                    return None, None
                elif "no timezone found" in error_str or "may be delisted" in error_str:
                    # Timezone/delisting error - skip this symbol
                    logger.warning(
                        "Symbol may be delisted or have timezone issues",
                        ticker=normalized_ticker,
                        error=str(e)
                    )
                    return None, None
                else:
                    logger.error(error_msg, ticker=normalized_ticker, error=str(e))
                    raise ProviderError(error_msg, "yfinance", normalized_ticker)
    
    def _process_ohlcv_data(
        self, 
        raw_data: pd.DataFrame, 
        symbol: UniverseSymbol
    ) -> pd.DataFrame:
        """
        Process and validate raw OHLCV data from yfinance.
        
        Args:
            raw_data: Raw data from yfinance
            symbol: Symbol metadata
            
        Returns:
            Processed and validated OHLCV data
            
        Raises:
            DataQualityError: If data doesn't meet quality standards
        """
        if raw_data.empty:
            raise DataQualityError(
                "Empty dataset returned", 
                "yfinance", 
                symbol.ticker
            )
        
        # Reset index to make Date a column
        data = raw_data.reset_index()
        
        # Rename columns to match our schema
        column_mapping = {
            'Date': 'Date',
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close',
            'Adj Close': 'Adj Close',
            'Volume': 'Volume'
        }
        
        data = data.rename(columns=column_mapping)
        
        # Add symbol metadata
        data['Ticker'] = symbol.ticker
        data['Exchange'] = symbol.exchange.value
        
        # Ensure proper data types
        data['Date'] = pd.to_datetime(data['Date'], utc=False)
        
        # Handle MultiIndex columns from yf.download() - flatten to simple column names
        if isinstance(data.columns, pd.MultiIndex):
            # For single ticker, columns are like ('Open', 'AAPL'), ('Close', 'AAPL')
            # We want just 'Open', 'Close', etc.
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
        
        # Convert numeric columns to proper dtypes
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce').astype('int64')
        
        # Data quality checks
        if len(data) < settings.min_data_points:
            raise DataQualityError(
                f"Insufficient data points: {len(data)} < {settings.min_data_points}",
                "yfinance",
                symbol.ticker
            )
        
        # Check for excessive missing values
        missing_pct = data[numeric_cols].isnull().sum().max() / len(data)
        if missing_pct > 0.1:  # More than 10% missing
            raise DataQualityError(
                f"Excessive missing values: {missing_pct:.1%}",
                "yfinance",
                symbol.ticker
            )
        
        # Remove rows with any missing critical values
        critical_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        data = data.dropna(subset=critical_columns)
        
        # Set Date as index
        data = data.set_index('Date')
        
        # Sort by date
        data = data.sort_index()
        
        logger.debug(
            "Processed OHLCV data",
            ticker=symbol.ticker,
            records=len(data),
            date_range=f"{data.index.min()} to {data.index.max()}",
            missing_pct=f"{missing_pct:.1%}"
        )
        
        return data
    
    async def _fetch_actions_data(
        self, 
        normalized_ticker: str, 
        symbol: UniverseSymbol
    ) -> Optional[pd.DataFrame]:
        """
        Fetch dividend and split actions data.
        
        Args:
            normalized_ticker: Normalized ticker string
            symbol: Symbol metadata
            
        Returns:
            Actions data or None if no actions found
        """
        try:
            # Create ticker object just for actions
            ticker_obj = yf.Ticker(normalized_ticker)
            actions = ticker_obj.actions
            
            if actions.empty:
                return None
            
            # Reset index and add metadata
            actions_data = actions.reset_index()
            actions_data['Ticker'] = symbol.ticker
            actions_data['Exchange'] = symbol.exchange.value
            
            logger.debug(
                "Fetched actions data",
                ticker=symbol.ticker,
                dividends=len(actions_data[actions_data['Dividends'] > 0]),
                splits=len(actions_data[actions_data['Stock Splits'] != 1])
            )
            
            return actions_data
            
        except Exception as e:
            logger.warning(
                f"Failed to fetch actions data: {str(e)}",
                ticker=symbol.ticker,
                error=str(e)
            )
            return None
    
    def normalize_ticker(self, ticker: str, exchange: ExchangeCode) -> str:
        """
        Normalize ticker symbol for the specific exchange.
        
        Args:
            ticker: Raw ticker symbol
            exchange: Exchange code
            
        Returns:
            Normalized ticker symbol for the provider
        """
        ticker = ticker.upper().strip()
        original_ticker = ticker
        
        # Add exchange-specific suffixes and formatting
        if exchange == ExchangeCode.HK:
            # Hong Kong stocks: pad numeric codes and add .HK suffix
            if not ticker.endswith('.HK'):
                # Pad numeric codes with leading zeros to 4 digits
                if ticker.isdigit():
                    ticker = ticker.zfill(4)
                ticker = f"{ticker}.HK"
        elif exchange == ExchangeCode.JP:
            # Japanese stocks need .T suffix for yfinance
            if not ticker.endswith('.T'):
                ticker = f"{ticker}.T"
        
        if ticker != original_ticker:
            logger.debug(
                "Normalized ticker",
                original=original_ticker,
                normalized=ticker,
                exchange=exchange.value
            )
        
        return ticker

    async def validate_symbol(self, symbol: UniverseSymbol) -> bool:
        """
        Validate that a symbol exists and has recent data.
        
        Args:
            symbol: The symbol to validate
            
        Returns:
            True if symbol is valid, False otherwise
        """
        try:
            await self._respect_rate_limit()
            
            normalized_ticker = self.normalize_ticker(symbol.ticker, symbol.exchange)
            ticker_obj = yf.Ticker(normalized_ticker)
            
            # Try to fetch basic info
            info = ticker_obj.info
            
            # Check if we got meaningful data
            if not info or 'symbol' not in info:
                return False
            
            # Try to fetch a small amount of recent data
            recent_data = ticker_obj.history(period="5d")
            
            return not recent_data.empty
            
        except Exception as e:
            logger.debug(
                f"Symbol validation failed: {str(e)}",
                ticker=symbol.ticker,
                error=str(e)
            )
            return False
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the YFinance provider."""
        return {
            "name": "YFinance",
            "version": yf.__version__,
            "description": "Yahoo Finance data provider",
            "supported_exchanges": ["US", "HK", "JP"],
            "rate_limit_delay": self.rate_limit_delay,
            "max_retries": self.max_retries,
            "features": {
                "ohlcv": True,
                "actions": True,
                "real_time": False,
                "fundamental": True
            }
        }