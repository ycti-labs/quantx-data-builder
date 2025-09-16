"""
Mock Data Provider for Testing

Generates synthetic OHLCV data for testing when real data sources are unavailable.
"""

from __future__ import annotations

import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, Dict, Any

from . import DataProvider, ProviderError
from ..config import ExchangeCode, UniverseSymbol
from ..logging import get_logger, Timer

logger = get_logger(__name__)


class MockDataProvider(DataProvider):
    """
    Mock implementation of DataProvider for testing.
    
    Generates realistic-looking synthetic OHLCV data for any symbol.
    """
    
    def __init__(self, base_price: float = 100.0, volatility: float = 0.02):
        """
        Initialize the mock provider.
        
        Args:
            base_price: Starting price for synthetic data
            volatility: Price volatility (standard deviation of returns)
        """
        self.base_price = base_price
        self.volatility = volatility
        
        logger.info(
            "Initialized Mock provider",
            base_price=base_price,
            volatility=volatility
        )
    
    async def fetch_ohlcv(
        self,
        symbol: UniverseSymbol,
        start_date: date,
        end_date: date,
        include_actions: bool = False
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        Generate synthetic OHLCV data.
        
        Args:
            symbol: The symbol to generate data for
            start_date: Start date for data generation
            end_date: End date for data generation
            include_actions: Whether to generate synthetic actions
            
        Returns:
            Tuple of (ohlcv_data, actions_data)
        """
        # Simulate network delay
        await asyncio.sleep(0.1)
        
        with Timer(
            logger, 
            "mock_fetch",
            ticker=symbol.ticker,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat()
        ):
            try:
                # Generate date range (business days only)
                date_range = pd.bdate_range(start=start_date, end=end_date)
                
                if len(date_range) == 0:
                    logger.warning("No business days in date range", ticker=symbol.ticker)
                    return None, None
                
                # Generate synthetic price data using random walk
                num_days = len(date_range)
                
                # Generate random returns
                np.random.seed(hash(symbol.ticker) % 2**32)  # Deterministic based on ticker
                returns = np.random.normal(0, self.volatility, num_days)
                
                # Calculate cumulative prices
                prices = self.base_price * np.exp(np.cumsum(returns))
                
                # Generate OHLCV data
                data = []
                for i, (date, price) in enumerate(zip(date_range, prices)):
                    # Add some intraday variation
                    daily_vol = self.volatility * 0.5
                    high = price * (1 + abs(np.random.normal(0, daily_vol)))
                    low = price * (1 - abs(np.random.normal(0, daily_vol)))
                    open_price = prices[i-1] if i > 0 else price
                    close_price = price
                    
                    # Ensure OHLC logic is correct
                    high = max(high, open_price, close_price)
                    low = min(low, open_price, close_price)
                    
                    # Generate volume (random but realistic)
                    volume = int(np.random.lognormal(15, 1))  # Typical stock volume
                    
                    data.append({
                        'Date': date,
                        'Open': round(open_price, 2),
                        'High': round(high, 2),
                        'Low': round(low, 2),
                        'Close': round(close_price, 2),
                        'Adj Close': round(close_price, 2),  # Simplified - no adjustments
                        'Volume': volume,
                        'Ticker': symbol.ticker,
                        'Exchange': symbol.exchange.value
                    })
                
                # Create DataFrame
                df = pd.DataFrame(data)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
                
                # Generate actions data if requested
                actions_data = None
                if include_actions:
                    actions_data = self._generate_actions_data(symbol, date_range)
                
                logger.info(
                    "Generated synthetic data",
                    ticker=symbol.ticker,
                    records=len(df),
                    date_range=f"{df.index.min()} to {df.index.max()}"
                )
                
                return df, actions_data
                
            except Exception as e:
                error_msg = f"Failed to generate mock data for {symbol.ticker}: {str(e)}"
                logger.error(error_msg, ticker=symbol.ticker, error=str(e))
                raise ProviderError(error_msg, "mock", symbol.ticker)
    
    def _generate_actions_data(
        self, 
        symbol: UniverseSymbol, 
        date_range: pd.DatetimeIndex
    ) -> Optional[pd.DataFrame]:
        """Generate synthetic dividend and split actions."""
        actions = []
        
        # Add a few random dividends (quarterly-ish)
        for date in date_range[::60]:  # Every ~60 days
            if np.random.random() < 0.3:  # 30% chance
                actions.append({
                    'Date': date,
                    'Dividends': round(np.random.uniform(0.5, 2.0), 2),
                    'Stock Splits': 1.0,
                    'Ticker': symbol.ticker,
                    'Exchange': symbol.exchange.value
                })
        
        if not actions:
            return None
        
        return pd.DataFrame(actions)
    
    async def validate_symbol(self, symbol: UniverseSymbol) -> bool:
        """Mock validation - always returns True."""
        return True
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the mock provider."""
        return {
            "name": "Mock",
            "version": "1.0.0",
            "description": "Synthetic data provider for testing",
            "supported_exchanges": ["US", "HK", "JP"],
            "base_price": self.base_price,
            "volatility": self.volatility,
            "features": {
                "ohlcv": True,
                "actions": True,
                "real_time": False,
                "synthetic": True
            }
        }