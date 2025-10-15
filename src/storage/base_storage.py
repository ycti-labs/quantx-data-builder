"""
Base storage interface for data persistence
Defines the contract that all storage implementations must follow
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd


class BaseStorage(ABC):
    """
    Abstract base class for storage implementations
    Supports both Azure Blob Storage and local filesystem
    """

    @abstractmethod
    def save_dataframe(self, df: pd.DataFrame, path: str) -> None:
        """
        Save a DataFrame to storage
        
        Args:
            df: DataFrame to save (typically OHLCV data)
            path: Relative path within storage (e.g., "data/AAPL/2024/daily.parquet")
        """
        pass

    @abstractmethod
    def load_dataframe(self, path: str) -> pd.DataFrame:
        """
        Load a DataFrame from storage
        
        Args:
            path: Relative path within storage
            
        Returns:
            DataFrame with loaded data
        """
        pass

    @abstractmethod
    def save_universe(self, symbols: List[str], path: str) -> None:
        """
        Save a list of symbols to storage as CSV
        
        Args:
            symbols: List of ticker symbols
            path: Relative path within storage (e.g., "meta/universe_phase1.csv")
        """
        pass

    @abstractmethod
    def load_universe(self, path: str) -> List[str]:
        """
        Load a list of symbols from storage
        
        Args:
            path: Relative path within storage
            
        Returns:
            List of ticker symbols
        """
        pass

    @abstractmethod
    def save_metadata(self, path: str, metadata: Dict[str, Any]) -> None:
        """
        Save metadata as JSON
        
        Args:
            path: Relative path within storage (e.g., "reports/backfill_report.json")
            metadata: Dictionary containing metadata
        """
        pass

    @abstractmethod
    def load_metadata(self, path: str) -> Dict[str, Any]:
        """
        Load metadata from JSON
        
        Args:
            path: Relative path within storage
            
        Returns:
            Dictionary containing metadata
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check if a file exists in storage
        
        Args:
            path: Relative path within storage
            
        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """
        List all files with a given prefix
        
        Args:
            prefix: Path prefix to search (e.g., "data/exchange=us/")
            
        Returns:
            List of file paths matching the prefix
        """
        pass
