"""
Local filesystem storage implementation
Used for development and testing
"""

from typing import List, Dict, Any
import pandas as pd
import json
import os
from pathlib import Path
import logging
from .base_storage import BaseStorage


logger = logging.getLogger(__name__)


class LocalStorage(BaseStorage):
    """
    Local filesystem storage implementation
    Mirrors the structure of Azure Blob Storage for compatibility
    """

    def __init__(self, root_path: str = "./data"):
        """
        Initialize local storage
        
        Args:
            root_path: Root directory for data storage (default: ./data)
        """
        self.root_path = Path(root_path)
        self.root_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized local storage at: {self.root_path.absolute()}")

    def _get_full_path(self, path: str) -> Path:
        """Convert relative path to full path"""
        return self.root_path / path

    def save_dataframe(self, df: pd.DataFrame, path: str) -> None:
        """Save DataFrame to local filesystem as Parquet"""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(
            full_path,
            engine='pyarrow',
            compression='zstd',
            index=True
        )
        logger.debug(f"Saved DataFrame ({len(df)} rows) to {full_path}")

    def load_dataframe(self, path: str) -> pd.DataFrame:
        """Load DataFrame from local filesystem Parquet file"""
        full_path = self._get_full_path(path)

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        df = pd.read_parquet(full_path)
        logger.debug(f"Loaded DataFrame ({len(df)} rows) from {full_path}")
        return df

    def save_universe(self, symbols: List[str], path: str) -> None:
        """Save universe list to CSV"""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame({"symbol": symbols})
        df.to_csv(full_path, index=False)
        logger.info(f"Saved {len(symbols)} symbols to {full_path}")

    def load_universe(self, path: str) -> List[str]:
        """Load universe list from CSV"""
        full_path = self._get_full_path(path)

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        df = pd.read_csv(full_path)
        symbols = df['symbol'].tolist()
        logger.info(f"Loaded {len(symbols)} symbols from {full_path}")
        return symbols

    def save_metadata(self, path: str, metadata: Dict[str, Any]) -> None:
        """Save metadata as JSON"""
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        logger.debug(f"Saved metadata to {full_path}")

    def load_metadata(self, path: str) -> Dict[str, Any]:
        """Load metadata from JSON"""
        full_path = self._get_full_path(path)

        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {full_path}")

        with open(full_path, 'r') as f:
            metadata = json.load(f)
        logger.debug(f"Loaded metadata from {full_path}")
        return metadata

    def exists(self, path: str) -> bool:
        """Check if a file exists"""
        full_path = self._get_full_path(path)
        return full_path.exists()

    def list_files(self, prefix: str) -> List[str]:
        """List all files with a given prefix"""
        prefix_path = self._get_full_path(prefix)

        if not prefix_path.exists():
            return []

        # If prefix is a directory, list all files recursively
        if prefix_path.is_dir():
            files = [
                str(p.relative_to(self.root_path))
                for p in prefix_path.rglob('*')
                if p.is_file()
            ]
        else:
            # If prefix is a file pattern, use glob
            parent = prefix_path.parent
            pattern = prefix_path.name
            files = [
                str(p.relative_to(self.root_path))
                for p in parent.glob(pattern)
                if p.is_file()
            ]

        logger.debug(f"Found {len(files)} files with prefix {prefix}")
        return files
