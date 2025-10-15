"""
Azure Blob Storage implementation
Uses Azure Managed Identity for authentication in production
"""

from typing import List, Dict, Any
import pandas as pd
import json
import io
import os
import logging
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential
from .base_storage import BaseStorage


logger = logging.getLogger(__name__)


class AzureBlobStorage(BaseStorage):
    """
    Azure Blob Storage implementation with managed identity support
    Automatically uses managed identity in Azure, connection string locally
    """

    def __init__(
        self,
        account_name: str,
        container_name: str,
        use_managed_identity: bool = True
    ):
        """
        Initialize Azure Blob Storage client
        
        Args:
            account_name: Azure Storage account name
            container_name: Container name for data storage
            use_managed_identity: Use managed identity (True) or connection string (False)
        """
        self.account_name = account_name
        self.container_name = container_name

        # Use Managed Identity in Azure, connection string for local dev
        if use_managed_identity:
            account_url = f"https://{account_name}.blob.core.windows.net"
            try:
                credential = DefaultAzureCredential()
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=credential
                )
                logger.info(f"Connected to Azure Blob Storage using managed identity: {account_name}")
            except Exception as e:
                logger.warning(f"Failed to use managed identity: {e}. Falling back to connection string.")
                connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
                if not connection_string:
                    raise ValueError(
                        "Managed identity failed and AZURE_STORAGE_CONNECTION_STRING not set"
                    )
                self.blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
        else:
            # Use connection string for local development
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            if not connection_string:
                raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
            self.blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            logger.info(f"Connected to Azure Blob Storage using connection string: {account_name}")

        self.container_client = self.blob_service_client.get_container_client(container_name)

    def save_dataframe(self, df: pd.DataFrame, path: str) -> None:
        """Save DataFrame to Azure Blob as Parquet with ZSTD compression"""
        blob_client = self.container_client.get_blob_client(path)

        # Convert to parquet in memory
        buffer = io.BytesIO()
        df.to_parquet(
            buffer,
            engine='pyarrow',
            compression='zstd',
            index=True
        )
        buffer.seek(0)

        # Upload with metadata
        blob_client.upload_blob(
            buffer,
            overwrite=True,
            metadata={
                "rows": str(len(df)),
                "columns": str(len(df.columns))
            }
        )
        logger.debug(f"Saved DataFrame ({len(df)} rows) to {path}")

    def load_dataframe(self, path: str) -> pd.DataFrame:
        """Load DataFrame from Azure Blob Parquet file"""
        blob_client = self.container_client.get_blob_client(path)

        # Download to memory
        buffer = io.BytesIO()
        blob_client.download_blob().readinto(buffer)
        buffer.seek(0)

        df = pd.read_parquet(buffer)
        logger.debug(f"Loaded DataFrame ({len(df)} rows) from {path}")
        return df

    def save_universe(self, symbols: List[str], path: str) -> None:
        """Save universe list to CSV"""
        df = pd.DataFrame({"symbol": symbols})
        blob_client = self.container_client.get_blob_client(path)

        csv_buffer = df.to_csv(index=False)
        blob_client.upload_blob(csv_buffer, overwrite=True)
        logger.info(f"Saved {len(symbols)} symbols to {path}")

    def load_universe(self, path: str) -> List[str]:
        """Load universe list from CSV"""
        blob_client = self.container_client.get_blob_client(path)

        csv_data = blob_client.download_blob().readall().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_data))

        symbols = df['symbol'].tolist()
        logger.info(f"Loaded {len(symbols)} symbols from {path}")
        return symbols

    def save_metadata(self, path: str, metadata: Dict[str, Any]) -> None:
        """Save metadata as JSON"""
        blob_client = self.container_client.get_blob_client(path)

        json_data = json.dumps(metadata, indent=2, default=str)
        blob_client.upload_blob(json_data, overwrite=True)
        logger.debug(f"Saved metadata to {path}")

    def load_metadata(self, path: str) -> Dict[str, Any]:
        """Load metadata from JSON"""
        blob_client = self.container_client.get_blob_client(path)

        json_data = blob_client.download_blob().readall().decode('utf-8')
        metadata = json.loads(json_data)
        logger.debug(f"Loaded metadata from {path}")
        return metadata

    def exists(self, path: str) -> bool:
        """Check if a blob exists"""
        blob_client = self.container_client.get_blob_client(path)
        return blob_client.exists()

    def list_files(self, prefix: str) -> List[str]:
        """List all blobs with a given prefix"""
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        paths = [blob.name for blob in blobs]
        logger.debug(f"Found {len(paths)} files with prefix {prefix}")
        return paths
