"""
Core data operations shared across all deployment targets
This is the single source of truth for all data fetching logic
Used by both Azure Functions and Container Apps
"""

import yfinance as yf
import pandas as pd
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class DataOperations:
    """
    Core data operations shared across all deployment targets
    Provides methods for daily incremental updates and historical backfills
    """

    def __init__(self, storage, max_workers: int = 10):
        """
        Initialize data operations
        
        Args:
            storage: Storage implementation (AzureBlobStorage or LocalStorage)
            max_workers: Maximum number of concurrent workers for parallel downloads
        """
        self.storage = storage
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)

    def fetch_daily_incremental(
        self,
        symbols: List[str],
        lookback_days: int = 5
    ) -> Dict:
        """
        Fetch incremental daily data (last N days)
        Optimized for daily scheduled updates
        
        Args:
            symbols: List of ticker symbols to fetch
            lookback_days: Number of days to look back (default 5 to handle weekends)
            
        Returns:
            Dictionary with success/failure statistics
        """
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=lookback_days)

        self.logger.info(
            f"📊 Fetching incremental data for {len(symbols)} symbols "
            f"from {start_date} to {end_date}"
        )
        
        return self._fetch_batch(symbols, start_date, end_date)

    def fetch_backfill(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        chunk_size: int = 100
    ) -> Dict:
        """
        Fetch historical data in chunks
        Optimized for long-running backfill operations
        
        Args:
            symbols: List of ticker symbols to fetch
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_size: Number of symbols to process per chunk
            
        Returns:
            Dictionary with success/failure statistics
        """
        self.logger.info(
            f"🔄 Starting backfill: {len(symbols)} symbols "
            f"from {start_date} to {end_date}"
        )

        # Convert string dates
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Process in chunks to avoid memory issues
        symbol_chunks = [
            symbols[i:i + chunk_size]
            for i in range(0, len(symbols), chunk_size)
        ]

        total_results = {"success": 0, "failed": 0, "errors": [], "skipped": 0}

        for idx, chunk in enumerate(symbol_chunks):
            self.logger.info(f"Processing chunk {idx + 1}/{len(symbol_chunks)}")
            
            chunk_results = self._fetch_batch(chunk, start, end)

            total_results["success"] += chunk_results["success"]
            total_results["failed"] += chunk_results["failed"]
            total_results["skipped"] += chunk_results.get("skipped", 0)
            total_results["errors"].extend(chunk_results["errors"])

        self.logger.info(
            f"✅ Backfill complete: {total_results['success']} succeeded, "
            f"{total_results['failed']} failed, {total_results['skipped']} skipped"
        )
        
        return total_results

    def refresh_universe(self, phase: str = "phase_1") -> List[str]:
        """
        Refresh stock universe from configured sources
        
        Args:
            phase: Universe phase name (e.g., "phase_1", "phase_2")
            
        Returns:
            List of all symbols in the refreshed universe
        """
        from src.universe.universe_builder import UniverseBuilder

        self.logger.info(f"🔄 Refreshing universe for {phase}")
        
        builder = UniverseBuilder()
        universes = builder.build_phase(phase)

        # Save to storage
        all_symbols = []
        for name, symbols in universes.items():
            output_path = f"meta/universe_{name}.csv"
            self.storage.save_universe(symbols, output_path)
            self.logger.info(f"💾 Saved {len(symbols)} symbols to {output_path}")
            all_symbols.extend(symbols)

        # Save metadata
        metadata = {
            "phase": phase,
            "last_updated": datetime.utcnow().isoformat(),
            "total_symbols": len(all_symbols),
            "universe_names": list(universes.keys())
        }
        self.storage.save_metadata(f"meta/universe_{phase}_metadata.json", metadata)

        return all_symbols

    def _fetch_batch(
        self,
        symbols: List[str],
        start_date: datetime.date,
        end_date: datetime.date
    ) -> Dict:
        """
        Internal method to fetch a batch of symbols in parallel
        
        Args:
            symbols: List of ticker symbols
            start_date: Start date
            end_date: End date
            
        Returns:
            Dictionary with success/failure statistics
        """
        results = {"success": 0, "failed": 0, "errors": [], "skipped": 0}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self._fetch_symbol,
                    symbol,
                    start_date,
                    end_date
                ): symbol
                for symbol in symbols
            }

            # Collect results
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    status = future.result()
                    if status == "success":
                        results["success"] += 1
                    elif status == "skipped":
                        results["skipped"] += 1
                    else:
                        results["failed"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"].append({
                        "symbol": symbol,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    self.logger.error(f"❌ Failed to fetch {symbol}: {str(e)}")

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _fetch_symbol(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: datetime.date
    ) -> str:
        """
        Fetch and save data for a single symbol with retry logic
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            Status string: "success", "skipped", or "failed"
        """
        try:
            # Determine exchange and format path
            exchange = self._get_exchange(symbol)
            ticker_formatted = symbol.replace('.', '_')
            
            # Fetch data from yfinance
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date,
                end=end_date,
                actions=True,
                auto_adjust=False  # Keep raw prices and adjustment factor
            )

            if df.empty:
                self.logger.warning(f"⚠️  No data returned for {symbol}")
                return "skipped"

            # Add metadata columns
            df['Ticker'] = symbol
            df['Exchange'] = exchange

            # Save to partitioned storage
            # Path format: data/exchange=<us|hk>/ticker=<SYMBOL>/year=YYYY/part-*.parquet
            year = start_date.year
            output_path = f"data/exchange={exchange}/ticker={ticker_formatted}/year={year}/data.parquet"
            
            # Append to existing data if file exists
            if self.storage.exists(output_path):
                existing_df = self.storage.load_dataframe(output_path)
                df = pd.concat([existing_df, df]).drop_duplicates(subset=['Date'], keep='last')
                df = df.sort_index()
            
            self.storage.save_dataframe(df, output_path)

            self.logger.info(f"✅ Fetched {len(df)} rows for {symbol}")
            return "success"

        except Exception as e:
            self.logger.error(f"❌ Error fetching {symbol}: {str(e)}")
            raise

    def _get_exchange(self, symbol: str) -> str:
        """
        Determine exchange from symbol format
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            Exchange code (e.g., "us", "hk", "jp")
        """
        # Hong Kong stocks typically have 4-digit codes
        if symbol.isdigit() and len(symbol) == 4:
            return "hk"
        
        # Symbols ending in .HK
        if symbol.endswith('.HK'):
            return "hk"
        
        # Symbols ending in .T (Tokyo)
        if symbol.endswith('.T'):
            return "jp"
        
        # Default to US
        return "us"

    def update_manifest(
        self,
        symbol: str,
        last_date: datetime.date,
        row_count: int,
        backfill_complete: bool = False
    ) -> None:
        """
        Update symbols manifest with latest fetch information
        
        Args:
            symbol: Ticker symbol
            last_date: Last date of available data
            row_count: Number of rows fetched
            backfill_complete: Whether historical backfill is complete
        """
        manifest_path = "meta/symbols_manifest.csv"

        try:
            # Load existing manifest
            if self.storage.exists(manifest_path):
                manifest_df = self.storage.load_dataframe(manifest_path)
            else:
                # Create new manifest
                manifest_df = pd.DataFrame(columns=[
                    'ticker', 'exchange', 'last_date', 'backfill_complete',
                    'last_updated', 'row_count'
                ])

            exchange = self._get_exchange(symbol)

            # Update or append
            if symbol in manifest_df['ticker'].values:
                idx = manifest_df[manifest_df['ticker'] == symbol].index[0]
                manifest_df.at[idx, 'last_date'] = last_date
                manifest_df.at[idx, 'last_updated'] = datetime.utcnow()
                manifest_df.at[idx, 'row_count'] = row_count
                manifest_df.at[idx, 'backfill_complete'] = backfill_complete
            else:
                new_row = pd.DataFrame([{
                    'ticker': symbol,
                    'exchange': exchange,
                    'last_date': last_date,
                    'backfill_complete': backfill_complete,
                    'last_updated': datetime.utcnow(),
                    'row_count': row_count
                }])
                manifest_df = pd.concat([manifest_df, new_row], ignore_index=True)

            # Save updated manifest
            self.storage.save_dataframe(manifest_df, manifest_path)

        except Exception as e:
            self.logger.error(f"Failed to update manifest for {symbol}: {str(e)}")
            # Don't raise - manifest update failure shouldn't stop the entire process
