"""
ESG Data Manager

Manages ESG (Environmental, Social, Governance) data for stock universes.
Loads data from local Excel/CSV files and uses GVKEY-ticker mapping
instead of fetching from external APIs.

ESG data includes:
- ESG composite scores
- Environmental pillar scores
- Social pillar scores
- Governance pillar scores
- Industry classifications (SIC codes)
"""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from universe import Universe

logger = logging.getLogger(__name__)


class ESGManager:
    """
    ESG data manager for stock ESG databases

    Manages the full lifecycle of ESG data including:
    - Loading: Read ESG data from local Excel/CSV files
    - Mapping: Convert GVKEY identifiers to ticker symbols
    - Storing: Hive-style partitioned Parquet storage
    - Reading: Load and query saved ESG data
    - Validation: Check coverage and detect missing data

    Unlike PriceManager and FundamentalManager, ESGManager does not fetch
    from external APIs. Instead, it loads data from local files and uses
    a GVKEY-ticker mapping to link ESG data to stock symbols.

    Data Structure:
        Source: data/raw/esg/data_matlab_ESG_withSIC.xlsx
        Mapping: data/curated/metadata/gvkey.parquet
        Output: data/curated/tickers/exchange=EXCHANGE/ticker=SYMBOL/esg/year=YYYY/part-000.parquet

    Note: ESG scores are published annually (YearESG) but the data includes monthly
    returns (YearMonth in YYYYMM format). The manager extracts both date and month
    from YearMonth for proper time-series alignment.

    Storage follows unified structure: all ticker data (prices, ESG, fundamentals)
    stored under data/curated/tickers/exchange={exchange}/ticker={symbol}/
    """

    def __init__(
        self,
        universe: Universe,
        esg_source_path: Optional[Path] = None,
        gvkey_mapping_path: Optional[Path] = None,
    ):
        """
        Initialize ESG data manager

        Args:
            universe: Universe instance for membership and data root
            esg_source_path: Path to ESG data file (Excel or CSV)
                           Default: data/raw/esg/data_matlab_ESG_withSIC.xlsx
            gvkey_mapping_path: Path to GVKEY-ticker mapping file
                              Default: data/curated/metadata/gvkey.parquet
        """
        self.universe = universe
        self.logger = logging.getLogger(__name__)

        # Set default paths
        data_root = Path(universe.data_root)
        self.esg_source_path = esg_source_path or (
            data_root / "raw" / "esg" / "data_matlab_ESG_withSIC.xlsx"
        )
        self.gvkey_mapping_path = gvkey_mapping_path or (
            data_root / "curated" / "metadata" / "gvkey.parquet"
        )

        # Cache for loaded data
        self._esg_data = None
        self._gvkey_mapping = None

    def _load_gvkey_mapping(self) -> pd.DataFrame:
        """
        Load GVKEY-ticker mapping from parquet file

        Returns:
            DataFrame with columns: gvkey, ticker
        """
        if self._gvkey_mapping is not None:
            return self._gvkey_mapping

        if not self.gvkey_mapping_path.exists():
            raise FileNotFoundError(
                f"GVKEY mapping file not found: {self.gvkey_mapping_path}\n"
                f"Please run examples/add_gvkey_mapping.py to create it."
            )

        self.logger.info(f"Loading GVKEY mapping from {self.gvkey_mapping_path}")
        df = pd.read_parquet(self.gvkey_mapping_path)

        # Ensure required columns exist
        if "gvkey" not in df.columns:
            raise ValueError(
                f"GVKEY mapping missing 'gvkey' column: {df.columns.tolist()}"
            )

        # Handle different ticker column names
        if "ticker" not in df.columns:
            if "tic" in df.columns:
                df = df.rename(columns={"tic": "ticker"})
            elif "symbol" in df.columns:
                df = df.rename(columns={"symbol": "ticker"})
            else:
                raise ValueError(
                    f"GVKEY mapping missing ticker column: {df.columns.tolist()}"
                )

        # Keep only necessary columns
        df = df[["gvkey", "ticker"]].copy()

        # Ensure gvkey is integer
        df["gvkey"] = df["gvkey"].astype(int)

        # Ensure ticker is string and uppercase
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

        # Remove duplicates (prefer first occurrence)
        df = df.drop_duplicates(subset=["gvkey"], keep="first")

        self.logger.info(f"Loaded {len(df):,} GVKEY-ticker mappings")
        self._gvkey_mapping = df
        return df

    def _load_esg_data(self) -> pd.DataFrame:
        """
        Load ESG data from Excel file

        Returns:
            DataFrame with columns: gvkey, PERMNO, YearESG, ESG Score, etc.
        """
        if self._esg_data is not None:
            return self._esg_data

        if not self.esg_source_path.exists():
            raise FileNotFoundError(f"ESG data file not found: {self.esg_source_path}")

        self.logger.info(f"Loading ESG data from {self.esg_source_path}")

        # Read Excel file
        if self.esg_source_path.suffix == ".xlsx":
            df = pd.read_excel(self.esg_source_path)
        elif self.esg_source_path.suffix == ".csv":
            df = pd.read_csv(self.esg_source_path)
        else:
            raise ValueError(f"Unsupported file format: {self.esg_source_path.suffix}")

        # Validate required columns
        required_cols = ["gvkey", "YearESG", "YearMonth", "ESG Score"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"ESG data missing required columns: {missing_cols}\n"
                f"Available columns: {df.columns.tolist()}"
            )

        # Ensure gvkey is integer
        df["gvkey"] = df["gvkey"].astype(int)

        # Parse YearMonth (YYYYMM format) into year and month
        df["year"] = df["YearMonth"] // 100
        df["month"] = df["YearMonth"] % 100

        # Create date column for proper time-series handling
        # Use end-of-month dates to align with financial data (prices, returns, etc.)
        # This ensures ESG signals can be properly joined with return data
        df["date"] = pd.to_datetime(
            df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
        ) + pd.offsets.MonthEnd(0)
        df["date"] = df["date"].dt.date

        self.logger.info(
            f"Loaded {len(df):,} ESG records for {df['gvkey'].nunique():,} companies "
            f"({df['year'].min()}-{df['year'].max()}, {df['date'].min()} to {df['date'].max()})"
        )
        self._esg_data = df
        return df

    def get_esg_data(
        self,
        symbol: Optional[str] = None,
        gvkey: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Get ESG data for a specific symbol or GVKEY

        Args:
            symbol: Ticker symbol (e.g., 'AAPL', 'MSFT')
            gvkey: GVKEY identifier (alternative to symbol)
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional)
            start_year: Start year (inclusive, optional) - alternative to start_date
            end_year: End year (inclusive, optional) - alternative to end_date

        Returns:
            DataFrame with ESG scores and metadata
            Columns include: ticker, gvkey, date, year, month, esg_score,
                            environmental_pillar_score, social_pillar_score, governance_pillar_score

        Raises:
            ValueError: If neither symbol nor gvkey is provided
        """
        if symbol is None and gvkey is None:
            raise ValueError("Must provide either symbol or gvkey")

        # Load data
        esg_df = self._load_esg_data()
        mapping_df = self._load_gvkey_mapping()

        # Convert symbol to gvkey if needed
        if symbol is not None:
            symbol = symbol.upper()
            gvkey_match = mapping_df[mapping_df["ticker"] == symbol]
            if gvkey_match.empty:
                self.logger.warning(f"Symbol not found in GVKEY mapping: {symbol}")
                return pd.DataFrame()
            gvkey = gvkey_match.iloc[0]["gvkey"]

        # Filter by gvkey
        df = esg_df[esg_df["gvkey"] == gvkey].copy()

        if df.empty:
            self.logger.warning(f"No ESG data found for GVKEY: {gvkey}")
            return pd.DataFrame()

        # Filter by date range (prefer date over year)
        if start_date is not None:
            start_date_parsed = pd.to_datetime(start_date).date()
            df = df[df["date"] >= start_date_parsed]
        elif start_year is not None:
            df = df[df["year"] >= start_year]

        if end_date is not None:
            end_date_parsed = pd.to_datetime(end_date).date()
            df = df[df["date"] <= end_date_parsed]
        elif end_year is not None:
            df = df[df["year"] <= end_year]

        # Add ticker symbol
        ticker_match = mapping_df[mapping_df["gvkey"] == gvkey]
        if not ticker_match.empty:
            df["ticker"] = ticker_match.iloc[0]["ticker"]
        else:
            df["ticker"] = f"GVKEY_{gvkey}"

        # Rename columns for consistency (use long names to match ESGFactorBuilder expectations)
        df = df.rename(
            columns={
                "YearESG": "esg_year",
                "ESG Score": "esg_score",
                "Environmental Pillar Score": "environmental_pillar_score",
                "Social Pillar Score": "social_pillar_score",
                "Governance Pillar Score": "governance_pillar_score",
                "SICCD": "sic_code",
                "Industry_Code": "industry_code",
                "PERMNO": "permno",
                "RET": "ret",
                "Year": "data_year",
                "YearMonth": "year_month",
            }
        )

        # Select relevant columns
        result_cols = [
            "ticker",
            "gvkey",
            "date",
            "year",
            "month",
            "esg_year",
            "esg_score",
            "environmental_pillar_score",
            "social_pillar_score",
            "governance_pillar_score",
        ]

        # Add optional columns if they exist
        optional_cols = [
            "permno",
            "ret",
            "data_year",
            "year_month",
            "sic_code",
            "industry_code",
        ]
        for col in optional_cols:
            if col in df.columns:
                result_cols.append(col)

        df = df[result_cols].copy()

        # Remove duplicates (keep one record per date per company)
        df = df.drop_duplicates(subset=["ticker", "gvkey", "date"], keep="first")

        # Sort by date
        df = df.sort_values("date").reset_index(drop=True)

        self.logger.info(
            f"✅ Retrieved {len(df)} ESG records for {df['ticker'].iloc[0]} "
            f"({df['date'].min()} to {df['date'].max()})"
        )

        return df

    def get_multiple_esg_data(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        skip_missing: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        Get ESG data for multiple symbols

        Args:
            symbols: List of ticker symbols
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional)
            start_year: Start year (inclusive, optional) - alternative to start_date
            end_year: End year (inclusive, optional) - alternative to end_date
            skip_missing: If True, skip symbols without data; if False, include empty DataFrames

        Returns:
            Dict mapping symbol -> DataFrame
        """
        results = {}

        for symbol in symbols:
            try:
                df = self.get_esg_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    start_year=start_year,
                    end_year=end_year,
                )

                if not df.empty or not skip_missing:
                    results[symbol] = df
                elif df.empty:
                    self.logger.debug(f"Skipping {symbol} (no ESG data)")

            except Exception as e:
                self.logger.error(f"Error getting ESG data for {symbol}: {e}")
                if not skip_missing:
                    results[symbol] = pd.DataFrame()

        return results

    def save_esg_data(
        self,
        df: pd.DataFrame,
        ticker: str,
        exchange: str = "us",
    ) -> List[Path]:
        """
        Save ESG data to Hive-style partitioned Parquet files

        Partition structure: exchange=EXCHANGE/ticker=SYMBOL/esg/year=YYYY/part-000.parquet
        All monthly records for a year are saved in a single file.
        Follows unified structure where all ticker data is co-located.

        Args:
            df: DataFrame with ESG data (must have 'year' column)
            ticker: Ticker symbol for partitioning
            exchange: Exchange code (default: 'us')

        Returns:
            List of saved file paths
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame for {ticker}, skipping save")
            return []

        if "year" not in df.columns:
            raise ValueError("DataFrame must have 'year' column for partitioning")

        ticker = ticker.upper()
        exchange = exchange.lower()

        # Unified structure: data/curated/tickers/exchange=us/ticker=AAPL/esg/
        base_path = (
            Path(self.universe.data_root)
            / "curated"
            / "tickers"
            / f"exchange={exchange}"
            / f"ticker={ticker}"
            / "esg"
        )
        saved_paths = []

        # Group by year only (all months in one file)
        for year, year_df in df.groupby("year"):
            year_path = base_path / f"year={year}"
            year_path.mkdir(parents=True, exist_ok=True)

            output_file = year_path / "part-000.parquet"

            # If file exists, merge with existing data
            if output_file.exists():
                existing_df = pd.read_parquet(output_file)
                year_df = pd.concat([existing_df, year_df], ignore_index=True)
                year_df = year_df.drop_duplicates(
                    subset=["ticker", "gvkey", "date"], keep="last"
                )

            # Save to parquet (all months for this year in one file)
            year_df.to_parquet(
                output_file, engine="pyarrow", compression="snappy", index=False
            )

            saved_paths.append(output_file)

        self.logger.info(
            f"💾 Saved ESG data for {ticker}: {len(saved_paths)} year(s) "
            f"({len(df)} records)"
        )

        return saved_paths

    def load_esg_data(
        self,
        ticker: str,
        exchange: str = "us",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Load saved ESG data from Parquet files

        Reads from: exchange=EXCHANGE/ticker=SYMBOL/esg/year=YYYY/part-000.parquet
        Follows unified structure where all ticker data is co-located.

        Args:
            ticker: Ticker symbol
            exchange: Exchange code (default: 'us')
            start_date: Start date in 'YYYY-MM-DD' format (optional)
            end_date: End date in 'YYYY-MM-DD' format (optional)
            start_year: Start year (inclusive, optional) - alternative to start_date
            end_year: End year (inclusive, optional) - alternative to end_date

        Returns:
            DataFrame with ESG data
        """
        ticker = ticker.upper()
        exchange = exchange.lower()

        # Unified structure: data/curated/tickers/exchange=us/ticker=AAPL/esg/
        base_path = (
            Path(self.universe.data_root)
            / "curated"
            / "tickers"
            / f"exchange={exchange}"
            / f"ticker={ticker}"
            / "esg"
        )

        if not base_path.exists():
            self.logger.warning(f"No ESG data found for {ticker} on {exchange}")
            return pd.DataFrame()

        # Find all year directories
        year_dirs = [
            d for d in base_path.iterdir() if d.is_dir() and d.name.startswith("year=")
        ]

        if not year_dirs:
            self.logger.warning(f"No year partitions found for {ticker}")
            return pd.DataFrame()

        # Read all parquet files directly from year directories
        dfs = []
        for year_dir in year_dirs:
            year = int(year_dir.name.split("=")[1])

            # Filter by year range if using year-based filtering
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue

            # Read parquet file directly from year directory
            parquet_file = year_dir / "part-000.parquet"
            if parquet_file.exists():
                df = pd.read_parquet(parquet_file)
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        # Combine all data
        result = pd.concat(dfs, ignore_index=True)

        # Filter out rows with None/NaN dates before applying date filters
        if "date" in result.columns:
            result = result.dropna(subset=["date"])

        # Apply date-based filtering if specified
        if "date" in result.columns and len(result) > 0:
            if start_date is not None:
                start_date_parsed = pd.to_datetime(start_date).date()
                result = result[result["date"] >= start_date_parsed]
            if end_date is not None:
                end_date_parsed = pd.to_datetime(end_date).date()
                result = result[result["date"] <= end_date_parsed]

        result = result.sort_values("date").reset_index(drop=True)

        if len(result) > 0 and "date" in result.columns:
            self.logger.info(
                f"📂 Loaded {len(result)} ESG records for {ticker} "
                f"({result['date'].min()} to {result['date'].max()})"
            )
        else:
            self.logger.info(f"📂 Loaded {len(result)} ESG records for {ticker}")

        return result

    def get_coverage_summary(self) -> pd.DataFrame:
        """
        Get summary of ESG data coverage by year

        Returns:
            DataFrame with columns: year, num_companies, num_records
        """
        esg_df = self._load_esg_data()

        summary = (
            esg_df.groupby("YearESG")
            .agg(num_companies=("gvkey", "nunique"), num_records=("gvkey", "count"))
            .reset_index()
        )

        summary = summary.rename(columns={"YearESG": "year"})
        summary = summary.sort_values("year")

        return summary

    def get_available_tickers(self, year: Optional[int] = None) -> List[str]:
        """
        Get list of tickers with ESG data

        Args:
            year: Filter by specific year (optional)

        Returns:
            List of ticker symbols
        """
        esg_df = self._load_esg_data()
        mapping_df = self._load_gvkey_mapping()

        # Filter by year if specified
        if year is not None:
            esg_df = esg_df[esg_df["YearESG"] == year]

        # Get unique gvkeys
        gvkeys = esg_df["gvkey"].unique()

        # Map to tickers
        tickers = (
            mapping_df[mapping_df["gvkey"].isin(gvkeys)]["ticker"].unique().tolist()
        )

        return sorted(tickers)

    def export_to_parquet(
        self,
        symbols: Optional[List[str]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> Dict[str, List[Path]]:
        """
        Export ESG data for multiple symbols to Parquet files

        Args:
            symbols: List of ticker symbols (if None, export all available)
            start_year: Start year (inclusive, optional)
            end_year: End year (inclusive, optional)

        Returns:
            Dict mapping symbol -> list of saved file paths
        """
        if symbols is None:
            symbols = self.get_available_tickers()
            self.logger.info(f"Exporting ESG data for {len(symbols)} symbols")

        results = {}

        for symbol in symbols:
            try:
                df = self.get_esg_data(
                    symbol=symbol, start_year=start_year, end_year=end_year
                )

                if not df.empty:
                    saved_paths = self.save_esg_data(df, symbol)
                    results[symbol] = saved_paths
                else:
                    self.logger.debug(f"No ESG data for {symbol}")

            except Exception as e:
                self.logger.error(f"Error exporting ESG data for {symbol}: {e}")

        return results

    def process_universe_esg(
        self,
        start_date: str,
        end_date: str,
        exchange: str = "us",
        dry_run: bool = False,
    ) -> Dict:
        """
        Process ESG data for entire universe with ticker mapping support

        This method follows a three-step procedure:
        1. Fetch historical memberships of universe for research period
        2. Go through ESG raw data one by one, with gvkey-ticker mapping
        3. Use TickerMapper to find if mapping is needed for missing tickers

        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            exchange: Exchange code (default: 'us')
            dry_run: If True, only report what would be done without saving

        Returns:
            Dictionary with results:
            {
                'processed': [...],      # Successfully processed tickers
                'mapped': {...},         # Ticker transitions applied (old → new)
                'skipped': [...],        # Tickers not in universe
                'no_esg_data': [...],    # Tickers with no ESG data
                'errors': [...]          # Processing errors
            }
        """
        from core.ticker_mapper import TickerMapper

        self.logger.info("=" * 80)
        self.logger.info("Processing Universe ESG Data with Ticker Mapping")
        self.logger.info("=" * 80)
        self.logger.info(f"Research Period: {start_date} to {end_date}")
        self.logger.info(f"Exchange: {exchange}")
        if dry_run:
            self.logger.info("DRY RUN MODE - no data will be saved")
        self.logger.info("=" * 80)

        # Step 1: Fetch historical memberships of universe for research period
        self.logger.info("\n📊 Step 1: Fetching historical universe members...")
        universe_members = self.universe.get_all_historical_members(
            start_date, end_date
        )
        self.logger.info(
            f"Found {len(universe_members)} historical members in universe"
        )

        # Convert to set for fast lookup
        universe_members_set = set(ticker.upper() for ticker in universe_members)

        # Step 2: Load ESG raw data and gvkey-ticker mapping
        self.logger.info("\n📂 Step 2: Loading ESG raw data and GVKEY mappings...")
        esg_df = self._load_esg_data()
        mapping_df = self._load_gvkey_mapping()

        # Filter ESG data by date range
        start_pd = pd.to_datetime(start_date).date()
        end_pd = pd.to_datetime(end_date).date()
        esg_df = esg_df[(esg_df["date"] >= start_pd) & (esg_df["date"] <= end_pd)]

        self.logger.info(
            f"Filtered ESG data: {len(esg_df):,} records for "
            f"{esg_df['gvkey'].nunique():,} companies in period"
        )

        # Get unique gvkeys from ESG data
        esg_gvkeys = esg_df["gvkey"].unique()

        # Map gvkeys to tickers
        ticker_map = (
            mapping_df[mapping_df["gvkey"].isin(esg_gvkeys)]
            .set_index("gvkey")["ticker"]
            .to_dict()
        )
        self.logger.info(
            f"GVKEY mapping coverage: {len(ticker_map):,} / {len(esg_gvkeys):,} companies"
        )

        # Step 3: Process each ticker with TickerMapper for transitions
        self.logger.info("\n🔄 Step 3: Processing tickers with transition mapping...")

        mapper = TickerMapper()
        results = {
            "processed": [],
            "mapped": {},
            "skipped": [],
            "no_esg_data": [],
            "errors": [],
        }

        # Group ESG data by gvkey for efficient processing
        esg_by_gvkey = {gvkey: group for gvkey, group in esg_df.groupby("gvkey")}

        processed_count = 0
        for gvkey, original_ticker in ticker_map.items():
            processed_count += 1
            if processed_count % 50 == 0 or processed_count == 1:
                self.logger.info(
                    f"Progress: {processed_count}/{len(ticker_map)} ({processed_count*100//len(ticker_map)}%)"
                )

            try:
                original_ticker = original_ticker.upper()

                # Check if ticker needs mapping (e.g., FB → META)
                resolved_ticker = mapper.resolve(original_ticker)

                # Handle delisted/acquired companies
                if resolved_ticker is None:
                    self.logger.debug(
                        f"Skipping {original_ticker}: delisted/acquired with no successor"
                    )
                    results["skipped"].append(original_ticker)
                    continue

                # Track if ticker was mapped
                if resolved_ticker != original_ticker:
                    results["mapped"][original_ticker] = resolved_ticker
                    self.logger.info(
                        f"Ticker transition: {original_ticker} → {resolved_ticker}"
                    )

                # Check if ticker (resolved) is in universe
                if resolved_ticker not in universe_members_set:
                    self.logger.debug(
                        f"Skipping {resolved_ticker} (was {original_ticker}): not in universe"
                    )
                    results["skipped"].append(f"{original_ticker} → {resolved_ticker}")
                    continue

                # Get ESG data for this gvkey
                if gvkey not in esg_by_gvkey:
                    results["no_esg_data"].append(resolved_ticker)
                    continue

                ticker_esg_df = esg_by_gvkey[gvkey].copy()
                ticker_esg_df["ticker"] = resolved_ticker  # Use resolved ticker

                # Apply column renaming to match expected format
                ticker_esg_df = ticker_esg_df.rename(
                    columns={
                        "YearESG": "esg_year",
                        "ESG Score": "esg_score",
                        "Environmental Pillar Score": "environmental_pillar_score",
                        "Social Pillar Score": "social_pillar_score",
                        "Governance Pillar Score": "governance_pillar_score",
                        "SICCD": "sic_code",
                        "Industry_Code": "industry_code",
                        "PERMNO": "permno",
                        "RET": "ret",
                        "Year": "data_year",
                        "YearMonth": "year_month",
                    }
                )

                if dry_run:
                    self.logger.info(
                        f"[DRY RUN] Would save {len(ticker_esg_df)} ESG records for "
                        f"{resolved_ticker} (GVKEY: {gvkey})"
                    )
                else:
                    # Save ESG data using resolved ticker
                    saved_paths = self.save_esg_data(
                        df=ticker_esg_df, ticker=resolved_ticker, exchange=exchange
                    )

                    results["processed"].append(
                        {
                            "ticker": resolved_ticker,
                            "original": (
                                original_ticker
                                if original_ticker != resolved_ticker
                                else None
                            ),
                            "gvkey": gvkey,
                            "records": len(ticker_esg_df),
                            "years": sorted(ticker_esg_df["year"].unique().tolist()),
                            "saved_paths": len(saved_paths),
                        }
                    )

            except Exception as e:
                self.logger.error(
                    f"Error processing {original_ticker} (GVKEY: {gvkey}): {e}"
                )
                results["errors"].append(
                    {"ticker": original_ticker, "gvkey": gvkey, "error": str(e)}
                )

        # Print summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("ESG PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total companies in ESG data: {len(ticker_map)}")
        self.logger.info(f"✓ Successfully processed: {len(results['processed'])}")
        self.logger.info(f"→ Ticker transitions applied: {len(results['mapped'])}")
        self.logger.info(
            f"⊘ Skipped (not in universe/delisted): {len(results['skipped'])}"
        )
        self.logger.info(f"⚠ No ESG data available: {len(results['no_esg_data'])}")
        self.logger.info(f"✗ Errors: {len(results['errors'])}")

        if results["mapped"]:
            self.logger.info("\nTicker Transitions Applied:")
            for orig, new in sorted(results["mapped"].items()):
                self.logger.info(f"  {orig} → {new}")

        if results["errors"]:
            self.logger.info(f"\nErrors ({len(results['errors'])} companies):")
            for error in results["errors"][:10]:  # Show first 10
                self.logger.info(
                    f"  {error['ticker']} (GVKEY: {error['gvkey']}): {error['error']}"
                )
            if len(results["errors"]) > 10:
                self.logger.info(f"  ... and {len(results['errors']) - 10} more")

        return results
