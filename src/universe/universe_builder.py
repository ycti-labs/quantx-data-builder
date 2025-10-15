"""
Universe builder - orchestrates universe construction from various sources
Supports multiple market universes with configurable data sources
"""

import pandas as pd
import requests
from typing import List, Dict
from pathlib import Path
import logging
from .config_loader import UniverseConfig

logger = logging.getLogger(__name__)


class UniverseBuilder:
    """
    Main class to orchestrate universe building from configured sources
    Handles multiple phases and dependencies between universes
    """

    def __init__(self, config_path: str = "config/universes.yaml"):
        """
        Initialize universe builder
        
        Args:
            config_path: Path to universe configuration YAML file
        """
        self.config = UniverseConfig(config_path)
        self.built_universes = {}

    def build_phase(self, phase: str) -> Dict[str, List[str]]:
        """
        Build all universes in a specific phase
        
        Args:
            phase: Phase name (e.g., "phase_1")
            
        Returns:
            Dictionary mapping universe names to symbol lists
        """
        logger.info(f"🏗️  Building Phase: {phase}")
        universes = self.config.get_universes_for_phase(phase)
        phase_results = {}

        for universe_config in universes:
            name = universe_config['name']

            # Check dependencies
            if not self._check_dependencies(universe_config):
                logger.warning(f"⚠️  Skipping {name}: dependencies not met")
                continue

            # Build universe based on source type
            try:
                symbols = self._build_universe(universe_config)
                phase_results[name] = symbols
                self.built_universes[name] = symbols
                logger.info(f"✅ Built {name}: {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"❌ Failed to build {name}: {str(e)}")

        return phase_results

    def build_all(self) -> Dict[str, List[str]]:
        """
        Build all phases sequentially
        
        Returns:
            Dictionary mapping all universe names to symbol lists
        """
        all_results = {}

        for phase in self.config.get_phases():
            phase_results = self.build_phase(phase)
            all_results.update(phase_results)

        return all_results

    def _build_universe(self, config: Dict) -> List[str]:
        """
        Build a single universe based on its configuration
        
        Args:
            config: Universe configuration dictionary
            
        Returns:
            List of ticker symbols
        """
        source_type = config.get('source_type', 'static')

        if source_type == 'wikipedia':
            return self._fetch_wikipedia_table(config)
        elif source_type == 'excel':
            return self._fetch_excel(config)
        elif source_type == 'csv':
            return self._fetch_csv(config)
        elif source_type == 'static':
            return config.get('symbols', [])
        elif source_type == 'local_file':
            return self._load_local_file(config)
        else:
            raise ValueError(f"Unknown source_type: {source_type}")

    def _fetch_wikipedia_table(self, config: Dict) -> List[str]:
        """Fetch symbols from Wikipedia table (e.g., S&P 500)"""
        url = config['url']
        table_index = config.get('table_index', 0)
        symbol_column = config.get('symbol_column', 'Symbol')

        logger.info(f"📥 Fetching from Wikipedia: {url}")
        
        tables = pd.read_html(url)
        df = tables[table_index]

        symbols = df[symbol_column].tolist()
        symbols = [str(s).strip().replace('.', '-') for s in symbols]

        # Add suffix if specified
        suffix = config.get('symbol_suffix', '')
        if suffix:
            symbols = [f"{s}{suffix}" for s in symbols]

        return symbols

    def _fetch_excel(self, config: Dict) -> List[str]:
        """Fetch symbols from Excel file (e.g., HKEX official list)"""
        url = config['url']
        symbol_column = config.get('symbol_column', 'Stock Code')

        logger.info(f"📥 Fetching from Excel: {url}")

        # Download Excel file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Read Excel from memory
        df = pd.read_excel(response.content)

        symbols = df[symbol_column].dropna().unique().tolist()

        # Format symbols (e.g., pad Hong Kong stocks to 4 digits)
        if config.get('format') == 'hk_pad':
            symbols = [str(int(s)).zfill(4) for s in symbols if pd.notna(s)]

        # Add suffix if specified
        suffix = config.get('symbol_suffix', '')
        if suffix:
            symbols = [f"{s}{suffix}" for s in symbols]

        return symbols

    def _fetch_csv(self, config: Dict) -> List[str]:
        """Fetch symbols from CSV file"""
        url = config['url']
        symbol_column = config.get('symbol_column', 'Symbol')

        logger.info(f"📥 Fetching from CSV: {url}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        df = pd.read_csv(response.content)
        symbols = df[symbol_column].dropna().unique().tolist()

        symbols = [str(s).strip() for s in symbols]

        # Add suffix if specified
        suffix = config.get('symbol_suffix', '')
        if suffix:
            symbols = [f"{s}{suffix}" for s in symbols]

        return symbols

    def _load_local_file(self, config: Dict) -> List[str]:
        """Load symbols from local text file (one symbol per line)"""
        path = Path(config['path'])

        logger.info(f"📂 Loading from local file: {path}")

        if not path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")

        with open(path, 'r') as f:
            symbols = [line.strip() for line in f if line.strip()]

        return symbols

    def _check_dependencies(self, universe_config: dict) -> bool:
        """
        Check if all dependencies are built
        
        Args:
            universe_config: Universe configuration dictionary
            
        Returns:
            True if all dependencies are satisfied, False otherwise
        """
        depends_on = universe_config.get('depends_on', [])
        return all(dep in self.built_universes for dep in depends_on)
