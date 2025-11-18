"""
Configuration Loader for Data Fetcher

Loads and validates settings from config/settings.yaml
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class TiingoConfig:
    """Tiingo API configuration"""
    api_key: str
    base_url: str = "https://api.tiingo.com"

    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.api_key or self.api_key == "${TIINGO_API_KEY}":
            # Try to get from environment variable
            env_key = os.getenv("TIINGO_API_KEY")
            if env_key:
                self.api_key = env_key
            else:
                raise ValueError(
                    "TIINGO_API_KEY not set. Either:\n"
                    "  1. Set environment variable: export TIINGO_API_KEY=your_key\n"
                    "  2. Update config/settings.yaml with your API key\n"
                    "  Get free API key at: https://www.tiingo.com/account/api/token"
                )


@dataclass
class UniverseConfig:
    """Configuration for a single universe"""
    name: str
    enabled: bool
    start_date: str

    def __post_init__(self):
        """Validate configuration after initialization"""
        # Validate date format
        try:
            datetime.strptime(self.start_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Invalid start_date format for {self.name}: {self.start_date}. "
                "Expected YYYY-MM-DD"
            )


@dataclass
class StorageConfig:
    """Storage configuration"""
    compression: str
    azure: Dict[str, Any]
    local: Dict[str, Any]


@dataclass
class FetcherSettings:
    """Fetcher configuration"""
    tiingo: TiingoConfig
    max_workers: int
    lookback_days: int
    chunk_size: int
    fetch_actions: bool
    retry: Dict[str, Any]


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str
    format: str


@dataclass
class ScheduleConfig:
    """Schedule configuration"""
    cron: str
    description: str


class FetcherConfig:
    """
    Main configuration class that loads and provides access to settings.yaml
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        """
        Load configuration from YAML file

        Args:
            config_path: Path to settings.yaml file
        """
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(self.config_path, 'r') as f:
            self._raw_config = yaml.safe_load(f)

        self._parse_config()
        logger.info(f"✅ Loaded configuration from {config_path}")

    def _parse_config(self):
        """Parse raw YAML config into structured dataclasses"""
        # Parse universes
        self.universes = {}
        universes_raw = self._raw_config.get("universes", {})

        for universe_name, universe_settings in universes_raw.items():
            # Handle list format from YAML (each item is a dict with one key)
            if isinstance(universe_settings, list):
                # Flatten list of single-key dicts into one dict
                config_dict = {}
                for item in universe_settings:
                    if isinstance(item, dict):
                        config_dict.update(item)
            else:
                config_dict = universe_settings

            self.universes[universe_name] = UniverseConfig(
                name=universe_name,
                enabled=config_dict.get("enable", False),
                start_date=config_dict.get("start_date", "2000-01-01")
            )

        # Parse storage config
        storage_raw = self._raw_config.get("storage", {})
        self.storage = StorageConfig(
            compression=storage_raw.get("compression", "snappy"),
            azure=storage_raw.get("azure", {}),
            local=storage_raw.get("local", {})
        )

        # Parse fetcher config
        fetcher_raw = self._raw_config.get("fetcher", {})

        # Parse Tiingo configuration
        tiingo_raw = fetcher_raw.get("tiingo", {})
        tiingo_config = TiingoConfig(
            api_key=tiingo_raw.get("api_key", "${TIINGO_API_KEY}"),
            base_url=tiingo_raw.get("base_url", "https://api.tiingo.com")
        )

        self.fetcher = FetcherSettings(
            tiingo=tiingo_config,
            max_workers=fetcher_raw.get("max_workers", 10),
            lookback_days=fetcher_raw.get("lookback_days", 5),
            chunk_size=fetcher_raw.get("chunk_size", 100),
            fetch_actions=fetcher_raw.get("fetch_actions", True),
            retry=fetcher_raw.get("retry", {})
        )

        # Parse logging config
        logging_raw = self._raw_config.get("logging", {})
        self.logging = LoggingConfig(
            level=logging_raw.get("level", "INFO"),
            format=logging_raw.get("format", "json")
        )

        # Parse schedules
        schedules_raw = self._raw_config.get("schedules", {})
        self.schedules = {}
        for schedule_name, schedule_data in schedules_raw.items():
            self.schedules[schedule_name] = ScheduleConfig(
                cron=schedule_data.get("cron", ""),
                description=schedule_data.get("description", "")
            )

    def get_enabled_universes(self) -> List[UniverseConfig]:
        """
        Get list of enabled universes

        Returns:
            List of UniverseConfig objects for enabled universes
        """
        return [
            config for config in self.universes.values()
            if config.enabled
        ]

    def get_universe_config(self, name: str) -> Optional[UniverseConfig]:
        """
        Get configuration for a specific universe

        Args:
            name: Universe name (e.g., "SP500", "NASDAQ100")

        Returns:
            UniverseConfig object or None if not found
        """
        return self.universes.get(name)

    def is_universe_enabled(self, name: str) -> bool:
        """
        Check if a universe is enabled

        Args:
            name: Universe name

        Returns:
            True if universe is enabled, False otherwise
        """
        config = self.get_universe_config(name)
        return config.enabled if config else False

    def get_storage_type(self) -> str:
        """
        Determine storage type based on configuration

        Returns:
            "azure" or "local"
        """
        # If Azure credentials are configured, use Azure
        if self.storage.azure.get("account_name"):
            return "azure"
        return "local"

    def __repr__(self) -> str:
        enabled = [u.name for u in self.get_enabled_universes()]
        return (
            f"FetcherConfig(\n"
            f"  enabled_universes={enabled},\n"
            f"  storage_type={self.get_storage_type()},\n"
            f"  max_workers={self.fetcher.max_workers}\n"
            f")"
        )
