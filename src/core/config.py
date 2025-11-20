"""
Configuration management for QuantX Data Builder

Provides a flexible configuration system with:
- Nested key access via dot notation (e.g., "fetcher.tiingo.api_key")
- Environment variable substitution
- Default value fallback
- Type-safe access methods
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

logger = logging.getLogger(__name__)


class Config:
    """
    Configuration manager with dot notation access and environment variable support

    Example:
        config = Config("config/settings.yaml")
        api_key = config.get("fetcher.tiingo.api_key")
        max_workers = config.get_int("fetcher.max_workers", default=5)
    """

    def __init__(self, config_file: Union[str, Path]):
        """
        Initialize configuration from YAML file

        Args:
            config_file: Path to YAML configuration file

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        self.config_file = Path(config_file)

        if not self.config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file}")

        self.cfg = self._load_config(self.config_file)
        logger.info(f"Loaded configuration from {config_file}")

    def _load_config(self, config_file: Path) -> Dict[str, Any]:
        """
        Load and parse YAML configuration file

        Args:
            config_file: Path to config file

        Returns:
            Parsed configuration dictionary
        """
        try:
            with open(config_file, 'r', encoding='utf-8') as file:
                # Use safe_load instead of load for security
                config = yaml.safe_load(file)
                return config or {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config file {config_file}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading config file {config_file}: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation

        Supports environment variable substitution:
        - ${ENV_VAR_NAME} will be replaced with environment variable value

        Args:
            key: Dot-separated key path (e.g., "fetcher.tiingo.api_key")
            default: Default value if key not found

        Returns:
            Configuration value or default

        Example:
            api_key = config.get("fetcher.tiingo.api_key")
            workers = config.get("fetcher.max_workers", default=5)
        """
        keys = key.split(".")
        value = self.cfg

        # Navigate through nested dictionary
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        # Handle environment variable substitution
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                logger.warning(
                    f"Environment variable {env_var} not set for config key '{key}', "
                    f"using default: {default}"
                )
                return default
            return env_value

        return value

    def get_int(self, key: str, default: int = 0) -> int:
        """
        Get integer configuration value

        Args:
            key: Dot-separated key path
            default: Default value if key not found or conversion fails

        Returns:
            Integer value or default
        """
        value = self.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert config value '{key}={value}' to int, using default: {default}")
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        """
        Get float configuration value

        Args:
            key: Dot-separated key path
            default: Default value if key not found or conversion fails

        Returns:
            Float value or default
        """
        value = self.get(key)
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert config value '{key}={value}' to float, using default: {default}")
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """
        Get boolean configuration value

        Recognizes: true/false, yes/no, 1/0 (case-insensitive)

        Args:
            key: Dot-separated key path
            default: Default value if key not found or conversion fails

        Returns:
            Boolean value or default
        """
        value = self.get(key)
        if value is None:
            return default

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            value_lower = value.lower()
            if value_lower in ('true', 'yes', '1'):
                return True
            elif value_lower in ('false', 'no', '0'):
                return False

        if isinstance(value, (int, float)):
            return bool(value)

        logger.warning(f"Cannot convert config value '{key}={value}' to bool, using default: {default}")
        return default

    def get_path(self, key: str, default: Optional[Path] = None) -> Optional[Path]:
        """
        Get Path configuration value

        Args:
            key: Dot-separated key path
            default: Default value if key not found

        Returns:
            Path object or default
        """
        value = self.get(key)
        if value is None:
            return default
        try:
            return Path(value)
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert config value '{key}={value}' to Path, using default: {default}")
            return default

    def get_list(self, key: str, default: Optional[list] = None) -> list:
        """
        Get list configuration value

        Args:
            key: Dot-separated key path
            default: Default value if key not found

        Returns:
            List value or default
        """
        value = self.get(key)
        if value is None:
            return default or []
        if isinstance(value, list):
            return value
        logger.warning(f"Config value '{key}={value}' is not a list, using default: {default}")
        return default or []

    def get_dict(self, key: str, default: Optional[dict] = None) -> dict:
        """
        Get dictionary configuration value

        Args:
            key: Dot-separated key path
            default: Default value if key not found

        Returns:
            Dictionary value or default
        """
        value = self.get(key)
        if value is None:
            return default or {}
        if isinstance(value, dict):
            return value
        logger.warning(f"Config value '{key}={value}' is not a dict, using default: {default}")
        return default or {}

    def has(self, key: str) -> bool:
        """
        Check if configuration key exists

        Args:
            key: Dot-separated key path

        Returns:
            True if key exists, False otherwise
        """
        keys = key.split(".")
        value = self.cfg

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Get full configuration as dictionary

        Returns:
            Complete configuration dictionary
        """
        return self.cfg.copy()

    def __repr__(self) -> str:
        """String representation of Config"""
        return f"Config(file={self.config_file}, keys={list(self.cfg.keys())})"


# Global config instance (optional - can be imported for convenience)
# Applications can choose to use this or create their own Config instances
_global_config: Optional[Config] = None


def get_config(config_file: Optional[Union[str, Path]] = None) -> Config:
    """
    Get or create global config instance

    Args:
        config_file: Path to config file (only used on first call)

    Returns:
        Global Config instance
    """
    global _global_config

    if _global_config is None:
        if config_file is None:
            config_file = "config/settings.yaml"
        _global_config = Config(config_file)

    return _global_config
