"""
Universe configuration loader
Loads and manages universe configurations from YAML files
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class UniverseConfig:
    """Load and manage universe configurations from YAML"""

    def __init__(self, config_path: str = "config/universes.yaml"):
        """
        Initialize universe configuration
        
        Args:
            config_path: Path to universes YAML configuration file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        if not self.config_path.exists():
            logger.warning(f"Config file not found: {self.config_path}")
            return {"universes": {}}

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Loaded universe config from {self.config_path}")
        return config

    def get_phases(self) -> List[str]:
        """
        Get all phase names in order
        
        Returns:
            List of phase names (e.g., ["phase_1", "phase_2"])
        """
        return list(self.config.get('universes', {}).keys())

    def get_universes_for_phase(self, phase: str) -> List[Dict[str, Any]]:
        """
        Get all enabled universes for a specific phase
        
        Args:
            phase: Phase name (e.g., "phase_1")
            
        Returns:
            List of universe configurations
        """
        universes = self.config.get('universes', {}).get(phase, [])
        return [u for u in universes if u.get('enabled', True)]

    def get_universe_config(self, name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific universe by name
        
        Args:
            name: Universe name (e.g., "us_sp500")
            
        Returns:
            Universe configuration dictionary
            
        Raises:
            ValueError: If universe not found
        """
        for phase in self.config.get('universes', {}).values():
            for universe in phase:
                if universe['name'] == name:
                    return universe
        raise ValueError(f"Universe '{name}' not found in config")
