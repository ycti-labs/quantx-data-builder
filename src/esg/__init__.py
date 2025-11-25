"""
ESG module

Provides ESG data management and factor construction capabilities:
- ESGManager: Load and manage ESG data for universe members
- ESGFactorBuilder: Construct long-short factor portfolios from ESG signals
"""

from .esg_factor import ESGFactorBuilder
from .esg_manager import ESGManager

__all__ = ["ESGManager", "ESGFactorBuilder"]
