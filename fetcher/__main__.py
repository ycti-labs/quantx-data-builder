"""
FinSight Data Fetcher CLI Entry Point

Enables running the CLI as a module: python -m fetcher.cli
"""

from .cli import app

if __name__ == "__main__":
    app()