from tiingo import TiingoClient
from core.config import Config
from core.ticker_mapper import TickerMapper
from universe import SP500Universe, universe
from market import PriceManager

config = Config("config/settings.yaml")
sp500_universe = SP500Universe(
    data_root=config.get("storage.local.root_path"),
)
tiingo_client = TiingoClient({
    'api_key': config.get("fetcher.tiingo.api_key"),
    'session': True
})


# Initialize
price_mgr = PriceManager(tiingo=tiingo_client, universe=sp500_universe)
mapper = TickerMapper()

# List of symbols with issues
symbols_to_fix = [
    # Rebrands
    'FB',       # → META
    'ANTM',     # → ELV
    'ABC',      # → COR
    'FLT',      # → CPAY
    'PKI',      # → RVTY
    'TMK',      # → GL
    'COG',      # → CTRA
    'HFC',      # → DINO
    'WLTW',     # → WTW
    'RE',       # → EG
    'ADS',      # → BFH
    'PEAK',     # → DOC

    # Multi-step transitions
    'CBS',      # → VIAC → PARA

    # Mergers
    'CCE',      # → CCEP
    'FBHS',     # → FBIN

    # Class share formats
    'BRK.B',    # → BRK-B
    'BF.B',     # → BF-B

    # These will be skipped (delisted/acquired)
    'LIFE',     # Acquired by TMO
    'SNDK',     # Acquired by WDC
    'POM',      # Acquired by EXC
    'FRC',      # Failed bank
    'ENDP',     # Bankruptcy
    'MNK',      # Bankruptcy
    'WIN',      # Bankruptcy
    'ESV',      # Complex restructuring
    'IGT',      # Merger issues
    'GPS',      # Check if needed
    'BLL',      # Check if needed
]

# daily
daily_results = price_mgr.fetch_missing_with_ticker_resolution(
    symbols=symbols_to_fix,
    frequency='daily',
    start_date=config.get('universe.sp500.start_date'),
    end_date=config.get('universe.sp500.end_date'),
    ticker_mapper=mapper,
    dry_run=False
)

# weekly
weekly_results = price_mgr.fetch_missing_with_ticker_resolution(
    symbols=symbols_to_fix,
    frequency='weekly',
    start_date=config.get('universe.sp500.start_date'),
    end_date=config.get('universe.sp500.end_date'),
    ticker_mapper=mapper,
    dry_run=False
)

monthly_results = price_mgr.fetch_missing_with_ticker_resolution(
    symbols=symbols_to_fix,
    frequency='monthly',
    start_date=config.get('universe.sp500.start_date'),
    end_date=config.get('universe.sp500.end_date'),
    ticker_mapper=mapper,
    dry_run=False
)