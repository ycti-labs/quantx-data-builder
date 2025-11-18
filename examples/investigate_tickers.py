#!/usr/bin/env python3
"""
Investigate Missing Tickers

Research what happened to tickers that are missing from the data.
Checks for ticker changes, mergers, acquisitions, delistings, etc.
"""

import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.market_data import FetcherConfig, PriceDataManager

# Known ticker changes and corporate actions for S&P 500 companies
TICKER_HISTORY = {
    'ABC': {
        'status': 'Acquired',
        'description': 'AmerisourceBergen',
        'action': 'Merged with Alliance Boots in 2013, became Walgreens Boots Alliance',
        'new_ticker': None,
        'note': 'Company was renamed to Cencora in 2023 but ticker stayed ABC until removed from S&P 500',
        'sp500_removal': '2013 or earlier'
    },
    'ADS': {
        'status': 'Ticker Change',
        'description': 'Alliance Data Systems',
        'action': 'Changed ticker to ALLY',
        'new_ticker': 'ALLY',
        'note': 'Company changed name to Bread Financial Holdings',
        'sp500_removal': '2021'
    },
    'ANTM': {
        'status': 'Ticker Change',
        'description': 'Anthem Inc.',
        'action': 'Changed ticker to ELV',
        'new_ticker': 'ELV',
        'note': 'Rebranded as Elevance Health in 2022',
        'date': '2022-06-28'
    },
    'BF.B': {
        'status': 'Special Character',
        'description': 'Brown-Forman Class B',
        'action': 'Period/dot in ticker symbol causes API issues',
        'new_ticker': 'BF-B or BF.B',
        'note': 'Try fetching as "BF-B" or URL encode the period',
        'workaround': 'Use BF-B or BF_B depending on data source'
    },
    'BLL': {
        'status': 'Unknown',
        'description': 'Ball Corporation',
        'action': 'Should be available in Tiingo',
        'new_ticker': 'BLL',
        'note': 'Active company, check if API call failed',
        'sp500_status': 'Currently in S&P 500'
    },
    'BRK.B': {
        'status': 'Special Character',
        'description': 'Berkshire Hathaway Class B',
        'action': 'Period/dot in ticker symbol causes API issues',
        'new_ticker': 'BRK-B or BRK.B',
        'note': 'Try fetching as "BRK-B" or URL encode the period',
        'workaround': 'Use BRK-B or BRK_B depending on data source'
    },
    'COG': {
        'status': 'Active',
        'description': 'Cabot Oil & Gas',
        'action': 'Merged with Cimarex Energy to form Coterra Energy',
        'new_ticker': 'CTRA',
        'note': 'Merger completed in October 2021',
        'date': '2021-10-01'
    },
    'FB': {
        'status': 'Ticker Change',
        'description': 'Facebook Inc.',
        'action': 'Changed ticker to META',
        'new_ticker': 'META',
        'note': 'Rebranded as Meta Platforms in 2021',
        'date': '2021-10-28'
    },
    'FBHS': {
        'status': 'Active',
        'description': 'Fortune Brands Home & Security',
        'action': 'Split into multiple companies',
        'new_ticker': 'FBIN',
        'note': 'Spun off into Fortune Brands Innovations (FBIN) in 2022',
        'date': '2022-10-03'
    },
    'FLT': {
        'status': 'Acquired',
        'description': 'FleetCor Technologies',
        'action': 'Acquired by private equity',
        'new_ticker': None,
        'note': 'Taken private in 2021, delisted',
        'date': '2021'
    },
    'FRC': {
        'status': 'Bankrupt/Delisted',
        'description': 'First Republic Bank',
        'action': 'Bank failure, seized by FDIC',
        'new_ticker': None,
        'note': 'Failed in May 2023, assets sold to JPMorgan Chase',
        'date': '2023-05-01',
        'sp500_removal': '2023-05'
    },
    'GPS': {
        'status': 'Active',
        'description': 'Gap Inc.',
        'action': 'Should be available in Tiingo',
        'new_ticker': 'GPS',
        'note': 'Removed from S&P 500 in 2020, but still public',
        'sp500_removal': '2020-09'
    },
    'HFC': {
        'status': 'Acquired',
        'description': 'HollyFrontier Corporation',
        'action': 'Merged with Sinclair Oil to form HF Sinclair',
        'new_ticker': 'HF',
        'note': 'Merger completed in 2022',
        'date': '2022-07-01'
    },
    'PEAK': {
        'status': 'Acquired',
        'description': 'Healthpeak Properties',
        'action': 'May have been acquired or ticker change',
        'new_ticker': 'DOC',
        'note': 'Check if this is Physicians Realty Trust merger',
        'sp500_status': 'Research needed'
    },
    'PKI': {
        'status': 'Active',
        'description': 'PerkinElmer',
        'action': 'Split into multiple companies',
        'new_ticker': None,
        'note': 'Split in 2023: Revvity (RVTY) and PerkinElmer (private)',
        'date': '2023'
    },
    'RE': {
        'status': 'Active',
        'description': 'Everest Re Group',
        'action': 'Should be available in Tiingo',
        'new_ticker': 'RE',
        'note': 'Currently in S&P 500, check if API call failed',
        'sp500_status': 'Currently in S&P 500'
    },
    'WLTW': {
        'status': 'Ticker Change',
        'description': 'Willis Towers Watson',
        'action': 'Changed ticker to WTW',
        'new_ticker': 'WTW',
        'note': 'Simplified ticker symbol',
        'date': '2020'
    }
}


def investigate_ticker(ticker: str, api_key: str) -> Dict:
    """
    Investigate a single ticker to see what data is available

    Args:
        ticker: Ticker symbol
        api_key: API key for data source

    Returns:
        Dictionary with investigation results
    """
    builder = PriceDataManager(api_key=api_key, data_root="data/curated")

    result = {
        'ticker': ticker,
        'found_in_tiingo': False,
        'alternative_ticker': None,
        'data_available': False,
        'error': None
    }

    # Try fetching a small sample
    try:
        df = builder.fetch_eod(ticker, start_date='2020-01-01', end_date='2020-01-31')
        if not df.empty:
            result['found_in_tiingo'] = True
            result['data_available'] = True
            result['sample_rows'] = len(df)
    except Exception as e:
        result['error'] = str(e)

    # Try alternative ticker if known
    if ticker in TICKER_HISTORY:
        info = TICKER_HISTORY[ticker]
        if info.get('new_ticker'):
            result['alternative_ticker'] = info['new_ticker']
            try:
                df = builder.fetch_eod(
                    info['new_ticker'],
                    start_date='2020-01-01',
                    end_date='2020-01-31'
                )
                if not df.empty:
                    result['alternative_found'] = True
                    result['alternative_rows'] = len(df)
            except Exception as e:
                result['alternative_error'] = str(e)

    return result


def print_ticker_analysis(missing_symbols: List[str]):
    """
    Print detailed analysis of missing tickers

    Args:
        missing_symbols: List of ticker symbols to analyze
    """
    print()
    print("=" * 80)
    print("Missing Tickers Analysis")
    print("=" * 80)
    print()
    print(f"Analyzing {len(missing_symbols)} missing tickers from 2014-2024 period...")
    print()

    # Group by status
    ticker_changes = []
    special_chars = []
    acquired = []
    delisted = []
    active_should_work = []
    unknown = []

    for ticker in missing_symbols:
        if ticker in TICKER_HISTORY:
            info = TICKER_HISTORY[ticker]
            status = info['status']

            if status == 'Ticker Change':
                ticker_changes.append((ticker, info))
            elif status == 'Special Character':
                special_chars.append((ticker, info))
            elif status == 'Acquired':
                acquired.append((ticker, info))
            elif status in ['Bankrupt/Delisted', 'Delisted']:
                delisted.append((ticker, info))
            elif status == 'Active':
                active_should_work.append((ticker, info))
            else:
                unknown.append((ticker, info))
        else:
            unknown.append((ticker, {'description': 'Unknown', 'note': 'Research needed'}))

    # Print each category
    if ticker_changes:
        print("=" * 80)
        print(f"🔄 TICKER CHANGES ({len(ticker_changes)} tickers)")
        print("=" * 80)
        print()
        print("These companies changed their ticker symbols.")
        print("Solution: Fetch data using the NEW ticker symbol.")
        print()
        for ticker, info in ticker_changes:
            print(f"  {ticker} → {info['new_ticker']}")
            print(f"    Company: {info['description']}")
            print(f"    Change: {info['action']}")
            print(f"    Date: {info.get('date', 'Unknown')}")
            print(f"    Note: {info['note']}")
            print()

    if special_chars:
        print("=" * 80)
        print(f"⚠️  SPECIAL CHARACTERS ({len(special_chars)} tickers)")
        print("=" * 80)
        print()
        print("These tickers contain periods/dots that may cause API issues.")
        print("Solution: Try alternative ticker formats.")
        print()
        for ticker, info in special_chars:
            print(f"  {ticker}")
            print(f"    Company: {info['description']}")
            print(f"    Problem: {info['action']}")
            print(f"    Workaround: {info.get('workaround', 'Try URL encoding')}")
            print(f"    Note: {info['note']}")
            print()

    if acquired:
        print("=" * 80)
        print(f"🤝 ACQUIRED/MERGED ({len(acquired)} tickers)")
        print("=" * 80)
        print()
        print("These companies were acquired or merged.")
        print("Solution: Historical data may exist, or use new ticker if applicable.")
        print()
        for ticker, info in acquired:
            print(f"  {ticker}")
            print(f"    Company: {info['description']}")
            print(f"    Action: {info['action']}")
            if info.get('new_ticker'):
                print(f"    New ticker: {info['new_ticker']}")
            print(f"    Date: {info.get('date', 'Unknown')}")
            print(f"    Note: {info['note']}")
            print()

    if delisted:
        print("=" * 80)
        print(f"❌ DELISTED/BANKRUPT ({len(delisted)} tickers)")
        print("=" * 80)
        print()
        print("These companies failed or were delisted.")
        print("Solution: Historical data may be unavailable or limited.")
        print()
        for ticker, info in delisted:
            print(f"  {ticker}")
            print(f"    Company: {info['description']}")
            print(f"    Action: {info['action']}")
            print(f"    Date: {info.get('date', 'Unknown')}")
            print(f"    Note: {info['note']}")
            print()

    if active_should_work:
        print("=" * 80)
        print(f"✅ ACTIVE (Should Work) ({len(active_should_work)} tickers)")
        print("=" * 80)
        print()
        print("These are active companies that SHOULD be available in Tiingo.")
        print("Solution: Retry fetch, check API limits, or verify ticker spelling.")
        print()
        for ticker, info in active_should_work:
            print(f"  {ticker}")
            print(f"    Company: {info['description']}")
            print(f"    Status: {info.get('sp500_status', 'Check status')}")
            print(f"    Note: {info['note']}")
            print()

    if unknown:
        print("=" * 80)
        print(f"❓ UNKNOWN ({len(unknown)} tickers)")
        print("=" * 80)
        print()
        print("These tickers need further research.")
        print()
        for ticker, info in unknown:
            print(f"  {ticker}")
            print(f"    {info.get('description', 'Unknown company')}")
            print(f"    {info.get('note', 'Research needed')}")
            print()


def generate_fetch_commands(missing_symbols: List[str]):
    """
    Generate fetch commands for available alternatives

    Args:
        missing_symbols: List of ticker symbols
    """
    print()
    print("=" * 80)
    print("🚀 RECOMMENDED ACTIONS")
    print("=" * 80)
    print()

    fetch_list = []
    special_handling = []

    for ticker in missing_symbols:
        if ticker in TICKER_HISTORY:
            info = TICKER_HISTORY[ticker]

            if info['status'] == 'Ticker Change' and info.get('new_ticker'):
                fetch_list.append(info['new_ticker'])
                print(f"  ✅ Replace {ticker} with {info['new_ticker']}")

            elif info['status'] == 'Special Character':
                new_ticker = ticker.replace('.', '-')
                special_handling.append((ticker, new_ticker))
                print(f"  ⚠️  Try {ticker} as '{new_ticker}'")

            elif info['status'] == 'Acquired' and info.get('new_ticker'):
                fetch_list.append(info['new_ticker'])
                print(f"  🤝 Use {info['new_ticker']} (successor to {ticker})")

            elif info['status'] == 'Active':
                fetch_list.append(ticker)
                print(f"  🔄 Retry {ticker} (should work)")

    print()
    print("=" * 80)
    print("Python Code to Fetch Alternatives:")
    print("=" * 80)
    print()
    print("```python")
    print("from src.market_data import FetcherConfig, PriceDataManager")
    print()
    print("config = FetcherConfig('config/settings.yaml')")
    print("builder = PriceDataManager(api_key=config.fetcher.tiingo.api_key)")
    print()
    print("# Fetch with new tickers")
    print(f"fetch_these = {fetch_list}")
    print()
    print("for symbol in fetch_these:")
    print("    try:")
    print("        builder.fetch_and_save(")
    print("            symbol=symbol,")
    print("            start_date='2014-01-01',")
    print("            end_date='2024-12-31',")
    print("            exchange='us'")
    print("        )")
    print("        print(f'✅ {symbol}')")
    print("    except Exception as e:")
    print("        print(f'❌ {symbol}: {e}')")
    print()

    if special_handling:
        print("# Special handling for tickers with dots")
        for old, new in special_handling:
            print(f"# Try '{new}' instead of '{old}'")
        print()

    print("```")
    print()


def main():
    """Main function"""
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 22 + "Ticker Investigation Tool" + " " * 31 + "║")
    print("║" + " " * 15 + "Research Missing Tickers (2014-2024)" + " " * 27 + "║")
    print("╚" + "=" * 78 + "╝")

    missing_symbols = [
        'ABC', 'ADS', 'ANTM', 'BF.B', 'BLL', 'BRK.B', 'COG',
        'FB', 'FBHS', 'FLT', 'FRC', 'GPS', 'HFC', 'PEAK',
        'PKI', 'RE', 'WLTW'
    ]

    # Print analysis
    print_ticker_analysis(missing_symbols)

    # Generate fetch commands
    generate_fetch_commands(missing_symbols)

    return 0


if __name__ == "__main__":
    sys.exit(main())
