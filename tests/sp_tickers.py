# build_sp_ticker_lists_spdr.py
# Generates S&P 500, S&P 1500 ticker lists using SPDR ETFs:
#   - SPY → S&P 500
#   - MDY → S&P MidCap 400
#   - SLY → S&P SmallCap 600
#
# Outputs:
#   - sp500_tickers.csv (single column: ticker)
#   - sp1500_tickers.csv (single column: ticker)
#   - sp500_constituents_full.csv (metadata)
#   - sp1500_constituents_full.csv (metadata)
#
# Notes:
# - Defaults to Yahoo-style share classes (BRK-B). Pass "dot" to use BRK.B.
# - Tries multiple known SPDR CSV endpoints per fund. Falls back to iShares IVV only for S&P 500.
# - Filters out cash/derivatives; keeps equities with non-empty tickers.

import io
import re
import sys
import time
import typing as t
import datetime as dt

import requests
import pandas as pd


SPDR_CANDIDATE_URLS = [
    # Updated 2024/2025 patterns - most likely current
    "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.csv",
    "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-us-{ticker}.csv",
    "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.csv",
    "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-us-{ticker}.csv",
    # Alternative CDN/content patterns
    "https://www.ssga.com/content/dam/ssga/pdfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.csv",
    "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-global-en-us-{ticker}.csv",
    # Simplified paths (if they restructured)
    "https://www.ssga.com/etfs/fund-data/{ticker}/holdings.csv",
    "https://www.ssga.com/etfs/{ticker}/holdings.csv",
]

# Multiple S&P 500 fallback options
IVV_URL = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=holdings&dataType=fund"
VOO_URL = "https://advisors.vanguard.com/investments/products/voo/vanguard-s-p-500-etf/portfolio-holdings"


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def http_get(url: str, headers: dict | None = None, max_retries: int = 3, timeout: int = 30) -> bytes:
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=h, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_exc = e
            time.sleep(1.25 * attempt)
    raise RuntimeError(f"Failed to download {url}: {last_exc}")


def download_spdr_holdings(ticker: str) -> tuple[bytes, str]:
    """
    Try multiple SPDR CSV endpoints for a given ETF ticker (e.g., SPY, MDY, SLY).
    Returns (content_bytes, source_url).
    """
    ticker = ticker.lower()
    last_err = None
    for template in SPDR_CANDIDATE_URLS:
        url = template.format(ticker=ticker)
        try:
            content = http_get(url)
            # Basic validation: must contain a header line with Ticker or Security
            txt_snippet = content[:2048].decode("utf-8", errors="ignore")
            if "Ticker" in txt_snippet or "Security" in txt_snippet or "Name" in txt_snippet:
                return content, url
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not retrieve SPDR holdings for {ticker.upper()}: {last_err}")


def parse_spdr_holdings(content: bytes) -> tuple[pd.DataFrame, t.Optional[str]]:
    """
    Parse SPDR CSV which often includes a few preface lines before the table.
    Returns (df, as_of_date_str).
    """
    text = content.decode("utf-8", errors="ignore")
    lines = text.splitlines()

    # Try to detect an 'as of' date in preface
    as_of = extract_as_of(lines)

    # Find header row
    header_idx = find_header_index(lines, header_keywords=("Ticker", "Name", "Security"))
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text), sep=None, engine="python")
    return df, as_of


def download_ishares_ivv() -> bytes:
    return http_get(IVV_URL, headers={"Referer": "https://www.ishares.com/"})


def download_vanguard_voo() -> bytes:
    """
    Vanguard VOO S&P 500 ETF holdings as additional fallback.
    Note: Vanguard format may be different, this is experimental.
    """
    voo_csv_url = "https://advisors.vanguard.com/pub/Pdf/p040.pdf"  # This might need adjustment
    # Vanguard often provides Excel/PDF, may need web scraping approach
    # For now, we'll try a generic approach
    try:
        return http_get(VOO_URL, headers={"Referer": "https://advisors.vanguard.com/"})
    except:
        # Fallback: try to find CSV endpoint by inspecting Vanguard's AJAX calls
        raise RuntimeError("Vanguard VOO download not yet implemented - VOO requires web scraping")


def parse_ishares_holdings(content: bytes) -> tuple[pd.DataFrame, t.Optional[str]]:
    text = content.decode("utf-8", errors="ignore")
    lines = text.splitlines()
    as_of = extract_as_of(lines)
    header_idx = find_header_index(lines, header_keywords=("Ticker", "Name", "Weight"))
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text), sep=None, engine="python")
    return df, as_of


def extract_as_of(lines: list[str]) -> t.Optional[str]:
    patterns = [
        r"as of\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
        r"as at\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
        r"holdings.*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})",
    ]
    head = "\n".join(lines[:50])
    for pat in patterns:
        m = re.search(pat, head, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def find_header_index(lines: list[str], header_keywords: tuple[str, ...]) -> int:
    for i, line in enumerate(lines):
        stripped = line.strip("\ufeff \t").strip()
        if not stripped:
            continue
        if any(k in stripped for k in header_keywords) and ("Ticker" in stripped or "Name" in stripped or "Security" in stripped):
            return i
    raise ValueError("Could not locate header row in CSV.")


def normalize_share_class(sym: str, style: str = "yahoo") -> str:
    """
    Normalize share class notation:
    - style='yahoo' → BRK-B
    - style='dot'   → BRK.B
    """
    if not isinstance(sym, str):
        return ""
    s = sym.strip().upper().replace(" ", "")
    if not s or s in {"—", "-", "--", "---"}:
        return ""
    if style == "yahoo":
        s = s.replace("/", "-").replace(".", "-")
    elif style == "dot":
        s = s.replace("/", ".")
    return s


def clean_any_holdings(df: pd.DataFrame, source_fund: str, as_of: t.Optional[str], ticker_style: str) -> pd.DataFrame:
    """
    Clean holdings DF from SPDR or iShares into a standard schema and filter to equities with non-empty tickers.
    """
    # Harmonize common column names
    colmap_candidates = [
        {"Ticker": "ticker"},
        {"Trading Symbol": "ticker"},
        {"Security Ticker": "ticker"},
        {"Name": "security_name"},
        {"Security Name": "security_name"},
        {"Company Name": "security_name"},
        {"Weight": "weight_pct"},
        {"Weight (%)": "weight_pct"},
        {"Cusip": "cusip", "CUSIP": "cusip"},
        {"SEDOL": "sedol"},
        {"ISIN": "isin"},
        {"Sector": "sector"},
        {"GICS Sector": "sector"},
        {"Country": "country"},
        {"Asset Class": "asset_class"},
        {"Asset Type": "asset_class"},
    ]
    for cmap in colmap_candidates:
        df = df.rename(columns={k: v for k, v in cmap.items() if k in df.columns})

    # Filter equities if we have an asset_class
    if "asset_class" in df.columns:
        df = df[df["asset_class"].astype(str).str.contains("Equity", case=False, na=False)]

    # Remove obvious non-equity rows by clues in name or ticker
    if "security_name" in df.columns:
        bad_name = df["security_name"].astype(str).str.contains(
            r"cash|derivative|futures|forward|swap|option|repurchase|money market|treasury bill",
            case=False, na=False
        )
        df = df[~bad_name]

    # Ensure ticker present
    if "ticker" not in df.columns:
        # Some SPDR files may include tickers within parentheses in Name; we won't guess here.
        raise ValueError("Holdings missing a 'Ticker' column after parsing.")

    # Drop rows with missing or placeholder tickers
    df["ticker"] = df["ticker"].astype(str)
    bad_ticker_vals = {"", "—", "-", "--", "---", "CASH", "USD", "US DOLLAR", "XUSD"}
    df = df[~df["ticker"].str.upper().isin(bad_ticker_vals)]

    # Normalize share-class notation
    df["ticker"] = df["ticker"].map(lambda x: normalize_share_class(x, ticker_style))
    df = df[df["ticker"].astype(bool)]

    # Normalize weight if present
    if "weight_pct" in df.columns:
        df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")
    else:
        df["weight_pct"] = pd.NA

    # Attach metadata
    df["source_fund"] = source_fund
    df["as_of_date"] = as_of if as_of else dt.date.today().isoformat()

    # Keep compact schema
    keep = ["ticker", "security_name", "weight_pct", "cusip", "sedol", "isin", "as_of_date", "source_fund"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    return df[keep].reset_index(drop=True)


def union_and_dedupe(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(dfs, ignore_index=True)
    agg = (combined
           .groupby("ticker", as_index=False)
           .agg({
               "security_name": "first",
               "weight_pct": "first",
               "cusip": "first",
               "sedol": "first",
               "isin": "first",
               "as_of_date": "first",
               "source_fund": lambda s: ";".join(sorted(set(map(str, s)))),
           }))
    return agg.rename(columns={"source_fund": "source_funds"})


def write_ticker_only(df: pd.DataFrame, path: str) -> None:
    tickers = sorted(df["ticker"].dropna().astype(str).unique())
    pd.DataFrame({"ticker": tickers}).to_csv(path, index=False)


def main():
    # CLI arg: ticker style
    ticker_style = "yahoo"
    if len(sys.argv) >= 2:
        arg = sys.argv[1].strip().lower()
        if arg in {"yahoo", "dot"}:
            ticker_style = arg
        else:
            print("Unknown style; using default 'yahoo'. Valid: yahoo|dot")

    # Download SPDR holdings
    print("Downloading SPDR holdings: SPY, MDY, SLY ...")
    
    # SPY with multiple fallbacks
    spy_df = None
    spy_sources_tried = []
    
    # Try 1: SPDR SPY
    try:
        print("Attempting SPY download from SPDR...")
        spy_bytes, spy_url = download_spdr_holdings("SPY")
        spy_raw, spy_asof = parse_spdr_holdings(spy_bytes)
        spy_df = clean_any_holdings(spy_raw, source_fund=f"SPY[{spy_url}]", as_of=spy_asof, ticker_style=ticker_style)
        spy_sources_tried.append("SPY[SPDR]")
        print(f"✅ SPY download successful from: {spy_url}")
    except Exception as e:
        print(f"❌ SPY download failed: {e}")
        spy_sources_tried.append(f"SPY[SPDR-FAILED: {str(e)[:50]}]")
    
    # Try 2: iShares IVV fallback
    if spy_df is None:
        try:
            print("Attempting IVV download from iShares...")
            ivv_bytes = download_ishares_ivv()
            ivv_raw, ivv_asof = parse_ishares_holdings(ivv_bytes)
            spy_df = clean_any_holdings(ivv_raw, source_fund="IVV[iShares]", as_of=ivv_asof, ticker_style=ticker_style)
            spy_sources_tried.append("IVV[iShares]")
            print("✅ IVV download successful from iShares")
        except Exception as e:
            print(f"❌ IVV download failed: {e}")
            spy_sources_tried.append(f"IVV[iShares-FAILED: {str(e)[:50]}]")
    
    # If all S&P 500 sources fail, raise error
    if spy_df is None:
        raise RuntimeError(f"Could not download S&P 500 holdings from any source. Tried: {'; '.join(spy_sources_tried)}")
    
    print(f"📊 S&P 500 data source: {spy_df['source_fund'].iloc[0] if len(spy_df) > 0 else 'Unknown'}")

    # MDY
    mdy_bytes, mdy_url = download_spdr_holdings("MDY")
    mdy_raw, mdy_asof = parse_spdr_holdings(mdy_bytes)
    mdy_df = clean_any_holdings(mdy_raw, source_fund=f"MDY[{mdy_url}]", as_of=mdy_asof, ticker_style=ticker_style)

    # SLY
    sly_bytes, sly_url = download_spdr_holdings("SLY")
    sly_raw, sly_asof = parse_spdr_holdings(sly_bytes)
    sly_df = clean_any_holdings(sly_raw, source_fund=f"SLY[{sly_url}]", as_of=sly_asof, ticker_style=ticker_style)

    # Build outputs
    sp500_full = spy_df.copy()
    sp1500_full = union_and_dedupe([spy_df, mdy_df, sly_df])

    print(f"S&P 500 proxy unique tickers (SPY/IVV): {sp500_full['ticker'].nunique()} (expected ~500)")
    print(f"S&P 1500 proxy unique tickers (SPY+MDY+SLY): {sp1500_full['ticker'].nunique()} (expected ~1500)")

    # Write outputs
    write_ticker_only(sp500_full, "sp500_tickers.csv")
    write_ticker_only(sp1500_full, "sp1500_tickers.csv")

    sp500_full.sort_values("ticker").to_csv("sp500_constituents_full.csv", index=False)
    sp1500_full.sort_values("ticker").to_csv("sp1500_constituents_full.csv", index=False)

    print("Done:")
    print("- sp500_tickers.csv (ticker)")
    print("- sp1500_tickers.csv (ticker)")
    print("- sp500_constituents_full.csv (metadata)")
    print("- sp1500_constituents_full.csv (metadata)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
