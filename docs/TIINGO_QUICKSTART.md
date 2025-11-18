# Tiingo API Quick Start

## Get Your Free API Key

1. Visit: https://www.tiingo.com
2. Sign up (free)
3. Get API key: https://www.tiingo.com/account/api/token

## Set Environment Variable

```bash
export TIINGO_API_KEY="your_api_key_here"
```

## Test It

```bash
source .venv/bin/activate
python examples/test_tiingo_fetch.py
```

## Free Tier Limits

- **500 requests/hour**
- **1,000 requests/day**
- Historical data from 1990s
- Splits & dividends included

## Example Output

```json
{
  "date": "2024-01-02",
  "open": 185.64,
  "high": 186.95,
  "low": 184.42,
  "close": 185.14,
  "volume": 82488100,
  "adjClose": 185.14,
  "divCash": 0.0,
  "splitFactor": 1.0
}
```

## Need Help?

See `docs/TIINGO_MIGRATION.md` for full documentation.
