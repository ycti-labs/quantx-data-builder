"""
Diagnostic analysis for high Sharpe ratios
"""

from pathlib import Path

import numpy as np
import pandas as pd

print("=" * 80)
print("ROOT CAUSE ANALYSIS: WHY ARE SHARPE RATIOS SO HIGH?")
print("=" * 80)
print()

data_root = Path("data")

# Get all expected returns
er_file = data_root / "results" / "expected_returns" / "expected_returns.parquet"
er = pd.read_parquet(er_file)
latest_er = er.groupby("ticker").last()

# Get factor premia
lambda_mkt = 0.011632  # 13.96% annual
lambda_esg = -0.003716  # -4.46% annual

print("FACTOR PREMIA (from extend_capm.py)")
print("-" * 80)
print(f"λ_market = {lambda_mkt:.6f} monthly ({lambda_mkt*12*100:.2f}% annual)")
print(f"λ_ESG    = {lambda_esg:.6f} monthly ({lambda_esg*12*100:.2f}% annual)")
print()

print("PROBLEM #1: HIGH MARKET PREMIUM")
print("-" * 80)
print(f"Market premium of 13.96% annual is historically HIGH")
print(f"Historical equity premium: ~6-8% annual (long-term average)")
print(f"Our premium is almost DOUBLE the historical average")
print()
print("Why? Short sample period (2016-2024) includes:")
print("  - Strong bull market 2016-2019")
print("  - V-shaped COVID recovery 2020-2021")
print("  - Tech boom periods")
print()

print("PROBLEM #2: NEGATIVE ESG PREMIUM WITH EXTREME BETAS")
print("-" * 80)
print(f'Negative ESG premium means "brown" stocks outperformed')
print(f"Stocks with NEGATIVE ESG betas get REWARDED")
print()
print("Example: Stock with β_ESG = -7.69")
print(f"  ESG contribution = -7.69 × ({lambda_esg:.6f})")
print(f"                   = -7.69 × (-0.00372)")
print(f"                   = {-7.69 * lambda_esg:.6f} monthly")
print(f"                   = {-7.69 * lambda_esg * 12 * 100:.2f}% annual (!)")
print()

print("PROBLEM #3: COMPOUNDING AMPLIFIES EXTREME RETURNS")
print("-" * 80)
print("Example: ER_monthly = 5.5% → ER_annual = (1.055)^12 - 1 = 89.7%")
print("         ER_monthly = 6.0% → ER_annual = (1.060)^12 - 1 = 101.2%")
print("Compounding magnifies already-high monthly returns")
print()

print("TOP 10 EXTREME EXPECTED RETURNS")
print("-" * 80)
top10 = latest_er.nlargest(10, "ER_annual")
for ticker in top10.index:
    row = top10.loc[ticker]
    mkt_contrib = row["beta_market"] * lambda_mkt
    esg_contrib = row["beta_ESG"] * lambda_esg
    rf_contrib = row["RF"]

    print(f'{ticker:6s}: {row["ER_annual"]*100:6.1f}% annual')
    print(f'        β_mkt={row["beta_market"]:6.2f} → {mkt_contrib*12*100:6.1f}% ann')
    print(f'        β_ESG={row["beta_ESG"]:6.2f} → {esg_contrib*12*100:6.1f}% ann')
    print(f"        RF={rf_contrib*12*100:6.1f}%")
print()

print("=" * 80)
print("SHARPE RATIO IMPACT")
print("=" * 80)
print()
print("High expected returns lead to high Sharpe ratios:")
print("  Sharpe = (ER - RF) / Volatility")
print()
print("Even with normal volatility (~10-15% annual), if ER is 25-50%,")
print("Sharpe ratios will be 1.5-3.0, which seems unusually high.")
print()
print("However, these are EXPECTED returns from a model, not realized.")
print("The model assumes future will resemble 2016-2024 period.")
print()

print("=" * 80)
print("SOLUTIONS")
print("=" * 80)
print()
print("Option 1: Use Historical Long-Term Equity Premium")
print("  - Set λ_market = 0.06/12 = 0.005 monthly (6% annual)")
print("  - More conservative, historically justified")
print("  - Ignores recent market trends")
print()
print("Option 2: Shrink Factor Premia Toward Historical Mean")
print("  - λ_market_adjusted = w * 0.006 + (1-w) * 0.01163")
print("  - Where w = shrinkage weight (e.g., 0.5)")
print("  - Balances recent vs long-term evidence")
print()
print("Option 3: Cap/Winsorize Extreme Betas")
print("  - Cap β_market at [-3, 3]")
print("  - Cap β_ESG at [-5, 5]")
print("  - Prevents extreme leverage from outliers")
print()
print("Option 4: Use Longer Sample Period")
print("  - Extend back to 2000 if data available")
print("  - Includes dot-com bubble, 2008 crisis")
print("  - More representative of full market cycles")
print()
print("Option 5: Accept High Sharpe as Model Estimate")
print("  - These are forward-looking estimates, not guarantees")
print("  - Recognize model uncertainty")
print("  - Use for relative ranking, not absolute forecasts")
