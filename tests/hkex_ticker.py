import pandas as pd

# Download HKEX securities list
url = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"
df = pd.read_excel(url, skiprows=2)

# Filter for equities only
equities = df[df['Category'] == 'Equity']

# Format ticker with .HK suffix
equities['Ticker'] = equities['Stock Code'].apply(lambda x: f"{int(x):04d}.HK")

# Save to CSV
equities[['Ticker', 'Name of Securities']].to_csv("hkex_equities.csv", index=False)

print(f"Saved {len(equities)} HKEX equity tickers.")
