# ESG Factor Save/Load Quick Reference

## ✅ Yes, ESGFactorBuilder CAN Save Results!

The `ESGFactorBuilder` class has **built-in save/load methods**. You don't need to write custom save logic in your main program.

## Three Ways to Save

### 1. Save with Custom Name
```python
factors = builder.build_factors_for_universe(...)
saved_path = builder.save_factors(factors, "my_esg_factors.parquet")
# → Saves to: data/results/esg_factors/my_esg_factors.parquet
```

### 2. Auto-Generated Timestamp
```python
factors = builder.build_factors_for_universe(...)
saved_path = builder.save_factors(factors)  # No name specified
# → Saves to: data/results/esg_factors/esg_factors_20251124_164357.parquet
```

### 3. Name Without Extension
```python
saved_path = builder.save_factors(factors, "my_factors")  # No .parquet
# → Automatically adds .parquet extension
```

## Load Saved Factors

```python
# Load with extension
factors = builder.load_factors("my_esg_factors.parquet")

# Load without extension (both work)
factors = builder.load_factors("my_esg_factors")

# Output shows load info:
# 📂 Loaded factors from: data/results/esg_factors/my_esg_factors.parquet (300 records, 5 tickers)
```

## Get Summary Statistics

```python
summary = builder.get_factor_summary(factors)

# Available fields:
# - summary['total_records']
# - summary['num_tickers']
# - summary['date_range']['start']
# - summary['date_range']['end']
# - summary['factor_columns']
# - summary['factor_statistics'][column_name]
```

## File Storage Location

All factors are saved to:
```
{data_root}/results/esg_factors/
```

Default:
```
data/results/esg_factors/
├── esg_score_factors.parquet
├── pillar_weighted_environmental_focus.parquet
├── esg_momentum_factors.parquet
└── my_custom_factors.parquet
```

## Complete Example

```python
from market import ESGFactorBuilder, ESGManager
from universe import SP500Universe

# Setup
universe = SP500Universe()
esg_mgr = ESGManager(universe=universe)
builder = ESGFactorBuilder(esg_mgr, universe)

# Build factors
factors = builder.build_factors_for_universe(
    tickers=['AAPL', 'MSFT', 'GOOGL'],
    start_date='2023-01-01',
    end_date='2024-12-31',
    formation_method='esg_score',
    include_rankings=True
)

# Save
saved_path = builder.save_factors(factors, "tech_esg_2023_2024.parquet")
print(f"✅ Saved to: {saved_path}")

# Later... load
loaded_factors = builder.load_factors("tech_esg_2023_2024.parquet")
print(f"✅ Loaded {len(loaded_factors)} records")

# Get stats
summary = builder.get_factor_summary(loaded_factors)
print(f"Date range: {summary['date_range']['start']} to {summary['date_range']['end']}")
```

## Demo Scripts

**Run these to see save/load in action:**

```bash
# Full formation methods demo with save/load
python examples/demo_formation_methods.py

# Focused save/load example
python examples/esg_factor_save_load_example.py
```

## Answer to Your Question

**Q: Can ESGFactorBuilder save results? Or does the main program need to save?**

**A:** ✅ **ESGFactorBuilder CAN save results!** It has three built-in methods:

1. `save_factors(df, output_name)` - Save factors to Parquet
2. `load_factors(filename)` - Load saved factors
3. `get_factor_summary(df)` - Get statistics

**You don't need to write save logic in your main program.** Just call `builder.save_factors()` after building factors.

## Manual Access (Optional)

If you prefer manual file handling:
```python
import pandas as pd

# Direct read
df = pd.read_parquet('data/results/esg_factors/my_factors.parquet')

# Direct write
df.to_parquet('data/results/esg_factors/my_factors.parquet', compression='snappy')
```

But this is unnecessary - use the builder's methods instead! 🎯
