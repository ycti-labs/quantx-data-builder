# Risk-Free Rate Manager Refactoring

## Summary

Split `RiskFreeRateManager` into two classes following the **Single Responsibility Principle**:

### Before (One Class)
```python
# Required API key even for loading cached data
manager = RiskFreeRateManager(
    fred_api_key="your_key",  # Always required!
    data_root="data/...",
    default_rate='3month'
)
```

**Problem:** Users need to pass API key even when just loading cached data.

### After (Two Classes)

#### 1. RiskFreeRateBuilder (Fetch & Save)
```python
# Use when you need to download data from FRED
builder = RiskFreeRateBuilder(
    fred_api_key="your_key",  # Required
    data_root="data/curated/references/risk_free_rate/freq=monthly",
    default_rate='3month'
)

# Fetch and save
rf_df = builder.build_and_save(
    start_date="2020-01-01",
    end_date="2024-12-31",
    rate_type="3month",
    frequency="monthly"
)
```

**Responsibilities:**
- Fetch data from FRED API
- Resample to target frequency
- Save to cache (Parquet)
- Merge with existing cached data

#### 2. RiskFreeRateManager (Load & Calculate)
```python
# Use when you just need to load cached data
manager = RiskFreeRateManager(
    data_root="data/curated/references/risk_free_rate/freq=monthly",
    default_rate='3month'
    # No API key needed!
)

# Load from cache
rf_df = manager.load_risk_free_rate(
    start_date="2020-01-01",
    end_date="2024-12-31"
)
```

**Responsibilities:**
- Load cached data
- Calculate risk-free returns
- Calculate excess returns
- Get summary statistics

## Benefits

1. **Simpler API**: No API key needed for 90% of use cases (loading)
2. **Clear Separation**: Fetch vs Load responsibilities
3. **Better Errors**: Clear messages about missing cache vs missing API key
4. **Follows SRP**: Each class has one clear purpose
5. **More Testable**: Can test loading without API key

## Migration Guide

### ESGFactorBuilder Integration

**Old Code:**
```python
# Constructor initialized both manager and builder logic
self.rf_manager = RiskFreeRateManager(
    fred_api_key=fred_api_key,
    data_root=rf_data_root,
    default_rate=rf_rate_type
)
```

**New Code:**
```python
# Separate builder and manager
if fred_api_key:
    self.rf_builder = RiskFreeRateBuilder(
        fred_api_key=fred_api_key,
        data_root=rf_data_root,
        default_rate=rf_rate_type
    )
else:
    self.rf_builder = None

# Manager always available (no API key needed)
self.rf_manager = RiskFreeRateManager(
    data_root=rf_data_root,
    default_rate=rf_rate_type
)
```

**Loading Logic:**
```python
def _load_risk_free_rate(self, returns_df):
    # 1. Try cache first
    if cache_path.exists():
        return load_from_cache()
    
    # 2. Try builder (if API key provided)
    if self.rf_builder is not None:
        return self.rf_builder.build_and_save(...)
    
    # 3. Try manager (no API key)
    try:
        return self.rf_manager.load_risk_free_rate(...)
    except FileNotFoundError:
        raise ValueError("No cache and no API key")
```

## Usage Patterns

### Pattern 1: First-Time Setup
```python
from market.risk_free_rate_manager import RiskFreeRateBuilder

builder = RiskFreeRateBuilder(
    fred_api_key=config.get("fred.api_key"),
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)

# Fetch and cache 20 years of data
builder.build_and_save(
    start_date="2004-01-01",
    end_date="2024-12-31",
    rate_type="3month",
    frequency="monthly"
)
```

### Pattern 2: Daily Usage (No API Key)
```python
from market.risk_free_rate_manager import RiskFreeRateManager

manager = RiskFreeRateManager(
    data_root="data/curated/references/risk_free_rate/freq=monthly"
)

rf_df = manager.load_risk_free_rate(
    start_date="2020-01-01",
    end_date="2024-12-31"
)
```

### Pattern 3: ESGFactorBuilder (Auto-Loading)
```python
# Without API key (uses cache only)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month"
    # No fred_api_key!
)

# With API key (auto-fetches if needed)
builder = ESGFactorBuilder(
    universe=universe,
    rf_rate_type="3month",
    fred_api_key=config.get("fred.api_key")
)
```

## File Structure

```
src/market/risk_free_rate_manager.py
├── RiskFreeRateBuilder
│   ├── __init__(fred_api_key, data_root, default_rate)
│   ├── fetch_risk_free_rate()
│   ├── _resample_to_frequency()
│   ├── build_and_save()
│   └── get_cache_path()
│
└── RiskFreeRateManager
    ├── __init__(data_root, default_rate)  # No API key!
    ├── load_risk_free_rate()
    ├── calculate_risk_free_returns()
    ├── calculate_excess_returns()
    ├── get_summary_statistics()
    └── get_cache_path()
```

## Testing

Run the demo script:
```bash
python tests/demo_risk_free_rate_split.py
```

## Breaking Changes

### RiskFreeRateManager Constructor
**Before:**
```python
RiskFreeRateManager(
    fred_api_key="key",      # Required
    data_root="path",
    default_rate="3month"
)
```

**After:**
```python
RiskFreeRateManager(
    data_root="path",        # API key removed!
    default_rate="3month"
)
```

### load_risk_free_rate Method
**Before:**
```python
manager.load_risk_free_rate(
    start_date="...",
    end_date="...",
    use_cache=True,    # Removed
    save_cache=True    # Removed
)
```

**After:**
```python
manager.load_risk_free_rate(
    start_date="...",
    end_date="..."
    # Always loads from cache
)
```

## Design Rationale

### Why Split?

1. **Single Responsibility Principle**
   - Builder: Fetch and persist data
   - Manager: Read and compute

2. **User Experience**
   - Most users just need to load data (no API key)
   - Only data engineers need to fetch (require API key)

3. **Error Clarity**
   - FileNotFoundError: Cache missing
   - ValueError: API key missing
   - No confusion between the two

4. **Testing**
   - Can test Manager without FRED API
   - Builder tests require API key
   - Clear separation of concerns

### Why Not Three Classes?

Could split further into:
- `RiskFreeRateFetcher` (FRED API)
- `RiskFreeRateCache` (Read/Write cache)
- `RiskFreeRateCalculator` (Compute returns)

**Decision:** Two classes sufficient
- Fetching is tightly coupled with saving
- Loading is tightly coupled with calculation
- Three classes would be over-engineering

## References

- **SOLID Principles**: Single Responsibility Principle (SRP)
- **Design Pattern**: Builder Pattern (for RiskFreeRateBuilder)
- **Python Best Practices**: Separation of concerns, clear APIs

## Future Enhancements

1. **Async Fetching**: Parallel downloads for multiple rate types
2. **Data Validation**: Check for gaps, outliers, anomalies
3. **Multiple Sources**: Support Bloomberg, Reuters, etc.
4. **Auto-Update**: Scheduled jobs to keep cache fresh
5. **Version Control**: Track data lineage and updates
