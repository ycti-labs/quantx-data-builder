# Architecture Refactoring: Separation of Concerns

## Summary

Refactored the codebase to follow the **Single Responsibility Principle** by separating universe membership management from price data fetching.

## Changes

### Before (Violated SRP)

`PriceDataManager` was doing TWO jobs:
1. Fetching and storing price data ✅ (core responsibility)
2. Managing universe membership queries ❌ (should be UniverseBuilder's job)

```python
# Old pattern - PriceDataManager doing everything
builder = PriceDataManager(api_key)
members = builder.get_current_members("sp500")  # PriceDataManager reads membership data
data = builder.fetch_complete_universe_history("sp500", "2020-01-01", "2024-12-31")
```

### After (Follows SRP)

**UniverseBuilder**: Owns all membership queries and universe construction
**PriceDataManager**: Owns all price data fetching and storage

```python
# New pattern - Clear separation
builder = PriceDataManager(api_key)

# Option 1: Use delegated methods (backward compatible)
members = builder.get_current_members("sp500")  # Delegates to builder.universe_builder

# Option 2: Direct access (explicit separation)
members = builder.universe_builder.get_current_members("sp500")

# Option 3: Dependency injection (best for testing)
universe = UniverseBuilder(data_root="data/curated")
builder = PriceDataManager(api_key, universe_builder=universe)
members = builder.universe_builder.get_current_members("sp500")
```

## Implementation Details

### 1. UniverseBuilder Enhancement

Added three membership query methods to `UniverseBuilder`:

```python
class UniverseBuilder:
    def __init__(self, config_path=None, data_root="data/curated"):
        """
        Args:
            config_path: Optional - needed for building universes
            data_root: Root directory for curated data
        """
        self.data_root = Path(data_root)
        # ...

    def get_universe_members(
        self, 
        universe: str, 
        as_of_date: Optional[str] = None
    ) -> List[str]:
        """Get members as of a specific date"""
        # Reads from data/curated/membership/universe={universe}/mode=intervals/
        
    def get_current_members(self, universe: str) -> List[str]:
        """Get current members (as of today)"""
        return self.get_universe_members(universe, as_of_date=None)
        
    def get_all_historical_members(
        self,
        universe: str,
        period_start: str,
        period_end: str
    ) -> List[str]:
        """Get ALL members during any part of period (eliminates survivorship bias)"""
        # Returns ~606 symbols for SP500 2020-2024 (not just current 500)
```

### 2. PriceDataManager Refactoring

Updated `PriceDataManager` to use `UniverseBuilder`:

```python
class PriceDataManager:
    def __init__(
        self, 
        api_key: str, 
        data_root: str = "data/curated",
        universe_builder: Optional[UniverseBuilder] = None
    ):
        """
        Args:
            api_key: Tiingo API key
            data_root: Root directory for curated data
            universe_builder: Optional UniverseBuilder instance (lazy init if None)
        """
        self.api_key = api_key
        self.data_root = Path(data_root)
        
        # Lazy initialization - creates UniverseBuilder if not provided
        if universe_builder is None:
            from ..universe.universe_builder import UniverseBuilder
            self.universe_builder = UniverseBuilder(data_root=str(data_root))
        else:
            self.universe_builder = universe_builder
    
    # Delegated methods for backward compatibility
    def get_universe_members(self, universe, as_of_date=None) -> List[str]:
        """Delegates to UniverseBuilder"""
        return self.universe_builder.get_universe_members(universe, as_of_date)
    
    def get_current_members(self, universe) -> List[str]:
        """Delegates to UniverseBuilder"""
        return self.universe_builder.get_current_members(universe)
    
    def get_all_historical_members(self, universe, start, end) -> List[str]:
        """Delegates to UniverseBuilder"""
        return self.universe_builder.get_all_historical_members(universe, start, end)
```

### 3. Internal Method Updates

Updated internal methods to use `UniverseBuilder`:

```python
# In fetch_complete_universe_history()
symbols = self.universe_builder.get_all_historical_members(
    universe, start_date, end_date or datetime.now().strftime('%Y-%m-%d')
)

# In fetch_universe_missing_data()
symbols = self.universe_builder.get_all_historical_members(
    universe, start_date, end_date
)
```

## Benefits

1. **Clear Separation of Concerns**
   - `UniverseBuilder` = Universe construction + membership queries
   - `PriceDataManager` = Price data fetching + storage
   - Each class has ONE primary responsibility

2. **Better Encapsulation**
   - Membership logic consolidated in one place
   - Easier to change storage format (Parquet → Database)
   - More intuitive API

3. **Improved Testability**
   - Can test membership queries independently
   - Can mock UniverseBuilder in PriceDataManager tests
   - Dependency injection pattern supported

4. **Backward Compatibility**
   - Lazy initialization maintains existing behavior
   - Delegated methods ensure old code still works
   - No breaking changes for existing scripts

5. **Extensibility**
   - Easy to add new membership query methods
   - Can swap UniverseBuilder implementations
   - Better foundation for multi-market support

## Testing

All functionality verified:

```bash
$ python -c "from src.fetcher.price_data_builder import PriceDataManager; ..."

✅ Test 1: Lazy Initialization - PASSED
✅ Test 2: Delegation Methods Exist - PASSED  
✅ Test 3: Query Membership Data - PASSED (499 members on 2024-12-31)
✅ Test 4: Get Historical Members - PASSED (606 members 2020-2024)
✅ Test 5: Dependency Injection - PASSED
```

## Migration Guide

### For Existing Code

No changes required! The delegated methods maintain backward compatibility:

```python
# This still works exactly as before
builder = PriceDataManager(api_key)
members = builder.get_current_members("sp500")
```

### For New Code (Recommended)

Use explicit separation for clarity:

```python
# Best practice - makes separation obvious
builder = PriceDataManager(api_key)
members = builder.universe_builder.get_current_members("sp500")
data = builder.fetch_complete_universe_history("sp500", "2020-01-01", "2024-12-31")
```

### For Testing (Recommended)

Use dependency injection:

```python
# Easy to mock for testing
mock_universe = MockUniverseBuilder()
builder = PriceDataManager(api_key, universe_builder=mock_universe)
# Test without hitting actual membership data
```

## Files Modified

- `src/universe/universe_builder.py` - Added membership query methods
- `src/universe/__init__.py` - Removed invalid import
- `src/fetcher/price_data_builder.py` - Added universe_builder parameter, delegated methods
- All example scripts - Continue working (backward compatible)

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                   User Application                       │
└───────────────────────┬─────────────────────────────────┘
                        │
           ┌────────────┴────────────┐
           │                         │
           ▼                         ▼
┌──────────────────────┐  ┌──────────────────────┐
│  PriceDataManager   │  │   UniverseBuilder    │
├──────────────────────┤  ├──────────────────────┤
│ - Fetch price data   │  │ - Build universes    │
│ - Save to Parquet    │◄─┤ - Query membership   │
│ - Check missing data │  │ - Point-in-time data │
└──────────────────────┘  └──────────────────────┘
           │                         │
           ▼                         ▼
┌──────────────────────┐  ┌──────────────────────┐
│  Tiingo API          │  │  Membership Parquet  │
│  (Price Data)        │  │  (Universe Data)     │
└──────────────────────┘  └──────────────────────┘
```

## Future Enhancements

1. **Cache membership queries** - Avoid repeated Parquet reads
2. **Support multiple storage backends** - Database, cloud storage
3. **Add universe versioning** - Track membership changes over time
4. **Multi-market support** - HK, JP, EU universes with same pattern
5. **Real-time membership updates** - Detect additions/removals automatically

## Related Documentation

- [Complete History Implementation](./COMPLETE_HISTORY_IMPLEMENTATION.md)
- [Intelligent Missing Data Fetch](./INTELLIGENT_MISSING_DATA_FETCH.md)
- [Point-in-Time Coverage Fix](./POINT_IN_TIME_COVERAGE_FIX.md)
