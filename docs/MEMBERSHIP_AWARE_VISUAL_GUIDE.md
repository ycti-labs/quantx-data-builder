## Membership-Aware Missing Data Check - Visual Examples

### Example 1: Symbol Joined After Required Start (TSLA)

```
Timeline:
                2018        2020-12-21        2024
                 |             |               |
Required:        [==========================]
Membership:                    [===============]
Effective:                     [===============]  <- Only check this period
Existing Data:                 [=========]
Missing:                                 [====]    <- Report this gap only

Result: PARTIAL (missing at end)
Fetch: None to 2024-12-31 (only membership period)
```

### Example 2: Required Period Before Membership

```
Timeline:
                2018        2020-12-21        2024
                 |             |               |
Required:        [======]
Membership:                    [===============]
Effective:       (empty - no overlap)

Result: COMPLETE (required period outside membership)
Fetch: None (no data needed)
```

### Example 3: Long-Standing Member (AAPL)

```
Timeline:
                2000        2020              2024
                 |             |               |
Required:                      [===============]
Membership:      [================================]
Effective:                     [===============]  <- Same as required
Existing Data:                 [===============]
Missing:         (none)

Result: COMPLETE
Fetch: None
```

### Example 4: Symbol Removed and Re-Added (Hypothetical)

```
Timeline:
                2010        2015    2017      2024
                 |             |      |         |
Required:        [===============================]
Membership:      [=====]            [===========]
Effective:       [=====]            [===========]  <- Two periods
Existing Data:   [=====]            [===========]
Missing:         (none in membership periods)

Note: Current implementation uses min(start) to max(end)
Future: Could check each interval separately
```

### Example 5: Partial Data Within Membership

```
Timeline:
                2020-12-21  2022   2024-06    2024-12-31
                 |           |       |           |
Required:        [==============================]
Membership:      [==============================]
Effective:       [==============================]
Existing Data:   [====]             [==========]
Missing (start):      (0d - within tolerance)
Missing (end):                      (183d)

Result: PARTIAL
Fetch: None to 2024-12-31 (fill end gap)
```

## Decision Tree

```
check_missing_data(symbol, required_start, required_end)
│
├─► Get membership_interval for symbol
│   │
│   ├─► Found?
│   │   │
│   │   ├─► YES: Calculate effective_period = intersection(required, membership)
│   │   │   │
│   │   │   ├─► No overlap?
│   │   │   │   └─► Return: status='complete', no fetch needed
│   │   │   │
│   │   │   └─► Has overlap?
│   │   │       └─► Use effective_period for gap calculation
│   │   │
│   │   └─► NO: Use full required_period (fallback)
│   │
│   └─► Get existing_data_range
│       │
│       ├─► No data exists?
│       │   └─► Return: status='missing', fetch=effective_period
│       │
│       └─► Data exists?
│           │
│           ├─► Calculate gaps within effective_period
│           │
│           ├─► Gaps within tolerance?
│           │   └─► Return: status='complete'
│           │
│           └─► Gaps exceed tolerance?
│               └─► Return: status='partial', fetch recommendations
```

## Logging Examples

### Scenario 1: Outside Membership (Complete)
```
✅ TSLA: Required period outside membership | 
Membership: (2020-12-21 to 2099-12-31), Required: (2018-01-01 to 2019-12-31)
```

### Scenario 2: No Existing Data
```
📭 TSLA: No existing data | 
Need to fetch period (2020-12-21 to 2024-12-31) within membership (2020-12-21 to 2099-12-31)
```

### Scenario 3: Complete Within Membership
```
✅ TSLA: Existing data COMPLETE | 
(2020-12-21 to 2024-12-26) | Membership: (2020-12-21 to 2099-12-31)
```

### Scenario 4: Partial Within Membership
```
⚠️  TSLA: Existing data PARTIAL | 
(2020-12-21 to 2024-06-30) | 
Missing: 0d at start, 183d at end (tolerance: ±2d) | 
Membership: (2020-12-21 to 2099-12-31)
```

### Scenario 5: Complete with Tolerance
```
✅ AAPL: Existing data COMPLETE (±2d) | 
(2020-01-03 to 2024-12-30) | Membership: (2000-01-01 to 2099-12-31)
```

## Code Flow

```python
# 1. Parse required dates
req_start = pd.to_datetime(required_start).date()
req_end = pd.to_datetime(required_end).date()

# 2. Get membership interval from Universe
membership_interval = self.universe.get_membership_interval(symbol)
#    Returns: (member_start, member_end) or None

# 3. Calculate effective period (intersection)
if membership_interval:
    member_start, member_end = membership_interval
    effective_start = max(req_start, member_start)
    effective_end = min(req_end, member_end)
    
    # 3a. Check if no overlap
    if effective_start > effective_end:
        return {'status': 'complete', ...}  # Outside membership
else:
    effective_start = req_start
    effective_end = req_end

# 4. Get existing data range
existing_range = self.get_existing_date_range(symbol)

# 5. Calculate gaps within effective period
if existing_range is None:
    # No data - need full effective period
    return {'status': 'missing', 'fetch_start': effective_start, ...}

actual_start, actual_end = existing_range
start_gap_days = max(0, (actual_start - effective_start).days)
end_gap_days = max(0, (effective_end - actual_end).days)

# 6. Apply tolerance and determine status
if start_gap_days <= tolerance and end_gap_days <= tolerance:
    return {'status': 'complete', ...}
else:
    return {'status': 'partial', 'fetch_start': ..., ...}
```

## Key Concepts

### Effective Period
The **effective period** is the intersection of:
1. **Required period**: What the user asked for
2. **Membership period**: When the symbol was actually in the universe

Only the effective period is checked for missing data.

### Tolerance
- Default: 2 days
- Accounts for weekends, holidays, and minor delays
- Applied to gaps at both start and end
- Prevents false "partial" status for nearly-complete data

### Status Values
- **complete**: Data fully covers effective period (within tolerance)
- **partial**: Data exists but has gaps exceeding tolerance
- **missing**: No data exists at all

### Fetch Recommendations
- `fetch_start` and `fetch_end` only recommend fetching within effective period
- Never recommend fetching pre-membership data
- `None` values mean no fetch needed for that boundary

## Benefits Summary

| Scenario | Old Behavior | New Behavior |
|----------|--------------|--------------|
| Pre-membership request | ❌ Reports as "missing" | ✅ Reports as "complete" |
| Symbol joined recently | ⚠️  Large start gap | ✅ Only checks from join date |
| Required period spans join | ⚠️  False partial status | ✅ Only checks membership period |
| Long-standing member | ✅ Works correctly | ✅ Works correctly |
| No membership data | ❌ Would break | ✅ Graceful fallback |
