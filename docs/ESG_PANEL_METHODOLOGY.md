# ESG Panel Data Methodology

## Research Question
Impact of ESG scores on stock returns (2014-2023) with growing ESG coverage.

## Data Characteristics
- **Price data:** 720 S&P 500 tickers (complete, 2014-2024)
- **ESG data:** Growing coverage from 185 (2005) → 492 (2023) companies
- **Matched data:** 454 tickers with both price AND ESG data

## Recommended Approach: Unbalanced Panel

### Rationale
1. **Maximizes statistical power** - Uses all 454 matched tickers
2. **Reflects reality** - ESG disclosure grew organically
3. **Standard in literature** - Consistent with Khan et al. (2016), Berg et al. (2022)
4. **Avoids artificial selection** - Don't restrict to "survivors"

### Implementation

```python
# 1. Create panel dataset
for year in range(2014, 2024):
    # Get tickers with BOTH price and ESG data in this year
    available = get_tickers_with_complete_data(year)
    
    # Extract data
    returns[year] = get_returns(available, year)
    esg[year] = get_esg_scores(available, year)
    controls[year] = get_controls(available, year)

# 2. Regression specification
model = """
    Return_{i,t} = α + β₁*ESG_{i,t} + β₂*Size_{i,t} + β₃*BM_{i,t} 
                 + β₄*Momentum_{i,t} + β₅*Years_ESG_{i,t}
                 + Firm_FE_i + Year_FE_t + ε_{i,t}
"""

# Key controls:
# - Firm_FE_i: Firm fixed effects (control for time-invariant characteristics)
# - Year_FE_t: Year fixed effects (control for market-wide trends)
# - Years_ESG_{i,t}: Years since ESG coverage started (selection control)
# - Size, BM, Momentum: Standard Fama-French controls
```

### Advantages
✅ Uses all available information
✅ Statistically powerful
✅ Standard in literature
✅ Captures ESG disclosure evolution

### Potential Concerns & Solutions

**Concern 1:** Selection bias (firms choosing to disclose ESG)
- **Solution:** Include `Years_ESG_Coverage` variable
- **Solution:** Firm fixed effects control for time-invariant selection
- **Robustness:** Test on balanced subsample

**Concern 2:** Different firms each year complicates interpretation
- **Solution:** Report descriptive stats by year
- **Solution:** Year fixed effects absorb time trends
- **Robustness:** Test on stable period (2018-2023)

**Concern 3:** Early ESG adopters may be different
- **Solution:** Include entry cohort dummies
- **Robustness:** Separate analysis by entry period

---

## Robustness Checks

### Check 1: Balanced Panel
```python
# Only firms with ESG data for ALL 10 years
balanced_tickers = get_tickers_with_complete_coverage(2014, 2023)
# Expected: ~185-200 firms
# Pros: Clean, same firms throughout
# Cons: Smaller sample, selection bias toward early adopters
```

### Check 2: Minimum Coverage Requirement
```python
# Require at least 5 years of ESG data
min_coverage = get_tickers_with_min_years(5)
# Expected: ~350-400 firms
# Pros: Balance between sample size and data quality
```

### Check 3: Entry Cohort Analysis
```python
# Compare results by when ESG coverage started
early_cohort = tickers_with_esg_before(2017)  # Early adopters
late_cohort = tickers_with_esg_after(2017)    # Late adopters
# Test if ESG-return relationship differs by cohort
```

### Check 4: Stable Period Only
```python
# Use only 2018-2023 when coverage is more stable
stable_period = get_data(start=2018, end=2023)
# Pros: Less entry/exit effects
# Cons: Shorter time series
```

---

## Expected Output for Email/Paper

**In your email:**
> "The ESG data shows growing coverage from 185 companies in 2005 to 492 in 2023. 
> For my 10-year analysis, I plan to use an unbalanced panel approach (all available 
> firm-years), which is standard in the ESG literature. This maximizes statistical 
> power while controlling for selection effects through firm and year fixed effects. 
> I will verify robustness using a balanced subsample of firms with complete ESG history."

**In your paper/presentation:**
> "Our sample consists of an unbalanced panel of S&P 500 firms from 2014-2023. 
> The number of firms with ESG data grows from X in 2014 to 454 in 2023, reflecting 
> the increasing adoption of ESG disclosure. We control for potential selection bias 
> by including firm fixed effects and a variable measuring years since ESG coverage 
> initiation. Results are robust to restricting the sample to firms with complete 
> 10-year ESG coverage (balanced panel, N=X firms)."

---

## Literature Support

### Papers Using Unbalanced Panels with Growing Coverage:

1. **Khan, Serafeim & Yoon (2016)** - "Corporate Sustainability: First Evidence on Materiality"
   - Journal: The Accounting Review
   - Sample: 1992-2013, growing ESG coverage
   - Method: Unbalanced panel, firm + year FE

2. **Berg, Koelbel & Rigobon (2022)** - "Aggregate Confusion: The Divergence of ESG Ratings"
   - Journal: Review of Finance
   - Sample: Different ESG providers cover different firms
   - Method: Unbalanced panel, selection controls

3. **Edmans (2011)** - "Does the Stock Market Fully Value Intangibles?"
   - Journal: Journal of Financial Economics
   - Sample: Best Companies list (staggered entry)
   - Method: Allow entry at different times, control for tenure

4. **Flammer (2015)** - "Does Corporate Social Responsibility Lead to Superior Performance?"
   - Journal: Management Science
   - Sample: CSR proposals over time
   - Method: Unbalanced panel with staggered treatment

---

## Decision Tree

```
Do you have complete ESG data for all firms for all years?
├─ YES → Use Balanced Panel (lucky you!)
└─ NO → Is coverage growing over time?
    ├─ YES (YOUR CASE) → Use Unbalanced Panel
    │   ├─ Primary: All available firm-years
    │   ├─ Controls: Firm FE + Year FE + Selection variables
    │   └─ Robustness: Balanced subsample, minimum coverage
    └─ NO → Is it random missingness?
        ├─ YES → Unbalanced panel OK
        └─ NO → Need selection model
```

---

## Summary Recommendation

**For your 10-year ESG study:**

✅ **Primary Analysis:** Unbalanced panel (all 454 tickers, all available years)

✅ **Controls:** Firm FE + Year FE + Years_ESG_Coverage

✅ **Robustness:** 
  1. Balanced panel (complete coverage only)
  2. Minimum 5-year coverage requirement
  3. Entry cohort comparison
  4. Stable period (2018-2023) only

✅ **Justification:** Standard practice in ESG literature, maximizes power, reflects reality

This approach is **defensible, standard, and rigorous** for academic research. 🎓
