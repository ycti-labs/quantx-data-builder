# PEP (PepsiCo) Risk Profile Analysis
**Comprehensive Risk Assessment & Portfolio Optimization**

---

## Slide 1: Executive Summary

### Risk Profile: PEP (PepsiCo)
**Analysis Period:** 2014-2025 (11 years) | **Frequency:** Monthly Returns

#### Key Metrics at a Glance
- **Beta (Systematic Risk):** [From regression output]
- **Total Volatility:** [Annualized standard deviation]
- **Jensen's Alpha:** [Annualized excess return]
- **Sharpe Ratio:** [Risk-adjusted performance]
- **R-squared:** [Market correlation strength]

#### Investment Classification
- Beta > 1: **Aggressive** (More volatile than market)
- Beta = 1: **Market-like** (Tracks market)
- Beta < 1: **Defensive** (Less volatile than market)

---

## Slide 2: Systematic Risk Analysis

### Beta: Market Sensitivity Measure

#### Full-Sample Beta (2014-2025)
- **Beta Coefficient:** [Value]
- **Statistical Significance:** p-value [Value]
- **Interpretation:** For every 1% market move, PEP moves [Beta]%

#### Rolling 60-Month Beta Dynamics
- **Average Beta:** [Mean of rolling windows]
- **Beta Range:** [Min] to [Max]
- **Standard Deviation:** [Volatility of beta]
- **Trend:** [Stable / Increasing / Decreasing]

#### Risk Implications
✓ Systematic risk cannot be diversified away
✓ Determines portfolio hedging requirements
✓ Key input for portfolio optimization

---

## Slide 3: Total Risk Decomposition

### Risk Components Breakdown

```
Total Risk = Systematic Risk + Unsystematic Risk
```

#### Risk Metrics (Annualized)
| Component | Value | % of Total |
|-----------|-------|------------|
| **Total Volatility** | [X.XX%] | 100% |
| **Systematic Risk** | β × σ_market | [XX%] |
| **Unsystematic Risk** | Residual σ | [XX%] |

#### Key Insights
- **Systematic Risk:** Market-driven, unavoidable
- **Unsystematic Risk:** Firm-specific, diversifiable
- **Correlation with SPY:** [Value] (Strong/Moderate/Weak)

#### Diversification Benefit
- [XX%] of risk can be eliminated through diversification
- Remaining [XX%] is market-driven

---

## Slide 4: Performance Metrics

### Risk-Adjusted Returns Analysis

#### 1. Jensen's Alpha (Ex-Post Performance)
- **Monthly Alpha:** [X.XXXX] ([X.XX%])
- **Annualized Alpha:** [X.XX%]
- **Interpretation:** [Outperformed/Underperformed] market by [X.XX%] annually
- **Statistical Significance:** [Significant/Not significant] at 5% level

#### 2. Sharpe Ratio (Total Risk Adjustment)
- **PEP Sharpe:** [X.XXX]
- **SPY Sharpe:** [X.XXX]
- **Comparison:** PEP [outperforms/underperforms] per unit of total risk

#### 3. Treynor Ratio (Systematic Risk Adjustment)
- **PEP Treynor:** [X.XX%]
- **SPY Treynor:** [X.XX%]
- **Interpretation:** Return per unit of beta (systematic risk only)

---

## Slide 5: Advanced Risk Metrics

### Downside Risk & Drawdown Analysis

#### Information Ratio
- **Alpha:** [X.XX%]
- **Tracking Error:** [X.XX%]
- **Information Ratio:** [X.XXX]
- **Meaning:** Alpha generated per unit of active risk

#### Sortino Ratio
- **Downside Deviation:** [X.XX%]
- **Sortino Ratio:** [X.XXX]
- **Focus:** Only penalizes downside volatility

#### Maximum Drawdown
- **Largest Peak-to-Trough Decline:** [-X.XX%]
- **Date Occurred:** [YYYY-MM-DD]
- **Recovery Considerations:** Important for risk management

---

## Slide 6: Comparative Performance

### PEP vs SPY Benchmark Comparison

| Metric | PEP | SPY | Winner |
|--------|-----|-----|--------|
| **Annual Return** | [X.XX%] | [X.XX%] | [PEP/SPY] |
| **Total Volatility** | [X.XX%] | [X.XX%] | [Lower wins] |
| **Beta** | [X.XXX] | 1.000 | - |
| **Sharpe Ratio** | [X.XXX] | [X.XXX] | [PEP/SPY] |
| **Treynor Ratio** | [X.XX%] | [X.XX%] | [PEP/SPY] |
| **Max Drawdown** | [-X.XX%] | [-X.XX%] | [Smaller wins] |
| **R-squared** | [X.XXX] | 1.000 | - |

#### Key Takeaways
- **Excess Return:** [X.XX%] vs market
- **Risk-Adjusted Performance:** [Better/Worse] than benchmark
- **Volatility Profile:** [More/Less] volatile than market

---

## Slide 7: Portfolio Formation - Theory

### Modern Portfolio Theory (Markowitz Optimization)

#### Objective
Construct optimal portfolio combining PEP + SPY to maximize risk-adjusted returns

#### Optimization Framework
```
Maximize: Sharpe Ratio = (Return - RFR) / Volatility
Subject to: Weights sum to 100%
```

#### Two Scenarios Analyzed
1. **Unconstrained:** Allow negative weights (shorting permitted)
2. **Constrained:** Long-only (weights between 0-100%)

#### Key Inputs
- **Expected Returns:** Historical average returns
- **Volatilities:** Standard deviations
- **Correlation:** PEP-SPY relationship
- **Covariance Matrix:** Risk relationships

---

## Slide 8: Portfolio Optimization Results

### Optimal Portfolio Weights

#### Scenario 1: Unconstrained (Shorting Allowed)
| Asset | Weight | Interpretation |
|-------|--------|----------------|
| **PEP** | [XXX.X%] | [Long/Short] position |
| **SPY** | [XXX.X%] | [Long/Short] position |

**Performance:**
- Expected Return: [X.XX%]
- Volatility: [X.XX%]
- Sharpe Ratio: [X.XXX]

#### Scenario 2: Constrained (Long-Only)
| Asset | Weight | Dollar Amount* |
|-------|--------|----------------|
| **PEP** | [XX.X%] | $[XX,XXX] |
| **SPY** | [XX.X%] | $[XX,XXX] |
*Based on $100,000 investment

**Performance:**
- Expected Return: [X.XX%]
- Volatility: [X.XX%]
- Sharpe Ratio: [X.XXX]

---

## Slide 9: Diversification Benefits

### Risk Reduction Through Diversification

#### Correlation Impact
- **PEP-SPY Correlation:** [X.XXX]
- **Implication:** [Perfect/High/Moderate/Low] correlation

#### Risk Reduction Achieved
- **Weighted Average Volatility:** [X.XX%]
- **Portfolio Volatility:** [X.XX%]
- **Risk Reduction:** [X.XX%] ([XX%] decrease)

#### Diversification Insights
✓ Portfolio volatility < weighted average (when correlation < 1)
✓ Optimal mix balances return and risk
✓ Lower correlation → Greater diversification benefit

#### Comparison vs Single Assets
- **Optimal Portfolio Sharpe:** [X.XXX]
- **PEP-Only Sharpe:** [X.XXX]
- **SPY-Only Sharpe:** [X.XXX]
- **Improvement:** +[X.XXX] Sharpe points

---

## Slide 10: Efficient Frontier

### Risk-Return Tradeoff Visualization

#### Efficient Frontier Characteristics
- **X-axis:** Portfolio Volatility (Risk)
- **Y-axis:** Expected Return
- **Color:** Sharpe Ratio (green = higher)

#### Key Points on Frontier
1. **100% SPY:** [Return: X.XX%, Vol: X.XX%]
2. **100% PEP:** [Return: X.XX%, Vol: X.XX%]
3. **Optimal Constrained:** [Return: X.XX%, Vol: X.XX%]
4. **Optimal Unconstrained:** [Return: X.XX%, Vol: X.XX%]

#### Observations
- Optimal portfolio lies on the efficient frontier
- Maximum Sharpe Ratio = tangency portfolio
- [Corner solution if one asset dominates]

---

## Slide 11: Investment Implications

### Practical Applications & Recommendations

#### For Portfolio Construction
✓ **Beta for hedging:** Use β = [X.XX] for market exposure management
✓ **Diversification:** [XX%] allocation to PEP, [XX%] to SPY (optimal)
✓ **Risk budget:** [XX%] of portfolio risk from PEP systematic risk

#### For Risk Management
✓ **Monitor beta stability:** Track rolling 60-month β trends
✓ **Drawdown limits:** Historical max drawdown of [X.XX%]
✓ **Correlation changes:** Watch for regime shifts affecting diversification

#### For Performance Evaluation
✓ **Alpha generation:** [Positive/Negative] [X.XX%] annually
✓ **Risk-adjusted metrics:** Sharpe [X.XX] vs SPY [X.XX]
✓ **Benchmark appropriateness:** R² = [X.XX] indicates [strong/weak] fit

---

## Slide 12: Key Takeaways & Recommendations

### Summary of Findings

#### Risk Profile
1. **Beta Classification:** [Aggressive/Neutral/Defensive] stock
2. **Risk Composition:** [XX%] systematic, [XX%] unsystematic
3. **Market Correlation:** [Strong/Moderate/Weak] relationship with SPY

#### Performance Assessment
1. **Alpha:** [Positive/Negative] excess return of [X.XX%]
2. **Sharpe Ratio:** [Better/Worse] than market benchmark
3. **Downside Risk:** Maximum drawdown of [X.XX%]

#### Portfolio Recommendations
1. **Optimal Allocation:** [XX%] PEP + [XX%] SPY (long-only)
2. **Expected Improvement:** Sharpe ratio of [X.XX] vs [X.XX] for SPY alone
3. **Risk Reduction:** [XX%] volatility decrease through diversification

#### Action Items
- [ ] Implement optimal portfolio weights
- [ ] Monitor beta stability monthly
- [ ] Review alpha persistence quarterly
- [ ] Adjust for correlation regime changes

---

## Appendix: Methodology

### Data & Analysis Details

#### Data Sources
- **Price Data:** Tiingo API (adjusted for splits/dividends)
- **Market Proxy:** SPY (S&P 500 ETF)
- **Risk-Free Rate:** 2% annually (~0.167% monthly)
- **Frequency:** Monthly returns

#### Statistical Methods
- **Regression:** OLS with constant term
- **Rolling Windows:** 60-month periods, 36-month minimum
- **Optimization:** scipy.optimize SLSQP method
- **Annualization:** √12 for volatility, compound for returns

#### Software & Tools
- **Python:** pandas, numpy, statsmodels
- **Visualization:** matplotlib, seaborn
- **Optimization:** scipy.optimize

#### Key Assumptions
- Historical returns proxy for future expectations
- Normal distribution of returns (for Sharpe ratio)
- Stationary covariance matrix
- No transaction costs or taxes included

---

**End of Presentation**

*For questions or detailed analysis, please refer to the full Jupyter notebook: `project_pep.ipynb`*
