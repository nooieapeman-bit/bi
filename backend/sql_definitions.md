# SQL Definitions for BI Analysis

This document contains the standard SQL definitions used for various BI metrics.

## 1. Trial User Identification (Valid Trials)

**Metric**: Valid Trial Users
**Tables**: `osaio.orders_osaio_eu`, `osaio.orders_osaio_us`, `osaio.orders_nooie_eu`, `osaio.orders_nooie_us`
**Standard Condition**:
- `amount = 0`: Zero payment amount.
- `status = 1`: Payment/Order success.
- `subscribe_id != ''`: Associated with a subscription ID (excludes non-subscription zero-billing events).

### Example Query (Monthly Unique Trial Users)
```sql
SELECT 
    FROM_UNIXTIME(pay_time, '%Y-%m') as month,
    COUNT(DISTINCT uid) as unique_trial_users
FROM osaio.orders_osaio_eu
WHERE amount = 0 
  AND status = 1 
  AND subscribe_id != ''
GROUP BY month
ORDER BY month;
```

---
