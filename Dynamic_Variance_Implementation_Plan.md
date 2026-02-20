# Dynamic Variance Threshold Implementation Plan

## 📋 Overview

**Problem:** `dbo.vw_recon_variance` currently has hardcoded variance thresholds (10% for cost and weight). We need to make these configurable per tenant with different thresholds for oz/lb/kg units.

**Solution:** Create a configuration table that the view joins to. This document outlines two approaches based on business requirements.

**Business Decision Needed:** Do we need to track historical threshold changes for audit/analysis purposes?

---

## 🔄 SCD Type 1: Simple Configuration (No History)

**Use this if:** Business only cares about current settings, no need to track changes over time.

### Table Schema

```sql
CREATE TABLE dbo.recon_variance_config (
    config_id INT PRIMARY KEY DEFAULT 1,
    
    -- Variance thresholds (percentage values)
    cost_variance_threshold_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_oz_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_lb_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_kg_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,  -- Future-proof
    
    -- Audit fields (UPDATE tracking only)
    updated_by NVARCHAR(100) NULL,
    updated_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT CK_ReconConfig_SingleRow CHECK (config_id = 1)  -- Only 1 row allowed
);

-- Insert the single configuration row
INSERT INTO dbo.recon_variance_config (
    config_id,
    cost_variance_threshold_pct,
    weight_variance_threshold_oz_pct,
    weight_variance_threshold_lb_pct,
    weight_variance_threshold_kg_pct
) VALUES (
    1,     -- Fixed ID
    10.0,  -- 10% cost variance
    10.0,  -- 10% oz variance
    10.0,  -- 10% lb variance
    10.0   -- 10% kg variance
);
```

### View Changes

```sql
CREATE OR ALTER VIEW dbo.vw_recon_variance AS
WITH config AS (
    -- No WHERE clause needed (only 1 row exists)
    SELECT 
        cost_variance_threshold_pct / 100.0 AS cost_threshold,
        weight_variance_threshold_oz_pct / 100.0 AS oz_threshold,
        weight_variance_threshold_lb_pct / 100.0 AS lb_threshold
    FROM dbo.recon_variance_config
),
standardized_weights AS (
    SELECT 
        shipment_package_id,
        tracking_number,
        CEILING(actual_weight_oz / 16.0) AS actual_weight_lb,
        CEILING(billed_weight_oz / 16.0) AS billed_weight_lb,
        billed_weight_oz,
        actual_weight_oz,
        wms_shipping_cost,
        billed_shipping_cost 
    FROM shipment_package
),
cost_audit AS (
    SELECT 
        sw.*,
        c.cost_threshold,
        c.oz_threshold,
        c.lb_threshold,
        CASE 
            WHEN sw.billed_shipping_cost > sw.wms_shipping_cost * (1 + c.cost_threshold) 
              OR sw.billed_shipping_cost < sw.wms_shipping_cost * (1 - c.cost_threshold) THEN 1 
            ELSE 0 
        END AS is_cost_exception,
        CASE 
            WHEN sw.billed_shipping_cost > sw.wms_shipping_cost * (1 + c.cost_threshold) THEN 'cost_high' 
            WHEN sw.billed_shipping_cost < sw.wms_shipping_cost * (1 - c.cost_threshold) THEN 'cost_low' 
            ELSE 'Normal' 
        END AS cost_exception_type,
        CONVERT(DECIMAL(10,2), sw.billed_shipping_cost - sw.wms_shipping_cost) AS cost_variance_amount
    FROM standardized_weights sw
    CROSS JOIN config c  -- Single row, safe to CROSS JOIN
),
weight_audit AS (
    SELECT
        *,
        -- Weight Exception Flag (Gated by cost exception, unit-specific thresholds)
        CASE 
            WHEN is_cost_exception = 0 THEN 0
            WHEN billed_weight_oz < 16 THEN
                CASE 
                    WHEN billed_weight_oz > (actual_weight_oz * (1 + oz_threshold)) 
                      OR billed_weight_oz < (actual_weight_oz * (1 - oz_threshold)) THEN 1 
                    ELSE 0 
                END
            ELSE
                CASE 
                    WHEN billed_weight_lb > (actual_weight_lb * (1 + lb_threshold)) 
                      OR billed_weight_lb < (actual_weight_lb * (1 - lb_threshold)) THEN 1 
                    ELSE 0 
                END
        END AS is_weight_exception,

        -- Weight Exception Type (unit-specific)
        CASE 
            WHEN is_cost_exception = 0 THEN 'Normal'
            WHEN billed_weight_oz < 16 THEN
                CASE 
                    WHEN billed_weight_oz > (actual_weight_oz * (1 + oz_threshold)) THEN 'weight_high' 
                    WHEN billed_weight_oz < (actual_weight_oz * (1 - oz_threshold)) THEN 'weight_low' 
                    ELSE 'Normal' 
                END
            ELSE
                CASE 
                    WHEN billed_weight_lb > (actual_weight_lb * (1 + lb_threshold)) THEN 'weight_high' 
                    WHEN billed_weight_lb < (actual_weight_lb * (1 - lb_threshold)) THEN 'weight_low' 
                    ELSE 'Normal' 
                END
        END AS weight_exception_type,

        -- Weight Variance Percent (always calculated)
        CASE 
            WHEN ISNULL(actual_weight_oz, 0) = 0 THEN 100.0
            WHEN billed_weight_oz < 16 THEN
                CAST(((billed_weight_oz - actual_weight_oz) / CAST(NULLIF(actual_weight_oz, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
            ELSE
                CAST(((billed_weight_lb - actual_weight_lb) / CAST(NULLIF(actual_weight_lb, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS weight_variance_percent
    FROM cost_audit
)
SELECT * FROM weight_audit WHERE is_cost_exception = 1;
```

### UI Update Pattern

```sql
-- Simple UPDATE workflow
UPDATE dbo.recon_variance_config
SET 
    cost_variance_threshold_pct = @NewCostThreshold,
    weight_variance_threshold_oz_pct = @NewOzThreshold,
    weight_variance_threshold_lb_pct = @NewLbThreshold,
    updated_by = @CurrentUser,
    updated_date = GETDATE()
WHERE config_id = 1;
```

### Pros & Cons

✅ **Pros:**
- Simple to implement
- Simple UI (single form with UPDATE)
- No `WHERE` clause in view (marginal performance gain)
- No risk of multiple active configs

❌ **Cons:**
- No audit trail of threshold changes
- Can't answer "What was the threshold on Jan 15?"
- Can't rollback to previous settings
- Can't analyze "Did exception rate change due to carrier or threshold adjustment?"

---

## 📚 SCD Type 2: Version History (Audit Trail)

**Use this if:** Business needs to track historical threshold changes for compliance, analysis, or rollback capabilities.

### Table Schema

```sql
CREATE TABLE dbo.recon_variance_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Variance thresholds (percentage values)
    cost_variance_threshold_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_oz_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_lb_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    weight_variance_threshold_kg_pct DECIMAL(5,2) NOT NULL DEFAULT 10.0,
    
    -- Active flag (only ONE row can be active at a time)
    is_active BIT NOT NULL DEFAULT 1,
    
    -- Audit fields (versioning support)
    effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    created_by NVARCHAR(100) NULL,
    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    config_notes NVARCHAR(500) NULL  -- "Increased oz tolerance for holiday season"
);

-- Unique filtered index ensures only ONE active config
CREATE UNIQUE INDEX UQ_ReconConfig_Active 
ON dbo.recon_variance_config (is_active) 
WHERE is_active = 1;

-- Insert initial active configuration
INSERT INTO dbo.recon_variance_config (
    cost_variance_threshold_pct,
    weight_variance_threshold_oz_pct,
    weight_variance_threshold_lb_pct,
    weight_variance_threshold_kg_pct,
    is_active,
    created_by,
    config_notes
) VALUES (
    10.0,  -- 10% cost variance
    10.0,  -- 10% oz variance
    10.0,  -- 10% lb variance
    10.0,  -- 10% kg variance
    1,     -- Active
    'SYSTEM',
    'Initial configuration'
);
```

### View Changes

```sql
CREATE OR ALTER VIEW dbo.vw_recon_variance AS
WITH config AS (
    -- Filter to get only the active config
    SELECT 
        cost_variance_threshold_pct / 100.0 AS cost_threshold,
        weight_variance_threshold_oz_pct / 100.0 AS oz_threshold,
        weight_variance_threshold_lb_pct / 100.0 AS lb_threshold
    FROM dbo.recon_variance_config
    WHERE is_active = 1  -- Only the active configuration
),
standardized_weights AS (
    SELECT 
        shipment_package_id,
        tracking_number,
        CEILING(actual_weight_oz / 16.0) AS actual_weight_lb,
        CEILING(billed_weight_oz / 16.0) AS billed_weight_lb,
        billed_weight_oz,
        actual_weight_oz,
        wms_shipping_cost,
        billed_shipping_cost 
    FROM shipment_package
),
cost_audit AS (
    SELECT 
        sw.*,
        c.cost_threshold,
        c.oz_threshold,
        c.lb_threshold,
        CASE 
            WHEN sw.billed_shipping_cost > sw.wms_shipping_cost * (1 + c.cost_threshold) 
              OR sw.billed_shipping_cost < sw.wms_shipping_cost * (1 - c.cost_threshold) THEN 1 
            ELSE 0 
        END AS is_cost_exception,
        CASE 
            WHEN sw.billed_shipping_cost > sw.wms_shipping_cost * (1 + c.cost_threshold) THEN 'cost_high' 
            WHEN sw.billed_shipping_cost < sw.wms_shipping_cost * (1 - c.cost_threshold) THEN 'cost_low' 
            ELSE 'Normal' 
        END AS cost_exception_type,
        CONVERT(DECIMAL(10,2), sw.billed_shipping_cost - sw.wms_shipping_cost) AS cost_variance_amount
    FROM standardized_weights sw
    CROSS JOIN config c  -- Single active row, safe to CROSS JOIN
),
weight_audit AS (
    SELECT
        *,
        -- Weight Exception Flag (Gated by cost exception, unit-specific thresholds)
        CASE 
            WHEN is_cost_exception = 0 THEN 0
            WHEN billed_weight_oz < 16 THEN
                CASE 
                    WHEN billed_weight_oz > (actual_weight_oz * (1 + oz_threshold)) 
                      OR billed_weight_oz < (actual_weight_oz * (1 - oz_threshold)) THEN 1 
                    ELSE 0 
                END
            ELSE
                CASE 
                    WHEN billed_weight_lb > (actual_weight_lb * (1 + lb_threshold)) 
                      OR billed_weight_lb < (actual_weight_lb * (1 - lb_threshold)) THEN 1 
                    ELSE 0 
                END
        END AS is_weight_exception,

        -- Weight Exception Type (unit-specific)
        CASE 
            WHEN is_cost_exception = 0 THEN 'Normal'
            WHEN billed_weight_oz < 16 THEN
                CASE 
                    WHEN billed_weight_oz > (actual_weight_oz * (1 + oz_threshold)) THEN 'weight_high' 
                    WHEN billed_weight_oz < (actual_weight_oz * (1 - oz_threshold)) THEN 'weight_low' 
                    ELSE 'Normal' 
                END
            ELSE
                CASE 
                    WHEN billed_weight_lb > (actual_weight_lb * (1 + lb_threshold)) THEN 'weight_high' 
                    WHEN billed_weight_lb < (actual_weight_lb * (1 - lb_threshold)) THEN 'weight_low' 
                    ELSE 'Normal' 
                END
        END AS weight_exception_type,

        -- Weight Variance Percent (always calculated)
        CASE 
            WHEN ISNULL(actual_weight_oz, 0) = 0 THEN 100.0
            WHEN billed_weight_oz < 16 THEN
                CAST(((billed_weight_oz - actual_weight_oz) / CAST(NULLIF(actual_weight_oz, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
            ELSE
                CAST(((billed_weight_lb - actual_weight_lb) / CAST(NULLIF(actual_weight_lb, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS weight_variance_percent
    FROM cost_audit
)
SELECT * FROM weight_audit WHERE is_cost_exception = 1;
```

### UI Update Pattern (Create New Version)

```sql
-- Transaction to ensure atomicity
BEGIN TRANSACTION;

    -- Step 1: Deactivate current config
    UPDATE dbo.recon_variance_config
    SET is_active = 0
    WHERE is_active = 1;

    -- Step 2: Insert new active config
    INSERT INTO dbo.recon_variance_config (
        cost_variance_threshold_pct,
        weight_variance_threshold_oz_pct,
        weight_variance_threshold_lb_pct,
        weight_variance_threshold_kg_pct,
        is_active,
        created_by,
        config_notes
    ) VALUES (
        @NewCostThreshold,
        @NewOzThreshold,
        @NewLbThreshold,
        @NewKgThreshold,
        1,  -- Active
        @CurrentUser,
        @ConfigNotes
    );

COMMIT TRANSACTION;
```

### UI Rollback Pattern (Reactivate Previous Version)

```sql
BEGIN TRANSACTION;

    -- Deactivate current
    UPDATE dbo.recon_variance_config
    SET is_active = 0
    WHERE is_active = 1;

    -- Reactivate previous config
    -- Option A: Just flip the flag (preserves original created_by/date)
    UPDATE dbo.recon_variance_config
    SET is_active = 1
    WHERE config_id = @PreviousConfigId;
    
    -- Option B: Insert as new row (shows rollback was intentional)
    INSERT INTO dbo.recon_variance_config (
        cost_variance_threshold_pct,
        weight_variance_threshold_oz_pct,
        weight_variance_threshold_lb_pct,
        weight_variance_threshold_kg_pct,
        is_active,
        created_by,
        config_notes
    )
    SELECT 
        cost_variance_threshold_pct,
        weight_variance_threshold_oz_pct,
        weight_variance_threshold_lb_pct,
        weight_variance_threshold_kg_pct,
        1,  -- Active
        @CurrentUser,
        'Rolled back to config_id ' + CAST(@PreviousConfigId AS VARCHAR)
    FROM dbo.recon_variance_config
    WHERE config_id = @PreviousConfigId;

COMMIT TRANSACTION;
```

### Historical Analysis Query

```sql
-- What threshold was active on a specific date?
-- Use window function to find the next config's effective date
WITH config_periods AS (
    SELECT 
        *,
        LEAD(effective_date) OVER (ORDER BY effective_date) AS deactivated_date
    FROM dbo.recon_variance_config
)
SELECT TOP 1 *
FROM config_periods
WHERE effective_date <= '2026-01-15'
  AND (deactivated_date IS NULL OR deactivated_date > '2026-01-15')
ORDER BY effective_date DESC;

-- View all threshold changes with who deactivated each
SELECT 
    c.config_id,
    c.cost_variance_threshold_pct,
    c.weight_variance_threshold_oz_pct,
    c.weight_variance_threshold_lb_pct,
    c.effective_date,
    c.created_by,
    c.created_date,
    LEAD(c.created_by) OVER (ORDER BY c.created_date) AS deactivated_by,
    LEAD(c.created_date) OVER (ORDER BY c.created_date) AS deactivated_date,
    c.config_notes,
    CASE WHEN c.is_active = 1 THEN '✅ ACTIVE' ELSE '' END AS status
FROM dbo.recon_variance_config c
ORDER BY c.effective_date DESC;
```

### Pros & Cons

✅ **Pros:**
- Full audit trail of all threshold changes
- Can answer "What threshold was active when File X was processed?"
- Easy rollback to previous settings
- Supports compliance/regulatory requirements
- Can analyze trend changes (carrier behavior vs threshold adjustments)
- Unique index prevents multiple active configs

❌ **Cons:**
- More complex UI workflow (deactivate + insert pattern)
- Slightly more complex queries (need `WHERE is_active = 1`)
- More storage (keeps old versions)
- Need transaction handling in UI

---

## 🔄 Migration Path (Type 1 → Type 2)

If we start with Type 1 and later need Type 2:

```sql
-- Step 1: Add new columns
ALTER TABLE dbo.recon_variance_config
ADD 
    is_active BIT NOT NULL DEFAULT 1,
    effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    created_by NVARCHAR(100) NULL,
    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    config_notes NVARCHAR(500) NULL;

-- Step 2: Drop single-row constraint
ALTER TABLE dbo.recon_variance_config
DROP CONSTRAINT CK_ReconConfig_SingleRow;

-- Step 3: Change to IDENTITY primary key
ALTER TABLE dbo.recon_variance_config
DROP CONSTRAINT PK__recon_va__[hash];  -- Drop existing PK

ALTER TABLE dbo.recon_variance_config
DROP COLUMN config_id;

ALTER TABLE dbo.recon_variance_config
ADD config_id INT IDENTITY(1,1) PRIMARY KEY;

-- Step 4: Add unique active index
CREATE UNIQUE INDEX UQ_ReconConfig_Active 
ON dbo.recon_variance_config (is_active) 
WHERE is_active = 1;

-- Step 5: Update view to add WHERE is_active = 1
```

---

## 📊 Decision Matrix

| Requirement | Type 1 | Type 2 |
|-------------|--------|--------|
| Simple configuration management | ✅ Best | ⚠️ More complex |
| Compliance/audit requirements | ❌ No trail | ✅ Full trail |
| "Why was this flagged last month?" | ❌ Can't answer | ✅ Can answer |
| Rollback capability | ❌ Manual | ✅ Built-in |
| Trend analysis (threshold vs behavior) | ❌ Limited | ✅ Full support |
| Development effort | ✅ Minimal | ⚠️ Moderate |
| Storage footprint | ✅ Single row | ⚠️ Grows over time |

---

## 🎯 Recommendation

**For a billing/finance system:** Type 2 is usually preferred because:
1. Regulatory compliance often requires audit trails
2. Finance teams need to explain variances historically
3. The storage overhead is negligible (config changes are infrequent)
4. Rollback capability prevents "oops" moments

**Start Simple:** If unsure, start with Type 1 and migrate later if needed.

---

## ✅ Implementation Checklist

Once business confirms SCD type:

- [ ] Create `dbo.recon_variance_config` table
- [ ] Insert initial configuration row(s)
- [ ] Create unique index (Type 2 only)
- [ ] Update `dbo.vw_recon_variance` view definition
- [ ] Add to `schema.sql`
- [ ] Test view with different threshold values
- [ ] Create UI workflow for updates
- [ ] Document for ops team
- [ ] Add validation query to ensure only 1 active config (Type 2)

---

## 📝 Questions for Business

1. Do you need to know what variance thresholds were used when analyzing past billing exceptions?
2. If an auditor asks why a shipment was flagged, do you need to show the threshold settings from that time?
3. Do you need the ability to rollback threshold changes if they cause too many/few exceptions?
4. Will you analyze "exception rate trends" and need to separate carrier behavior changes from threshold changes?

**If any answer is YES → Use Type 2**
**If all answers are NO → Use Type 1**

