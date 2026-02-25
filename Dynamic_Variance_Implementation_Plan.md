# Dynamic Variance Threshold Implementation Plan

## 📋 Overview

**Problem:** `dbo.vw_recon_variance` currently has hardcoded variance thresholds (10% for cost and weight). We need UI-configurable, **range-based** thresholds where:

- The UI defines **cost ranges** (e.g., $0–$50, $50–$200, $200+) each with its own variance threshold
- The UI defines **weight ranges** (e.g., 0–16 oz, 16–160 oz, 160+ oz) each with its own variance threshold
- Each range's threshold is either **percentage (`pct`) or absolute (`abs`)** — one mode per range, never both
- Each **weight range specifies a comparison unit** (`oz`, `lb`, `kg`) — determines how weights are converted before comparison and what unit the `abs` threshold is expressed in
- Ranges within a metric are **explicitly non-overlapping** (validated at write time)

**Solution:** A range-based configuration table that the view joins to, matching each shipment to the appropriate cost and weight range to determine the applicable threshold, comparison mode, and weight unit.

**Business Decision Needed:** Do we need to track historical threshold changes for audit/analysis purposes?

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Cost range lookup value | `billed_shipping_cost` | Range based on carrier's billed amount |
| Weight range lookup value | `billed_weight_oz` | Range boundaries always stored in oz for consistent lookup |
| Weight comparison unit | Per-range `weight_unit` (`oz`/`lb`/`kg`) | Each weight range specifies what unit to convert to before comparing |
| Weight unit conversion | oz→lb: `CEILING(oz/16)`, oz→kg: `oz/35.274` | lb uses CEILING (preserves original rounding behavior) |
| Absolute cost unit | Dollars ($) | Direct dollar amount |
| Absolute weight unit | Matches `weight_unit` of the range | e.g., `abs` threshold of 2.0 on a `lb` range means ±2 lb |
| No matching range behavior | Not flagged (exception = 0) | If a shipment falls outside all defined ranges, it's not checked |
| Weight exception gating | Still gated by cost exception | Preserves existing business logic |

> **Important:** Range boundaries (`range_min`, `range_max`) for weight are **always in ounces** regardless of `weight_unit`. This ensures consistent range lookup. The UI should convert lb/kg boundaries to oz before saving (lb×16, kg×35.274).

---

## 🔄 SCD Type 1: Simple Configuration (No History)

**Use this if:** Business only cares about current settings, no need to track changes over time.

### Table Schema

```sql
CREATE TABLE dbo.recon_variance_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Metric & Range
    metric_type VARCHAR(10) NOT NULL,             -- 'cost' or 'weight'
    range_min DECIMAL(12,2) NOT NULL DEFAULT 0,   -- Lower bound (inclusive), weight always in oz
    range_max DECIMAL(12,2) NULL,                 -- Upper bound (exclusive), NULL = unbounded
    
    -- Threshold definition (pct OR abs, never both)
    threshold_type VARCHAR(3) NOT NULL,           -- 'pct' or 'abs'
    threshold_value DECIMAL(10,2) NOT NULL,       -- Percent value OR absolute amount (in weight_unit for weight, $ for cost)
    
    -- Weight comparison unit (only for weight ranges)
    weight_unit VARCHAR(2) NULL,                  -- 'oz', 'lb', 'kg' — determines comparison unit & abs unit
    
    -- Audit
    updated_by NVARCHAR(100) NULL,
    updated_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT CK_MetricType CHECK (metric_type IN ('cost', 'weight')),
    CONSTRAINT CK_ThresholdType CHECK (threshold_type IN ('pct', 'abs')),
    CONSTRAINT CK_RangeValid CHECK (range_max IS NULL OR range_min < range_max),
    CONSTRAINT CK_ThresholdPositive CHECK (threshold_value > 0),
    CONSTRAINT CK_RangeMinNonNegative CHECK (range_min >= 0),
    CONSTRAINT CK_WeightUnit CHECK (
        (metric_type = 'cost' AND weight_unit IS NULL) OR
        (metric_type = 'weight' AND weight_unit IN ('oz', 'lb', 'kg'))
    )
);

-- Initial configuration: Cost ranges (weight_unit = NULL for cost)
INSERT INTO dbo.recon_variance_config 
    (metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
VALUES
    ('cost',   0.00,   50.00, 'pct', 15.0, NULL),    -- $0–$50:   15% tolerance
    ('cost',  50.00,  200.00, 'pct', 10.0, NULL),    -- $50–$200:  10% tolerance
    ('cost', 200.00,    NULL, 'pct',  5.0, NULL);     -- $200+:      5% tolerance

-- Initial configuration: Weight ranges (boundaries in oz, comparison in specified unit)
INSERT INTO dbo.recon_variance_config 
    (metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
VALUES
    ('weight',   0.00,  16.00, 'abs',  2.0, 'oz'),   -- 0–16 oz:    ±2 oz absolute (compare in oz)
    ('weight',  16.00, 160.00, 'pct', 10.0, 'lb'),   -- 16–160 oz:  10% tolerance (compare in lb, CEILING)
    ('weight', 160.00,   NULL, 'pct',  5.0, 'lb');    -- 160+ oz:     5% tolerance (compare in lb, CEILING)
```

### View Changes

```sql
CREATE OR ALTER VIEW dbo.vw_recon_variance AS
WITH cost_config AS (
    SELECT range_min, range_max, threshold_type, threshold_value
    FROM dbo.recon_variance_config
    WHERE metric_type = 'cost'
),
weight_config AS (
    SELECT range_min, range_max, threshold_type, threshold_value, weight_unit
    FROM dbo.recon_variance_config
    WHERE metric_type = 'weight'
),
base_data AS (
    SELECT 
        sp.shipment_package_id,
        sp.tracking_number,
        sp.billed_weight_oz,
        sp.actual_weight_oz,
        sp.wms_shipping_cost,
        sp.billed_shipping_cost,
        -- Matched cost range & threshold
        cc.range_min   AS cost_range_min,
        cc.range_max   AS cost_range_max,
        cc.threshold_type  AS cost_threshold_type,
        cc.threshold_value AS cost_threshold_value,
        -- Matched weight range & threshold
        wc.range_min   AS weight_range_min,
        wc.range_max   AS weight_range_max,
        wc.threshold_type  AS weight_threshold_type,
        wc.threshold_value AS weight_threshold_value,
        wc.weight_unit
    FROM shipment_package sp
    LEFT JOIN cost_config cc
        ON sp.billed_shipping_cost >= cc.range_min
        AND (cc.range_max IS NULL OR sp.billed_shipping_cost < cc.range_max)
    LEFT JOIN weight_config wc
        ON sp.billed_weight_oz >= wc.range_min
        AND (wc.range_max IS NULL OR sp.billed_weight_oz < wc.range_max)
),
cost_audit AS (
    SELECT 
        bd.*,
        -- Cost variance (always calculated for display)
        CONVERT(DECIMAL(10,2), bd.billed_shipping_cost - bd.wms_shipping_cost) AS cost_variance_amount,
        CASE 
            WHEN ISNULL(bd.wms_shipping_cost, 0) = 0 THEN 100.0
            ELSE CAST(((bd.billed_shipping_cost - bd.wms_shipping_cost) 
                  / CAST(NULLIF(bd.wms_shipping_cost, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS cost_variance_pct,

        -- Cost Exception Flag (threshold-type aware)
        CASE 
            WHEN bd.cost_threshold_type = 'pct' THEN
                CASE 
                    WHEN bd.billed_shipping_cost > bd.wms_shipping_cost * (1 + bd.cost_threshold_value / 100.0)
                      OR bd.billed_shipping_cost < bd.wms_shipping_cost * (1 - bd.cost_threshold_value / 100.0) THEN 1
                    ELSE 0
                END
            WHEN bd.cost_threshold_type = 'abs' THEN
                CASE 
                    WHEN ABS(bd.billed_shipping_cost - bd.wms_shipping_cost) > bd.cost_threshold_value THEN 1
                    ELSE 0
                END
            ELSE 0  -- No matching range = not flagged
        END AS is_cost_exception,

        -- Cost Exception Type
        CASE 
            WHEN bd.cost_threshold_type = 'pct' THEN
                CASE 
                    WHEN bd.billed_shipping_cost > bd.wms_shipping_cost * (1 + bd.cost_threshold_value / 100.0) THEN 'cost_high'
                    WHEN bd.billed_shipping_cost < bd.wms_shipping_cost * (1 - bd.cost_threshold_value / 100.0) THEN 'cost_low'
                    ELSE 'Normal'
                END
            WHEN bd.cost_threshold_type = 'abs' THEN
                CASE 
                    WHEN (bd.billed_shipping_cost - bd.wms_shipping_cost) > bd.cost_threshold_value THEN 'cost_high'
                    WHEN (bd.billed_shipping_cost - bd.wms_shipping_cost) < -bd.cost_threshold_value THEN 'cost_low'
                    ELSE 'Normal'
                END
            ELSE 'Normal'
        END AS cost_exception_type
    FROM base_data bd
),
-- Convert weights to the comparison unit specified by the matched weight range
weight_converted AS (
    SELECT
        ca.*,
        CASE ca.weight_unit
            WHEN 'oz' THEN ca.billed_weight_oz
            WHEN 'lb' THEN CEILING(ca.billed_weight_oz / 16.0)
            WHEN 'kg' THEN CAST(ca.billed_weight_oz / 35.274 AS DECIMAL(10,4))
            ELSE ca.billed_weight_oz
        END AS billed_weight_cmp,
        CASE ca.weight_unit
            WHEN 'oz' THEN ca.actual_weight_oz
            WHEN 'lb' THEN CEILING(ca.actual_weight_oz / 16.0)
            WHEN 'kg' THEN CAST(ca.actual_weight_oz / 35.274 AS DECIMAL(10,4))
            ELSE ca.actual_weight_oz
        END AS actual_weight_cmp
    FROM cost_audit ca
),
weight_audit AS (
    SELECT
        wc.*,
        -- Weight variance in comparison unit (always calculated for display)
        CONVERT(DECIMAL(10,2), wc.billed_weight_cmp - wc.actual_weight_cmp) AS weight_variance,
        CASE 
            WHEN ISNULL(wc.actual_weight_cmp, 0) = 0 THEN 100.0
            ELSE CAST(((wc.billed_weight_cmp - wc.actual_weight_cmp) 
                  / CAST(NULLIF(wc.actual_weight_cmp, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS weight_variance_pct,

        -- Weight Exception Flag (gated by cost exception, threshold-type aware)
        CASE 
            WHEN wc.is_cost_exception = 0 THEN 0
            WHEN wc.weight_threshold_type = 'pct' THEN
                CASE 
                    WHEN wc.billed_weight_cmp > wc.actual_weight_cmp * (1 + wc.weight_threshold_value / 100.0)
                      OR wc.billed_weight_cmp < wc.actual_weight_cmp * (1 - wc.weight_threshold_value / 100.0) THEN 1
                    ELSE 0
                END
            WHEN wc.weight_threshold_type = 'abs' THEN
                CASE 
                    WHEN ABS(wc.billed_weight_cmp - wc.actual_weight_cmp) > wc.weight_threshold_value THEN 1
                    ELSE 0
                END
            ELSE 0  -- No matching range = not flagged
        END AS is_weight_exception,

        -- Weight Exception Type
        CASE 
            WHEN wc.is_cost_exception = 0 THEN 'Normal'
            WHEN wc.weight_threshold_type = 'pct' THEN
                CASE 
                    WHEN wc.billed_weight_cmp > wc.actual_weight_cmp * (1 + wc.weight_threshold_value / 100.0) THEN 'weight_high'
                    WHEN wc.billed_weight_cmp < wc.actual_weight_cmp * (1 - wc.weight_threshold_value / 100.0) THEN 'weight_low'
                    ELSE 'Normal'
                END
            WHEN wc.weight_threshold_type = 'abs' THEN
                CASE 
                    WHEN (wc.billed_weight_cmp - wc.actual_weight_cmp) > wc.weight_threshold_value THEN 'weight_high'
                    WHEN (wc.billed_weight_cmp - wc.actual_weight_cmp) < -wc.weight_threshold_value THEN 'weight_low'
                    ELSE 'Normal'
                END
            ELSE 'Normal'
        END AS weight_exception_type
    FROM weight_converted wc
)
SELECT * FROM weight_audit WHERE is_cost_exception = 1;
```

### UI Update Pattern (Full Replace via Stored Procedure)

```sql
CREATE OR ALTER PROCEDURE dbo.usp_update_recon_variance_config
    @config_json NVARCHAR(MAX),   -- JSON array of range definitions
    @updated_by  NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Parse JSON into temp table
    SELECT 
        metric_type,
        range_min,
        range_max,
        threshold_type,
        threshold_value,
        weight_unit
    INTO #new_config
    FROM OPENJSON(@config_json)
    WITH (
        metric_type    VARCHAR(10)    '$.metric_type',
        range_min      DECIMAL(12,2)  '$.range_min',
        range_max      DECIMAL(12,2)  '$.range_max',
        threshold_type VARCHAR(3)     '$.threshold_type',
        threshold_value DECIMAL(10,2) '$.threshold_value',
        weight_unit    VARCHAR(2)     '$.weight_unit'
    );

    -- Step 2: Validate metric_type values
    IF EXISTS (SELECT 1 FROM #new_config WHERE metric_type NOT IN ('cost', 'weight'))
    BEGIN
        DROP TABLE #new_config;
        THROW 50001, 'Invalid metric_type. Must be "cost" or "weight".', 1;
    END;

    -- Step 3: Validate threshold_type values
    IF EXISTS (SELECT 1 FROM #new_config WHERE threshold_type NOT IN ('pct', 'abs'))
    BEGIN
        DROP TABLE #new_config;
        THROW 50002, 'Invalid threshold_type. Must be "pct" or "abs".', 1;
    END;

    -- Step 4: Validate weight_unit (required for weight, must be NULL for cost)
    IF EXISTS (SELECT 1 FROM #new_config WHERE metric_type = 'weight' AND weight_unit NOT IN ('oz', 'lb', 'kg'))
    BEGIN
        DROP TABLE #new_config;
        THROW 50006, 'Weight ranges require weight_unit ("oz", "lb", or "kg").', 1;
    END;

    IF EXISTS (SELECT 1 FROM #new_config WHERE metric_type = 'cost' AND weight_unit IS NOT NULL)
    BEGIN
        DROP TABLE #new_config;
        THROW 50007, 'Cost ranges must not have a weight_unit (should be null).', 1;
    END;

    -- Step 5: Validate range boundaries
    IF EXISTS (SELECT 1 FROM #new_config WHERE range_max IS NOT NULL AND range_min >= range_max)
    BEGIN
        DROP TABLE #new_config;
        THROW 50003, 'Invalid range: range_min must be less than range_max.', 1;
    END;

    -- Step 6: Validate threshold values are positive
    IF EXISTS (SELECT 1 FROM #new_config WHERE threshold_value <= 0)
    BEGIN
        DROP TABLE #new_config;
        THROW 50004, 'Threshold value must be positive.', 1;
    END;

    -- Step 7: Validate NON-OVERLAPPING ranges per metric
    -- Two ranges [a_min, a_max) and [b_min, b_max) overlap when:
    --   a_min < ISNULL(b_max, ∞) AND b_min < ISNULL(a_max, ∞)
    IF EXISTS (
        SELECT 1
        FROM #new_config a
        JOIN #new_config b
            ON  a.metric_type = b.metric_type
            AND a.range_min < b.range_min   -- avoid self-join & duplicate pairs
        WHERE a.range_min < ISNULL(b.range_max, 999999999.99)
          AND b.range_min < ISNULL(a.range_max, 999999999.99)
    )
    BEGIN
        DROP TABLE #new_config;
        THROW 50005, 'Overlapping ranges detected. Ranges within a metric must be non-overlapping.', 1;
    END;

    -- Step 8: All validations passed → atomic replace
    BEGIN TRANSACTION;

        DELETE FROM dbo.recon_variance_config;

        INSERT INTO dbo.recon_variance_config 
            (metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit, updated_by, updated_date)
        SELECT 
            metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit,
            @updated_by, GETDATE()
        FROM #new_config;

    COMMIT TRANSACTION;

    -- Step 9: Return result
    SELECT 
        'SUCCESS' AS status, 
        COUNT(*) AS ranges_configured,
        SUM(CASE WHEN metric_type = 'cost' THEN 1 ELSE 0 END) AS cost_ranges,
        SUM(CASE WHEN metric_type = 'weight' THEN 1 ELSE 0 END) AS weight_ranges
    FROM #new_config;

    DROP TABLE #new_config;
END;
```

**Example UI call:**

```sql
EXEC dbo.usp_update_recon_variance_config
    @config_json = N'[
        {"metric_type":"cost",   "range_min":0,    "range_max":50,   "threshold_type":"pct", "threshold_value":15.0, "weight_unit":null},
        {"metric_type":"cost",   "range_min":50,   "range_max":200,  "threshold_type":"pct", "threshold_value":10.0, "weight_unit":null},
        {"metric_type":"cost",   "range_min":200,  "range_max":null, "threshold_type":"abs", "threshold_value":8.00, "weight_unit":null},
        {"metric_type":"weight", "range_min":0,    "range_max":16,   "threshold_type":"abs", "threshold_value":2.0,  "weight_unit":"oz"},
        {"metric_type":"weight", "range_min":16,   "range_max":160,  "threshold_type":"pct", "threshold_value":10.0, "weight_unit":"lb"},
        {"metric_type":"weight", "range_min":160,  "range_max":null, "threshold_type":"pct", "threshold_value":5.0,  "weight_unit":"lb"}
    ]',
    @updated_by = N'admin@company.com';
```

### Pros & Cons

✅ **Pros:**
- Simple single-table design
- Full replace pattern avoids partial state issues
- Non-overlapping validation at write time
- No risk of stale/orphan data

❌ **Cons:**
- No audit trail of threshold changes
- Can't answer "What was the threshold on Jan 15?"
- Can't rollback to previous settings

---

## 📚 SCD Type 2: Version History (Audit Trail)

**Use this if:** Business needs to track historical threshold changes for compliance, analysis, or rollback capabilities.

### Table Schema

```sql
-- Header: groups a set of ranges into a version
CREATE TABLE dbo.recon_variance_config_version (
    version_id INT IDENTITY(1,1) PRIMARY KEY,
    
    is_active BIT NOT NULL DEFAULT 1,
    effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    created_by NVARCHAR(100) NULL,
    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    config_notes NVARCHAR(500) NULL   -- "Increased oz tolerance for holiday season"
);

-- Only ONE active version at a time
CREATE UNIQUE INDEX UQ_ConfigVersion_Active 
ON dbo.recon_variance_config_version (is_active) 
WHERE is_active = 1;

-- Detail: range-based thresholds belonging to a version
CREATE TABLE dbo.recon_variance_config_range (
    range_id INT IDENTITY(1,1) PRIMARY KEY,
    version_id INT NOT NULL,
    
    -- Metric & Range
    metric_type VARCHAR(10) NOT NULL,             -- 'cost' or 'weight'
    range_min DECIMAL(12,2) NOT NULL DEFAULT 0,   -- Lower bound (inclusive), weight always in oz
    range_max DECIMAL(12,2) NULL,                 -- Upper bound (exclusive), NULL = unbounded
    
    -- Threshold definition
    threshold_type VARCHAR(3) NOT NULL,           -- 'pct' or 'abs'
    threshold_value DECIMAL(10,2) NOT NULL,       -- Percent value OR absolute amount (in weight_unit for weight, $ for cost)
    
    -- Weight comparison unit (only for weight ranges)
    weight_unit VARCHAR(2) NULL,                  -- 'oz', 'lb', 'kg' — determines comparison unit & abs unit
    
    CONSTRAINT FK_Range_Version FOREIGN KEY (version_id) 
        REFERENCES dbo.recon_variance_config_version(version_id),
    CONSTRAINT CK_Range_MetricType CHECK (metric_type IN ('cost', 'weight')),
    CONSTRAINT CK_Range_ThresholdType CHECK (threshold_type IN ('pct', 'abs')),
    CONSTRAINT CK_Range_Valid CHECK (range_max IS NULL OR range_min < range_max),
    CONSTRAINT CK_Range_ThresholdPositive CHECK (threshold_value > 0),
    CONSTRAINT CK_Range_MinNonNegative CHECK (range_min >= 0),
    CONSTRAINT CK_Range_WeightUnit CHECK (
        (metric_type = 'cost' AND weight_unit IS NULL) OR
        (metric_type = 'weight' AND weight_unit IN ('oz', 'lb', 'kg'))
    )
);

-- Insert initial version
INSERT INTO dbo.recon_variance_config_version 
    (is_active, created_by, config_notes)
VALUES 
    (1, 'SYSTEM', 'Initial configuration');

DECLARE @v_id INT = SCOPE_IDENTITY();

-- Cost ranges
INSERT INTO dbo.recon_variance_config_range 
    (version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
VALUES
    (@v_id, 'cost',   0.00,   50.00, 'pct', 15.0, NULL),
    (@v_id, 'cost',  50.00,  200.00, 'pct', 10.0, NULL),
    (@v_id, 'cost', 200.00,    NULL, 'pct',  5.0, NULL);

-- Weight ranges (boundaries in oz, comparison unit per range)
INSERT INTO dbo.recon_variance_config_range 
    (version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
VALUES
    (@v_id, 'weight',   0.00,  16.00, 'abs',  2.0, 'oz'),   -- compare in oz
    (@v_id, 'weight',  16.00, 160.00, 'pct', 10.0, 'lb'),   -- compare in lb (CEILING)
    (@v_id, 'weight', 160.00,   NULL, 'pct',  5.0, 'lb');    -- compare in lb (CEILING)
```

### View Changes

```sql
CREATE OR ALTER VIEW dbo.vw_recon_variance AS
WITH active_version AS (
    SELECT version_id
    FROM dbo.recon_variance_config_version
    WHERE is_active = 1
),
cost_config AS (
    SELECT r.range_min, r.range_max, r.threshold_type, r.threshold_value
    FROM dbo.recon_variance_config_range r
    JOIN active_version av ON av.version_id = r.version_id
    WHERE r.metric_type = 'cost'
),
weight_config AS (
    SELECT r.range_min, r.range_max, r.threshold_type, r.threshold_value, r.weight_unit
    FROM dbo.recon_variance_config_range r
    JOIN active_version av ON av.version_id = r.version_id
    WHERE r.metric_type = 'weight'
),
base_data AS (
    SELECT 
        sp.shipment_package_id,
        sp.tracking_number,
        sp.billed_weight_oz,
        sp.actual_weight_oz,
        sp.wms_shipping_cost,
        sp.billed_shipping_cost,
        -- Matched cost range & threshold
        cc.range_min   AS cost_range_min,
        cc.range_max   AS cost_range_max,
        cc.threshold_type  AS cost_threshold_type,
        cc.threshold_value AS cost_threshold_value,
        -- Matched weight range & threshold
        wc.range_min   AS weight_range_min,
        wc.range_max   AS weight_range_max,
        wc.threshold_type  AS weight_threshold_type,
        wc.threshold_value AS weight_threshold_value,
        wc.weight_unit
    FROM shipment_package sp
    LEFT JOIN cost_config cc
        ON sp.billed_shipping_cost >= cc.range_min
        AND (cc.range_max IS NULL OR sp.billed_shipping_cost < cc.range_max)
    LEFT JOIN weight_config wc
        ON sp.billed_weight_oz >= wc.range_min
        AND (wc.range_max IS NULL OR sp.billed_weight_oz < wc.range_max)
),
cost_audit AS (
    SELECT 
        bd.*,
        -- Cost variance (always calculated for display)
        CONVERT(DECIMAL(10,2), bd.billed_shipping_cost - bd.wms_shipping_cost) AS cost_variance_amount,
        CASE 
            WHEN ISNULL(bd.wms_shipping_cost, 0) = 0 THEN 100.0
            ELSE CAST(((bd.billed_shipping_cost - bd.wms_shipping_cost) 
                  / CAST(NULLIF(bd.wms_shipping_cost, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS cost_variance_pct,

        -- Cost Exception Flag (threshold-type aware)
        CASE 
            WHEN bd.cost_threshold_type = 'pct' THEN
                CASE 
                    WHEN bd.billed_shipping_cost > bd.wms_shipping_cost * (1 + bd.cost_threshold_value / 100.0)
                      OR bd.billed_shipping_cost < bd.wms_shipping_cost * (1 - bd.cost_threshold_value / 100.0) THEN 1
                    ELSE 0
                END
            WHEN bd.cost_threshold_type = 'abs' THEN
                CASE 
                    WHEN ABS(bd.billed_shipping_cost - bd.wms_shipping_cost) > bd.cost_threshold_value THEN 1
                    ELSE 0
                END
            ELSE 0  -- No matching range = not flagged
        END AS is_cost_exception,

        -- Cost Exception Type
        CASE 
            WHEN bd.cost_threshold_type = 'pct' THEN
                CASE 
                    WHEN bd.billed_shipping_cost > bd.wms_shipping_cost * (1 + bd.cost_threshold_value / 100.0) THEN 'cost_high'
                    WHEN bd.billed_shipping_cost < bd.wms_shipping_cost * (1 - bd.cost_threshold_value / 100.0) THEN 'cost_low'
                    ELSE 'Normal'
                END
            WHEN bd.cost_threshold_type = 'abs' THEN
                CASE 
                    WHEN (bd.billed_shipping_cost - bd.wms_shipping_cost) > bd.cost_threshold_value THEN 'cost_high'
                    WHEN (bd.billed_shipping_cost - bd.wms_shipping_cost) < -bd.cost_threshold_value THEN 'cost_low'
                    ELSE 'Normal'
                END
            ELSE 'Normal'
        END AS cost_exception_type
    FROM base_data bd
),
-- Convert weights to the comparison unit specified by the matched weight range
weight_converted AS (
    SELECT
        ca.*,
        -- Billed weight in comparison unit
        CASE ca.weight_unit
            WHEN 'oz' THEN ca.billed_weight_oz
            WHEN 'lb' THEN CEILING(ca.billed_weight_oz / 16.0)
            WHEN 'kg' THEN CAST(ca.billed_weight_oz / 35.274 AS DECIMAL(10,4))
            ELSE ca.billed_weight_oz
        END AS billed_weight_cmp,
        -- Actual weight in comparison unit
        CASE ca.weight_unit
            WHEN 'oz' THEN ca.actual_weight_oz
            WHEN 'lb' THEN CEILING(ca.actual_weight_oz / 16.0)
            WHEN 'kg' THEN CAST(ca.actual_weight_oz / 35.274 AS DECIMAL(10,4))
            ELSE ca.actual_weight_oz
        END AS actual_weight_cmp
    FROM cost_audit ca
),
weight_audit AS (
    SELECT
        wc.*,
        -- Weight variance in comparison unit (always calculated for display)
        CONVERT(DECIMAL(10,2), wc.billed_weight_cmp - wc.actual_weight_cmp) AS weight_variance,
        CASE 
            WHEN ISNULL(wc.actual_weight_cmp, 0) = 0 THEN 100.0
            ELSE CAST(((wc.billed_weight_cmp - wc.actual_weight_cmp) 
                  / CAST(NULLIF(wc.actual_weight_cmp, 0) AS FLOAT)) * 100 AS DECIMAL(10,2))
        END AS weight_variance_pct,

        -- Weight Exception Flag (gated by cost exception, threshold-type aware)
        CASE 
            WHEN wc.is_cost_exception = 0 THEN 0
            WHEN wc.weight_threshold_type = 'pct' THEN
                CASE 
                    WHEN wc.billed_weight_cmp > wc.actual_weight_cmp * (1 + wc.weight_threshold_value / 100.0)
                      OR wc.billed_weight_cmp < wc.actual_weight_cmp * (1 - wc.weight_threshold_value / 100.0) THEN 1
                    ELSE 0
                END
            WHEN wc.weight_threshold_type = 'abs' THEN
                CASE 
                    WHEN ABS(wc.billed_weight_cmp - wc.actual_weight_cmp) > wc.weight_threshold_value THEN 1
                    ELSE 0
                END
            ELSE 0  -- No matching range = not flagged
        END AS is_weight_exception,

        -- Weight Exception Type
        CASE 
            WHEN wc.is_cost_exception = 0 THEN 'Normal'
            WHEN wc.weight_threshold_type = 'pct' THEN
                CASE 
                    WHEN wc.billed_weight_cmp > wc.actual_weight_cmp * (1 + wc.weight_threshold_value / 100.0) THEN 'weight_high'
                    WHEN wc.billed_weight_cmp < wc.actual_weight_cmp * (1 - wc.weight_threshold_value / 100.0) THEN 'weight_low'
                    ELSE 'Normal'
                END
            WHEN wc.weight_threshold_type = 'abs' THEN
                CASE 
                    WHEN (wc.billed_weight_cmp - wc.actual_weight_cmp) > wc.weight_threshold_value THEN 'weight_high'
                    WHEN (wc.billed_weight_cmp - wc.actual_weight_cmp) < -wc.weight_threshold_value THEN 'weight_low'
                    ELSE 'Normal'
                END
            ELSE 'Normal'
        END AS weight_exception_type
    FROM weight_converted wc
)
SELECT * FROM weight_audit WHERE is_cost_exception = 1;
```

### UI Update Pattern (Create New Version via Stored Procedure)

```sql
CREATE OR ALTER PROCEDURE dbo.usp_update_recon_variance_config
    @config_json  NVARCHAR(MAX),   -- JSON array of range definitions
    @created_by   NVARCHAR(100),
    @config_notes NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Parse JSON into temp table
    SELECT 
        metric_type,
        range_min,
        range_max,
        threshold_type,
        threshold_value,
        weight_unit
    INTO #new_ranges
    FROM OPENJSON(@config_json)
    WITH (
        metric_type    VARCHAR(10)    '$.metric_type',
        range_min      DECIMAL(12,2)  '$.range_min',
        range_max      DECIMAL(12,2)  '$.range_max',
        threshold_type VARCHAR(3)     '$.threshold_type',
        threshold_value DECIMAL(10,2) '$.threshold_value',
        weight_unit    VARCHAR(2)     '$.weight_unit'
    );

    -- Step 2: Validate metric_type values
    IF EXISTS (SELECT 1 FROM #new_ranges WHERE metric_type NOT IN ('cost', 'weight'))
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50001, 'Invalid metric_type. Must be "cost" or "weight".', 1;
    END;

    -- Step 3: Validate threshold_type values
    IF EXISTS (SELECT 1 FROM #new_ranges WHERE threshold_type NOT IN ('pct', 'abs'))
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50002, 'Invalid threshold_type. Must be "pct" or "abs".', 1;
    END;

    -- Step 4: Validate weight_unit (required for weight, must be NULL for cost)
    IF EXISTS (SELECT 1 FROM #new_ranges WHERE metric_type = 'weight' AND weight_unit NOT IN ('oz', 'lb', 'kg'))
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50006, 'Weight ranges require weight_unit ("oz", "lb", or "kg").', 1;
    END;

    IF EXISTS (SELECT 1 FROM #new_ranges WHERE metric_type = 'cost' AND weight_unit IS NOT NULL)
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50007, 'Cost ranges must not have a weight_unit (should be null).', 1;
    END;

    -- Step 5: Validate range boundaries
    IF EXISTS (SELECT 1 FROM #new_ranges WHERE range_max IS NOT NULL AND range_min >= range_max)
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50003, 'Invalid range: range_min must be less than range_max.', 1;
    END;

    -- Step 6: Validate threshold values are positive
    IF EXISTS (SELECT 1 FROM #new_ranges WHERE threshold_value <= 0)
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50004, 'Threshold value must be positive.', 1;
    END;

    -- Step 7: Validate NON-OVERLAPPING ranges per metric
    -- Two half-open ranges [a_min, a_max) and [b_min, b_max) overlap when:
    --   a_min < ISNULL(b_max, ∞) AND b_min < ISNULL(a_max, ∞)
    IF EXISTS (
        SELECT 1
        FROM #new_ranges a
        JOIN #new_ranges b
            ON  a.metric_type = b.metric_type
            AND a.range_min < b.range_min        -- avoid self & duplicate pairs
        WHERE a.range_min < ISNULL(b.range_max, 999999999.99)
          AND b.range_min < ISNULL(a.range_max, 999999999.99)
    )
    BEGIN
        DROP TABLE #new_ranges;
        THROW 50005, 'Overlapping ranges detected. Ranges within a metric must be non-overlapping.', 1;
    END;

    -- Step 8: All validations passed → atomic version swap
    BEGIN TRANSACTION;

        -- Deactivate current version
        UPDATE dbo.recon_variance_config_version
        SET is_active = 0
        WHERE is_active = 1;

        -- Create new version
        INSERT INTO dbo.recon_variance_config_version 
            (is_active, created_by, config_notes)
        VALUES 
            (1, @created_by, @config_notes);

        DECLARE @new_version_id INT = SCOPE_IDENTITY();

        -- Insert range rows
        INSERT INTO dbo.recon_variance_config_range 
            (version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
        SELECT 
            @new_version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit
        FROM #new_ranges;

    COMMIT TRANSACTION;

    -- Step 9: Return result
    SELECT 
        'SUCCESS' AS status,
        @new_version_id AS version_id,
        COUNT(*) AS ranges_configured,
        SUM(CASE WHEN metric_type = 'cost' THEN 1 ELSE 0 END) AS cost_ranges,
        SUM(CASE WHEN metric_type = 'weight' THEN 1 ELSE 0 END) AS weight_ranges
    FROM #new_ranges;

    DROP TABLE #new_ranges;
END;
```

**Example UI call:**

```sql
EXEC dbo.usp_update_recon_variance_config
    @config_json = N'[
        {"metric_type":"cost",   "range_min":0,    "range_max":50,   "threshold_type":"pct", "threshold_value":15.0, "weight_unit":null},
        {"metric_type":"cost",   "range_min":50,   "range_max":200,  "threshold_type":"pct", "threshold_value":10.0, "weight_unit":null},
        {"metric_type":"cost",   "range_min":200,  "range_max":null, "threshold_type":"abs", "threshold_value":8.00, "weight_unit":null},
        {"metric_type":"weight", "range_min":0,    "range_max":16,   "threshold_type":"abs", "threshold_value":2.0,  "weight_unit":"oz"},
        {"metric_type":"weight", "range_min":16,   "range_max":160,  "threshold_type":"pct", "threshold_value":10.0, "weight_unit":"lb"},
        {"metric_type":"weight", "range_min":160,  "range_max":null, "threshold_type":"pct", "threshold_value":5.0,  "weight_unit":"lb"}
    ]',
    @created_by = N'admin@company.com',
    @config_notes = N'Tightened high-value cost tolerance, added abs threshold for 200+';
```

### UI Rollback Pattern (Reactivate Previous Version)

```sql
CREATE OR ALTER PROCEDURE dbo.usp_rollback_recon_variance_config
    @previous_version_id INT,
    @created_by NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    -- Verify the target version exists
    IF NOT EXISTS (SELECT 1 FROM dbo.recon_variance_config_version WHERE version_id = @previous_version_id)
    BEGIN
        THROW 50010, 'Version not found.', 1;
    END;

    BEGIN TRANSACTION;

        -- Deactivate current version
        UPDATE dbo.recon_variance_config_version
        SET is_active = 0
        WHERE is_active = 1;

        -- Create new version as a copy of the previous one
        INSERT INTO dbo.recon_variance_config_version 
            (is_active, created_by, config_notes)
        VALUES 
            (1, @created_by, 'Rolled back to version_id ' + CAST(@previous_version_id AS VARCHAR));

        DECLARE @new_version_id INT = SCOPE_IDENTITY();

        -- Copy ranges from the previous version (including weight_unit)
        INSERT INTO dbo.recon_variance_config_range 
            (version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit)
        SELECT 
            @new_version_id, metric_type, range_min, range_max, threshold_type, threshold_value, weight_unit
        FROM dbo.recon_variance_config_range
        WHERE version_id = @previous_version_id;

    COMMIT TRANSACTION;

    SELECT 'SUCCESS' AS status, @new_version_id AS new_version_id;
END;
```

### Historical Analysis Queries

```sql
-- What ranges were active on a specific date?
WITH version_periods AS (
    SELECT 
        v.*,
        LEAD(v.effective_date) OVER (ORDER BY v.effective_date) AS next_effective_date
    FROM dbo.recon_variance_config_version v
)
SELECT 
    vp.version_id,
    vp.effective_date,
    vp.created_by,
    vp.config_notes,
    r.metric_type,
    r.range_min,
    r.range_max,
    r.threshold_type,
    r.threshold_value,
    r.weight_unit
FROM version_periods vp
JOIN dbo.recon_variance_config_range r ON r.version_id = vp.version_id
WHERE vp.effective_date <= '2026-01-15'
  AND (vp.next_effective_date IS NULL OR vp.next_effective_date > '2026-01-15')
ORDER BY r.metric_type, r.range_min;

-- View all version changes (summary)
SELECT 
    v.version_id,
    v.effective_date,
    v.created_by,
    v.created_date,
    v.config_notes,
    COUNT(r.range_id) AS total_ranges,
    SUM(CASE WHEN r.metric_type = 'cost' THEN 1 ELSE 0 END) AS cost_ranges,
    SUM(CASE WHEN r.metric_type = 'weight' THEN 1 ELSE 0 END) AS weight_ranges,
    CASE WHEN v.is_active = 1 THEN '✅ ACTIVE' ELSE '' END AS status
FROM dbo.recon_variance_config_version v
LEFT JOIN dbo.recon_variance_config_range r ON r.version_id = v.version_id
GROUP BY v.version_id, v.effective_date, v.created_by, v.created_date, v.config_notes, v.is_active
ORDER BY v.effective_date DESC;
```

### Pros & Cons

✅ **Pros:**
- Full audit trail of all threshold changes
- Can answer "What ranges were active when File X was processed?"
- Easy rollback to any previous version
- Supports compliance/regulatory requirements
- Can analyze trend changes (carrier behavior vs threshold adjustments)
- Unique index prevents multiple active versions

❌ **Cons:**
- Two-table design (version header + range detail)
- More complex UI workflow (deactivate + insert pattern)
- More storage (keeps old versions and their ranges)
- Need transaction handling in UI

---

## 🔒 Non-Overlapping Range Validation

Ranges use **half-open intervals**: `[range_min, range_max)` — inclusive lower bound, exclusive upper bound. `NULL` upper bound means unbounded (∞).

### How It Works

Two ranges overlap when each range's lower bound falls inside the other range:

```
Range A: [0, 50)     Range B: [30, 100)   → OVERLAP (30 < 50 AND 0 < 100)
Range A: [0, 50)     Range B: [50, 100)   → NO OVERLAP (50 is NOT < 50)
Range A: [0, 50)     Range B: [50, NULL)  → NO OVERLAP (contiguous, not overlapping)
```

### Validation Query (standalone check)

```sql
-- Detect overlapping ranges in current config
-- Type 1: runs against dbo.recon_variance_config
-- Type 2: filter by version_id
SELECT 
    a.metric_type,
    a.range_min AS range_a_min, a.range_max AS range_a_max,
    b.range_min AS range_b_min, b.range_max AS range_b_max,
    'OVERLAP' AS issue
FROM dbo.recon_variance_config a     -- (or _range for Type 2)
JOIN dbo.recon_variance_config b     -- (or _range for Type 2)
    ON  a.metric_type = b.metric_type
    AND a.config_id < b.config_id    -- (or range_id for Type 2)
WHERE a.range_min < ISNULL(b.range_max, 999999999.99)
  AND b.range_min < ISNULL(a.range_max, 999999999.99);
-- If this returns 0 rows → ranges are clean
```

### Gap Detection Query (optional — find uncovered ranges)

```sql
-- Detect gaps between ranges (shipments in gaps won't be flagged)
WITH ordered_ranges AS (
    SELECT 
        metric_type,
        range_min,
        range_max,
        LEAD(range_min) OVER (PARTITION BY metric_type ORDER BY range_min) AS next_range_min
    FROM dbo.recon_variance_config   -- (or _range with version filter for Type 2)
)
SELECT 
    metric_type,
    range_max AS gap_start,
    next_range_min AS gap_end,
    'GAP - shipments in this range will NOT be checked' AS warning
FROM ordered_ranges
WHERE range_max IS NOT NULL
  AND next_range_min IS NOT NULL
  AND range_max < next_range_min;
-- If this returns 0 rows → ranges are contiguous (no gaps)
```

---

## ⚖️ Weight Unit Conversion Reference

The `weight_unit` column on each weight range determines how the system converts `billed_weight_oz` and `actual_weight_oz` before comparison.

| `weight_unit` | Conversion Formula | Notes |
|---------------|-------------------|-------|
| `oz` | Use raw oz values | No conversion needed |
| `lb` | `CEILING(weight_oz / 16.0)` | Rounds UP to nearest lb (preserves original behavior) |
| `kg` | `weight_oz / 35.274` | Exact conversion, no rounding |

### How `weight_unit` Affects Each Threshold Type

| `threshold_type` | `weight_unit` | What `threshold_value` Means | Example |
|-------------------|---------------|------------------------------|---------|
| `pct` | `oz` | ±X% of actual weight in oz | `10.0` → ±10% of 12 oz = ±1.2 oz |
| `pct` | `lb` | ±X% of actual weight in lb (CEILINGed) | `10.0` → ±10% of 5 lb = ±0.5 lb |
| `pct` | `kg` | ±X% of actual weight in kg | `10.0` → ±10% of 2.27 kg = ±0.227 kg |
| `abs` | `oz` | ±X oz absolute tolerance | `2.0` → ±2 oz |
| `abs` | `lb` | ±X lb absolute tolerance | `1.0` → ±1 lb (after CEILING conversion) |
| `abs` | `kg` | ±X kg absolute tolerance | `0.5` → ±0.5 kg |

### Why `lb` Uses CEILING

The original view used `CEILING` for lb to match carrier billing behavior — carriers bill by rounded-up whole pounds. A 17 oz package is billed as 2 lb, not 1.0625 lb. This means:

- `billed_weight_oz = 17` → `billed_weight_lb = CEILING(17/16) = 2`
- `actual_weight_oz = 15` → `actual_weight_lb = CEILING(15/16) = 1`
- Variance in lb = 2 - 1 = 1 lb (100%)
- Variance in oz = 17 - 15 = 2 oz (13.3%)

The UI operator chooses which granularity is appropriate for each weight range.

### Range Boundaries Are Always in Ounces

Regardless of `weight_unit`, the `range_min` and `range_max` columns are **always stored in ounces**. This ensures the range lookup (`billed_weight_oz >= range_min`) works consistently.

If the UI displays ranges in lb or kg, it must convert before saving:

| UI Input | Stored `range_min` | Stored `range_max` |
|----------|-------------------|-------------------|
| 0–1 lb | 0.00 | 16.00 |
| 1–10 lb | 16.00 | 160.00 |
| 10+ lb | 160.00 | NULL |
| 0–1 kg | 0.00 | 35.27 |
| 1–5 kg | 35.27 | 176.37 |

---

## 📖 Example Configurations

### Example 1: All Percentage, oz/lb Split (mirrors original behavior)

```
Cost:   $0–$50 → 20% | $50–$200 → 10% | $200+ → 5%
Weight: 0–16 oz → 15% (oz) | 16–160 oz → 10% (lb) | 160+ oz → 5% (lb)
```

### Example 2: Mixed pct/abs, All oz Comparison

```
Cost:   $0–$25 → ±$3 abs | $25–$100 → 12% | $100+ → 7%
Weight: 0–16 oz → ±2 oz abs (oz) | 16+ oz → ±8 oz abs (oz)
```

### Example 3: Mixed pct/abs with lb Absolute Threshold

```
Cost:   $0+ → 10% (single range covers everything)
Weight: 0–16 oz → ±2 oz abs (oz) | 16–160 oz → ±1 lb abs (lb) | 160+ oz → 5% (lb)
```

### Example 4: kg-based for International Carrier

```
Cost:   $0+ → 10%
Weight: 0–1000 oz → 8% (kg) | 1000+ oz → 5% (kg)
```

### Threshold Mode Behavior

| Mode | `weight_unit` | Meaning | Exception if... |
|------|---------------|---------|-----------------|
| `pct` 10.0 | `oz` | ±10% in oz | `\|billed_oz - actual_oz\| > actual_oz × 10%` |
| `pct` 10.0 | `lb` | ±10% in lb | `\|CEIL(billed/16) - CEIL(actual/16)\| > CEIL(actual/16) × 10%` |
| `pct` 10.0 | `kg` | ±10% in kg | `\|billed_kg - actual_kg\| > actual_kg × 10%` |
| `abs` 2.0 | `oz` | ±2 oz | `\|billed_oz - actual_oz\| > 2` |
| `abs` 1.0 | `lb` | ±1 lb | `\|CEIL(billed/16) - CEIL(actual/16)\| > 1` |
| `abs` 0.5 | `kg` | ±0.5 kg | `\|billed_kg - actual_kg\| > 0.5` |
| `abs` 3.0 | N/A (cost) | ±$3 | `\|billed_cost - wms_cost\| > 3` |

---

## 🔄 Migration Path (Type 1 → Type 2)

```sql
-- Step 1: Create version header table
CREATE TABLE dbo.recon_variance_config_version (
    version_id INT IDENTITY(1,1) PRIMARY KEY,
    is_active BIT NOT NULL DEFAULT 1,
    effective_date DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    created_by NVARCHAR(100) NULL,
    created_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    config_notes NVARCHAR(500) NULL
);

CREATE UNIQUE INDEX UQ_ConfigVersion_Active 
ON dbo.recon_variance_config_version (is_active) 
WHERE is_active = 1;

-- Step 2: Insert initial version from current config
INSERT INTO dbo.recon_variance_config_version 
    (is_active, created_by, config_notes)
VALUES 
    (1, 'MIGRATION', 'Migrated from Type 1 single-table config');

DECLARE @v_id INT = SCOPE_IDENTITY();

-- Step 3: Rename existing table to _range and add version_id
EXEC sp_rename 'dbo.recon_variance_config', 'recon_variance_config_range';
EXEC sp_rename 'dbo.recon_variance_config_range.config_id', 'range_id', 'COLUMN';

ALTER TABLE dbo.recon_variance_config_range
ADD version_id INT NOT NULL DEFAULT @v_id;

ALTER TABLE dbo.recon_variance_config_range
ADD CONSTRAINT FK_Range_Version FOREIGN KEY (version_id) 
    REFERENCES dbo.recon_variance_config_version(version_id);

-- Step 4: Drop Type 1 audit columns (now on version header)
ALTER TABLE dbo.recon_variance_config_range DROP COLUMN updated_by;
ALTER TABLE dbo.recon_variance_config_range DROP COLUMN updated_date;

-- Step 5: Update view to join through version table
```

---

## 📊 Decision Matrix

| Requirement | Type 1 | Type 2 |
|-------------|--------|--------|
| Simple configuration management | ✅ Best | ⚠️ More complex |
| Range-based thresholds | ✅ Supported | ✅ Supported |
| pct/abs flexibility per range | ✅ Supported | ✅ Supported |
| Weight unit per range (oz/lb/kg) | ✅ Supported | ✅ Supported |
| Non-overlapping validation | ✅ Via stored proc | ✅ Via stored proc |
| Compliance/audit requirements | ❌ No trail | ✅ Full trail |
| "Why was this flagged last month?" | ❌ Can't answer | ✅ Can answer |
| Rollback capability | ❌ Manual | ✅ Built-in |
| Trend analysis (threshold vs behavior) | ❌ Limited | ✅ Full support |
| Development effort | ✅ Minimal | ⚠️ Moderate |
| Storage footprint | ✅ Current ranges only | ⚠️ Grows over time |

---

## 🎯 Recommendation

**For a billing/finance system:** Type 2 is usually preferred because:
1. Regulatory compliance often requires audit trails
2. Finance teams need to explain variances historically
3. The storage overhead is negligible (config changes are infrequent)
4. Rollback capability prevents "oops" moments
5. Range changes can significantly affect exception counts — you want to know when/why

**Start Simple:** If unsure, start with Type 1 and migrate later if needed.

---

## ✅ Implementation Checklist

Once business confirms SCD type:

- [ ] Create configuration table(s) (single table for Type 1, header + detail for Type 2)
- [ ] Insert initial range configuration (with appropriate `weight_unit` per range)
- [ ] Create unique active index (Type 2 only)
- [ ] Create `usp_update_recon_variance_config` stored procedure (with non-overlapping + weight_unit validation)
- [ ] Create `usp_rollback_recon_variance_config` stored procedure (Type 2 only)
- [ ] Update `dbo.vw_recon_variance` view definition (range-based LEFT JOINs + unit conversion)
- [ ] Add to `schema.sql`
- [ ] Test view with different threshold types (pct vs abs)
- [ ] Test view with different weight units (oz vs lb vs kg)
- [ ] Test non-overlapping validation (expect rejection on overlap)
- [ ] Test gap behavior (shipments outside all ranges should not be flagged)
- [ ] Verify lb CEILING behavior matches expected rounding
- [ ] Create UI workflow for range management (with lb/kg → oz boundary conversion)
- [ ] Document for ops team

---

## 📝 Questions for Business

1. Do you need to know what variance thresholds were used when analyzing past billing exceptions?
2. If an auditor asks why a shipment was flagged, do you need to show the threshold settings from that time?
3. Do you need the ability to rollback threshold changes if they cause too many/few exceptions?
4. Will you analyze "exception rate trends" and need to separate carrier behavior changes from threshold changes?
5. Should shipments that fall **outside all defined ranges** (gaps) be flagged as exceptions, or silently skipped?
6. Should the UI enforce **contiguous ranges** (no gaps), or are gaps acceptable?

**If Q1–Q4 any YES → Use Type 2**
**If Q1–Q4 all NO → Use Type 1**
**Q5–Q6 → Determines gap handling behavior**
