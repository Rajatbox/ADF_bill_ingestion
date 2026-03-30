# MPS-Aware Variance Comparison Plan

## Overview

**Problem:** FedEx Multi-Piece Shipments (MPS) cause false variance flags. FedEx bills all charges at the group level (on the MPS_HEADER/PARENT), while the WMS stores prorated estimated costs per package in `shipment_package`. This grain mismatch means:

- MPS_PARENT: billed = $150 (full group) vs WMS estimated = $50 (prorated) → flags `cost_high`
- MPS_CHILD: billed = $0 (no charges) vs WMS estimated = $50 → flags `cost_low`
- Every MPS package is guaranteed to flag, even when the bill is perfectly accurate

**Solution:** Make `vw_recon_variance` MPS-aware. For FedEx MPS groups, aggregate WMS estimated costs to the group level and compare against the parent's full billed cost. Exclude children from variance flagging. No changes to the billing pipeline.

**Carrier scope:** FedEx only (carrier_id = 10). Other carriers unaffected.

---

## Approaches Considered

### Approach 1: Prorate billed costs in `Load_to_gold.sql` (display only)

Write prorated `billed_shipping_cost` to `shipment_package` so per-package comparison works.

**Rejected because:** The adjustment workflow breaks. When users apply adjustments to a flagged package, the billing layer still has $0 on children. Adjusting a child creates a nonsensical negative cost, and a compensating deduction must be manually applied to the parent. Fragile and error-prone.

### Approach 2: Prorate charges at the ELT level (`Insert_ELT_&_CB.sql`)

Redistribute the HEADER row's 50 charge column pairs across PARENT/CHILD rows during delta → fedex_bill insert.

**Rejected because:**
- 50 charge description/amount column pairs to redistribute per row via window functions
- Charge routing downstream still breaks (`vw_FedExCharges` outputs `express_or_ground_tracking_id`, which routes all charges to the parent regardless)
- Requires changes to `vw_FedExCharges` and `Insert_Unified_tables.sql` Part 2 join logic
- Duplicates MPS classification logic (already in `Insert_Unified_tables.sql`) into the ELT step
- `fedex_bill` loses audit fidelity — no longer matches source CSV
- Correction invoices need same proration logic but may lack weight data
- Estimated effort: ~1–2 weeks

### Approach 3: Prorate charges in `shipment_charges` (billing pipeline)

Distribute each individual charge proportionally across all MPS packages.

**Rejected because:**
- Heaviest change (~2–3 weeks), highest risk
- Fabricates charge-level detail FedEx doesn't provide
- Rounding across 50 charge types × N packages
- Correction records arrive with NULL weight/dimensions — need to persist proration ratios
- Muddies the audit trail (charges in `shipment_charges` no longer match source)

### Approach 4: View-level MPS aggregation (Selected)

Add `group_tracking_id` to `shipment_attributes` and make `vw_recon_variance` aggregate WMS costs to the MPS group level before comparing.

**Selected because:**
- Billing pipeline untouched — `shipment_charges`, `vw_shipment_summary`, `fedex_bill` all unchanged
- Corrections work as-is — new charges land on parent, view re-aggregates WMS at group level
- Adjustments are natural — only the parent appears in variance results, parent holds the real billed total
- Business logic lives in the view layer where it belongs
- No rounding, no charge redistribution, no audit trail impact
- Estimated effort: ~1 day

---

## Implementation Plan

### Change 1: Schema — `billing.shipment_attributes`

Add one nullable column:

```sql
ALTER TABLE billing.shipment_attributes
ADD group_tracking_id NVARCHAR(255) NULL;
```

| MPS Role | `group_tracking_id` value |
|----------|---------------------------|
| NORMAL_SINGLE | `NULL` |
| MPS_PARENT | `express_or_ground_tracking_id` |
| MPS_CHILD | `express_or_ground_tracking_id` |
| All other carriers | `NULL` |

---

### Change 2: FedEx `Insert_Unified_tables.sql` — populate `group_tracking_id`

The CTE already computes `express_or_ground_tracking_id as group_id` in `fx_hoisted` (line 118).

**fx_final** — add one column:

```sql
CASE WHEN mps_role = 'NORMAL_SINGLE' THEN NULL ELSE group_id END AS group_tracking_id
```

**INSERT statement** — add `group_tracking_id` to both the column list and SELECT list.

No logic changes. One CASE expression, one column in the INSERT.

---

### Change 3: `vw_recon_variance` — MPS-aware comparison

The key design: compute group-level SUMs **before** filtering out children (window functions execute before WHERE).

```sql
CREATE VIEW dbo.vw_recon_variance AS
WITH base AS (
    SELECT
        sp.shipment_package_id,
        sp.tracking_number,
        sp.actual_weight_oz,
        sp.billed_weight_oz,
        sp.wms_shipping_cost,
        sp.billed_shipping_cost,
        sa.carrier_id,
        sa.group_tracking_id,

        -- MPS: aggregate WMS cost to group level (includes all children in the SUM)
        CASE
            WHEN sa.carrier_id = 10 AND sa.group_tracking_id IS NOT NULL
            THEN SUM(sp.wms_shipping_cost) OVER (PARTITION BY sa.group_tracking_id)
            ELSE sp.wms_shipping_cost
        END AS comparison_wms_cost,

        -- MPS: aggregate actual weight to group level
        CASE
            WHEN sa.carrier_id = 10 AND sa.group_tracking_id IS NOT NULL
            THEN SUM(sp.actual_weight_oz) OVER (PARTITION BY sa.group_tracking_id)
            ELSE sp.actual_weight_oz
        END AS comparison_actual_weight_oz,

        -- MPS: aggregate billed weight to group level
        CASE
            WHEN sa.carrier_id = 10 AND sa.group_tracking_id IS NOT NULL
            THEN SUM(sp.billed_weight_oz) OVER (PARTITION BY sa.group_tracking_id)
            ELSE sp.billed_weight_oz
        END AS comparison_billed_weight_oz

    FROM shipment_package sp
    LEFT JOIN billing.shipment_attributes sa
        ON sa.tracking_number = sp.tracking_number
),
-- Filter AFTER aggregation: keep non-MPS + MPS parents only
standardized_weights AS (
    SELECT
        shipment_package_id,
        tracking_number,
        CEILING(comparison_actual_weight_oz / 16.0) AS actual_weight_lb,
        comparison_billed_weight_oz / 16.0 AS billed_weight_lb,
        comparison_billed_weight_oz AS billed_weight_oz,
        CEILING(comparison_actual_weight_oz) AS actual_weight_oz,
        comparison_wms_cost AS wms_shipping_cost,
        billed_shipping_cost
    FROM base
    WHERE group_tracking_id IS NULL                -- all non-MPS packages (all carriers)
       OR tracking_number = group_tracking_id      -- MPS parent only (has the billed cost)
),
cost_audit AS (
    -- existing cost comparison logic, unchanged
),
weight_audit AS (
    -- existing weight comparison logic, unchanged
)
SELECT * FROM weight_audit WHERE is_cost_exception = 1;
```

**How it works:**

1. `base` CTE joins `shipment_package` to `shipment_attributes` for `carrier_id` and `group_tracking_id`
2. Window functions SUM the WMS cost, actual weight, and billed weight across the MPS group — computed before any filtering, so children's values are included in the aggregation
3. `standardized_weights` filters to parent-only for MPS groups. The parent row now carries group-level totals for comparison
4. Downstream CTEs (`cost_audit`, `weight_audit`) are unchanged — they compare `billed_shipping_cost` vs `wms_shipping_cost`, which are now apples-to-apples
5. For non-FedEx carriers or FedEx NORMAL_SINGLE: `group_tracking_id IS NULL`, all CASE expressions fall through to ELSE. Zero behavioral change

---

### Change 4: `Load_to_gold.sql` — No changes

- **Part 2:** Still writes `billed_shipping_cost` from `vw_shipment_summary`. Parent gets the full billed amount, children get $0. This is the true billed amount — correct.
- **Part 3:** Uses `vw_recon_variance` for ledger status. Now MPS-aware. Children have no charges → no ledger entries regardless.

---

### Change 5: Backfill existing data

One-time script to populate `group_tracking_id` for already-processed FedEx rows:

```sql
UPDATE sa
SET sa.group_tracking_id = fb.express_or_ground_tracking_id
FROM billing.shipment_attributes sa
JOIN billing.fedex_bill fb
    ON sa.tracking_number = COALESCE(NULLIF(fb.msp_tracking_id, ''), fb.express_or_ground_tracking_id)
WHERE sa.carrier_id = 10
  AND sa.group_tracking_id IS NULL
  AND fb.msp_tracking_id IS NOT NULL
  AND NULLIF(fb.msp_tracking_id, '') IS NOT NULL;
```

---

## What this does NOT change

| Component | Impact |
|-----------|--------|
| `shipment_charges` | Untouched — all charges stay on parent |
| `vw_shipment_summary` | Untouched — parent shows full cost |
| `fedex_bill` | Untouched — matches source CSV |
| `vw_FedExCharges` | Untouched — charge unpivot unchanged |
| `carrier_cost_ledger` | Untouched — itemized charges on parent, status from MPS-aware view |
| `Insert_ELT_&_CB.sql` | Untouched — delta → fedex_bill unchanged |
| Other carriers | Untouched — `group_tracking_id` is NULL, CASE falls through |
| Correction invoices | Untouched — corrections add charges to parent, view re-aggregates WMS at group level |

---

## Adjustment workflow

Under this approach, the variance view only surfaces MPS parents (with the full group billed cost). Children are excluded from flagging.

- If the group is flagged, the adjustment targets the **parent** (which holds the real $150 billed total)
- No cascading to children, no negative cost orphans
- This is semantically correct — FedEx bills at the group level, adjustments should be at the group level

---

## Edge cases

| Edge case | Behavior |
|-----------|----------|
| MPS child in WMS but not in billing | No `shipment_attributes` match → `group_tracking_id` = NULL → treated as normal package |
| MPS child in billing but not in WMS | No `shipment_package` row → not in variance view |
| `wms_shipping_cost` NULL for some children | `SUM` ignores NULLs → group total is partial. Mitigate with `ISNULL(wms_shipping_cost, 0)` if needed |
| Correction invoice after initial load | New charges on parent → `billed_shipping_cost` increases via view recalc → next `Load_to_gold` run updates `shipment_package` → variance re-evaluates at group level |
| MPS group where all children have zero weight in WMS | Weight comparison at group level still works — sum of zeros is zero |
| Non-FedEx carrier with MPS in the future | Add their `carrier_id` to the CASE condition. `group_tracking_id` column is already carrier-agnostic |

---

## Effort estimate

| Task | Estimate |
|------|----------|
| Schema: add `group_tracking_id` column | 0.5 hr |
| FedEx `Insert_Unified_tables.sql`: 1 CASE + column in INSERT | 0.5 hr |
| `vw_recon_variance`: base CTE + filter logic | 2–3 hrs |
| Backfill script | 1 hr |
| Testing (MPS groups, normal singles, corrections, other carriers) | 3–4 hrs |
| Documentation update (`fedex.md`) | 0.5 hr |
| **Total** | **~1–1.5 days** |

---

## Verification queries

### Confirm group_tracking_id populated correctly

```sql
SELECT
    sa.tracking_number,
    sa.group_tracking_id,
    CASE
        WHEN sa.group_tracking_id IS NULL THEN 'NORMAL_SINGLE'
        WHEN sa.tracking_number = sa.group_tracking_id THEN 'MPS_PARENT'
        ELSE 'MPS_CHILD'
    END AS mps_role
FROM billing.shipment_attributes sa
WHERE sa.carrier_id = 10
  AND sa.group_tracking_id IS NOT NULL
ORDER BY sa.group_tracking_id, mps_role;
```

### Verify group-level WMS aggregation matches expected

```sql
SELECT
    sa.group_tracking_id,
    COUNT(*) AS packages_in_group,
    SUM(sp.wms_shipping_cost) AS group_wms_total,
    MAX(CASE WHEN sp.tracking_number = sa.group_tracking_id THEN sp.billed_shipping_cost END) AS parent_billed_cost,
    SUM(sp.wms_shipping_cost) - MAX(CASE WHEN sp.tracking_number = sa.group_tracking_id THEN sp.billed_shipping_cost END) AS variance
FROM billing.shipment_attributes sa
JOIN shipment_package sp ON sp.tracking_number = sa.tracking_number
WHERE sa.carrier_id = 10
  AND sa.group_tracking_id IS NOT NULL
GROUP BY sa.group_tracking_id
ORDER BY ABS(SUM(sp.wms_shipping_cost) - MAX(CASE WHEN sp.tracking_number = sa.group_tracking_id THEN sp.billed_shipping_cost END)) DESC;
```

### Confirm children excluded from variance view

```sql
SELECT rv.tracking_number, sa.group_tracking_id
FROM dbo.vw_recon_variance rv
JOIN billing.shipment_attributes sa ON sa.tracking_number = rv.tracking_number
WHERE sa.carrier_id = 10
  AND sa.group_tracking_id IS NOT NULL
  AND rv.tracking_number <> sa.group_tracking_id;
-- Expected: 0 rows (no children in variance results)
```
