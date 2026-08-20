# Service Charges — Invoice-Level Billing Flow

## Background

Some carrier bills contain account-level charges (e.g. payment processing fees, billing adjustments) that have no tracking number. These are invoice-level fees, not shipment-level. The current ETL flow assumes all charges have a tracking number and drops these rows silently.

The goal is to route these charges through the full pipeline — `ups_bill → shipment_charges → carrier_cost_ledger` — so that replay and reprocessing logic picks them up correctly, without altering the existing tracked-shipment flow.

The solution uses a single sentinel row in `billing.shipment_attributes` with `tracking_number = 'Service_charges'` as a permanent anchor for all invoice-level charge rows across all carriers and bills.

---

## Carrier Status — Null Tracking Number Flow

Analysis of whether null tracking numbers reach the `{carrier}_bill` staging table in each `Insert_ELT_&_CB.sql`:

| Carrier | Status | Reason |
|---|---|---|
| DHL | **PASS** | No filter on tracking_number |
| FedEx | **PASS** | No filter on tracking_id |
| UniUni | **PASS** | No tracking filter |
| UPS | **PASS** | NULLIF only sanitizes empty strings, does not drop nulls |
| Veho | **PASS** | No filter on tracking_id |
| Bukuship | **NEEDS FIX** | `NULLIF(TRIM(TrackingNumber), '') IS NOT NULL` filter exists but Bukuship bills invoices (not packages), so service charges apply |
| EasyPost | **INTENTIONALLY FILTERED** | Aggregator — issues per-shipment invoices only, no account-level service charges |
| Eliteworks | **INTENTIONALLY FILTERED** | Aggregator — same reasoning as EasyPost |
| FlavorCloud | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |
| Passport | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |
| Shippo | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |
| ShipX | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |
| Speedship | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |
| USPS Modern | **INTENTIONALLY FILTERED** | Aggregator — same reasoning |

**Rule:** Aggregators invoice per-shipment and never produce account-level service charges. Direct carriers (DHL, FedEx, UPS, etc.) and invoice-based carriers like Bukuship do. Only PASS + NEEDS FIX carriers require `Insert_Unified_tables.sql` changes.

---

## Files to Change

| File | Change type |
|---|---|
| `schema.sql` | ALTER + seed INSERT |
| `parent_pipeline/Load_to_gold.sql` | Add exclusion filter to Part 3 + add new Part 4 |
| `bukuship_transform/Insert_ELT_&_CB.sql` | Remove null tracking filter |
| `bukuship_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel (+ all PASS carriers below) |
| `dhl_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel |
| `fedex_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel |
| `uniuni_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel |
| `ups_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel |
| `veho_transform/Insert_Unified_tables.sql` | Route null-tracking rows to sentinel |
| `parent_pipeline/Replay_Carrier_Bill_CCL.sql` | Already written — no changes needed |

---

## Steps

### 1. Make `billing.shipment_attributes.carrier_id` nullable

`carrier_id` is currently `NOT NULL`. The sentinel row is carrier-agnostic (covers all carriers), so it cannot hold a real carrier_id.

```sql
ALTER TABLE billing.shipment_attributes
ALTER COLUMN carrier_id INT NULL;
```

---

### 2. Seed the sentinel row (one-time)

Insert one row into `billing.shipment_attributes` with `tracking_number = 'Service_charges'` and `carrier_id = NULL`. All invoice-level charges across all carriers and bills will point to this row's `id` via the FK in `billing.shipment_charges`.

```sql
INSERT INTO billing.shipment_attributes (carrier_id, tracking_number, created_date, updated_date)
VALUES (NULL, 'Service_charges', GETUTCDATE(), GETUTCDATE());
```

Store the generated `id` — this is the **sentinel_id** referenced in the next step.

---

### 3a. Remove null tracking filter in `bukuship_transform/Insert_ELT_&_CB.sql`

Bukuship bills at the invoice level, not the package level, so account-level service charges appear in its bills. The existing filter drops them before they reach `billing.bukuship_bill`.

Remove the filter (around line 180):
```sql
-- remove this line
AND NULLIF(TRIM(d.TrackingNumber), '') IS NOT NULL
```

---

### 3b. Update `Insert_Unified_tables.sql` for Bukuship, DHL, FedEx, UniUni, UPS, Veho

These are the carriers that produce account-level service charges. Each transform has an `Insert_Unified_tables.sql` that resolves `shipment_attribute_id` via a join on `tracking_number`. Null-tracking rows currently fail to join and are silently dropped.

**Change:** Route null-tracking rows to the sentinel row using `ISNULL` on the join.

Where the script resolves `shipment_attribute_id` (join from the `{carrier}_bill` table to `billing.shipment_attributes`), change:

```sql
-- before
ON sa.tracking_number = ub.tracking_number

-- after
ON sa.tracking_number = ISNULL(ub.tracking_number, 'Service_charges')
```

This makes null-tracking rows resolve to the sentinel row's `id`, which gets written into `shipment_charges.shipment_attribute_id`.

The existing unique constraint on `billing.shipment_charges` — `(carrier_bill_id, tracking_number, charge_type_id)` — handles idempotency. No additional NOT EXISTS guard is needed here.

---

### 4. Update `parent_pipeline/Load_to_gold.sql` — Part 3

The `#FileShipments` temp table deduplicates rows using `PARTITION BY sa.tracking_number`. If null-tracking rows are allowed into `#FileShipments`, they all land in the same NULL partition and the dedup deletes all but one. Exclude them explicitly so Part 4 handles them cleanly.

In the Part 3 INSERT (into `dbo.carrier_cost_ledger`), add this to the `WHERE` clause:

```sql
AND NULLIF(fs.tracking_number, '') IS NOT NULL  -- invoice-level charges handled in Part 4
```

---

### 5. Add Part 4 to `parent_pipeline/Load_to_gold.sql`

Add after Part 3. This step inserts invoice-level charges directly from `billing.shipment_charges`, bypassing `#FileShipments` entirely. No join to `billing.shipment_attributes` is needed — all data comes from `shipment_charges` and `carrier_bill`.

Status is always `'unknown'` — these charges have no shipment to match against WMS.

```sql
/*
================================================================================
Part 4: Insert invoice-level (null-tracking) charges into cost ledger
================================================================================
Reads directly from billing.shipment_charges for rows where tracking_number
IS NULL (routed to the 'Service_charges' sentinel during ingestion).
Bypasses #FileShipments — no WMS resolution is possible for these rows.
================================================================================
*/
INSERT INTO dbo.carrier_cost_ledger (
    carrier_invoice_number,
    carrier_invoice_date,
    tracking_number,
    shipment_date,
    carrier_id,
    shipping_method_id,
    category,
    cost_item,
    amount,
    charge_type_id,
    shipment_package_id,
    carrier_bill_id,
    shipment_attribute_id,
    status
)
SELECT
    cb.bill_number,
    cb.bill_date,
    NULL,
    NULL,
    sc.carrier_id,
    NULL,
    ctc.category,
    ct.charge_name,
    sc.amount,
    sc.charge_type_id,
    NULL,
    sc.carrier_bill_id,
    sc.shipment_attribute_id,
    'unknown'
FROM billing.shipment_charges sc
JOIN billing.carrier_bill cb
    ON  cb.carrier_bill_id = sc.carrier_bill_id
    AND cb.file_id         = @File_id
JOIN dbo.charge_types ct
    ON  ct.charge_type_id  = sc.charge_type_id
JOIN dbo.charge_type_category ctc
    ON  ctc.category_id    = ct.charge_category_id
WHERE sc.carrier_id        = @Carrier_id
  AND sc.tracking_number IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM dbo.carrier_cost_ledger ccl
      WHERE ccl.carrier_bill_id      = sc.carrier_bill_id
        AND ccl.charge_type_id       = sc.charge_type_id
        AND ccl.tracking_number IS NULL
  );

SET @InvoiceChargesInserted = @@ROWCOUNT;
```

Also declare `@InvoiceChargesInserted INT` with the other variables at the top of the proc, and include it in the final `SELECT` result set.

---

## What Does Not Need to Change

- `billing.shipment_attributes.tracking_number` — already nullable, no change needed
- `billing.shipment_charges.shipment_attribute_id` — stays NOT NULL, FK integrity preserved
- `dbo.carrier_cost_ledger.tracking_number` — already nullable
- `parent_pipeline/Replay_Carrier_Bill_CCL.sql` — already handles both tracked and invoice-level charges via Steps 3 and 4
- `parent_pipeline/Backfill_Carrier_Cost_Ledger.sql` — operates on `shipment_package_id IS NULL` rows; invoice-level charges will stay `unknown` permanently and are correctly excluded from WMS backfill by the `JOIN dbo.shipment_package ON spw.tracking_number = ccl.tracking_number` (NULL tracking means no match)
