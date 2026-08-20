# USPS Carrier - Business Reference

## Overview
USPS (direct, EPS-based billing feed) is a narrow-format CSV with one row per charge event (`tranType` = `PURCHASE`, `ADJUSTMENT`, or `REFUND`). Distinct from the `usps_modern_transform` pipeline — reuses the same `dbo.carrier` row (`carrier_id = 13`, `carrier_name = 'USPS'`). A single file can contain multiple `epsAcctNum` values.

---

## Invoice Information

Invoices are grouped by `eps_acct_num` — one `carrier_bill` row per distinct account per file (not one row per file), since a single export can span multiple EPS accounts.

| Field | Source | Notes |
|-------|--------|-------|
| Bill Number | Synthetic | `{eps_acct_num}_{MIN(tran_date) yyyyMMdd}_{MAX(tran_date) yyyyMMdd}` |
| Bill Date | `tranDate` | `MAX(tran_date)` across the account's rows in the file |
| Account Number | `epsAcctNum` | EPS account identifier |
| Total Amount | `postage` | `SUM(postage)` across the account's rows |
| Num Shipments | `pic` | `COUNT(DISTINCT pic)` — stored as `tracking_number` in `usps_bill` |

---

## Shipment Attributes

Minimal population — no weight/dimension columns exist in this feed, so no unit conversion applies.

| Field | Source | Notes |
|-------|--------|-------|
| Tracking Number | `pic` | Business key — renamed to `tracking_number` in `usps_bill` |
| Shipment Date | `tranDate` | `MIN(tran_date)` grouped by tracking number — set once, never revised |

`shipping_method`, dims, and weight are left NULL. `mailClass` (FC/PM/EX/BS/CP) is stored on `usps_bill` but not currently mapped to `shipping_method`.

---

## Charge Mapping

Charge type is derived from `tranType` via CASE logic in `Sync_Reference_Data.sql` and `Insert_Unified_tables.sql` — dynamically registered, not pre-seeded:

| tranType | charge_name | charge_category_id | freight |
|----------|-------------|---------------------|---------|
| PURCHASE | Postage | 15 (Transportation) | 1 |
| ADJUSTMENT | Adjustment | 16 (Adjustment) | 0 |
| REFUND | Refund | 11 (Other) | 0 |
| *(any other/future value)* | the raw tranType value | 11 (Other) | 0 |

The fallback branch means a new `tranType` the feed starts emitting later is captured as its own distinct charge type automatically, rather than silently dropped or merged into an existing bucket.

`amount` for `shipment_charges` = `postage` column (not `tranAmt`, which is a per-batch/manifest total repeated across the rows in that batch — `postage` is the per-tracking-number allocation and reconciles to `tranAmt` when summed per `epsTranId`).

---

## Tables

### Staging (Delta) Table
`billing.delta_usps_bill` — 1:1 with the 21-column CSV, all VARCHAR.

### Normalized Table
`billing.usps_bill` — typed, linked to `carrier_bill` via `carrier_bill_id`. Columns: `eps_acct_num, eps_tran_id, tran_amt, tran_date, tran_type, tracking_number, postage, assessment_type, assessment_details, cust_ref_num1, cust_ref_num2, mail_class`. `tracking_number` is the renamed `pic` column (per business rule: `pic = tracking_number`) — the raw `delta_usps_bill` staging table keeps the original `pic` name. `cust_ref_num1`/`cust_ref_num2` are retained for order/reference reconciliation even though no current mapping rule reads them downstream. Excluded (always empty in observed exports, or genuinely unused): `ach_debit_trans_id, ach_withdrawal_amount, efn, crid, master_mid, permit_number, permit_type, permit_finance_number, dispute_id` — see `billing.delta_usps_bill` for the raw values if ever needed.

---

## Validation Test

```sql
DECLARE @File_id INT = /* test file_id */;

WITH file_total AS (
    SELECT SUM(cb.total_amount) AS expected
    FROM billing.carrier_bill cb
    WHERE cb.file_id = @File_id
),
charges_total AS (
    SELECT SUM(sc.amount) AS actual
    FROM billing.shipment_charges sc
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id
)
SELECT
    expected,
    actual,
    ABS(expected - actual) AS difference,
    CASE WHEN ABS(expected - actual) < 0.01 THEN 'PASS' ELSE 'FAIL' END AS result
FROM file_total, charges_total;
```

---

## Key Business Rules

1. **Carrier Identity**: Reuses existing `dbo.carrier` row `carrier_id = 13` (`carrier_name = 'USPS'`) — no new carrier seed row.
2. **Invoice Grouping**: One `carrier_bill` row per distinct `eps_acct_num` found in the file, not one row per file.
3. **Amount Field**: `postage`, not `tranAmt` — `tranAmt` is a batch/manifest-level total repeated across multiple tracking-number rows sharing the same `epsTranId`.
4. **Charge Type Discovery**: Dynamic, CASE-driven in `Sync_Reference_Data.sql` — no external seed migration needed for the 3 known charge types, and any future new `tranType` self-registers under its own name.
5. **Duplicate Prevention**: Same file cannot be processed twice (file-based idempotency via `file_id`).
6. **Row-level natural key**: `(eps_tran_id, pic)` is unique per row in the source feed (verified against the sample file) — useful for debugging/tracing, though not used as the DB idempotency key (which follows the project's standard `carrier_bill_id`-based convention).
7. **Column Rename**: `pic` (raw CSV) → `tracking_number` (typed `usps_bill` and all unified-layer tables) per business rule.
8. **No Unit Conversion**: This feed carries no weight or dimension columns.
