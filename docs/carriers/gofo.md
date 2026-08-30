# GOFO Carrier - Business Reference

## Overview
GOFO is a direct last-mile carrier providing billing data as a fixed 32-column wide-format CSV, one row per shipment. Each file covers a half-month invoice period (1st-15th or 16th-31st) for a single customer/account. GOFO invoices per shipment only -- every row carries a real tracking number, so there is **no account-level charge path and no `Service_charges` sentinel** for this carrier (unlike UPS/DHL/FedEx/Bukuship/UniUni/Veho).

---

## File Structure

The raw file has 2 metadata rows before the real header:
1. `Invoice Number：CUS260505001202607002    Invoice Period：07/16/2026-07/31/2026   Invoicing currency：USD`
2. `"payable amount：133,101.12"`
3. Real header row (32 columns)
4. Data rows

The ADF copy activity uses `skipLineCount: 2` so `billing.delta_gofo_bill` only ever sees the 32-column data rows. **The invoice number is not present in any data row** -- it's fetched from the row-1 metadata line via an ADF Lookup activity + dynamic content (see "Invoice Number / Bill Date" below), which reproduces the exact string seen there (`CUS260505001` + `202607` + `002`).

---

## Invoice Number / Bill Date (Fetched via ADF, Not Fabricated)

The invoice number is never present in any data row, in any format version, so `bill_number`/`bill_date` cannot be derived from body columns at all.

Instead, both values are read directly from the row-1 metadata line (present and consistently formatted across GOFO files):

```
Invoice Number：CUS260505001202607002    Invoice Period：07/16/2026-07/31/2026   Invoicing currency：USD
```

An ADF Lookup activity reads this row (before the Copy activity's `skipLineCount: 2` skips past it), and dynamic content expressions extract the invoice number and the invoice period's end date. These are passed into `Insert_ELT_&_CB.sql` as `@Invoice_Number` and `@Bill_Date` -- no computation happens in SQL. Full ADF wiring + expressions: `gofo_transform/Attribute_fetch.md`.

**Note on `A-scan Date`**: an older GOFO file format lacked this column entirely; per the client, the current/latest format includes it and this is what's expected going forward. `A-scan Date` is part of `delta_gofo_bill`/`gofo_bill` and drives `shipment_attributes.shipment_date` (see below).

---

## Shipment Physical Attributes

### Weight Fields
- **Actual weight (lbs)** -- physically measured weight
- **Invoicing weight (lbs)** -- weight actually billed on; this is what flows to `shipment_attributes.billed_weight_oz`

**Conversion**: LB x 16 -> OZ.

### Dimension Field
- **Dimensions (inch)** -- single column, format `L*W*H` (e.g. `8.000*4.500*4.000`), always inches, always well-formed in the sample file.

**Conversion**: Already in inches -- split on `*` only, no unit conversion. Parsed via `CHARINDEX`/`SUBSTRING` CROSS APPLY (same pattern as UPS's `x`-delimited dimensions), using `TRY_CAST` since dimensions are an optional field per Design Constraint #3.

### Service & Routing Information

| Field | Source Column | Notes |
|-------|---------------|-------|
| Tracking Number | `Tracking Number` | Business key; always equals `Order Number` in the sample file |
| Zone | `Zone` | Numeric destination zone (1-7); blank on credit-only rows |
| Prealerted Hub / Injection Hub | `Prealerted Hub`, `Injection Hub` | Retained in `gofo_bill` for reference; not part of the unified layer (no matching columns on `shipment_attributes`) |
| Shipment Date | `A-scan Date` | Scan/induction date into GOFO's network; used for `shipment_attributes.shipment_date` (current file format -- see "Invoice Number / Bill Date" above) |
| Delivery Date | `Delivery Date` | Stored in `gofo_bill` for reference only |

---

## Missing Shipping Method (Credit-Only Rows)

~30 of 37,650 rows per file are credit-only adjustments: `Product`, `Zone`, `Prealerted Hub`, `Injection Hub` are blank, all 13 fee columns are 0 except a negative `Credit for Order Value` equal to `Total`. These rows still carry a real, unique tracking number and populated weight/dimensions.

`Product` defaults to `'GOFO Parcel Pickup'` (the only value observed in this file) whenever it's blank, applied in `Insert_ELT_&_CB.sql` so every downstream script sees a non-blank shipping method.

---

## Charge Types (13 Total)

Unpivoted inline via `OUTER APPLY (VALUES ...)` wherever needed (`Sync_Reference_Data.sql` Block 2, `Insert_Unified_tables.sql` Part 2) -- no dedicated view. Unlike FedEx/DHL, where the charge-description-to-column mapping is genuinely dynamic/pivoted and a view earns its keep, GOFO's 13 columns are fixed and known at design time, so a persistent `vw_GOFOCharges` object would just be unnecessary indirection with (after Block 3 below) effectively one caller.

| Charge Name (CSV) | freight | charge_category_id | Category Name |
|---|---|---|---|
| Delivery Fee | 1 | 11 | Other |
| Overweight Fees | 0 | 11 | Other |
| Oversized Fees | 0 | 11 | Other |
| Return Fees | 0 | 11 | Other |
| Remote Area Fees | 0 | 11 | Other |
| Fuel Surcharges | 0 | 11 | Other |
| Delivery Area Fees | 0 | 11 | Other |
| Additional Interception Fees | 0 | 11 | Other |
| Relabelling Fee | 0 | 11 | Other |
| Return Reship Fee | 0 | 11 | Other |
| Credit for Delivery Fees | 0 | 16 | Adjustment |
| Credit for Order Value | 0 | 16 | Adjustment |
| Other | 0 | 11 | Other |

Per Design Constraint #11, only `Adjustment` (16) and `Other` (11) are used -- no invented categories. `Delivery Fee` is the only `freight = 1` charge (base transportation cost).

`Credit for Order Value` is frequently negative (refund rows) -- the view filters `<> 0`, not `> 0`, so negative amounts pass through correctly.

---

## Unit Standardization

### Weight Conversion
- **Target Unit**: Ounces (OZ)
- **LB -> OZ**: Multiply by 16 (`invoicing_weight_lbs`, not `actual_weight_lbs`)

### Dimension Conversion
- **Target Unit**: Inches (IN)
- **IN -> IN**: No conversion needed (GOFO bills in inches)

---

## Schema

### Staging Table
`billing.delta_gofo_bill` -- 32 VARCHAR columns, 1:1 with CSV structure (post `skipLineCount`).

### Normalized Table
`billing.gofo_bill` -- typed, linked to `carrier_bill` via `carrier_bill_id`. Dimensions pre-split into `dim_length_in`/`dim_width_in`/`dim_height_in` at ELT time.

### Charge Type Seeding
`Sync_Reference_Data.sql` Block 3 statically seeds all 13 charge types once (idempotent); Block 2's dynamic discovery is left running as a live fallback rather than commented out, since it's a no-op once Block 3 has run.

---

## Validation Test

See `gofo_transform/validation_test.sql`. On the example file: `SUM([Total])` = 133,101 which matches the `payable amount：133,101.12` in the file's metadata row (rounding aside).

---

## Key Business Rules

1. **Account Number**: Sourced from `Customer ID` column (e.g. `CUS260505001`)
2. **Invoice Grouping**: One `carrier_bill` row per file (one customer, one half-month period)
3. **Invoice Number / Bill Date**: Not present in any data row -- fetched from the row-1 metadata line via ADF Lookup + dynamic content, passed in as `@Invoice_Number`/`@Bill_Date`
4. **No Service Charges Sentinel**: Every row has a real tracking number; GOFO does not need the `Service_charges` sentinel pattern used by UPS/DHL/FedEx/Bukuship/UniUni/Veho
5. **Missing Shipping Method**: `Product` defaults to `'GOFO Parcel Pickup'` when blank (credit-only rows)
6. **Charge Filtering**: Only non-zero charges (including negative adjustments) stored in `shipment_charges`
7. **Duplicate Prevention**: Same file cannot be processed twice (file-based idempotency)
8. **Shipping Method Discovery**: New product types automatically added to `dbo.shipping_method` on each run
9. **File Format Version**: `A-scan Date` is present in the current/latest GOFO format (expected going forward) and drives `shipment_date`; an older format on hand lacks it, but that's not the format being built against
