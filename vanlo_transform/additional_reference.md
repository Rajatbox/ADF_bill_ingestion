# Vanlo - Additional Reference

**Document Version:** 1.0
**Last Updated:** March 12, 2026
**Status:** Active

---

## Overview

Vanlo is a shipping aggregator platform that routes shipments through multiple underlying carriers (USPS, UniUni, etc.). Data is exported as a 29-column CSV with one row per shipment.

---

## Business Rules

### Account Number
- Use **FALCON** as the hardcoded account number (no account column in the CSV).
- File path structure: `falcon/vanlo/FALCON/bill.csv`

### Package Column Parsing
- The `Package` column contains dimensions in `"L x W x H"` format (e.g., `"7.5 x 6.0 x 1.25"`).
- Delimiter: `" x "` (space-x-space).
- Use the UPS `CHARINDEX` + `SUBSTRING` + `CROSS APPLY` pattern to extract length, width, and height.
- Dimensions are already in **inches (IN)** — no unit conversion needed.

### Charge Structure
- **Single cost column** (`Cost`) — no need for dynamic charge type seeding.
- Use **Transportation Cost** as the single charge type name.
- Charge type is pre-populated via one-time `Seed_Charge_Types.sql` script.
- `freight = 1`, `charge_category_id = 11` (Other).

### Service Column Splitting
- There is **no separate carrier column** in the CSV.
- The `Service` column combines carrier and shipping method (e.g., `"USPS GroundAdvantage"`).
- Split on the **first space**: left part = integrated carrier, right part = shipping method.
- Examples:
  - `"USPS GroundAdvantage"` → carrier: `USPS`, method: `GroundAdvantage`
  - `"USPS Priority"` → carrier: `USPS`, method: `Priority`
  - `"UniUni UniUni"` → carrier: `UniUni`, method: `UniUni`

### Tracking Number
- Use the `Tracking Code` column as the tracking number.

### UniUni Row Exclusion
- UniUni carrier rows (`Service LIKE 'UniUni%'`) must be **excluded from all tables after the delta layer**.
- Delta table (`billing.delta_vanlo_bill`): loads ALL rows including UniUni.
- Normalized table (`billing.vanlo_bill`): excludes UniUni rows.
- Unified tables (`shipment_attributes`, `shipment_charges`): excludes UniUni rows.
- Reason: UniUni shipments are billed separately through their own pipeline.

### Aggregator Handling
- Vanlo is an **aggregator** (`is_aggregator = 1` in `dbo.carrier`).
- The integrated carrier (actual fulfillment carrier) is extracted from the `Service` column.
- `integrated_carrier_id` is populated in both `dbo.shipping_method` and `billing.shipment_attributes`.
- Follows the pattern documented in `docs/carriers/aggregators.md`.

---

## CSV Structure

| Index | Column Name      | Type    | Notes |
|-------|------------------|---------|-------|
| 0     | ID               | String  | Shipment ID (e.g., `shp_084d0f...`) |
| 1     | Date             | String  | Format: `"2026-03-03 00:40:31 UTC"` |
| 2     | Order Number     | String  | Order reference |
| 3     | Tracking Code    | String  | Tracking number (used as business key) |
| 4     | Reference        | String  | Additional reference |
| 5     | From Name        | String  | Sender name |
| 6     | From Company     | String  | Sender company |
| 7     | From Street1     | String  | Sender address line 1 |
| 8     | From Street2     | String  | Sender address line 2 |
| 9     | From City        | String  | Sender city |
| 10    | From State       | String  | Sender state |
| 11    | From Postal Code | String  | Sender postal code |
| 12    | From Country     | String  | Sender country |
| 13    | To Name          | String  | Recipient name |
| 14    | To Company       | String  | Recipient company |
| 15    | To Street1       | String  | Recipient address line 1 |
| 16    | To Street2       | String  | Recipient address line 2 |
| 17    | To City          | String  | Recipient city |
| 18    | To State         | String  | Recipient state |
| 19    | To Postal Code   | String  | Recipient postal code |
| 20    | To Country       | String  | Recipient country |
| 21    | Package          | String  | Dimensions: `"L x W x H"` in inches |
| 22    | Weight           | Decimal | Weight in ounces (OZ) |
| 23    | Zone             | String  | Shipping zone |
| 24    | Service          | String  | Combined carrier + method (e.g., `"USPS GroundAdvantage"`) |
| 25    | Status           | String  | Shipment status (Delivered, In_transit, etc.) |
| 26    | Cost             | Decimal | Single charge amount (USD) |
| 27    | Label URL        | String  | URL to shipping label |
| 28    | PrintCustom      | String  | Custom print data (e.g., PO numbers) |

### ADF Copy Data Configuration
- **First Row as Header:** Checked (reads column names from CSV)
- **Skip Line Count:** 0 (header row defines schema)
- **Column Mapping:** Automatic by column name

---

## Invoice Generation

- **Invoice Number:** `'Vanlo_' + FORMAT(MAX(Date) AS DATE, 'yyyy-MM-dd')`
  - Example: `"Vanlo_2026-03-03"`
- **Invoice Date:** `CAST(LEFT(MAX(Date), 10) AS DATE)`
- **Invoice Grouping:** All non-UniUni shipments in a file grouped under a single invoice

---

## Unit Conversions
- **Weight:** Already in **OZ** — no conversion needed.
- **Dimensions:** Already in **IN** — no conversion needed.

---

## Unique Service Values (from sample data)
- `USPS GroundAdvantage` → carrier: USPS, method: GroundAdvantage
- `USPS Priority` → carrier: USPS, method: Priority
- `UniUni UniUni` → carrier: UniUni, method: UniUni *(excluded from normalized/unified)*

---

## Data Volume
- Sample file: ~2,898 data rows
- Mix of USPS and UniUni shipments
