# Veho Carrier - Business Reference

## Overview
Veho is a direct last-mile carrier providing billing data in a wide-format CSV with one row per tracking ID. Each file typically covers a single invoice date. Each row carries up to three charge slots (Charge Name/Code/Amount 1-3), with charge codes GP, DAS, ADC, or unknown codes used to classify charges.

---

## Invoice Information

| Field | Source Column | Notes |
|-------|--------------|-------|
| Invoice Number | `Invoice Number` | e.g., `20260629eew3TBj2` |
| Invoice Date | `Invoice Date` | Format: `YYYY-MM-DD` |
| Account Number | `Account Number` | Always `1123` for this client |
| Invoice Total | `Invoice Total` | May contain commas — e.g., `"9,266.78"` — stripped before casting |

---

## Shipment Physical Attributes

### Weight Fields
- **Actual Weight** — Physical weight of the package (LB)
- **Billable Weight** — Billed weight used for charge calculation (LB); this is what flows to `shipment_attributes.billed_weight_oz`

**Conversion**: LB × 16 → OZ for unified layer.

### Dimension Fields
- **Length**, **Width**, **Height** — Package dimensions (IN)

**Conversion**: Already in inches — stored as-is. No conversion applied.

### Service & Routing Information

| Field | Source Column | Notes |
|-------|--------------|-------|
| Tracking ID | `Tracking ID` | Business key; starts with `VH` |
| Package ID | `Package ID` | Internal Veho package ID |
| Zone | `Zone` | Numeric destination zone (1–7+) |
| Injection Market | `Injection Market` | Origin hub market (e.g., `Dallas`) |
| Delivery Market | `Delivery Market` | Destination market (e.g., `Greensboro`) |
| Delivery Zip | `Delivery Zip` | Recipient ZIP code |
| Shipment Date | `Created Timestamp` | Date + time shipment was created; format: `June 21, 2026, 6:18 AM` |
| Tendered Timestamp | `Tendered Timestamp` | When shipment was tendered to network |
| External ID | `External ID` | Client-supplied reference (e.g., `#2191128`) |

---

## Charge Slots

Each row in the Veho CSV carries up to three charge slots. Each slot has a Name, Code, and Amount column.

| Slot | Columns |
|------|---------|
| 1 | `Charge Name 1`, `Charge Code 1`, `Charge Code 1 Amount` |
| 2 | `Charge Name 2`, `Charge Code 2`, `Charge Code 2 Amount` |
| 3 | `Charge Name 3`, `Charge Code 3`, `Charge Code 3 Amount` |

Unoccupied slots are empty strings — treated as NULL in the normalized table.

---

## Charge Types (3 Total)

| Charge Name (CSV) | Charge Code | charge_category_id | Category Name | is_freight |
|-------------------|-------------|--------------------|---------------|------------|
| Ground Plus | GP | 15 | Transportation | 1 |
| Delivery Area Surcharge | DAS | 4 | Delivery Area Surcharge | 0 |
| Address Correction | ADC | 3 | Correction/Compliance | 0 |
| *(any other)* | *(other)* | 11 | Other | 0 |

**Note**: Charge types are seeded once via a one-time setup script (`Insert_Charge_Types_OneTime.sql`), not synced dynamically each run.

---

## Shipping Method Derivation

`shipping_method` for `shipment_attributes` and `veho_bill` is derived from `[Charge Name 1]` **only when** `[Charge Code 1]` is not `ADC`.

- If `Charge Code 1 = 'ADC'`, `shipping_method = NULL` (address correction rows have no meaningful service type).
- If all three charge slots are ADC or empty, `shipping_method = NULL`.

**Example**: A row with `GP / Ground Plus` in slot 1 → `shipping_method = 'Ground Plus'`.

---

## Unit Standardization

### Weight Conversion
- **Target Unit**: Ounces (OZ)
- **LB → OZ**: Multiply by 16

### Dimension Conversion
- **Target Unit**: Inches (IN)
- **IN → IN**: No conversion needed (Veho bills in inches)

---

## Timestamp Parsing

The `[Created Timestamp]` and `[Tendered Timestamp]` columns use a non-standard format:

```
June 21, 2026, 6:18 AM
```

These are parsed using `CONVERT(DATETIME, <value>, 100)` which handles the `Mon DD YYYY HH:MMAM/PM` style. Both date and time components are preserved in `shipment_date` (stored as `DATETIME`).

---

## Schema

### Staging Table
`billing.delta_veho_bill` — 39 VARCHAR columns, 1:1 with CSV structure.

### Normalized Table
`billing.veho_bill` — typed, linked to `carrier_bill` via `carrier_bill_id`. All three charge slots stored as typed columns. Address fields included for reference.

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

1. **Account Number**: Sourced from `Account Number` column (value `1123`)
2. **Invoice Grouping**: One `carrier_bill` row per distinct `(Invoice Number, Invoice Date)`
3. **Invoice Total**: Contains commas — must strip with `REPLACE` before casting to `DECIMAL`
4. **Charge Filtering**: Only non-zero charges with non-NULL charge names stored in `shipment_charges`
5. **Shipping Method**: Derived from Charge Name 1 unless Charge Code 1 is `ADC`; NULL if ADC
6. **Duplicate Prevention**: Same file cannot be processed twice (file-based idempotency)
7. **Shipping Method Discovery**: New service types (e.g., future Veho products) automatically added to `dbo.shipping_method` on each run
