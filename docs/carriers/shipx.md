# Shipx Carrier - Business Reference

## Overview
Shipx is a direct carrier providing billing data in a wide-format CSV with one row per shipment. Each invoice can contain multiple shipments. Each row carries two charge columns — Delivery Charge (base freight) and Fuel Surcharge — plus a Total Charge.

---

## Invoice Information

| Field | Description |
|-------|-------------|
| Invoice Number | Unique invoice identifier (e.g., `001381`) |
| Invoice Date | Invoice date (e.g., `5/25/2026`) |
| Company Id | Customer account number — used as `account_number` in carrier_bill |

---

## Shipment Physical Attributes

### Weight Fields
- **Weight** — Billed weight (numeric)
- **Weight UOM** — Unit of measure; expected value: `LBS`

**Conversion**: LBS × 16 → OZ for unified layer.

### Dimension Fields
- **Length**, **Width**, **Height** — Package dimensions (numeric)
- **Dims UOM** — Unit of measure; expected value: `INCHES`

**Conversion**: INCHES stored as-is; no conversion needed. CASE handles CM/MM for robustness.

### Service & Routing Information
| Field | Source Column | Notes |
|-------|--------------|-------|
| Tracking Number | `Tracking Number` | Business key; starts with `SX` |
| Shipment Number | `Shipment Number` | Internal Shipx shipment ID |
| Service Level | `Service Level` | Shipping method (e.g., `express`) |
| Zone | `Zone` | Destination zone (e.g., `Zone 1`) |
| Shipment Date | `Creation Date` | When shipment was created |
| Actual Delivery Date | `Actual Delivery Date` | When delivered |
| Status | `Status` | e.g., `Shipment Delivered` |

---

## Charge Types (2 Total)

| Charge Name | Description | is_freight | charge_category_id |
|-------------|-------------|-----------|-------------------|
| Delivery Charge | Base transportation rate | 1 (yes) | 15 (Transportation) |
| Fuel Surcharge | Fuel cost adjustment | 0 (no) | 8 (Fuel) |

**Note**: Charge types are seeded once via a one-time setup script. Only non-zero charge amounts are stored in `shipment_charges`.

---

## Unit Standardization

### Weight Conversion
- **Target Unit**: Ounces (OZ)
- **LBS / LB → OZ**: Multiply by 16
- **KG → OZ**: Multiply by 35.274

### Dimension Conversion
- **Target Unit**: Inches (IN)
- **INCHES / INCH / IN → IN**: No conversion needed
- **CM → IN**: Divide by 2.54
- **MM → IN**: Divide by 25.4

---

## Schema

### Staging Table
`billing.delta_shipx_bill` — 38 VARCHAR(255) columns, 1:1 with CSV structure.

### Normalized Table
`billing.shipx_bill` — typed, linked to `carrier_bill` via `carrier_bill_id`. Excludes address fields (not consumed by unified layer).

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

1. **Account Number**: Sourced from `Company Id` field
2. **Invoice Grouping**: One `carrier_bill` row per distinct `(Invoice Number, Invoice Date)`
3. **Charge Filtering**: Only non-zero charges are stored in `shipment_charges`
4. **Duplicate Prevention**: Same file cannot be processed twice (file-based idempotency)
5. **Service Level Discovery**: New `Service Level` values are automatically added to `dbo.shipping_method`
