# Eliteworks Carrier - Business Reference

## Overview
Eliteworks provides shipment tracking and billing data through their platform. Data is exported as a CSV file with one row per shipment, containing comprehensive shipment details including costs, dimensions, and tracking information.

---

## Invoice Information

Eliteworks uses a **date-based invoice grouping** strategy:
- **Invoice Number**: Generated as `Eliteworks_YYYY-MM-DD` based on the latest shipment timestamp in the file
- **Invoice Date**: CAST(MAX(time_utc) AS DATE)
- **Account Number**: Taken from the `USER` column (e.g., "Falcon IT")
- **Invoice Grouping**: All shipments in a file are grouped under a single invoice using the latest timestamp's date portion

| Field | Description |
|-------|-------------|
| Invoice Number | Auto-generated: `Eliteworks_YYYY-MM-DD` format |
| Invoice Date | Date of the latest shipment in the file |
| USER | Customer account name (e.g., "Falcon IT") |
| Platform Charged (With Corrections) | Total billed amount per shipment |

---

## Shipment Physical Attributes

### Weight Field
- **Shipment Weight (Oz)** - Weight in ounces
- **Weight Unit**: Already in **OZ** (no conversion needed)

**Business Rule**: Weight is already provided in ounces, which is the standard unit for the unified data model.

### Dimension Fields
- **Package Length (In)** - Length in inches
- **Package Width (In)** - Width in inches  
- **Package Height (In)** - Height in inches
- **Dimension Unit**: Already in **IN** (no conversion needed)

**Business Rule**: Dimensions are already provided in inches, which is the standard unit for the unified data model.

### Service Information
- **Service** - Shipping method (e.g., "Ground Advantage")
- **Carrier** - Carrier name (typically USPS for Eliteworks platform shipments)
- **Zone** - Destination zone number
- **Tracking** - Carrier tracking number
- **Status** - Shipment status (e.g., DELIVERED, IN TRANSIT)
- **Time (UTC)** - Timestamp when shipment was created in the system
- **First Scan** - Date/time of first carrier scan
- **Delivered** - Date/time shipment was delivered

### Address Information
- **From Fields**: Name, Company, Street, City, State, Postal, Country
- **To Fields**: Name, Company, Street, City, State, Postal, Country

---

## Charge Structure

### Single Charge Per Shipment

Eliteworks uses a **simplified charge structure** with one authoritative charge per shipment:

| Charge Name | Description |
|-------------|-------------|
| **Platform Charged (With Corrections)** | The authoritative billed amount (includes Base Rate + Store Markup with corrections applied) |

### Supporting Fields (Audit Only)
These fields are preserved in the `eliteworks_bill` table for audit/reporting but are **NOT** stored in `shipment_charges`:

| Field | Description | Purpose |
|-------|-------------|---------|
| Charged | Base carrier charge | Audit only |
| Store Markup | Platform markup applied | Audit only |

**Important**: Only **Platform Charged** is stored in `shipment_charges` to avoid double-counting. This ensures:
```
SUM(shipment_charges.amount) = carrier_bill.total_amount
```

---

## Unit Standardization

For analytics and cross-carrier comparison:

### Weight Conversion
- **Source Unit**: Ounces (OZ)
- **Target Unit**: Ounces (OZ)
- **Conversion**: None needed (already in standard unit)

### Dimension Conversion
- **Source Unit**: Inches (IN)
- **Target Unit**: Inches (IN)
- **Conversion**: None needed (already in standard unit)

---

## Data Validation

### Total Amount Reconciliation
The sum of all Platform Charged values should equal the invoice total.

**Validation Formula**:
```
SUM(Platform Charged per Shipment) = Invoice Total Amount
```

### Required Fields
- Time (UTC) (cannot be blank)
- Tracking Number (cannot be blank)
- USER account (cannot be blank)

Missing required fields will cause the file to be rejected.

---

## Key Business Rules

1. **Invoice Grouping**: All shipments in a file are grouped under one invoice based on the latest shipment date
2. **Single Charge**: Only Platform Charged (with corrections) is stored as the authoritative charge
3. **No Double-Counting**: Base Rate and Store Markup are audit fields only, not stored in shipment_charges
4. **No Unit Conversion**: Weight and dimensions are already in standard units (OZ, IN)
5. **Service Type Discovery**: New shipping methods are automatically added to reference data
6. **Duplicate Prevention**: Same file cannot be processed twice (file-based idempotency)
7. **Zero/Negative Charges**: Only non-zero charges are stored in shipment_charges

---

## Sample Data Structure

### Example Shipment
```
Time (UTC): 2026-02-08 18:47:25
USER: Falcon IT
Tracking: 9234690389686403017675
Status: DELIVERED
Carrier: USPS
Service: Ground Advantage

Physical Attributes:
- Shipment Weight: 28.00 OZ (no conversion needed)
- Package Length: 7.00 IN (no conversion needed)
- Package Width: 5.00 IN (no conversion needed)
- Package Height: 5.00 IN (no conversion needed)
- Zone: 5

Charges:
- Charged (Base Rate): $7.20 (audit only)
- Store Markup: $0.00 (audit only)
- Platform Charged (With Corrections): $7.20 (authoritative charge)
```

**Invoice Grouping**:
- All shipments with Time (UTC) on 2026-02-08 would be grouped as:
  - Invoice Number: `Eliteworks_2026-02-08`
  - Invoice Date: `2026-02-08`

---

## Notes for Business Users

- **Platform**: Eliteworks is a shipping management platform, not a direct carrier
- **Actual Carriers**: Shipments typically use USPS as the carrier
- **Charge Transparency**: Platform Charged includes all costs (base rate + markup + corrections)
- **Invoice Frequency**: Typically one invoice per file/export date
- **Unit Consistency**: All measurements already in standard units (OZ for weight, IN for dimensions)
- **Corrections Applied**: Platform Charged already includes any necessary corrections or adjustments

---

## Questions or Issues?

For data discrepancies or questions about charges:
1. Verify tracking number exists in the Eliteworks platform
2. Check if service type matches what was requested
3. Compare Platform Charged against contract rates
4. Contact Eliteworks support for billing disputes or corrections

