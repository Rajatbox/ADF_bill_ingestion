# FlavorCloud Carrier - Business Reference

## Overview
FlavorCloud provides international shipping and logistics services with detailed billing information. Data is provided as a CSV file with one row per shipment, containing comprehensive international shipping details including duties, taxes, landed costs, and carrier charges.

---

## Invoice Information

| Field | Description |
|-------|-------------|
| Invoice Number | Unique invoice identifier (e.g., "FLCL-1jfpthha9r78") |
| Date | Invoice date in "Mon dd, yyyy" format (e.g., "Jan 25, 2026") |
| Origin Location | Account/fulfillment location (e.g., "Falcon Fulfillment UT") |
| Shipment Total Charges (USD) | Total charges per shipment (all costs combined) |

### Date Formats
FlavorCloud uses multiple date formats:
- **Invoice Date**: "Mon dd, yyyy" format (parsed with SQL CONVERT style 107)
- **Shipment Date**: "mm-dd-yyyy" format (parsed with SQL CONVERT style 110)
- **Due Date**: "mm-dd-yyyy" format (parsed with SQL CONVERT style 110)

### Special CSV Features
- **Total Row**: CSV contains a "Total" row at the end (filtered out during processing)
- **Summary Rows**: CSV may contain summary distribution rows without shipment numbers (filtered out)

---

## Shipment Physical Attributes

### Weight Fields
- **Total Weight** - Package weight
- **Weight Unit**: Typically **LB** (pounds)

**Business Rule**: Weight must be converted from LB to OZ for standardization.

### Dimension Fields
- **Length** - Package length
- **Width** - Package width
- **Height** - Package height
- **Dimension Unit**: Typically **IN** (inches)

**Business Rule**: Dimensions are typically already in inches (standard unit).

### Service Information
- **Service Level** - Shipping method (e.g., "STANDARD", "EXPRESS")
- **Terms of Trade** - Incoterms (e.g., "DDP" - Delivered Duty Paid)
- **Carrier** - Actual carrier used (e.g., "UPS", "APC LAX")
- **Shipment Number** - Tracking number (used as tracking_number in unified model)
- **Order Number** - Customer order reference
- **Destination Country** - 2-letter country code (e.g., "IT", "CH", "NO", "AU")
- **Ship To Address Zip** - Destination postal code

---

## Charge Structure (6 Charge Types)

### International Shipping Charges

FlavorCloud uses a **wide-format charge structure** with 6 distinct charge types per shipment:

| Charge Name | Description | Freight Flag |
|-------------|-------------|--------------|
| **Shipping Charges** | Carrier transportation cost | freight=1 |
| **Commissions** | FlavorCloud commission | freight=0 |
| **Duties** | Import duties | freight=0 |
| **Taxes** | Import taxes (VAT, GST, etc.) | freight=0 |
| **Fees** | Miscellaneous import/export fees | freight=0 |
| **Insurance** | Shipment insurance | freight=0 |

### Derived Fields (Not Stored)
These fields are calculated/summary values that are **NOT** stored in `shipment_charges`:

| Field | Description | Why Not Stored |
|-------|-------------|----------------|
| LandedCost (Duty + Taxes + Fees) | Sum of duties + taxes + fees | Would cause double-counting |
| Order Value | Declared value of goods | Not a charge |
| Shipment Total Charges | Sum of all charges | Calculated from individual charges |

**Important**: Only the 6 individual charge types are stored in `shipment_charges`. This ensures:
```
SUM(shipment_charges.amount) = Shipment Total Charges
```

---

## Unit Standardization

For analytics and cross-carrier comparison:

### Weight Conversion
- **Source Unit**: LB (pounds)
- **Target Unit**: OZ (ounces)
- **Conversion**: LB × 16 = OZ
- **Example**: 1.7 LB × 16 = 27.2 OZ

**Fallback conversions**:
- KG × 35.274 = OZ (if weight is in kilograms)

### Dimension Conversion
- **Source Unit**: IN (inches)
- **Target Unit**: IN (inches)
- **Conversion**: None needed (already in standard unit)

**Fallback conversions**:
- CM ÷ 2.54 = IN (if dimensions are in centimeters)
- MM ÷ 25.4 = IN (if dimensions are in millimeters)

---

## Data Validation

### Total Amount Reconciliation
The sum of all 6 individual charges should equal Shipment Total Charges.

**Validation Formula**:
```
SUM(Shipping Charges + Commissions + Duties + Taxes + Fees + Insurance) = Shipment Total Charges
```

### Required Fields
- Invoice Number (cannot be blank or "Total")
- Shipment Number (cannot be blank)
- Date (must be parseable)
- Origin Location (cannot be blank)

Missing required fields will cause the file to be rejected.

---

## Key Business Rules

1. **International Focus**: FlavorCloud specializes in international shipments with landed cost calculations
2. **DDP Standard**: Most shipments use DDP (Delivered Duty Paid) terms
3. **Charge Breakdown**: All 6 charge types are itemized for transparency
4. **Zero-Amount Filtering**: Only non-zero charges are stored in shipment_charges
5. **LandedCost Exclusion**: LandedCost is NOT stored (it's Duties + Taxes + Fees, would double-count)
6. **Service Type Discovery**: New service levels are automatically added to reference data
7. **Duplicate Prevention**: Same invoice + date cannot be processed twice (file-based idempotency)
8. **Multi-Carrier**: FlavorCloud uses multiple carriers (UPS, APC LAX, etc.)

---

## Sample Data Structure

### Example International Shipment
```
Invoice Number: FLCL-1jfpthha9r78
Date: Jan 25, 2026
Order Number: 2671918
Shipment Number: 16889P0122260001156670 (tracking number)
Service Level: STANDARD
Terms of Trade: DDP
Origin Location: Falcon Fulfillment UT
Destination Country: IT
Carrier: APC LAX

Physical Attributes:
- Total Weight: 1.7 LB → Converted to 27.2 OZ
- Length: 10 IN (no conversion needed)
- Width: 7 IN (no conversion needed)
- Height: 7 IN (no conversion needed)

Charges:
- Shipping Charges: $18.98 (freight charge)
- Commissions: $6.15
- Duties: $0.00
- Taxes: $27.72 (VAT)
- Fees: $8.04
- Insurance: $0.00
- LandedCost: $35.76 (NOT stored - calculated as Duties + Taxes + Fees)
- Shipment Total Charges: $60.89 (validates against sum of 6 stored charges)

Order Information:
- Order Value: $107.00 (declared value)
- Shipment Date: 01-22-2026
- Payment Terms: Upon Receipt
- Due Date: 01-26-2026
```

---

## International Shipping Context

### Terms of Trade (Incoterms)
- **DDP (Delivered Duty Paid)**: Seller (shipper) pays all duties, taxes, and fees
- Buyer receives package with no additional charges

### Landed Cost Components
1. **Duties**: Import duties based on product classification and country rates
2. **Taxes**: VAT, GST, or other import taxes
3. **Fees**: Customs processing, brokerage, or handling fees

### Common Destination Countries
- **EU**: Italy (IT), Germany, France, etc. - typically have VAT charges
- **Switzerland (CH)**: Separate customs procedures, VAT applies
- **Norway (NO)**: Outside EU, has import VAT and duties
- **Australia (AU)**: GST applies on imports

---

## Notes for Business Users

- **Platform**: FlavorCloud is an international shipping platform managing customs, duties, and taxes
- **Carrier Selection**: FlavorCloud selects optimal carriers based on destination and service level
- **DDP Benefit**: Customers receive packages without surprise customs charges
- **VAT/Tax Handling**: FlavorCloud pre-calculates and pays import taxes on behalf of shipper
- **Commission Structure**: FlavorCloud charges commission for international shipping management
- **Invoice Frequency**: Typically weekly or bi-weekly
- **Unit Consistency**: Weight converted to OZ, dimensions typically already in IN

---

## Questions or Issues?

For data discrepancies or questions about charges:
1. Verify shipment number (tracking) exists in your order system
2. Check if duties/taxes match destination country regulations
3. Compare landed cost calculations against quoted rates
4. Verify service level matches what was requested (STANDARD vs EXPRESS)
5. Contact FlavorCloud support for billing disputes or customs-related questions
6. Review Terms of Trade (DDP) to confirm who is responsible for import charges

