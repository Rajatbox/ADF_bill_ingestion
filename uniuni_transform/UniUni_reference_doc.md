# UniUni Carrier - Business Reference

## Overview
UniUni provides billing data in a wide-format CSV with one row per shipment. Each invoice can contain multiple shipments, and each shipment has multiple charge types.

---

## Invoice Information

| Field | Description |
|-------|-------------|
| Invoice Number | Unique invoice identifier |
| Invoice Time | Invoice date |
| Merchant ID | Customer account number |
| Total Billed Amount | Total charges per shipment |

---

## Shipment Physical Attributes

### Weight Fields
- **Billable Weight** - Weight used for billing calculation
- **Scaled Weight** - Weight from scale measurement  
- **Dim Weight** - Dimensional weight (length × width × height ÷ divisor)
- **Weight Units**: LBS or OZS

**Business Rule**: Dimensional weight (dim_weight) is used as the primary billing weight.

### Dimension Fields
- **Package Length** - Package length
- **Package Width** - Package width
- **Package Height** - Package height
- **Dimension Units**: CM or IN

### Service Information
- **Service Type** - Shipping method (e.g., Ground, Express)
- **Zone** - Destination zone number
- **Shipped Time** - Date shipment was picked up
- **Induction Time** - Date shipment entered UniUni network
- **Induction Facility ZipCode** - Origin facility location
- **Tracking Number** - Shipment tracking identifier

---

## Charge Types (17 Total)

### Base Charges
| Charge Name | Description |
|-------------|-------------|
| Base Rate | Base transportation rate |
| Discount Fee | Volume or contract discount applied |
| Billed Fee | Net rate after discount (Base + Discount) |

### Service Charges
| Charge Name | Description |
|-------------|-------------|
| Signature Fee | Signature required service |
| Pick Up Fee | Scheduled pickup service |
| Relabel Fee | Package relabeling service |

### Surcharges
| Charge Name | Description |
|-------------|-------------|
| Over Dimension Fee | Oversized package (exceeds standard dimensions) |
| Over Max Size Fee | Maximum size limit exceeded |
| Over Weight Fee | Package exceeds weight threshold |
| Fuel Surcharge | Fuel cost adjustment |
| Peak Season Surcharge | High-volume season surcharge |
| Delivery Area Surcharge | Remote or rural delivery area |
| Delivery Area Surcharge Extend | Extended remote area |
| Truck Fee | Truck delivery required |
| Credit Card Surcharge | Credit card processing fee |

### Fees & Credits
| Charge Name | Description |
|-------------|-------------|
| Miscellaneous Fee | Other charges not categorized above |
| Credit | Credits applied (negative amount) |
| Approved Claim | Approved damage/loss claims (negative amount) |

**Note**: Credits and Approved Claims appear as negative amounts and reduce the total bill.

---

## Unit Standardization

For analytics and cross-carrier comparison, all measurements are converted to standard units:

### Weight Conversion
- **Target Unit**: Ounces (OZ)
- **LBS to OZ**: Multiply by 16
- **OZS to OZ**: No conversion needed

### Dimension Conversion
- **Target Unit**: Inches (IN)
- **CM to IN**: Multiply by 0.393701
- **IN to IN**: No conversion needed

---

## Data Validation

### Total Amount Reconciliation
The sum of all individual shipment charges should equal the invoice total from the billing file.

**Validation Formula**:
```
SUM(All Charges per Shipment) = Invoice Total Amount
```

### Required Fields
- Invoice Number (cannot be blank)
- Invoice Time (cannot be blank)
- Tracking Number (cannot be blank)
- Merchant ID (cannot be blank)

Missing required fields will cause the file to be rejected.

---

## Key Business Rules

1. **Primary Billing Weight**: Dimensional weight (dim_weight) is used for billing calculations
2. **Charge Filtering**: Only non-zero charges are stored in the system
3. **Negative Amounts**: Credits and approved claims appear as negative amounts
4. **Service Type Discovery**: New shipping methods are automatically added to reference data
5. **Duplicate Prevention**: Same invoice cannot be processed twice (based on Invoice Number + Invoice Time)

---

## Sample Data Structure

### Example Shipment
```
Invoice Number: 123456789
Invoice Time: 2026-02-15
Tracking Number: 1Z999AA10123456784
Merchant ID: ACCT001
Service Type: Ground Service

Physical Attributes:
- Dim Weight: 5.5 LBS → Converted to 88 OZ
- Package Length: 30 CM → Converted to 11.81 IN
- Package Width: 20 CM → Converted to 7.87 IN  
- Package Height: 15 CM → Converted to 5.91 IN
- Zone: 5

Charges:
- Base Rate: $8.50
- Discount Fee: -$1.20
- Fuel Surcharge: $1.75
- Delivery Area Surcharge: $3.50
- Total Billed Amount: $12.55
```

---

## Notes for Business Users

- **Invoice Frequency**: UniUni typically issues invoices weekly
- **Charge Transparency**: All charges are itemized in the billing file
- **Credits Processing**: Credits and claims are applied in the same invoice period
- **Service Level Accuracy**: Service Type field matches contract rate sheets
- **Weight Discrepancies**: If billed weight differs from actual weight, dimensional weight rules apply

---

## Questions or Issues?

For data discrepancies or questions about charges:
1. Verify tracking number exists in your shipping system
2. Check if service type matches what was requested
3. Review dimensional weight calculation if weight charges seem incorrect
4. Contact UniUni billing support for charge disputes
